use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::{broadcast, mpsc};

use crate::api_client::ApiClient;
use crate::compaction::CompactionManager;
use crate::config::Config;
use crate::core_loop::{write_turn_dump, HarnessEvent};
use crate::db::Database;
use crate::http_server::BroadcastMsg;
use crate::process::{ProcessEvent, ProcessSupervisor};
use crate::python;
use crate::renderer;
use crate::types::*;

/// Dim-gray log line, visually distinct from chat output.
macro_rules! dimlog {
    ($($arg:tt)*) => {
        ::std::println!("\x1b[2m{}\x1b[0m", format_args!($($arg)*))
    };
}

fn truncate_for_log(s: &str, max: usize) -> String {
    let first = s.lines().next().unwrap_or("");
    let truncated: String = first.chars().take(max).collect();
    if truncated.len() < first.len() || s.contains('\n') {
        format!("{}…", truncated)
    } else {
        truncated
    }
}

/// Why an agent's run loop ended.
pub enum FinishReason {
    /// Agent called done() explicitly.
    Done,
    /// Max turns exceeded.
    MaxTurns(u32),
    /// Shutdown signal received.
    Shutdown,
    /// Parent sent kill_child().
    Killed,
    /// Event channel closed (parent dropped).
    ChannelClosed,
}

/// Format a lineage vector into a human-readable string.
/// e.g. vec!["root", "planner", "worker"] → "worker, child of planner, child of root"
pub fn format_lineage(lineage: &[String]) -> String {
    if lineage.len() <= 1 {
        return lineage.first().cloned().unwrap_or_default();
    }
    let name = &lineage[lineage.len() - 1];
    let ancestors: Vec<String> = lineage[..lineage.len() - 1]
        .iter()
        .rev()
        .map(|a| format!("child of {}", a))
        .collect();
    format!("{}, {}", name, ancestors.join(", "))
}

pub struct AgentLoop {
    pub name: String,
    pub permissions: AgentPermissions,
    pub state: HarnessState,
    config: Arc<Config>,
    db: Arc<Database>,
    api_client: ApiClient,
    process_supervisor: ProcessSupervisor,
    compaction: CompactionManager,
    event_rx: mpsc::UnboundedReceiver<HarnessEvent>,
    event_tx: mpsc::UnboundedSender<HarnessEvent>,
    deployment_context: String,
    /// Per-child stable prefix (role instructions + reference images) that
    /// renders before event_history and sits in the cached region. None for
    /// the root agent.
    role_prefix: Option<renderer::RolePrefix>,
    broadcast_tx: Option<broadcast::Sender<BroadcastMsg>>,
    dump_dir: Option<PathBuf>,
    dump_to_stdout: bool,
    turn_counter: u32,
    active_children: u32,
    max_children: u32,
    /// Shared accumulator for the parent agent (None for children).
    token_accumulator: Option<Arc<Mutex<TokenAccumulator>>>,
    /// Local token accumulators for child agents (not shared).
    local_input_tokens: u64,
    local_output_tokens: u64,
    local_cache_creation_tokens: u64,
    local_cache_read_tokens: u64,
    /// Shared agent registry for naming and inter-agent messaging.
    registry: Arc<AgentRegistry>,
    /// Set when a KillSignal is received; checked each turn boundary.
    killed: bool,
    /// Shutdown signal. When true, run() exits at the next cancellation point
    /// — including mid-API-retry, since run_turn() is wrapped in select!.
    shutdown: tokio::sync::watch::Receiver<bool>,
    /// Explicit return values from done(**kwargs). Populated on the turn that
    /// calls done(); read by run_child_agent_loop after run() returns.
    done_result: HashMap<String, serde_json::Value>,
    /// Python executor. Owns the RustPython interpreter; one per agent loop.
    executor: python::Executor,
}

impl AgentLoop {
    pub fn new(
        name: String,
        permissions: AgentPermissions,
        state: HarnessState,
        config: Arc<Config>,
        db: Arc<Database>,
        api_client: ApiClient,
        process_supervisor: ProcessSupervisor,
        event_rx: mpsc::UnboundedReceiver<HarnessEvent>,
        event_tx: mpsc::UnboundedSender<HarnessEvent>,
        deployment_context: String,
        role_prefix: Option<renderer::RolePrefix>,
        broadcast_tx: Option<broadcast::Sender<BroadcastMsg>>,
        dump_dir: Option<PathBuf>,
        dump_to_stdout: bool,
        token_accumulator: Option<Arc<Mutex<TokenAccumulator>>>,
        registry: Arc<AgentRegistry>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        let max_children: u32 = std::env::var("CLAUDE_SERVER_MAX_CHILDREN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        Self {
            name,
            permissions,
            state,
            config,
            db,
            api_client,
            process_supervisor,
            compaction: CompactionManager::new(),
            event_rx,
            event_tx,
            deployment_context,
            role_prefix,
            broadcast_tx,
            dump_dir,
            dump_to_stdout,
            turn_counter: 0,
            active_children: 0,
            max_children,
            token_accumulator,
            local_input_tokens: 0,
            local_output_tokens: 0,
            local_cache_creation_tokens: 0,
            local_cache_read_tokens: 0,
            registry,
            killed: false,
            shutdown,
            done_result: HashMap::new(),
            executor: python::Executor::new(),
        }
    }

    fn broadcast(&self, msg: BroadcastMsg) {
        if let Some(ref tx) = self.broadcast_tx {
            let _ = tx.send(msg);
        }
    }

    pub async fn run(&mut self) -> FinishReason {
        dimlog!("[{}] Agent loop started", self.name);
        let mut idle = false;

        loop {
            // Drain any pending events
            self.drain_events();

            // Check timers
            self.check_timers();

            // Check if compaction needed (only if permitted)
            if self.permissions.can_compact
                && !self.compaction.active
                && CompactionManager::should_trigger(&self.state, self.config.compact_at)
            {
                dimlog!(
                   "[{}] Triggering compaction (input_tokens {} > threshold {})",
                    self.name, self.state.last_input_tokens, self.config.compact_at
                );
                self.compaction
                    .trigger(&mut self.state, self.config.compact_target);
            }

            if *self.shutdown.borrow() {
                dimlog!("[{}] Shutdown requested", self.name);
                return FinishReason::Shutdown;
            }

            // If work queue is empty, wait for events (messages, process
            // completions, timer fires). Agents exit explicitly via done().
            if self.state.work_queue.is_empty() {
                if !idle {
                    let timers = self.state.timer_manager.list();
                    if timers.is_empty() {
                        dimlog!("[{}] Idle, waiting for events...", self.name);
                    } else {
                        let next_in = self.state.timer_manager.next_deadline()
                            .map(|d| (d - Utc::now()).to_std().unwrap_or(Duration::ZERO))
                            .unwrap_or(Duration::ZERO);
                        dimlog!(
                            "[{}] Idle, waiting for events... [{} timer{} active, next in {}]",
                            self.name,
                            timers.len(),
                            if timers.len() == 1 { "" } else { "s" },
                            format!("{:?}", next_in)
                        );
                    }
                    idle = true;
                    self.broadcast(BroadcastMsg::Status {
                        status: "idle".to_string(),
                    });
                }

                let timer_sleep = match self.state.timer_manager.next_deadline() {
                    Some(deadline) => {
                        let duration = (deadline - Utc::now())
                            .to_std()
                            .unwrap_or(Duration::ZERO);
                        tokio::time::sleep(duration)
                    }
                    None => tokio::time::sleep(Duration::from_secs(86400)),
                };

                tokio::select! {
                    event = self.event_rx.recv() => {
                        match event {
                            Some(event) => self.apply_event(event),
                            None => {
                                dimlog!("[{}] Event channel closed, shutting down", self.name);
                                return FinishReason::ChannelClosed;
                            }
                        }
                    }
                    _ = timer_sleep => {
                        self.check_timers();
                    }
                    _ = self.shutdown.changed() => {}
                }
                continue;
            }

            idle = false;

            if self.killed {
                dimlog!("[{}] Kill signal received, exiting", self.name);
                return FinishReason::Killed;
            }

            // Check max_turns limit
            if let Some(max) = self.permissions.max_turns {
                if self.turn_counter >= max {
                    dimlog!(
                       "[{}] Max turns ({}) reached, exiting",
                        self.name, max
                    );
                    return FinishReason::MaxTurns(max);
                }
            }

            // Run a turn. Wrapping in select! makes the entire turn future
            // (API call, retry sleeps, Python exec) cancellable on shutdown —
            // dropping the future cancels in-flight HTTP requests and sleeps.
            let mut shutdown = self.shutdown.clone();
            let turn_result = tokio::select! {
                r = self.run_turn() => r,
                _ = shutdown.changed() => {
                    dimlog!("[{}] Shutdown requested (mid-turn)", self.name);
                    return FinishReason::Shutdown;
                }
            };
            match turn_result {
                Ok(true) => {
                    dimlog!("[{}] done() called, exiting", self.name);
                    return FinishReason::Done;
                }
                Ok(false) => {}
                Err(e) => {
                    eprintln!("[{}] Turn error: {}", self.name, e);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                        _ = self.shutdown.changed() => {}
                    }
                }
            }

            // Persist state (root only — children, even persistent ones, don't)
            if self.permissions.agent_name.is_root() {
                if let Err(e) = self.db.save_state(&self.state) {
                    eprintln!("[{}] Failed to persist state: {}", self.name, e);
                }
            }
        }
    }

    /// Run a single turn. Returns Ok(true) if the agent called done().
    async fn run_turn(&mut self) -> Result<bool> {
        // Build compaction state if active
        let compaction_state = if self.compaction.active {
            let mut cs = self
                .compaction
                .compaction_state(self.state.last_input_tokens);
            cs.estimated_post_compaction = self.compaction.estimate_post_compaction(
                &self.executor,
                &self.state,
                &self.deployment_context,
                &self.config.render_config,
                self.config.compact_at,
            );
            Some(cs)
        } else {
            None
        };

        // Render context
        let lineage_str = format_lineage(&self.permissions.lineage);
        // Load pinned memory from DB (shared across agents, injected into system prompt)
        let pinned = self.db.load_pinned().unwrap_or_default();
        let pinned_snapshot: HashMap<String, String> = pinned.iter().cloned().collect();
        let pinned_summary = if pinned.is_empty() {
            None
        } else {
            let total_chars: usize = pinned.iter().map(|(s, c)| s.len() + c.len()).sum();
            Some((pinned.len(), total_chars))
        };

        let agent_identity = renderer::AgentIdentity {
            name: self.permissions.agent_name.as_str(),
            lineage: &lineage_str,
            turn_counter: self.turn_counter,
            max_turns: self.permissions.max_turns,
        };
        let rendered = renderer::render_context(
            &self.state,
            &self.deployment_context,
            self.role_prefix.as_ref(),
            compaction_state.as_ref(),
            &self.config.render_config,
            self.config.compact_at,
            Some(&agent_identity),
            pinned_summary,
        );

        let seg_sizes: Vec<usize> = rendered.cached_segments.iter().map(String::len).collect();
        dimlog!(
           "[{}] Rendered context: {} chars (cached segs: {:?}), queue: {} items, attachments: {}",
            self.name,
            rendered.text.len(),
            seg_sizes,
            self.state.work_queue.len(),
            rendered.attachments.len()
        );

        // Broadcast thinking status
        self.broadcast(BroadcastMsg::Status {
            status: "thinking".to_string(),
        });

        // Collect sensitive values for trace scrubbing. Check both local
        // and pinned memory; extract string repr.
        let sensitive_values: Vec<String> = self.state.sensitive_keys.iter()
            .filter_map(|k| {
                self.state.memory.get(k).map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .or_else(|| pinned.iter().find(|(pk, _)| pk == k).map(|(_, v)| v.clone()))
            })
            .filter(|v| v.len() >= 8)  // skip trivially short values (false-positive risk)
            .collect();

        // Call Claude API
        let api_result = self.api_client.call(&rendered, &pinned, &self.name, self.turn_counter, &sensitive_values).await?;

        let turn_cost = (api_result.input_tokens as f64 * self.config.cost_per_m_input
            + api_result.output_tokens as f64 * self.config.cost_per_m_output
            + api_result.cache_creation_tokens as f64 * self.config.cost_per_m_cache_write
            + api_result.cache_read_tokens as f64 * self.config.cost_per_m_cache_read)
            / 1_000_000.0;
        let total_in = api_result.input_tokens
            + api_result.cache_creation_tokens
            + api_result.cache_read_tokens;
        let cache_hit_pct = if total_in > 0 {
            100.0 * api_result.cache_read_tokens as f64 / total_in as f64
        } else {
            0.0
        };
        dimlog!(
            "[{}] API response: in={} out={} cache_write={} cache_read={} | ${:.4}/turn, {:.0}% cache hit",
            self.name,
            api_result.input_tokens,
            api_result.output_tokens,
            api_result.cache_creation_tokens,
            api_result.cache_read_tokens,
            turn_cost,
            cache_hit_pct
        );

        // Update token tracking
        self.state.last_input_tokens = api_result.input_tokens;
        self.turn_counter += 1;

        // Accumulate tokens
        if let Some(ref acc) = self.token_accumulator {
            let mut acc = acc.lock().unwrap();
            acc.input_tokens += api_result.input_tokens;
            acc.output_tokens += api_result.output_tokens;
            acc.cache_creation_tokens += api_result.cache_creation_tokens;
            acc.cache_read_tokens += api_result.cache_read_tokens;
            acc.turns += 1;
        } else {
            // Child agent: track locally
            self.local_input_tokens += api_result.input_tokens;
            self.local_output_tokens += api_result.output_tokens;
            self.local_cache_creation_tokens += api_result.cache_creation_tokens;
            self.local_cache_read_tokens += api_result.cache_read_tokens;
        }

        // Load process outputs for shell_output() calls
        let process_outputs = self.db.load_all_process_outputs().unwrap_or_default();

        // Broadcast executing status
        self.broadcast(BroadcastMsg::Status {
            status: "executing".to_string(),
        });

        // Execute Python
        let lineage_str = format_lineage(&self.permissions.lineage);
        let exec_result = self.executor.execute_with_timeout(
            &self.state,
            &api_result.code,
            self.compaction.active,
            &process_outputs,
            self.config.python_timeout_secs,
            self.permissions.child_depth_remaining,
            self.permissions.agent_name.as_str(),
            &lineage_str,
            &pinned_snapshot,
        );

        let mut is_error = exec_result.is_error;
        let stdout = exec_result.stdout.clone();
        let mut error_text = exec_result.error_text.clone();

        dimlog!(
           "[{}] Executed (error={}): {}",
            self.name,
            is_error,
            &api_result.code.lines().next().unwrap_or("(empty)")
        );

        if !stdout.is_empty() {
            let preview = truncate_for_log(&stdout, 200);
            let extra = stdout.lines().count().saturating_sub(preview.lines().count());
            print!("\x1b[2m[stdout] {}", preview);
            if extra > 0 {
                print!(" [+{} more lines]", extra);
            }
            println!("\x1b[0m");
        }

        // Apply side effects (only if no error)
        let mut done_called = false;
        if !is_error {
            done_called = exec_result.side_effects.done_called;
            if done_called {
                self.done_result = exec_result.side_effects.done_result.clone();
            }
            match self.apply_side_effects(exec_result.side_effects) {
                Ok(deferred) => {
                    self.perform_deferred_ops(deferred).await;
                }
                Err(msg) => {
                    is_error = true;
                    error_text = msg;
                    done_called = false; // rollback — don't honor done() on error
                }
            }
        }

        // Record in history (after side effects so entry_id is fresh)
        let entry_id = self.state.id_generator.next();
        let output = if is_error {
            format!("[ERROR]\n{}", error_text)
        } else {
            stdout
        };

        self.state.event_history.push(HistoryEntry::Execution {
            id: entry_id,
            time: Utc::now(),
            code: api_result.code.clone(),
            output: output.clone(),
            is_error,
        });

        // Dump turn
        if self.dump_to_stdout || self.dump_dir.is_some() {
            write_turn_dump(
                &self.name,
                self.turn_counter,
                &rendered,
                (
                    api_result.input_tokens,
                    api_result.output_tokens,
                    api_result.cache_creation_tokens,
                    api_result.cache_read_tokens,
                ),
                api_result.thinking.as_deref(),
                &api_result.code,
                &output,
                is_error,
                self.dump_to_stdout,
                self.dump_dir.as_deref(),
            );
        }

        Ok(done_called)
    }

    fn drain_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            self.apply_event(event);
        }
    }

    fn check_timers(&mut self) {
        let now = Utc::now();
        let fired = self
            .state
            .timer_manager
            .check_and_fire(now, &mut self.state.id_generator);
        for item in fired {
            dimlog!("[{}] Timer fired: {}", self.name, item.id);
            self.state.work_queue.push(item);
        }
    }

    fn apply_event(&mut self, event: HarnessEvent) {
        match event {
            HarnessEvent::UserMessage {
                chat_id,
                user,
                content,
                attachments,
                message_ref,
            } => {
                let id = self.state.id_generator.next();
                dimlog!(
                   "[{}] User message from {}: {} (id={}){}",
                    self.name,
                    user,
                    crate::renderer::trunc(&content, 50),
                    id,
                    if attachments.is_empty() { String::new() } else { format!(" [{} attachments]", attachments.len()) }
                );
                self.state.work_queue.push(WorkItem {
                    id,
                    priority: 9,
                    time: Utc::now(),
                    item_type: WorkItemType::UserMessage {
                        chat_id,
                        user,
                        content,
                        message_ref,
                    },
                    attachments,
                });
            }
            HarnessEvent::Process(pe) => match pe {
                ProcessEvent::Completed { pid, exit_code } => {
                    let prio = self
                        .state
                        .process_manager
                        .get(&pid)
                        .map(|p| p.success_prio)
                        .unwrap_or(5);
                    if let Some(p) = self.state.process_manager.get_mut(&pid) {
                        p.status = ProcessStatus::Completed { exit_code };
                    }
                    let output_preview = self.load_output_preview(&pid.0);
                    let id = self.state.id_generator.next();
                    self.state.work_queue.push(WorkItem {
                        id,
                        priority: prio,
                        time: Utc::now(),
                        item_type: WorkItemType::ProcessCompleted {
                            pid,
                            exit_code,
                            output_preview,
                        },
                        attachments: Vec::new(),
                    });
                }
                ProcessEvent::Failed { pid, error } => {
                    let prio = self
                        .state
                        .process_manager
                        .get(&pid)
                        .map(|p| p.fail_prio)
                        .unwrap_or(7);
                    if let Some(p) = self.state.process_manager.get_mut(&pid) {
                        p.status = ProcessStatus::Failed {
                            error: error.clone(),
                        };
                    }
                    let output_preview = self.load_output_preview(&pid.0);
                    let id = self.state.id_generator.next();
                    self.state.work_queue.push(WorkItem {
                        id,
                        priority: prio,
                        time: Utc::now(),
                        item_type: WorkItemType::ProcessFailed {
                            pid,
                            error,
                            output_preview,
                        },
                        attachments: Vec::new(),
                    });
                }
                ProcessEvent::Timeout { pid } => {
                    let prio = self
                        .state
                        .process_manager
                        .get(&pid)
                        .map(|p| p.fail_prio)
                        .unwrap_or(7);
                    let id = self.state.id_generator.next();
                    self.state.work_queue.push(WorkItem {
                        id,
                        priority: prio,
                        time: Utc::now(),
                        item_type: WorkItemType::ProcessTimeout { pid },
                        attachments: Vec::new(),
                    });
                }
            },
            HarnessEvent::ChildCompleted {
                child_name,
                result,
                turns_used,
                success,
                summary,
                priority,
                child_input_tokens,
                child_output_tokens,
                child_cache_creation_tokens,
                child_cache_read_tokens,
            } => {
                self.active_children = self.active_children.saturating_sub(1);
                let id = self.state.id_generator.next();
                dimlog!(
                   "[{}] Child agent {} completed (success={}, turns={})",
                    self.name, child_name, success, turns_used
                );

                // Add child's accumulated tokens to the shared accumulator
                if let Some(ref acc) = self.token_accumulator {
                    let mut acc = acc.lock().unwrap();
                    acc.input_tokens += child_input_tokens;
                    acc.output_tokens += child_output_tokens;
                    acc.cache_creation_tokens += child_cache_creation_tokens;
                    acc.cache_read_tokens += child_cache_read_tokens;
                }

                let cost_usd = (child_input_tokens as f64 * self.config.cost_per_m_input
                    + child_output_tokens as f64 * self.config.cost_per_m_output
                    + child_cache_creation_tokens as f64 * self.config.cost_per_m_cache_write
                    + child_cache_read_tokens as f64 * self.config.cost_per_m_cache_read)
                    / 1_000_000.0;
                let total_in = child_input_tokens + child_cache_read_tokens;
                let cache_hit_pct = if total_in > 0 {
                    (child_cache_read_tokens * 100 / total_in) as u8
                } else {
                    0
                };

                self.state.work_queue.push(WorkItem {
                    id,
                    priority,
                    time: Utc::now(),
                    item_type: WorkItemType::ChildAgentCompleted {
                        child_name,
                        result,
                        turns_used,
                        success,
                        summary,
                        cost_usd,
                        cache_hit_pct,
                    },
                    attachments: Vec::new(),
                });
            }
            HarnessEvent::AgentMessage {
                from,
                content,
                priority,
            } => {
                let id = self.state.id_generator.next();
                dimlog!(
                   "[{}] Agent message from {}: {}",
                    self.name,
                    from,
                    crate::renderer::trunc(&content, 50)
                );
                self.state.work_queue.push(WorkItem {
                    id,
                    priority,
                    time: Utc::now(),
                    item_type: WorkItemType::AgentMessage { from, content },
                    attachments: Vec::new(),
                });
            }
            HarnessEvent::ExternalEvent {
                source,
                event_type,
                data,
                priority,
            } => {
                let id = self.state.id_generator.next();
                dimlog!(
                   "[{}] External event from {}: {} (id={})",
                    self.name, source, event_type, id
                );
                self.state.work_queue.push(WorkItem {
                    id,
                    priority,
                    time: Utc::now(),
                    item_type: WorkItemType::ExternalEvent {
                        source,
                        event_type,
                        data,
                    },
                    attachments: Vec::new(),
                });
            }
            HarnessEvent::KillSignal => {
                self.killed = true;
            }
        }
    }

    /// Load the last ~500 chars of process output as a preview.
    fn load_output_preview(&self, pid: &str) -> Option<String> {
        self.db
            .load_process_output(pid)
            .ok()
            .map(|o| {
                let trimmed = o.trim_end();
                if trimmed.len() > 500 {
                    format!("...{}", &trimmed[trimmed.len() - 500..])
                } else {
                    trimmed.to_string()
                }
            })
            .filter(|s| !s.is_empty())
    }

    /// Apply side effects synchronously, returning any deferred async operations
    /// (block_for waiters and process kills) to be awaited by the caller.
    /// Returns Err if message validation fails (recipient not found), in which
    /// case NO side effects have been applied (atomic rollback).
    fn apply_side_effects(
        &mut self,
        effects: python::SideEffectCollector,
    ) -> Result<DeferredOps, String> {
        // Validate all agent message recipients BEFORE applying anything
        for msg in &effects.agent_messages {
            if !self.registry.exists(&msg.recipient) {
                return Err(format!(
                    "message_agent failed: agent '{}' not found",
                    msg.recipient
                ));
            }
        }

        let mut deferred = DeferredOps::default();

        // Update ID generator
        self.state.id_generator = effects.id_gen;

        // Memory operations
        for (key, value) in effects.memory_sets {
            self.state.memory.insert(key, value);
        }
        for key in effects.memory_deletes {
            self.state.memory.remove(&key);
            self.state.memory_priorities.remove(&key);
        }
        for (key, priority) in effects.memory_priority_sets {
            self.state.memory_priorities.insert(key, priority);
        }

        // Queue removes
        for id in effects.queue_removes {
            self.state.work_queue.remove(&AgentId(id));
        }

        // Timer operations
        for req in effects.timer_adds {
            let schedule = if let Some(secs) = req.every_secs {
                TimerSchedule::Recurring {
                    every: Duration::from_secs(secs),
                    next_fire: Utc::now()
                        + chrono::Duration::from_std(Duration::from_secs(secs))
                            .unwrap_or(chrono::Duration::seconds(1)),
                }
            } else if let Some(epoch) = req.at_epoch {
                let at = chrono::DateTime::from_timestamp(epoch as i64, 0)
                    .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(1));
                TimerSchedule::OneShot { at }
            } else {
                let at = Utc::now() + chrono::Duration::minutes(1);
                TimerSchedule::OneShot { at }
            };

            let timer = Timer {
                id: req.id,
                description: req.description,
                priority: req.priority,
                schedule,
                created_at: Utc::now(),
                pending_ack: false,
            };
            self.state.timer_manager.add(timer);
        }
        for id in effects.timer_cancels {
            self.state.timer_manager.cancel(&AgentId(id));
        }

        // Timer acknowledgments
        for id in effects.timer_acks {
            self.state.timer_manager.acknowledge(&AgentId(id));
        }

        // Filter operations
        for filter in effects.filter_adds {
            self.state.work_queue.add_filter(filter);
        }
        for name in effects.filter_removes {
            self.state.work_queue.remove_filter(&name);
        }

        // History operations
        for id in effects.history_removes {
            let aid = AgentId(id);
            if self.state.event_history.is_modifiable(&aid) || self.compaction.active {
                self.state.event_history.remove(&aid);
            }
        }
        for (id, desc) in effects.history_replaces {
            let aid = AgentId(id);
            if self.state.event_history.is_modifiable(&aid) || self.compaction.active {
                self.state.event_history.replace_with_summary(&aid, desc);
            }
        }
        for text in effects.history_adds {
            if self.compaction.active {
                let id = self.state.id_generator.next();
                self.state.event_history.push(HistoryEntry::Summary {
                    id,
                    time: Utc::now(),
                    description: text,
                });
            }
        }

        // Process starts
        for req in effects.process_starts {
            let managed = ManagedProcess {
                id: req.id.clone(),
                cmd: req.cmd.clone(),
                args: req.args.clone(),
                env: req.env.clone(),
                description: req.description.clone(),
                status: ProcessStatus::Running,
                success_prio: req.success_prio,
                fail_prio: req.fail_prio,
                started_at: Utc::now(),
                os_pid: None,
            };
            self.state.process_manager.add(managed);

            let block_for_ms = req.block_for_ms;
            match self.process_supervisor.spawn(req.clone()) {
                Ok(completion_rx) => {
                    if let (Some(ms), Some(rx)) = (block_for_ms, completion_rx) {
                        deferred.block_for_waiters.push((ms, rx));
                    }
                }
                Err(e) => {
                    eprintln!("[{}] Failed to spawn process: {}", self.name, e);
                    if let Some(p) = self.state.process_manager.get_mut(&req.id) {
                        p.status = ProcessStatus::Failed {
                            error: format!("spawn failed: {}", e),
                        };
                    }
                    let wid = self.state.id_generator.next();
                    self.state.work_queue.push(WorkItem {
                        id: wid,
                        priority: req.fail_prio,
                        time: Utc::now(),
                        item_type: WorkItemType::ProcessFailed {
                            pid: req.id,
                            error: format!("spawn failed: {}", e),
                            output_preview: None,
                        },
                        attachments: Vec::new(),
                    });
                }
            }
        }

        // Process kills
        deferred.process_kills = effects.process_kills;
        deferred.stdin_writes = effects.stdin_writes;
        deferred.stdin_closes = effects.stdin_closes;

        // Child agent kills: send KillSignal via registry. Unknown names are
        // logged but don't fail the turn — the child may have already exited.
        for name in effects.child_kills {
            match self.registry.send_to(&name, HarnessEvent::KillSignal) {
                Ok(true) => dimlog!("[{}] Sent kill signal to child '{}'", self.name, name),
                Ok(false) => dimlog!("[{}] kill_child('{}'): already completed", self.name, name),
                Err(e) => eprintln!("[{}] kill_child('{}') failed: {}", self.name, name, e),
            }
        }

        // view() calls → one View work item (priority 10, lands at head)
        if !effects.view_paths.is_empty() {
            let id = self.state.id_generator.next();
            self.state.work_queue.push(WorkItem {
                id,
                priority: 10,
                time: Utc::now(),
                item_type: WorkItemType::View { paths: effects.view_paths },
                attachments: Vec::new(),
            });
        }

        // Fork requests (child agent spawns)
        for req in effects.fork_requests {
            // Build channels and registration entries for atomic registration
            let mut child_channels = Vec::new();
            let mut registration_entries = Vec::new();
            for child in &req.children {
                let child_lineage = {
                    let mut l = self.permissions.lineage.clone();
                    l.push(child.name.clone());
                    l
                };
                let (child_event_tx, child_event_rx) = mpsc::unbounded_channel::<HarnessEvent>();
                registration_entries.push((child.name.clone(), child_event_tx.clone()));
                child_channels.push((child_event_tx, child_event_rx, child_lineage));
            }

            if let Err(e) = self.registry.register_batch(registration_entries) {
                eprintln!("[{}] Fork failed: {}", self.name, e);
                // Push a single error work item
                let wid = self.state.id_generator.next();
                self.state.work_queue.push(WorkItem {
                    id: wid,
                    priority: 7,
                    time: Utc::now(),
                    item_type: WorkItemType::ChildAgentCompleted {
                        child_name: format!("fork-failed"),
                        result: HashMap::new(),
                        turns_used: 0,
                        success: false,
                        summary: format!("Fork failed: {}", e),
                        cost_usd: 0.0,
                        cache_hit_pct: 0,
                    },
                    attachments: Vec::new(),
                });
                continue;
            }

            // Insert SystemAlert describing the fork
            let names: Vec<&str> = req.children.iter().map(|c| c.name.as_str()).collect();
            let alert_id = self.state.id_generator.next();
            self.state.event_history.push(HistoryEntry::SystemAlert {
                id: alert_id,
                time: Utc::now(),
                message: format!("Forked {} children: {}", req.children.len(), names.join(", ")),
            });

            // Spawn each child
            for child_settings in req.children.into_iter() {
                if self.active_children >= self.max_children {
                    eprintln!(
                        "[{}] Cannot spawn child '{}': max {} concurrent children reached",
                        self.name, child_settings.name, self.max_children
                    );
                    self.registry.deregister(&child_settings.name);
                    let wid = self.state.id_generator.next();
                    self.state.work_queue.push(WorkItem {
                        id: wid,
                        priority: 7,
                        time: Utc::now(),
                        item_type: WorkItemType::ChildAgentCompleted {
                            child_name: child_settings.name,
                            result: HashMap::new(),
                            turns_used: 0,
                            success: false,
                            summary: format!(
                                "Could not spawn: max {} concurrent children reached",
                                self.max_children
                            ),
                            cost_usd: 0.0,
                            cache_hit_pct: 0,
                        },
                        attachments: Vec::new(),
                    });
                    continue;
                }

                self.active_children += 1;
                let (child_event_tx, child_event_rx, child_lineage) = child_channels.remove(0);

                let child_name_str = child_settings.name.clone();
                let max_turns = child_settings.max_turns;
                let child_depth = self.permissions.child_depth_remaining.saturating_sub(1);
                let model = child_settings.model.unwrap_or_else(|| self.config.model.clone());

                dimlog!(
                   "[{}] Forking child '{}' (model={}, max_turns={:?}, depth_remaining={})",
                    self.name, child_name_str, model, max_turns, child_depth
                );

                // Create child's own process event channel and supervisor
                let (child_process_event_tx, mut child_process_event_rx) =
                    mpsc::unbounded_channel::<ProcessEvent>();
                let child_process_supervisor = ProcessSupervisor::new(
                    child_process_event_tx,
                    self.db.clone(),
                    format!("http://{}/event", self.config.listen_addr),
                    child_name_str.clone(),
                );

                // Forward process events to child's main event channel
                let child_event_tx_for_process = child_event_tx.clone();
                tokio::spawn(async move {
                    while let Some(pe) = child_process_event_rx.recv().await {
                        if child_event_tx_for_process
                            .send(HarnessEvent::Process(pe))
                            .is_err()
                        {
                            break;
                        }
                    }
                });

                // Clone parent state, then clear timers/processes/queue
                let mut child_state = self.state.clone();
                child_state.work_queue = WorkQueue::new();
                child_state.timer_manager = TimerManager::new();
                child_state.process_manager = ProcessManager::new();
                child_state.last_input_tokens = 0;

                if !child_settings.inherit_history {
                    child_state.event_history = EventHistory::new();
                    let alert_id = child_state.id_generator.next();
                    child_state.event_history.push(HistoryEntry::SystemAlert {
                        id: alert_id,
                        time: Utc::now(),
                        message: format!(
                            "Forked from '{}' with fresh history. Task: {}",
                            self.permissions.agent_name, child_settings.task
                        ),
                    });
                }

                // Add task as work item. Attach paths go on this item as
                // metadata; if present, also push a View item at head so the
                // child sees the files on turn 1.
                let task_id = child_state.id_generator.next();
                child_state.work_queue.push(WorkItem {
                    id: task_id,
                    priority: 9,
                    time: Utc::now(),
                    item_type: WorkItemType::UserMessage {
                        chat_id: "child-agent".to_string(),
                        user: self.permissions.agent_name.to_string(),
                        content: child_settings.task,
                        message_ref: None,
                    },
                    attachments: child_settings.attach.clone(),
                });
                if !child_settings.attach.is_empty() {
                    let view_id = child_state.id_generator.next();
                    child_state.work_queue.push(WorkItem {
                        id: view_id,
                        priority: 10,
                        time: Utc::now(),
                        item_type: WorkItemType::View { paths: child_settings.attach },
                        attachments: Vec::new(),
                    });
                }

                // Create child API client
                let child_config = Arc::new(Config {
                    model: model.clone(),
                    api_key: self.config.api_key.clone(),
                    api_base_url: self.config.api_base_url.clone(),
                    max_tokens: self.config.max_tokens,
                    context_window: self.config.context_window,
                    db_path: self.config.db_path.clone(),
                    system_prompt_path: self.config.system_prompt_path.clone(),
                    deployment_context_path: self.config.deployment_context_path.clone(),
                    listen_addr: self.config.listen_addr,
                    compact_at: self.config.compact_at,
                    compact_target: self.config.compact_target,
                    render_config: self.config.render_config.clone(),
                    python_timeout_secs: self.config.python_timeout_secs,
                    cost_per_m_input: self.config.cost_per_m_input,
                    cost_per_m_output: self.config.cost_per_m_output,
                    cost_per_m_cache_read: self.config.cost_per_m_cache_read,
                    cost_per_m_cache_write: self.config.cost_per_m_cache_write,
                });

                let system_prompt = self
                    .config
                    .load_system_prompt()
                    .unwrap_or_else(|_| String::new());

                let child_api_client =
                    match ApiClient::new_with_prompt(child_config.clone(), &system_prompt) {
                        Ok(c) => c,
                        Err(e) => {
                            self.active_children = self.active_children.saturating_sub(1);
                            self.registry.deregister(&child_name_str);
                            let wid = self.state.id_generator.next();
                            self.state.work_queue.push(WorkItem {
                                id: wid,
                                priority: 7,
                                time: Utc::now(),
                                item_type: WorkItemType::ChildAgentCompleted {
                                    child_name: child_name_str,
                                    result: HashMap::new(),
                                    turns_used: 0,
                                    success: false,
                                    summary: format!("Failed to create API client: {}", e),
                                    cost_usd: 0.0,
                                    cache_hit_pct: 0,
                                },
                                attachments: Vec::new(),
                            });
                            continue;
                        }
                    };

                let child_permissions = AgentPermissions {
                    can_compact: child_settings.can_compact,
                    max_turns,
                    child_depth_remaining: child_depth,
                    agent_name: AgentName::new_child(&child_name_str)
                        .expect("child name validated at registration"),
                    lineage: child_lineage,
                };

                let dump_dir = self.dump_dir.clone();
                let db = self.db.clone();
                let parent_tx = self.event_tx.clone();
                let registry = self.registry.clone();

                let child_role_prefix =
                    if child_settings.prefix_context.is_some() || !child_settings.prefix_attach.is_empty() {
                        Some(renderer::RolePrefix {
                            context: child_settings.prefix_context.unwrap_or_default(),
                            attach: child_settings.prefix_attach,
                        })
                    } else {
                        None
                    };
                tokio::spawn(run_child_agent_loop(
                    child_name_str,
                    child_permissions,
                    child_state,
                    child_config,
                    db,
                    child_api_client,
                    child_process_supervisor,
                    child_event_rx,
                    child_event_tx,
                    self.deployment_context.clone(),
                    child_role_prefix,
                    dump_dir,
                    7, // default priority for ChildAgentCompleted
                    parent_tx,
                    registry,
                    self.shutdown.clone(),
                ));
            }
        }

        // Agent messages (already validated above)
        for msg in effects.agent_messages {
            match self.registry.send_to(
                &msg.recipient,
                HarnessEvent::AgentMessage {
                    from: self.permissions.agent_name.to_string(),
                    content: msg.content,
                    priority: msg.priority,
                },
            ) {
                Ok(true) => {} // delivered
                Ok(false) => {
                    dimlog!(
                       "[{}] Message to '{}' dropped (agent completed)",
                        self.name, msg.recipient
                    );
                }
                Err(e) => {
                    eprintln!("[{}] Failed to deliver agent message: {}", self.name, e);
                }
            }
        }

        // Outbound messages
        for msg in effects.messages {
            dimlog!(
                "[message] -> chat:{} | {}",
                msg.chat_id,
                truncate_for_log(&msg.content, 60)
            );
            let id = self.db.save_outbound_message(&msg.chat_id, &msg.content, &msg.attachments)
                .unwrap_or_else(|e| {
                    eprintln!("[{}] save_outbound_message failed (schema mismatch? rm the .db): {}", self.name, e);
                    0
                });
            self.broadcast(BroadcastMsg::Message {
                chat_id: msg.chat_id,
                content: msg.content,
                attachments: msg.attachments,
                id,
                created_at: chrono::Utc::now()
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
                react_to: msg.react_to,
            });
        }

        // Pinned memory (shared, cached in system prompt)
        for (key, content) in effects.memory_pins {
            if let Err(e) = self.db.save_pin(&key, &content) {
                eprintln!("[{}] Failed to pin '{}': {}", self.name, key, e);
            }
        }
        for key in effects.memory_unpins {
            if let Err(e) = self.db.delete_pin(&key) {
                eprintln!("[{}] Failed to unpin '{}': {}", self.name, key, e);
            }
        }
        for (key, sensitive) in effects.sensitive_marks {
            if sensitive {
                self.state.sensitive_keys.insert(key);
            } else {
                self.state.sensitive_keys.remove(&key);
            }
        }

        // Agent-requested compaction (e.g. scheduled via timer)
        if effects.compaction_requested
            && self.permissions.can_compact
            && !self.compaction.active
        {
            dimlog!("[{}] Compaction requested by agent", self.name);
            self.compaction
                .trigger(&mut self.state, self.config.compact_target);
        }

        // Compaction script
        for append in effects.compaction_script_appends {
            self.compaction.script.push_str(&append);
        }
        if effects.compact_called && self.compaction.active {
            dimlog!("[{}] Compaction executed", self.name);
            let compact_result = self.executor.execute_with_timeout(
                &self.state,
                &self.compaction.script,
                true,
                &HashMap::new(),
                self.config.python_timeout_secs,
                0, // compaction doesn't need to spawn children
                self.permissions.agent_name.as_str(),
                &format_lineage(&self.permissions.lineage),
                &HashMap::new(), // compaction doesn't need pinned memory
            );

            if compact_result.is_error {
                eprintln!(
                    "[{}] Compaction script error: {}",
                    self.name, compact_result.error_text
                );
                let id = self.state.id_generator.next();
                self.state.event_history.push(HistoryEntry::Execution {
                    id,
                    time: Utc::now(),
                    code: "compact()".to_string(),
                    output: format!("[ERROR]\n{}", compact_result.error_text),
                    is_error: true,
                });
            } else {
                for id in compact_result.side_effects.history_removes {
                    self.state.event_history.remove(&AgentId(id));
                }
                for (id, desc) in compact_result.side_effects.history_replaces {
                    self.state
                        .event_history
                        .replace_with_summary(&AgentId(id), desc);
                }
                for text in compact_result.side_effects.history_adds {
                    let id = self.state.id_generator.next();
                    self.state.event_history.push(HistoryEntry::Summary {
                        id,
                        time: Utc::now(),
                        description: text,
                    });
                }

                // Remove the Compaction work item
                let compaction_items: Vec<AgentId> = self
                    .state
                    .work_queue
                    .items()
                    .iter()
                    .filter(|i| matches!(i.item_type, WorkItemType::Compaction))
                    .map(|i| i.id.clone())
                    .collect();
                for id in compaction_items {
                    self.state.work_queue.remove(&id);
                }

                self.compaction.complete();
                dimlog!("[{}] Compaction complete", self.name);
            }
        }

        Ok(deferred)
    }

    /// Perform the deferred async operations returned by apply_side_effects.
    async fn perform_deferred_ops(&mut self, deferred: DeferredOps) {
        // Wait for block_for processes
        for (ms, rx) in deferred.block_for_waiters {
            let _ = tokio::time::timeout(Duration::from_millis(ms), rx).await;
            self.drain_events();
        }

        // Process kills
        for id in deferred.process_kills {
            if let Err(e) = self.process_supervisor.kill(&id).await {
                eprintln!("[{}] Failed to kill process {}: {}", self.name, id, e);
            }
        }

        // Interactive process stdin
        for (pid, data) in deferred.stdin_writes {
            self.process_supervisor.send_stdin(&pid, data).await;
        }
        for pid in deferred.stdin_closes {
            self.process_supervisor.close_stdin(&pid).await;
        }
    }
}

/// Deferred async operations collected during apply_side_effects.
#[derive(Default)]
struct DeferredOps {
    /// (timeout_ms, oneshot_receiver) pairs for block_for process waits
    block_for_waiters: Vec<(u64, tokio::sync::oneshot::Receiver<()>)>,
    /// Process IDs to kill
    process_kills: Vec<String>,
    stdin_writes: Vec<(String, Vec<u8>)>,
    stdin_closes: Vec<String>,
}

/// Standalone async function to run a child agent loop.
/// Extracted from AgentLoop::apply_side_effects to break the recursive type
/// dependency that would otherwise make the future from AgentLoop::run() not Send.
async fn run_child_agent_loop(
    child_name: String,
    child_permissions: AgentPermissions,
    child_state: HarnessState,
    child_config: Arc<Config>,
    db: Arc<Database>,
    child_api_client: ApiClient,
    child_process_supervisor: ProcessSupervisor,
    child_event_rx: mpsc::UnboundedReceiver<HarnessEvent>,
    child_event_tx: mpsc::UnboundedSender<HarnessEvent>,
    child_deployment: String,
    child_role_prefix: Option<renderer::RolePrefix>,
    dump_dir: Option<PathBuf>,
    priority: u8,
    parent_tx: mpsc::UnboundedSender<HarnessEvent>,
    registry: Arc<AgentRegistry>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut child_loop = AgentLoop::new(
        child_name.clone(),
        child_permissions,
        child_state,
        child_config,
        db,
        child_api_client,
        child_process_supervisor,
        child_event_rx,
        child_event_tx,
        child_deployment,
        child_role_prefix,
        None,  // children don't broadcast
        dump_dir,
        false, // children don't dump to stdout
        None,  // children track tokens locally
        registry.clone(),
        shutdown,
    );

    let reason = child_loop.run().await;
    let turns_used = child_loop.turn_counter;

    let (final_success, final_summary) = match reason {
        FinishReason::Done => (true, "Called done()".to_string()),
        FinishReason::MaxTurns(max) => (false, format!("Max turns ({}) exceeded", max)),
        FinishReason::Shutdown => (false, "Shutdown".to_string()),
        FinishReason::Killed => (false, "Killed by parent".to_string()),
        FinishReason::ChannelClosed => (false, "Parent disconnected".to_string()),
    };

    dimlog!(
       "[{}] Finished (success={}, turns={}, reason={})",
        child_name, final_success, turns_used, final_summary
    );

    // Deregister from the agent registry
    registry.deregister(&child_name);

    let _ = parent_tx.send(HarnessEvent::ChildCompleted {
        child_name,
        result: child_loop.done_result,
        turns_used,
        success: final_success,
        summary: final_summary,
        priority,
        child_input_tokens: child_loop.local_input_tokens,
        child_output_tokens: child_loop.local_output_tokens,
        child_cache_creation_tokens: child_loop.local_cache_creation_tokens,
        child_cache_read_tokens: child_loop.local_cache_read_tokens,
    });
}
