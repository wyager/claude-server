use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::{broadcast, mpsc};

use crate::api_client::ApiClient;
use crate::child_agent;
use crate::compaction::CompactionManager;
use crate::config::Config;
use crate::db::Database;
use crate::http_server::BroadcastMsg;
use crate::process::{ProcessEvent, ProcessSupervisor};
use crate::python;
use crate::renderer;
use crate::types::*;

/// Events from external sources that feed into the core loop.
#[derive(Debug)]
pub enum HarnessEvent {
    UserMessage {
        chat_id: String,
        user: String,
        content: String,
    },
    Process(ProcessEvent),
    ChildCompleted {
        child_id: AgentId,
        result_memory: HashMap<String, serde_json::Value>,
        turns_used: u32,
        success: bool,
        summary: String,
        priority: u8,
    },
    Shutdown,
}

pub struct CoreLoop {
    pub state: HarnessState,
    config: Arc<Config>,
    db: Arc<Database>,
    api_client: ApiClient,
    process_supervisor: ProcessSupervisor,
    compaction: CompactionManager,
    event_rx: mpsc::UnboundedReceiver<HarnessEvent>,
    event_tx: mpsc::UnboundedSender<HarnessEvent>,
    deployment_context: String,
    dump_turns: bool,
    dump_dir: Option<PathBuf>,
    turn_counter: u32,
    broadcast_tx: broadcast::Sender<BroadcastMsg>,
    active_children: u32,
    max_children: u32,
}

impl CoreLoop {
    pub fn new(
        state: HarnessState,
        config: Arc<Config>,
        db: Arc<Database>,
        api_client: ApiClient,
        process_supervisor: ProcessSupervisor,
        event_rx: mpsc::UnboundedReceiver<HarnessEvent>,
        event_tx: mpsc::UnboundedSender<HarnessEvent>,
        deployment_context: String,
        dump_turns: bool,
        dump_dir: Option<PathBuf>,
        broadcast_tx: broadcast::Sender<BroadcastMsg>,
    ) -> Self {
        let max_children: u32 = std::env::var("CLAUDE_SERVER_MAX_CHILDREN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        Self {
            state,
            config,
            db,
            api_client,
            process_supervisor,
            compaction: CompactionManager::new(),
            event_rx,
            event_tx,
            deployment_context,
            dump_turns,
            dump_dir,
            turn_counter: 0,
            broadcast_tx,
            active_children: 0,
            max_children,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("[core] Agent loop started");
        let mut idle = false;

        loop {
            // Drain any pending events
            self.drain_events();

            // Check timers
            self.check_timers();

            // Check if compaction needed
            if !self.compaction.active
                && CompactionManager::should_trigger(&self.state, self.config.compact_at)
            {
                println!("[core] Triggering compaction (input_tokens {} > threshold {})",
                    self.state.last_input_tokens, self.config.compact_at);
                self.compaction
                    .trigger(&mut self.state, self.config.compact_target);
            }

            // If work queue is empty, block until an event arrives or a timer fires
            if self.state.work_queue.is_empty() {
                if !idle {
                    println!("[core] Idle, waiting for events...");
                    idle = true;
                    let _ = self.broadcast_tx.send(BroadcastMsg::Status {
                        status: "idle".to_string(),
                    });
                }

                // Sleep until the next timer deadline, or a long time if no timers
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
                            Some(HarnessEvent::Shutdown) => {
                                println!("[core] Shutdown requested");
                                break;
                            }
                            Some(event) => self.apply_event(event),
                            None => {
                                println!("[core] Event channel closed, shutting down");
                                break;
                            }
                        }
                    }
                    _ = timer_sleep => {
                        self.check_timers();
                    }
                }
                continue;
            }

            idle = false;

            // Run a turn
            if let Err(e) = self.run_turn().await {
                eprintln!("[core] Turn error: {}", e);
                // Wait a bit before retrying
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            // Persist state
            if let Err(e) = self.db.save_state(&self.state) {
                eprintln!("[core] Failed to persist state: {}", e);
            }
        }

        Ok(())
    }

    async fn run_turn(&mut self) -> Result<()> {
        // Build compaction state if active
        let compaction_state = if self.compaction.active {
            let mut cs =
                self.compaction
                    .compaction_state(self.state.last_input_tokens);
            cs.estimated_post_compaction = self.compaction.estimate_post_compaction(
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
        let rendered = renderer::render_context(
            &self.state,
            &self.deployment_context,
            compaction_state.as_ref(),
            &self.config.render_config,
            self.config.compact_at,
        );

        println!(
            "[core] Rendered context: {} chars, queue: {} items",
            rendered.text.len(),
            self.state.work_queue.len()
        );

        // Broadcast thinking status
        let _ = self.broadcast_tx.send(BroadcastMsg::Status {
            status: "thinking".to_string(),
        });

        // Call Claude API
        let api_result = self.api_client.call(&rendered).await?;

        println!(
            "[core] API response: {} input tokens, {} output tokens (cache: {} created, {} read)",
            api_result.input_tokens,
            api_result.output_tokens,
            api_result.cache_creation_tokens,
            api_result.cache_read_tokens
        );

        // Dump turn (to stdout and/or file)
        self.turn_counter += 1;
        if self.dump_turns || self.dump_dir.is_some() {
            write_turn_dump(
                "parent",
                self.turn_counter,
                &rendered.text,
                api_result.thinking.as_deref(),
                &api_result.code,
                self.dump_turns,
                self.dump_dir.as_deref(),
            );
        }

        // Update token tracking
        self.state.last_input_tokens = api_result.input_tokens;

        // Load process outputs for shell_output() calls
        let process_outputs = self.db.load_all_process_outputs().unwrap_or_default();

        // Broadcast executing status
        let _ = self.broadcast_tx.send(BroadcastMsg::Status {
            status: "executing".to_string(),
        });

        // Execute Python
        let exec_result = python::execute_with_timeout(
            &self.state,
            &api_result.code,
            self.compaction.active,
            &process_outputs,
            self.config.python_timeout_secs,
        );

        // Record in history
        let entry_id = self.state.id_generator.next();
        let output = if exec_result.is_error {
            format!("[ERROR]\n{}", exec_result.error_text)
        } else {
            exec_result.stdout.clone()
        };

        println!(
            "[core] Executed entry {} (error={}): {}",
            entry_id,
            exec_result.is_error,
            &api_result.code.lines().next().unwrap_or("(empty)")
        );

        if !exec_result.stdout.is_empty() {
            print!("[stdout] {}", exec_result.stdout);
        }

        self.state.event_history.push(HistoryEntry::Execution {
            id: entry_id,
            time: Utc::now(),
            code: api_result.code,
            output,
            is_error: exec_result.is_error,
        });

        // Apply side effects (only if no error)
        if !exec_result.is_error {
            self.apply_side_effects(exec_result.side_effects).await;
        }

        Ok(())
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
            println!("[core] Timer fired: {}", item.id);
            self.state.work_queue.push(item);
        }
    }

    fn apply_event(&mut self, event: HarnessEvent) {
        match event {
            HarnessEvent::UserMessage {
                chat_id,
                user,
                content,
            } => {
                let id = self.state.id_generator.next();
                println!("[core] User message from {}: {} (id={})", user, &content[..content.len().min(50)], id);
                self.state.work_queue.push(WorkItem {
                    id,
                    priority: 9,
                    time: Utc::now(),
                    item_type: WorkItemType::UserMessage {
                        chat_id,
                        user,
                        content,
                    },
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
                    // Load output preview (race-condition-free: reader finishes before event)
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
                    });
                }
            },
            HarnessEvent::ChildCompleted {
                child_id,
                result_memory,
                turns_used,
                success,
                summary,
                priority,
            } => {
                self.active_children = self.active_children.saturating_sub(1);
                let id = self.state.id_generator.next();
                println!(
                    "[core] Child agent {} completed (success={}, turns={})",
                    child_id, success, turns_used
                );
                self.state.work_queue.push(WorkItem {
                    id,
                    priority,
                    time: Utc::now(),
                    item_type: WorkItemType::ChildAgentCompleted {
                        child_id,
                        result_memory,
                        turns_used,
                        success,
                        summary,
                    },
                });
            }
            HarnessEvent::Shutdown => {} // handled in run()
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

    async fn apply_side_effects(&mut self, effects: python::SideEffectCollector) {
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
                // One-shot at a specific time
                let at = chrono::DateTime::from_timestamp(epoch as i64, 0)
                    .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(1));
                TimerSchedule::OneShot { at }
            } else {
                // No schedule specified — default to 1 min from now
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

        // Timer acknowledgments — re-arm recurring timers
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
                alert_timer: Duration::from_secs(req.alert_timer_secs),
                success_prio: req.success_prio,
                fail_prio: req.fail_prio,
                started_at: Utc::now(),
                os_pid: None,
            };
            self.state.process_manager.add(managed);

            let block_for_ms = req.block_for_ms;
            match self.process_supervisor.spawn(req.clone()) {
                Ok(completion_rx) => {
                    // If block_for is set, wait for the process to complete or timeout
                    if let (Some(ms), Some(rx)) = (block_for_ms, completion_rx) {
                        let _ = tokio::time::timeout(
                            Duration::from_millis(ms),
                            rx,
                        )
                        .await;
                        // Whether it completed or timed out, drain any pending events
                        // so they appear in the work queue for the current turn
                        self.drain_events();
                    }
                }
                Err(e) => {
                    eprintln!("[core] Failed to spawn process: {}", e);
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
                    });
                }
            }
        }

        // Process kills
        for id in effects.process_kills {
            if let Err(e) = self.process_supervisor.kill(&id).await {
                eprintln!("[core] Failed to kill process {}: {}", id, e);
            }
        }

        // Child agent spawns
        for req in effects.child_agent_starts {
            if self.active_children >= self.max_children {
                eprintln!(
                    "[core] Cannot spawn child agent: max {} concurrent children reached",
                    self.max_children
                );
                // Insert an error work item so the agent knows
                let wid = self.state.id_generator.next();
                self.state.work_queue.push(WorkItem {
                    id: wid,
                    priority: req.priority,
                    time: Utc::now(),
                    item_type: WorkItemType::ChildAgentCompleted {
                        child_id: req.id,
                        result_memory: HashMap::new(),
                        turns_used: 0,
                        success: false,
                        summary: format!(
                            "Could not spawn: max {} concurrent children reached",
                            self.max_children
                        ),
                    },
                });
                continue;
            }

            self.active_children += 1;
            println!(
                "[core] Spawning child agent {} (model={}, max_turns={})",
                req.id, req.model, req.max_turns
            );

            let config = self.config.clone();
            let db = self.db.clone();
            let parent_tx = self.event_tx.clone();
            let deployment_context = self.deployment_context.clone();
            let system_prompt = config
                .load_system_prompt()
                .unwrap_or_else(|_| String::new());
            let dump_dir = self.dump_dir.clone();

            tokio::spawn(async move {
                child_agent::run_child_agent(
                    req,
                    config,
                    db,
                    deployment_context,
                    system_prompt,
                    parent_tx,
                    dump_dir,
                )
                .await;
            });
        }

        // Outbound messages
        for msg in effects.messages {
            println!("[message] -> chat:{} | {}", msg.chat_id, msg.content);
            if let Ok(id) = self.db.save_outbound_message(&msg.chat_id, &msg.content) {
                let _ = self.broadcast_tx.send(BroadcastMsg::Message {
                    chat_id: msg.chat_id,
                    content: msg.content,
                    id,
                    created_at: chrono::Utc::now()
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                });
            }
        }

        // Compaction script
        for append in effects.compaction_script_appends {
            self.compaction.script.push_str(&append);
        }
        if effects.compact_called && self.compaction.active {
            println!("[core] Compaction executed");
            // Execute the compaction script by running it as Python against cloned state
            let compact_result = python::execute_with_timeout(
                &self.state,
                &self.compaction.script,
                true,
                &HashMap::new(),
                self.config.python_timeout_secs,
            );

            if compact_result.is_error {
                eprintln!(
                    "[core] Compaction script error: {}",
                    compact_result.error_text
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
                // Apply compaction side effects (history removes/adds)
                // We need to apply these in compaction mode
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
                println!("[core] Compaction complete");
            }
        }
    }
}

/// Write a turn dump to stdout and/or a file.
pub fn write_turn_dump(
    agent_name: &str,
    turn_number: u32,
    context: &str,
    thinking: Option<&str>,
    code: &str,
    to_stdout: bool,
    dump_dir: Option<&std::path::Path>,
) {
    let sep = "=".repeat(80);
    let dash = "-".repeat(80);

    let mut dump = String::with_capacity(context.len() + code.len() + 512);
    dump.push_str(&format!("{}\n", sep));
    dump.push_str(&format!(
        "[{}] Turn {} — CONTEXT SENT TO MODEL ({} chars)\n",
        agent_name,
        turn_number,
        context.len()
    ));
    dump.push_str(&format!("{}\n\n", sep));
    dump.push_str(context);
    dump.push_str(&format!("\n{}\n", dash));

    if let Some(thinking) = thinking {
        dump.push_str(&format!("\n{}\n", sep));
        dump.push_str(&format!("[{}] Turn {} — AGENT THINKING\n", agent_name, turn_number));
        dump.push_str(&format!("{}\n\n", sep));
        dump.push_str(thinking);
        dump.push_str(&format!("\n{}\n", dash));
    }

    dump.push_str(&format!("\n{}\n", sep));
    dump.push_str(&format!(
        "[{}] Turn {} — AGENT RESPONSE (Python code)\n",
        agent_name, turn_number
    ));
    dump.push_str(&format!("{}\n\n", sep));
    dump.push_str(code);
    dump.push_str(&format!("\n{}\n", dash));

    if to_stdout {
        println!("{}", dump);
    }

    if let Some(dir) = dump_dir {
        let filename = format!("{}-{:03}-dump.txt", agent_name, turn_number);
        let path = dir.join(&filename);
        if let Err(e) = std::fs::write(&path, &dump) {
            eprintln!("[dump] Failed to write {}: {}", path.display(), e);
        }
    }
}

