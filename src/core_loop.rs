use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::mpsc;

use crate::api_client::ApiClient;
use crate::compaction::CompactionManager;
use crate::config::Config;
use crate::db::Database;
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
    TimerTick,
    Process(ProcessEvent),
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
    deployment_context: String,
}

impl CoreLoop {
    pub fn new(
        state: HarnessState,
        config: Arc<Config>,
        db: Arc<Database>,
        api_client: ApiClient,
        process_supervisor: ProcessSupervisor,
        event_rx: mpsc::UnboundedReceiver<HarnessEvent>,
        deployment_context: String,
    ) -> Self {
        Self {
            state,
            config,
            db,
            api_client,
            process_supervisor,
            compaction: CompactionManager::new(),
            event_rx,
            deployment_context,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("[core] Agent loop started");

        loop {
            // Drain any pending events
            self.drain_events();

            // Check timers
            self.check_timers();

            // Check if compaction needed
            if !self.compaction.active
                && CompactionManager::should_trigger(&self.state, self.config.compaction_ratio)
            {
                println!("[core] Triggering compaction");
                self.compaction
                    .trigger(&mut self.state, self.config.compaction_target_ratio);
            }

            // If work queue is empty, wait for an event
            if self.state.work_queue.is_empty() {
                println!("[core] Work queue empty, waiting for events...");
                match self.event_rx.recv().await {
                    Some(event) => {
                        if matches!(event, HarnessEvent::Shutdown) {
                            println!("[core] Shutdown requested");
                            break;
                        }
                        self.apply_event(event);
                    }
                    None => {
                        println!("[core] Event channel closed, shutting down");
                        break;
                    }
                }
                continue;
            }

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
        );

        println!(
            "[core] Rendered context: {} chars, queue: {} items",
            rendered.text.len(),
            self.state.work_queue.len()
        );

        // Call Claude API
        let api_result = self.api_client.call(&rendered).await?;

        println!(
            "[core] API response: {} input tokens, {} output tokens (cache: {} created, {} read)",
            api_result.input_tokens,
            api_result.output_tokens,
            api_result.cache_creation_tokens,
            api_result.cache_read_tokens
        );

        // Update token tracking
        self.state.last_input_tokens = api_result.input_tokens;

        // Load process outputs for shell_output() calls
        let process_outputs = self.db.load_all_process_outputs().unwrap_or_default();

        // Execute Python
        let exec_result = python::execute(
            &self.state,
            &api_result.code,
            self.compaction.active,
            &process_outputs,
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
            HarnessEvent::TimerTick => {
                self.check_timers();
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
                    let id = self.state.id_generator.next();
                    self.state.work_queue.push(WorkItem {
                        id,
                        priority: prio,
                        time: Utc::now(),
                        item_type: WorkItemType::ProcessCompleted { pid, exit_code },
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
                    let id = self.state.id_generator.next();
                    self.state.work_queue.push(WorkItem {
                        id,
                        priority: prio,
                        time: Utc::now(),
                        item_type: WorkItemType::ProcessFailed { pid, error },
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
            HarnessEvent::Shutdown => {} // handled in run()
        }
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
            } else {
                // One-shot: parse timestamp or default to 1 min from now
                let at = Utc::now() + chrono::Duration::minutes(1);
                TimerSchedule::OneShot { at }
            };

            let timer = Timer {
                id: req.id,
                description: req.description,
                priority: req.priority,
                schedule,
                created_at: Utc::now(),
            };
            self.state.timer_manager.add(timer);
        }
        for id in effects.timer_cancels {
            self.state.timer_manager.cancel(&AgentId(id));
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
                status: ProcessStatus::Running,
                alert_timer: Duration::from_secs(req.alert_timer_secs),
                success_prio: req.success_prio,
                fail_prio: req.fail_prio,
                started_at: Utc::now(),
                os_pid: None,
            };
            self.state.process_manager.add(managed);

            if let Err(e) = self.process_supervisor.spawn(req) {
                eprintln!("[core] Failed to spawn process: {}", e);
            }
        }

        // Process kills
        for id in effects.process_kills {
            if let Err(e) = self.process_supervisor.kill(&id).await {
                eprintln!("[core] Failed to kill process {}: {}", id, e);
            }
        }

        // Outbound messages
        for msg in effects.messages {
            println!("[message] -> chat:{} | {}", msg.chat_id, msg.content);
            let _ = self
                .db
                .save_outbound_message(&msg.chat_id, &msg.content);
        }

        // Compaction script
        for append in effects.compaction_script_appends {
            self.compaction.script.push_str(&append);
        }
        if effects.compact_called && self.compaction.active {
            println!("[core] Compaction executed");
            // Execute the compaction script by running it as Python against cloned state
            let compact_result = python::execute(
                &self.state,
                &self.compaction.script,
                true,
                &HashMap::new(),
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

/// Run the timer tick loop. Sends TimerTick events periodically.
pub async fn timer_tick_loop(
    event_tx: mpsc::UnboundedSender<HarnessEvent>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        if event_tx.send(HarnessEvent::TimerTick).is_err() {
            break; // channel closed
        }
    }
}
