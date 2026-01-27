use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc;

use crate::api_client::ApiClient;
use crate::config::Config;
use crate::core_loop::{self, HarnessEvent};
use crate::db::Database;
use crate::python::{self, ChildAgentRequest};
use crate::renderer;
use crate::types::*;

/// Run a child agent loop until its work queue is empty or max_turns is reached.
pub async fn run_child_agent(
    req: ChildAgentRequest,
    config: Arc<Config>,
    db: Arc<Database>,
    deployment_context: String,
    system_prompt: String,
    parent_tx: mpsc::UnboundedSender<HarnessEvent>,
    dump_dir: Option<PathBuf>,
) {
    let child_id = req.id.clone();
    let priority = req.priority;
    let max_turns = req.max_turns;

    // Create the child's API client with the requested model
    let child_config = Arc::new(Config {
        model: req.model.clone(),
        api_key: config.api_key.clone(),
        api_base_url: config.api_base_url.clone(),
        max_tokens: config.max_tokens,
        context_window: config.context_window,
        db_path: config.db_path.clone(),
        system_prompt_path: config.system_prompt_path.clone(),
        deployment_context_path: config.deployment_context_path.clone(),
        listen_addr: config.listen_addr,
        compact_at: config.compact_at,
        compact_target: config.compact_target,
        render_config: config.render_config.clone(),
        python_timeout_secs: config.python_timeout_secs,
    });

    let api_client = match ApiClient::new_with_prompt(child_config.clone(), &system_prompt) {
        Ok(c) => c,
        Err(e) => {
            let _ = parent_tx.send(HarnessEvent::ChildCompleted {
                child_id,
                result_memory: HashMap::new(),
                turns_used: 0,
                success: false,
                summary: format!("Failed to create API client: {}", e),
                priority,
            });
            return;
        }
    };

    // Create child state with seed memory
    let mut state = HarnessState::new(config.context_window, config.max_tokens);
    state.memory = req.seed_memory;

    // Seed the work queue with the task as a UserMessage
    let task_id = state.id_generator.next();
    state.work_queue.push(WorkItem {
        id: task_id,
        priority: 9,
        time: Utc::now(),
        item_type: WorkItemType::UserMessage {
            chat_id: "child-agent".to_string(),
            user: "parent-agent".to_string(),
            content: req.task,
        },
    });

    // Child system prompt preamble
    let child_deployment = format!(
        "You are a sub-agent spawned by a parent agent to complete a specific task.\n\
        Complete the assigned task, store your results in memory, and stop.\n\
        You can use send_message() if the task requires communicating with users.\n\
        You cannot use shell_exec() or spawn_agent() — those will raise errors.\n\
        Your parent receives your final memory contents when you complete.\n\
        \n{}",
        deployment_context
    );

    let mut turns_used: u32 = 0;
    let mut last_error = String::new();

    loop {
        // Check termination conditions
        if state.work_queue.is_empty() {
            break;
        }
        if turns_used >= max_turns {
            last_error = format!("Max turns ({}) exceeded", max_turns);
            break;
        }

        // Render context
        let rendered = renderer::render_context(
            &state,
            &child_deployment,
            None,
            &config.render_config,
            config.compact_at,
        );

        // Call API
        let api_result = match api_client.call(&rendered).await {
            Ok(r) => r,
            Err(e) => {
                last_error = format!("API error: {}", e);
                eprintln!("[child:{}] API error: {}", child_id, e);
                // Retry after delay
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        state.last_input_tokens = api_result.input_tokens;

        // Execute Python
        let process_outputs = db.load_all_process_outputs().unwrap_or_default();
        let exec_result = python::execute_with_timeout(
            &state,
            &api_result.code,
            false,
            &process_outputs,
            config.python_timeout_secs,
            false, // children cannot spawn grandchildren
        );

        let is_error = exec_result.is_error;
        let stdout = exec_result.stdout.clone();
        let error_text = exec_result.error_text.clone();

        println!(
            "[child:{}] Turn {} (error={}): {}",
            child_id,
            turns_used + 1,
            is_error,
            api_result.code.lines().next().unwrap_or("(empty)")
        );

        // Apply side effects BEFORE generating entry ID (same fix as parent)
        if !is_error {
            apply_child_side_effects(&mut state, exec_result.side_effects, &db);
        }

        // Record in history (after side effects so entry_id is fresh)
        let entry_id = state.id_generator.next();
        let output = if is_error {
            format!("[ERROR]\n{}", error_text)
        } else {
            stdout
        };

        state.event_history.push(HistoryEntry::Execution {
            id: entry_id,
            time: Utc::now(),
            code: api_result.code.clone(),
            output: output.clone(),
            is_error,
        });

        // Dump turn to file if dump_dir is set (after execution so output is included)
        if dump_dir.is_some() {
            let agent_name = format!("child-{}", child_id);
            core_loop::write_turn_dump(
                &agent_name,
                turns_used + 1,
                &rendered.text,
                api_result.thinking.as_deref(),
                &api_result.code,
                &output,
                is_error,
                false,
                dump_dir.as_deref(),
            );
        }

        turns_used += 1;
    }

    // Build summary
    let success = last_error.is_empty();
    let summary = if !success {
        if last_error.len() > 200 {
            format!("{}...", &last_error[..200])
        } else {
            last_error
        }
    } else {
        "Completed successfully".to_string()
    };

    println!(
        "[child:{}] Finished (success={}, turns={})",
        child_id, success, turns_used
    );

    let _ = parent_tx.send(HarnessEvent::ChildCompleted {
        child_id,
        result_memory: state.memory,
        turns_used,
        success,
        summary,
        priority,
    });
}

/// Apply side effects for a child agent.
/// Children can do everything the parent can except spawn_agent (blocked at the Python level).
/// Process spawning is not yet supported (requires a full event loop with ProcessSupervisor).
fn apply_child_side_effects(state: &mut HarnessState, effects: python::SideEffectCollector, db: &Database) {
    state.id_generator = effects.id_gen;

    // Memory
    for (key, value) in effects.memory_sets {
        state.memory.insert(key, value);
    }
    for key in effects.memory_deletes {
        state.memory.remove(&key);
    }
    for (key, priority) in effects.memory_priority_sets {
        state.memory_priorities.insert(key, priority);
    }

    // Queue removes
    for id in effects.queue_removes {
        state.work_queue.remove(&AgentId(id));
    }

    // History operations
    for id in effects.history_removes {
        let aid = AgentId(id);
        if state.event_history.is_modifiable(&aid) {
            state.event_history.remove(&aid);
        }
    }
    for (id, desc) in effects.history_replaces {
        let aid = AgentId(id);
        if state.event_history.is_modifiable(&aid) {
            state.event_history.replace_with_summary(&aid, desc);
        }
    }

    // Timer operations (children can use timers for their own work)
    for req in effects.timer_adds {
        let schedule = if let Some(secs) = req.every_secs {
            TimerSchedule::Recurring {
                every: std::time::Duration::from_secs(secs),
                next_fire: Utc::now()
                    + chrono::Duration::from_std(std::time::Duration::from_secs(secs))
                        .unwrap_or(chrono::Duration::seconds(1)),
            }
        } else if let Some(epoch) = req.at_epoch {
            let at = chrono::DateTime::from_timestamp(epoch as i64, 0)
                .unwrap_or_else(|| Utc::now() + chrono::Duration::minutes(1));
            TimerSchedule::OneShot { at }
        } else {
            TimerSchedule::OneShot {
                at: Utc::now() + chrono::Duration::minutes(1),
            }
        };
        state.timer_manager.add(Timer {
            id: req.id,
            description: req.description,
            priority: req.priority,
            schedule,
            created_at: Utc::now(),
            pending_ack: false,
        });
    }
    for id in effects.timer_cancels {
        state.timer_manager.cancel(&AgentId(id));
    }
    for id in effects.timer_acks {
        state.timer_manager.acknowledge(&AgentId(id));
    }

    // Outbound messages
    for msg in effects.messages {
        println!("[child] message -> chat:{} | {}", msg.chat_id, msg.content);
        let _ = db.save_outbound_message(&msg.chat_id, &msg.content);
    }

    // Note: process starts are NOT handled in the child loop. Supporting them
    // would require a full event loop with ProcessSupervisor — a future enhancement.
    // spawn_agent is blocked at the Python level with a RuntimeError.
}
