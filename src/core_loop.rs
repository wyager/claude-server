use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::sync::{broadcast, mpsc};

use crate::agent_loop::AgentLoop;
use crate::api_client::ApiClient;
use crate::config::Config;
use crate::db::Database;
use crate::http_server::BroadcastMsg;
use crate::process::{ProcessEvent, ProcessSupervisor};
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
        child_name: String,
        result_memory: HashMap<String, serde_json::Value>,
        turns_used: u32,
        success: bool,
        summary: String,
        priority: u8,
        child_input_tokens: u64,
        child_output_tokens: u64,
        child_cache_creation_tokens: u64,
        child_cache_read_tokens: u64,
    },
    AgentMessage {
        from: String,
        content: String,
        priority: u8,
    },
    ExternalEvent {
        source: String,
        event_type: String,
        data: serde_json::Value,
        priority: u8,
    },
    Shutdown,
}

pub struct CoreLoop {
    agent_loop: AgentLoop,
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
        token_accumulator: Arc<Mutex<TokenAccumulator>>,
        registry: Arc<AgentRegistry>,
    ) -> Self {
        // Register root agent in the registry
        registry
            .register("root".to_string(), vec!["root".to_string()], event_tx.clone())
            .expect("Failed to register root agent");

        let permissions = AgentPermissions {
            can_compact: true,
            max_turns: None,        // unlimited for parent
            child_depth_remaining: 1, // parent can spawn children, children can't spawn grandchildren
            agent_name: "root".to_string(),
            lineage: vec!["root".to_string()],
        };

        let agent_loop = AgentLoop::new(
            "root".to_string(),
            permissions,
            state,
            config,
            db,
            api_client,
            process_supervisor,
            event_rx,
            event_tx,
            deployment_context,
            Some(broadcast_tx),
            dump_dir,
            dump_turns,
            Some(token_accumulator),
            registry,
        );

        Self { agent_loop }
    }

    pub async fn run(&mut self) -> Result<()> {
        let _reason = self.agent_loop.run().await;
        Ok(())
    }

    pub fn state(&self) -> &HarnessState {
        &self.agent_loop.state
    }
}

/// Write a turn dump to stdout and/or a file.
pub fn write_turn_dump(
    agent_name: &str,
    turn_number: u32,
    context: &str,
    thinking: Option<&str>,
    code: &str,
    output: &str,
    is_error: bool,
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

    if !output.is_empty() {
        dump.push_str(&format!("\n{}\n", sep));
        dump.push_str(&format!(
            "[{}] Turn {} — EXECUTION {}\n",
            agent_name,
            turn_number,
            if is_error { "ERROR" } else { "OUTPUT" }
        ));
        dump.push_str(&format!("{}\n\n", sep));
        dump.push_str(output);
        if !output.ends_with('\n') {
            dump.push('\n');
        }
        dump.push_str(&format!("{}\n", dash));
    }

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
