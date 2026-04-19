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
        attachments: Vec<String>,
        message_ref: Option<String>,
    },
    Process(ProcessEvent),
    ChildCompleted {
        child_name: String,
        result: HashMap<String, serde_json::Value>,
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
    /// Sent by a parent via kill_child(). Receiving agent exits at the next
    /// turn boundary with FinishReason::Killed.
    KillSignal,
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
        usage_log: Arc<Mutex<crate::types::UsageLog>>,
        registry: Arc<AgentRegistry>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        subscribers: Arc<crate::http_server::SubscriberRegistry>,
    ) -> Self {
        // Register root agent in the registry
        registry
            .register("root".to_string(), event_tx.clone())
            .expect("Failed to register root agent");

        let permissions = AgentPermissions {
            can_compact: true,
            max_turns: None,        // unlimited for parent
            child_depth_remaining: 1, // parent can spawn children, children can't spawn grandchildren
            agent_name: AgentName::Root,
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
            None,  // root agent has no role_prefix
            Some(broadcast_tx),
            dump_dir,
            dump_turns,
            Some(token_accumulator),
            usage_log,
            registry,
            shutdown,
            subscribers,
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
/// FNV-1a hash — fast, non-cryptographic, good enough to detect content drift
/// between consecutive turns. Collision risk is irrelevant here.
fn quick_hash(s: &str) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", h)
}

fn snip(s: &str, n: usize) -> String {
    use crate::renderer::trunc;
    if s.len() <= n * 2 { return s.replace('\n', "⏎") }
    // Tail slice needs the *start* index snapped forward to a char boundary.
    let mut tail_start = s.len() - n;
    while !s.is_char_boundary(tail_start) { tail_start += 1; }
    format!("{}…{}", trunc(s, n).replace('\n', "⏎"), s[tail_start..].replace('\n', "⏎"))
}

pub fn write_turn_dump(
    agent_name: &str,
    turn_number: u32,
    rendered: &crate::renderer::RenderedContext,
    usage: (u64, u64, u64, u64),  // (input, output, cache_write, cache_read)
    thinking: Option<&str>,
    code: &str,
    output: &str,
    is_error: bool,
    to_stdout: bool,
    dump_dir: Option<&std::path::Path>,
) {
    let context = &rendered.text;
    let attachments = &rendered.attachments;
    let sep = "=".repeat(80);
    let dash = "-".repeat(80);

    let mut dump = String::with_capacity(context.len() + code.len() + 512);

    // Cache-debug section: block structure + hashes. Diffing hashes across
    // consecutive turns pinpoints which block's content drifted.
    dump.push_str(&format!("{}\n", sep));
    dump.push_str(&format!("[{}] Turn {} — CACHE BLOCKS\n", agent_name, turn_number));
    dump.push_str(&format!("{}\n\n", sep));
    let (in_t, out_t, cw, cr) = usage;
    dump.push_str(&format!(
        "API usage: in={} out={} cache_write={} cache_read={}\n\n",
        in_t, out_t, cw, cr
    ));
    if rendered.prefix_text.is_empty() {
        dump.push_str("prefix_text: (empty — merged into seg1)\n");
    } else {
        dump.push_str(&format!(
            "prefix_text: {} chars  hash={}  {}\n",
            rendered.prefix_text.len(),
            quick_hash(&rendered.prefix_text),
            snip(&rendered.prefix_text, 40)
        ));
    }
    if rendered.prefix_attachments.is_empty() {
        dump.push_str("prefix_attachments: (none)\n");
    } else {
        for (i, a) in rendered.prefix_attachments.iter().enumerate() {
            let last = i == rendered.prefix_attachments.len() - 1;
            dump.push_str(&format!(
                "prefix_attach[{}]: {} {}\n",
                i, a.path.display(),
                if last { "  [+cache_control]" } else { "" }
            ));
        }
    }
    for (i, seg) in rendered.cached_segments.iter().enumerate() {
        dump.push_str(&format!(
            "cached_seg[{}]: {} chars  hash={}  [+cache_control]\n  {}\n",
            i, seg.len(), quick_hash(seg), snip(seg, 60)
        ));
    }
    if rendered.cached_segments.is_empty() {
        dump.push_str("cached_segments: (none — all in tail)\n");
    }
    let cached_len: usize = rendered.cached_segments.iter().map(String::len).sum();
    let tail = &rendered.text[rendered.prefix_text.len() + cached_len..];
    dump.push_str(&format!(
        "tail: {} chars  hash={}\n  {}\n",
        tail.len(), quick_hash(tail), snip(tail, 60)
    ));
    dump.push_str(&format!("\nAPI block order: [system+cc]{}{}{}{}[tail:{}]\n",
        if rendered.prefix_text.is_empty() { "" } else { " [prefix_text] " },
        (0..rendered.prefix_attachments.len()).map(|i|
            if i == rendered.prefix_attachments.len()-1 { "[img+cc]" } else { "[img]" }
        ).collect::<String>(),
        if rendered.prefix_attachments.is_empty() { "" } else { " " },
        rendered.cached_segments.iter().map(|s| format!("[seg:{}+cc]", s.len())).collect::<String>(),
        tail.len()
    ));
    dump.push_str(&format!("{}\n\n", dash));

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

    if !attachments.is_empty() {
        dump.push_str(&format!("\n{}\n", sep));
        dump.push_str(&format!(
            "[{}] Turn {} — ATTACHMENTS ({} file{})\n",
            agent_name,
            turn_number,
            attachments.len(),
            if attachments.len() == 1 { "" } else { "s" }
        ));
        dump.push_str(&format!("{}\n\n", sep));
        for att in attachments {
            let size = std::fs::metadata(&att.path).map(|m| m.len()).unwrap_or(0);
            dump.push_str(&format!("  {} ({} bytes)\n", att.path.display(), size));
        }
        dump.push_str(&format!("{}\n", dash));
    }

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
