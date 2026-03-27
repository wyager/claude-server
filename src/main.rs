mod agent_loop;
mod api_client;
mod bridges;
mod chat;
mod compaction;
mod config;
mod core_loop;
mod db;
mod feedback;
mod http_server;
mod process;
mod python;
mod renderer;
mod docs;
mod source_dump;
mod types;
mod watchers;
mod webhook_proxy;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, mpsc};

use http_server::BroadcastMsg;
use types::TokenAccumulator;

/// Agent-facing changelog, keyed by the version that introduced each change.
/// On upgrade, the agent sees all entries with version > its stored version.
/// Keep entries terse and action-oriented — what the agent should DO
/// differently, not implementation details. Safe to prune very old entries
/// once no deployment could possibly still be on that version.
const AGENT_CHANGELOG: &[(&str, &str)] = &[
    ("0.2.0", "\
- `memory.mark_sensitive(key)` — redacts that key's value from feedback API
  traces. Mark credentials, tokens, seed phrases NOW so future
  `feedback --with-api-trace` doesn't leak them.
- `memory.pin(key, content)` renders in FULL (markdown, no truncation).
  Local memory truncates at ~120 chars in <agent_state>. Move long
  architecture docs / operational recipes to pin.
- External event routing: POST /event accepts `\"agent\":\"<name>\"` to route
  to a specific agent. `$CLAUDE_SERVER_AGENT_NAME` env var (auto-injected
  into every spawned process) holds the spawning agent's name. If a child
  spawns a watcher, the watcher can route events straight to the child —
  root never wakes. Include `\"agent\":\"$CLAUDE_SERVER_AGENT_NAME\"` in
  watcher POST bodies.
- `send_message(chat_id, content, react_to=message_ref)` — react with an
  emoji instead of sending a message. UserMessage items carry `message_ref`."),
    ("0.2.1", "\
- `watch mqtt --payload=structured` — parse {attachments:[{name,base64}],data:{}}
  from MQTT, decode to --attach-dir, send file paths. No more Python receiver
  needed for camera pipelines. Also --payload=raw for unparsed binary."),
];

/// Parse "X.Y.Z" into a comparable tuple. Unparseable → (0,0,0) so it sorts
/// first (agent sees everything, which is the safe default for "unknown").
fn parse_ver(v: &str) -> (u32, u32, u32) {
    let mut it = v.split('.').map(|p| p.parse().unwrap_or(0));
    (it.next().unwrap_or(0), it.next().unwrap_or(0), it.next().unwrap_or(0))
}

/// Collect changelog entries for versions strictly after `prev`, up to and
/// including `current`. Returns None if nothing applies.
fn changelog_since(prev: &str, current: &str) -> Option<String> {
    let prev_v = parse_ver(prev);
    let cur_v = parse_ver(current);
    let entries: Vec<_> = AGENT_CHANGELOG.iter()
        .filter(|(v, _)| { let pv = parse_ver(v); pv > prev_v && pv <= cur_v })
        .collect();
    if entries.is_empty() { return None; }
    let mut out = format!("Harness upgraded from {} to {}. Changes you should act on:\n\n", prev, current);
    for (v, text) in entries {
        out.push_str(&format!("## {}\n{}\n\n", v, text));
    }
    Some(out)
}

const ENV_HELP: &str = "\
Environment variables:
  ANTHROPIC_API_KEY                 API key (required for daemon)
  CLAUDE_SERVER_MODEL               Model name (default: claude-opus-4-6)
  CLAUDE_SERVER_LISTEN              API listen address (default: 127.0.0.1:3000)
  CLAUDE_SERVER_DB                  SQLite path (default: claude-server.db)
  CLAUDE_SERVER_SYSTEM_PROMPT       System prompt file (default: system_prompt.txt)
  CLAUDE_SERVER_DEPLOYMENT_CONTEXT  Deployment context file
  CLAUDE_SERVER_FEEDBACK_URL        Feedback server URL (default: https://feedback.yager.io/feedback)";

#[derive(Parser)]
#[command(
    name = "claude-server",
    about = "Long-running persistent Claude agent harness",
    after_help = ENV_HELP,
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Run headless (no stdin/stdout chat)
    #[arg(long)]
    daemon: bool,

    /// Print the full context and agent response each turn
    #[arg(long)]
    dump_turns: bool,

    /// Write turn dumps to files in <path> (parent + children)
    #[arg(long, value_name = "PATH")]
    dump_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the chat web UI
    Chat(chat::ChatArgs),
    /// Dump embedded harness source tarball
    Source(source_dump::SourceArgs),
    /// Start a messaging bridge
    Bridge {
        #[command(subcommand)]
        bridge: bridges::BridgeCmd,
    },
    /// Send a harness bug report to the feedback server
    Feedback(feedback::FeedbackArgs),
    /// Run the feedback collection server
    FeedbackServer(feedback::ServerArgs),
    /// Start an event watcher (fs, mqtt, imap)
    Watch {
        #[command(subcommand)]
        watch: watchers::WatchCmd,
    },
    /// Authenticated public webhook ingress (GitHub, Slack, generic)
    WebhookProxy(webhook_proxy::WebhookArgs),
    /// Print bundled deployment recipes
    #[command(trailing_var_arg = true)]
    Docs { args: Vec<String> },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Chat(a)) => chat::run_chat_server(a),
        Some(Command::Source(a)) => source_dump::run(a),
        Some(Command::Bridge { bridge }) => bridges::run(bridge),
        Some(Command::Feedback(a)) => feedback::run_client(a),
        Some(Command::FeedbackServer(a)) => feedback::run_server(a),
        Some(Command::Watch { watch }) => watchers::run(watch),
        Some(Command::WebhookProxy(a)) => webhook_proxy::run(a),
        Some(Command::Docs { args }) => docs::run(&args),
        None => {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            let result = rt.block_on(run_daemon(cli.dump_turns, cli.dump_dir, !cli.daemon));
            // tokio::io::stdin() uses a blocking thread that can't be cancelled,
            // so the runtime's Drop would hang waiting for it. shutdown_background
            // abandons it instead.
            rt.shutdown_background();
            if let Err(e) = result {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

async fn run_daemon(dump_turns: bool, dump_dir: Option<PathBuf>, local_chat: bool) -> Result<()> {
    let config = Arc::new(config::Config::from_env()?);

    println!("Claude Server starting...");
    println!("  Model: {}", config.model);
    println!("  Listen: {}", config.listen_addr);
    println!("  DB: {:?}", config.db_path);
    println!("  Compact at: {} tokens, target: {} tokens", config.compact_at, config.compact_target);
    println!("  Python timeout: {}s", config.python_timeout_secs);
    if let Some(ref dir) = dump_dir {
        std::fs::create_dir_all(dir)?;
        println!("  Dump dir: {:?}", dir);
    }

    // Initialize Python
    python::initialize_python();
    println!("  Python initialized");

    // Open database
    let database = Arc::new(db::Database::open(&config.db_path)?);
    println!("  Database opened");

    // Load deployment context
    let deployment_context = config.load_deployment_context()?;
    if !deployment_context.is_empty() {
        println!("  Deployment context: {} chars", deployment_context.len());
    }

    // Load or create state
    let state = match database.load_state()? {
        Some(mut s) => {
            let stale_procs = s.process_manager.processes().len();
            println!(
                "  Resumed state (queue: {}, history: {}, memory: {} keys, timers: {}, dropping {} stale process entries)",
                s.work_queue.len(),
                s.event_history.entries().len(),
                s.memory.len(),
                s.timer_manager.list().len(),
                stale_procs
            );
            // Wipe process tracker: child processes from the previous harness
            // instance are dead (SIGKILL'd on parent exit), but their tracker
            // entries would otherwise deserialize as "running" ghosts. Agents
            // re-spawn everything from memory on AgentStartup anyway.
            s.process_manager = types::ProcessManager::new();

            // Version check: if the harness upgraded, attach the agent-facing
            // changelog so the agent learns new capabilities on its first turn.
            // Per-version entries are range-selected so a 0.2→0.5 jump shows
            // exactly the 0.3, 0.4, 0.5 entries — nothing missed, nothing extra.
            let current = env!("CARGO_PKG_VERSION");
            let prev = s.last_harness_version.take().unwrap_or_else(|| "unknown".into());
            let changelog = changelog_since(&prev, current);
            if changelog.is_some() {
                println!("  Harness upgraded: {} → {} (changelog will be shown to agent)", prev, current);
            }
            s.last_harness_version = Some(current.to_string());

            // Inject startup item so the agent gets a turn to reconnect any
            // bridges/processes it tracked in memory before the restart.
            let id = s.id_generator.next();
            s.work_queue.push(types::WorkItem {
                id,
                priority: 9,
                time: chrono::Utc::now(),
                item_type: types::WorkItemType::AgentStartup { changelog },
                attachments: Vec::new(),
            });
            s
        }
        None => {
            let mut s = types::HarnessState::new(config.context_window, config.max_tokens);
            s.last_harness_version = Some(env!("CARGO_PKG_VERSION").to_string());
            database.save_state(&s)?;
            println!("  Created fresh state");
            s
        }
    };

    // Create event channels
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (process_event_tx, mut process_event_rx) = mpsc::unbounded_channel();
    let (broadcast_tx, _) = broadcast::channel::<http_server::BroadcastMsg>(256);

    // Create API client
    let trace_size: usize = std::env::var("CLAUDE_SERVER_API_TRACE_SIZE")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(10);
    let api_trace = Arc::new(Mutex::new(api_client::ApiTrace::new(trace_size)));
    let api_client = api_client::ApiClient::new(config.clone())?
        .with_trace(api_trace.clone());
    println!("  API client ready");

    // Create process supervisor
    let event_url = format!("http://{}/event", config.listen_addr);
    let process_supervisor = process::ProcessSupervisor::new(process_event_tx, database.clone(), event_url, "root".to_string());

    // Create token accumulator
    let token_accumulator = Arc::new(Mutex::new(TokenAccumulator::default()));

    // Create agent registry (shared with HTTP server for /event routing)
    let registry = Arc::new(types::AgentRegistry::new());
    let registry_for_http = registry.clone();

    // Shutdown signal — watch channel so every select! can race against it.
    // Setting it cancels in-flight turns (API retries, sleeps) via future drop.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Create core loop
    let mut core = core_loop::CoreLoop::new(
        state,
        config.clone(),
        database.clone(),
        api_client,
        process_supervisor,
        event_rx,
        event_tx.clone(),
        deployment_context,
        dump_turns,
        dump_dir,
        broadcast_tx.clone(),
        token_accumulator.clone(),
        registry,
        shutdown_rx,
    );

    // Forward process events to the main event channel
    let event_tx_for_process = event_tx.clone();
    tokio::spawn(async move {
        while let Some(pe) = process_event_rx.recv().await {
            if event_tx_for_process
                .send(core_loop::HarnessEvent::Process(pe))
                .is_err()
            {
                break;
            }
        }
    });

    // Graceful shutdown on Ctrl+C. Second Ctrl+C force-exits.
    let shutdown_tx_sig = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n[signal] Ctrl+C received, shutting down gracefully (press again to force-exit)...");
        let _ = shutdown_tx_sig.send(true);
        tokio::signal::ctrl_c().await.ok();
        println!("\n[signal] Force exit.");
        std::process::exit(130);
    });

    let local_chat_rx = local_chat.then(|| broadcast_tx.subscribe());

    // Start HTTP server
    let router = http_server::create_router(
        event_tx.clone(),
        database.clone(),
        broadcast_tx,
        token_accumulator.clone(),
        config.clone(),
        shutdown_tx.clone(),
        registry_for_http,
        api_trace,
    );
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    println!("  HTTP server listening on {}", config.listen_addr);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("[http] Server error: {}", e);
        }
    });

    println!("Claude Server ready.\n");

    if let Some(rx) = local_chat_rx {
        println!("Invoke with `--daemon` to run headless without this chat interface.");
        println!("Attach another CLI chat: claude-server bridge stdio --api-url http://{}", config.listen_addr);
        println!("HTTP API also available at http://{}", config.listen_addr);
        println!();
        spawn_local_chat(event_tx.clone(), rx, shutdown_tx.clone());
    } else {
        println!("CLI chat:  claude-server bridge stdio --api-url http://{}", config.listen_addr);
        println!("Web UI:    claude-server chat --api-url http://{}", config.listen_addr);
        println!();
    }

    // Run core loop (blocks until shutdown)
    core.run().await?;

    // Save final state
    database.save_state(core.state())?;
    println!("State saved. Goodbye.");

    Ok(())
}

const LOCAL_CHAT_ID: &str = "local";

fn spawn_local_chat(
    event_tx: mpsc::UnboundedSender<core_loop::HarnessEvent>,
    mut broadcast_rx: broadcast::Receiver<BroadcastMsg>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
) {
    // Outbound: agent → stdout. Prompt is printed on the `idle` status broadcast
    // so it always lands after the agent loop's own "Idle, waiting..." log line.
    tokio::spawn(async move {
        loop {
            match broadcast_rx.recv().await {
                Ok(BroadcastMsg::Message { chat_id, content, .. }) if chat_id == LOCAL_CHAT_ID => {
                    println!("\n\x1b[1;36m── claude ──────────────────────\x1b[0m");
                    println!("{}", content);
                    println!("\x1b[1;36m────────────────────────────────\x1b[0m");
                }
                Ok(BroadcastMsg::Status { status }) if status == "idle" => {
                    prompt();
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Inbound: stdin → agent
    tokio::spawn(async move {
        let stdin = BufReader::new(tokio::io::stdin());
        let mut lines = stdin.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if line.is_empty() {
                prompt();
                continue;
            }
            if event_tx
                .send(core_loop::HarnessEvent::UserMessage {
                    chat_id: LOCAL_CHAT_ID.to_string(),
                    user: "local".to_string(),
                    content: line.to_string(),
                    attachments: Vec::new(),
                    message_ref: None,
                })
                .is_err()
            {
                break;
            }
        }
        // stdin closed → graceful shutdown
        let _ = shutdown_tx.send(true);
    });
}

fn prompt() {
    use std::io::Write;
    print!("\x1b[1;32m> \x1b[0m");
    std::io::stdout().flush().ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ver() {
        assert_eq!(parse_ver("0.2.1"), (0, 2, 1));
        assert_eq!(parse_ver("1.10.0"), (1, 10, 0));
        assert!(parse_ver("0.10.0") > parse_ver("0.2.0")); // numeric, not lexical
        assert_eq!(parse_ver("unknown"), (0, 0, 0)); // unparseable → sorts first
        assert_eq!(parse_ver(""), (0, 0, 0));
    }

    #[test]
    fn test_changelog_since() {
        // Jump spanning both entries
        let c = changelog_since("0.1.0", "0.2.1").unwrap();
        assert!(c.contains("## 0.2.0"));
        assert!(c.contains("## 0.2.1"));
        assert!(c.contains("mark_sensitive"));
        assert!(c.contains("watch mqtt"));

        // Single-step upgrade
        let c = changelog_since("0.2.0", "0.2.1").unwrap();
        assert!(!c.contains("## 0.2.0"));
        assert!(c.contains("## 0.2.1"));

        // Same version → no changelog
        assert!(changelog_since("0.2.1", "0.2.1").is_none());

        // Unknown prev → shows everything (safe default for old DBs)
        let c = changelog_since("unknown", "0.2.1").unwrap();
        assert!(c.contains("## 0.2.0"));
        assert!(c.contains("## 0.2.1"));
    }
}
