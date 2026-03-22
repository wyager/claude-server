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
mod source_dump;
mod types;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, mpsc};

use http_server::BroadcastMsg;
use types::TokenAccumulator;

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
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Chat(a)) => chat::run_chat_server(a),
        Some(Command::Source(a)) => source_dump::run(a),
        Some(Command::Bridge { bridge }) => bridges::run(bridge),
        Some(Command::Feedback(a)) => feedback::run_client(a),
        Some(Command::FeedbackServer(a)) => feedback::run_server(a),
        None => {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            if let Err(e) = rt.block_on(run_daemon(cli.dump_turns, cli.dump_dir, !cli.daemon)) {
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
        Some(s) => {
            println!(
                "  Resumed state (queue: {}, history: {}, memory: {} keys, timers: {})",
                s.work_queue.len(),
                s.event_history.entries().len(),
                s.memory.len(),
                s.timer_manager.list().len()
            );
            s
        }
        None => {
            let s = types::HarnessState::new(config.context_window, config.max_tokens);
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
    let api_client = api_client::ApiClient::new(config.clone())?;
    println!("  API client ready");

    // Create process supervisor
    let event_url = format!("http://{}/event", config.listen_addr);
    let process_supervisor = process::ProcessSupervisor::new(process_event_tx, database.clone(), event_url, "root".to_string());

    // Create token accumulator
    let token_accumulator = Arc::new(Mutex::new(TokenAccumulator::default()));

    // Create agent registry
    let registry = Arc::new(types::AgentRegistry::new());

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

    // Graceful shutdown on Ctrl+C
    let event_tx_for_signal = event_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n[signal] Ctrl+C received, shutting down...");
        let _ = event_tx_for_signal.send(core_loop::HarnessEvent::Shutdown);
    });

    let local_chat_rx = local_chat.then(|| broadcast_tx.subscribe());

    // Start HTTP server
    let router = http_server::create_router(
        event_tx.clone(),
        database.clone(),
        broadcast_tx,
        token_accumulator.clone(),
        config.clone(),
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
        spawn_local_chat(event_tx.clone(), rx);
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
                })
                .is_err()
            {
                break;
            }
        }
        // stdin closed → graceful shutdown
        let _ = event_tx.send(core_loop::HarnessEvent::Shutdown);
    });
}

fn prompt() {
    use std::io::Write;
    print!("\x1b[1;32m> \x1b[0m");
    std::io::stdout().flush().ok();
}
