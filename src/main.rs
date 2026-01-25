mod api_client;
mod chat;
mod compaction;
mod config;
mod core_loop;
mod db;
mod http_server;
mod process;
mod python;
mod renderer;
mod types;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("chat") => chat::run_chat_server(&args[2..]),
        Some("--help") | Some("-h") => {
            println!("Usage: claude-server [OPTIONS] [COMMAND]");
            println!();
            println!("Commands:");
            println!("  (default)   Start the agent daemon");
            println!("  chat        Start the chat web UI");
            println!();
            println!("Options (daemon mode):");
            println!("  --dump-turns   Print the full context and agent response each turn");
            println!();
            println!("Environment variables:");
            println!("  ANTHROPIC_API_KEY             API key (required for daemon)");
            println!("  CLAUDE_SERVER_MODEL            Model name (default: claude-opus-4-5-20251101)");
            println!("  CLAUDE_SERVER_LISTEN           API listen address (default: 127.0.0.1:3000)");
            println!("  CLAUDE_SERVER_DB               SQLite path (default: claude-server.db)");
            println!("  CLAUDE_SERVER_SYSTEM_PROMPT     System prompt file (default: system_prompt.txt)");
            println!("  CLAUDE_SERVER_DEPLOYMENT_CONTEXT Deployment context file");
        }
        _ => {
            let dump_turns = args.iter().any(|a| a == "--dump-turns");
            let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
            if let Err(e) = rt.block_on(run_daemon(dump_turns)) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

async fn run_daemon(dump_turns: bool) -> Result<()> {
    let config = Arc::new(config::Config::from_env()?);

    println!("Claude Server starting...");
    println!("  Model: {}", config.model);
    println!("  Listen: {}", config.listen_addr);
    println!("  DB: {:?}", config.db_path);

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

    // Create API client
    let api_client = api_client::ApiClient::new(config.clone())?;
    println!("  API client ready");

    // Create process supervisor
    let process_supervisor = process::ProcessSupervisor::new(process_event_tx, database.clone());

    // Create core loop
    let mut core = core_loop::CoreLoop::new(
        state,
        config.clone(),
        database.clone(),
        api_client,
        process_supervisor,
        event_rx,
        deployment_context,
        dump_turns,
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

    // Start HTTP server
    let router = http_server::create_router(event_tx.clone(), database.clone());
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    println!("  HTTP server listening on {}", config.listen_addr);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("[http] Server error: {}", e);
        }
    });

    println!("Claude Server ready.\n");
    println!("Send messages with:");
    println!(
        "  curl -X POST http://{}/message -H 'Content-Type: application/json' \\",
        config.listen_addr
    );
    println!("    -d '{{\"user\":\"you@example.com\",\"content\":\"Hello Claude!\"}}'");
    println!();
    println!("Or start the chat UI with: claude-server chat");
    println!();

    // Run core loop (blocks until shutdown)
    core.run().await?;

    // Save final state
    database.save_state(&core.state)?;
    println!("State saved. Goodbye.");

    Ok(())
}
