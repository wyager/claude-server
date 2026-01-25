mod config;
mod db;
mod python;
mod renderer;
mod types;

use anyhow::Result;

fn main() -> Result<()> {
    let config = config::Config::from_env()?;

    println!("Claude Server starting...");
    println!("  Model: {}", config.model);
    println!("  Listen: {}", config.listen_addr);
    println!("  DB: {:?}", config.db_path);

    // Load system prompt
    let system_prompt = config.load_system_prompt()?;
    println!("  System prompt: {} chars", system_prompt.len());

    // Load deployment context
    let deployment_context = config.load_deployment_context()?;
    if !deployment_context.is_empty() {
        println!("  Deployment context: {} chars", deployment_context.len());
    }

    // Open database
    let database = db::Database::open(&config.db_path)?;
    println!("  Database opened");

    // Load or create state
    let state = match database.load_state()? {
        Some(s) => {
            println!("  Resumed existing state (queue: {} items, history: {} entries)",
                s.work_queue.len(), s.event_history.entries().len());
            s
        }
        None => {
            let s = types::HarnessState::new(config.context_window, config.max_tokens);
            database.save_state(&s)?;
            println!("  Created fresh state");
            s
        }
    };

    // Test rendering
    let rendered = renderer::render_context(
        &state,
        &deployment_context,
        None,
        &config.render_config,
    );
    println!("  Context renders to {} chars", rendered.text.len());

    println!("Foundation + persistence + rendering verified.");
    println!("Full runtime not yet implemented.");

    Ok(())
}
