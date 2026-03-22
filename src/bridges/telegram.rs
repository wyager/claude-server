use anyhow::{Context, Result};
use serde_json::{json, Value};
use tokio::sync::mpsc;

pub fn run(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return;
    }

    let (api_url, rest) = super::parse_api_url(args);
    let mut token = None;
    let mut peer = None;
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--token" => {
                token = rest.get(i + 1).cloned();
                i += 2;
            }
            "--peer" => {
                peer = rest.get(i + 1).cloned();
                i += 2;
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                print_help();
                std::process::exit(1);
            }
        }
    }

    let token = token.unwrap_or_else(|| {
        eprintln!("--token is required");
        print_help();
        std::process::exit(1);
    });
    let peer: i64 = peer
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| {
            eprintln!("--peer is required (numeric Telegram chat ID)");
            print_help();
            std::process::exit(1);
        });

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(api_url, token, peer)) {
        eprintln!("[telegram bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

fn print_help() {
    println!("Usage: claude-server bridge telegram --token TOKEN --peer CHAT_ID [OPTIONS]");
    println!();
    println!("Relay Telegram messages via the Bot API (long-polling, no webhook needed).");
    println!();
    println!("Options:");
    println!("  --token TOKEN      Bot token from @BotFather");
    println!("  --peer CHAT_ID     Numeric chat ID to relay with (DM the bot, then");
    println!("                     check https://api.telegram.org/bot<token>/getUpdates)");
    println!("  --api-url URL      Claude Server API URL (default: http://127.0.0.1:3000)");
    println!();
    println!("chat_id will be \"telegram:<peer>\".");
}

async fn run_async(api_url: String, token: String, peer: i64) -> Result<()> {
    let client = reqwest::Client::new();
    let base = format!("https://api.telegram.org/bot{}", token);

    // Validate token
    let me: Value = client
        .get(format!("{}/getMe", base))
        .send()
        .await?
        .json()
        .await
        .context("getMe failed — check your bot token")?;
    let username = me["result"]["username"].as_str().unwrap_or("?");
    eprintln!("[telegram bridge] bot @{} connected, relaying chat {}", username, peer);

    let chat_id = format!("telegram:{}", peer);
    let (tx, rx) = mpsc::unbounded_channel();

    // Inbound: long-poll getUpdates
    let poll_client = client.clone();
    let poll_base = base.clone();
    tokio::spawn(async move {
        let mut offset = 0i64;
        loop {
            let url = format!("{}/getUpdates?offset={}&timeout=30", poll_base, offset);
            let resp: Value = match poll_client
                .get(&url)
                .timeout(std::time::Duration::from_secs(35))
                .send()
                .await
                .and_then(|r| r.error_for_status())
            {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("[telegram bridge] parse error: {}", e);
                        continue;
                    }
                },
                Err(e) => {
                    eprintln!("[telegram bridge] getUpdates error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            for update in resp["result"].as_array().into_iter().flatten() {
                if let Some(id) = update["update_id"].as_i64() {
                    offset = id + 1;
                }
                let msg = &update["message"];
                if msg["chat"]["id"].as_i64() == Some(peer) {
                    if let Some(text) = msg["text"].as_str() {
                        if tx.send(text.to_string()).is_err() {
                            return;
                        }
                    }
                }
            }
        }
    });

    // Outbound: sendMessage
    let send_base = base.clone();
    super::relay_loop(&api_url, &chat_id, &format!("tg:{}", peer), rx, move |content| {
        let client = client.clone();
        let url = format!("{}/sendMessage", send_base);
        async move {
            let resp = client
                .post(&url)
                .json(&json!({"chat_id": peer, "text": content}))
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("sendMessage returned {}: {}", resp.status(), resp.text().await.unwrap_or_default());
            }
            Ok(())
        }
    })
    .await
}
