use std::io::Write;

use anyhow::{Context, Result};
use futures::StreamExt;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use super::ApiUrl;

const CHAT_ID: &str = "local";

pub fn run(args: ApiUrl) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(args.api_url)) {
        eprintln!("[stdio bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

fn prompt() {
    print!("\x1b[1;32m> \x1b[0m");
    std::io::stdout().flush().ok();
}

async fn run_async(api_url: String) -> Result<()> {
    let client = reqwest::Client::new();
    let msg_url = format!("{}/message", api_url);
    let sse_url = format!("{}/messages/{}/stream", api_url, CHAT_ID);

    // Inbound: stdin → POST /message
    let post_client = client.clone();
    tokio::spawn(async move {
        let stdin = BufReader::new(tokio::io::stdin());
        let mut lines = stdin.lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if line.is_empty() {
                prompt();
                continue;
            }
            let body = serde_json::json!({
                "chat_id": CHAT_ID,
                "user": "local",
                "content": line,
            });
            if let Err(e) = post_client.post(&msg_url).json(&body).send().await {
                eprintln!("[stdio bridge] POST failed: {}", e);
            }
        }
        std::process::exit(0);
    });

    // Outbound: SSE → stdout with cyan-box rendering + prompt on idle
    let resp = client
        .get(&sse_url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .with_context(|| format!("connecting to {}", sse_url))?;
    if !resp.status().is_success() {
        anyhow::bail!("SSE returned {}: is the daemon running at {}?", resp.status(), api_url);
    }

    eprintln!("\x1b[2mConnected to {}\x1b[0m", api_url);
    prompt();

    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let mut event = String::new();
    let mut data = String::new();

    while let Some(chunk) = stream.next().await {
        buf.push_str(&String::from_utf8_lossy(&chunk?));
        while let Some(pos) = buf.find("\n\n") {
            let block: String = buf.drain(..pos + 2).collect();
            for line in block.lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    event = rest.trim().to_string();
                } else if let Some(rest) = line.strip_prefix("data:") {
                    data = rest.trim().to_string();
                }
            }
            if let Ok(v) = serde_json::from_str::<Value>(&data) {
                match event.as_str() {
                    "message" => {
                        if let Some(content) = v["content"].as_str() {
                            println!("\n\x1b[1;36m── claude ──────────────────────\x1b[0m");
                            println!("{}", content);
                            println!("\x1b[1;36m────────────────────────────────\x1b[0m");
                        }
                    }
                    "status" => {
                        if v["status"].as_str() == Some("idle") {
                            prompt();
                        }
                    }
                    _ => {}
                }
            }
            event.clear();
            data.clear();
        }
    }

    Ok(())
}
