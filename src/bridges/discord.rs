use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const INTENTS: u32 = (1 << 9) | (1 << 15); // GUILD_MESSAGES | MESSAGE_CONTENT

pub fn run(args: &[String]) {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return;
    }

    let (api_url, rest) = super::parse_api_url(args);
    let mut token = None;
    let mut channel = None;
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--token" => {
                token = rest.get(i + 1).cloned();
                i += 2;
            }
            "--channel" => {
                channel = rest.get(i + 1).cloned();
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
        std::process::exit(1);
    });
    let channel = channel.unwrap_or_else(|| {
        eprintln!("--channel is required");
        std::process::exit(1);
    });

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(api_url, token, channel)) {
        eprintln!("[discord bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

fn print_help() {
    println!("Usage: claude-server bridge discord --token TOKEN --channel ID [OPTIONS]");
    println!();
    println!("Relay Discord messages via the Gateway websocket.");
    println!("Create a bot at https://discord.com/developers/applications, enable the");
    println!("MESSAGE CONTENT intent, and invite it to your server.");
    println!();
    println!("Options:");
    println!("  --token TOKEN      Bot token");
    println!("  --channel ID       Channel ID (enable Developer Mode, right-click channel)");
    println!("  --api-url URL      Claude Server API URL (default: http://127.0.0.1:3000)");
    println!();
    println!("chat_id will be \"discord:<channel>\".");
}

async fn run_async(api_url: String, token: String, channel: String) -> Result<()> {
    let http = reqwest::Client::new();
    let chat_id = format!("discord:{}", channel);
    let (tx, rx) = mpsc::unbounded_channel();

    // Inbound: Gateway with reconnect loop
    let gw_token = token.clone();
    let gw_channel = channel.clone();
    let gw_http = http.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = gateway_loop(&gw_http, &gw_token, &gw_channel, &tx).await {
                eprintln!("[discord bridge] gateway error: {:#}, reconnecting in 5s", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            if tx.is_closed() {
                return;
            }
        }
    });

    // Outbound: POST to channel
    let out_channel = channel.clone();
    let out_token = token.clone();
    super::relay_loop(&api_url, &chat_id, &format!("discord:{}", channel), rx, move |content| {
        let http = http.clone();
        let url = format!("https://discord.com/api/v10/channels/{}/messages", out_channel);
        let token = out_token.clone();
        async move {
            let resp = http
                .post(&url)
                .header("Authorization", format!("Bot {}", token))
                .json(&json!({"content": content}))
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("send returned {}: {}", resp.status(), resp.text().await.unwrap_or_default());
            }
            Ok(())
        }
    })
    .await
}

async fn gateway_loop(
    http: &reqwest::Client,
    token: &str,
    channel: &str,
    tx: &mpsc::UnboundedSender<String>,
) -> Result<()> {
    let gw: Value = http
        .get("https://discord.com/api/v10/gateway")
        .send()
        .await?
        .json()
        .await?;
    let wss = format!("{}/?v=10&encoding=json", gw["url"].as_str().context("no gateway url")?);
    eprintln!("[discord bridge] connecting gateway");

    let (ws, _) = connect_async(&wss).await.context("gateway connect")?;
    let (mut write, mut read) = ws.split();
    let mut last_seq: Option<u64> = None;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(3600)); // reset on Hello
    heartbeat.tick().await; // consume immediate first tick

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let payload = json!({"op": 1, "d": last_seq}).to_string();
                write.send(Message::Text(payload.into())).await?;
            }
            msg = read.next() => {
                let text = match msg.context("gateway stream ended")?? {
                    Message::Text(t) => t,
                    Message::Ping(p) => { write.send(Message::Pong(p)).await?; continue; }
                    Message::Close(_) => return Ok(()),
                    _ => continue,
                };
                let v: Value = serde_json::from_str(&text)?;
                if let Some(s) = v["s"].as_u64() { last_seq = Some(s); }

                match v["op"].as_u64() {
                    Some(10) => { // Hello
                        let ms = v["d"]["heartbeat_interval"].as_u64().unwrap_or(41250);
                        heartbeat = tokio::time::interval(Duration::from_millis(ms));
                        heartbeat.tick().await;
                        let ident = json!({
                            "op": 2,
                            "d": {
                                "token": token,
                                "intents": INTENTS,
                                "properties": {"os": "linux", "browser": "claude-server", "device": "claude-server"}
                            }
                        }).to_string();
                        write.send(Message::Text(ident.into())).await?;
                    }
                    Some(0) => { // Dispatch
                        if v["t"].as_str() == Some("MESSAGE_CREATE") {
                            let d = &v["d"];
                            if d["channel_id"].as_str() == Some(channel)
                                && d["author"]["bot"].as_bool() != Some(true)
                            {
                                if let Some(text) = d["content"].as_str() {
                                    if !text.is_empty() {
                                        tx.send(text.to_string()).ok();
                                    }
                                }
                            }
                        } else if v["t"].as_str() == Some("READY") {
                            eprintln!("[discord bridge] gateway ready");
                        }
                    }
                    Some(7) | Some(9) => return Ok(()), // Reconnect / Invalid Session
                    Some(11) => {} // Heartbeat ACK
                    _ => {}
                }
            }
        }
    }
}
