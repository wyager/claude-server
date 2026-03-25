use anyhow::{Context, Result};
use clap::Args;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const INTENTS: u32 = (1 << 9) | (1 << 15); // GUILD_MESSAGES | MESSAGE_CONTENT

#[derive(Args)]
pub struct DiscordArgs {
    /// Bot token (enable MESSAGE CONTENT intent in the developer portal)
    #[arg(long)]
    pub token: String,
    /// Channel ID (enable Developer Mode, right-click channel)
    #[arg(long)]
    pub channel: String,
    #[command(flatten)]
    pub api: super::ApiUrl,
}

pub fn run(args: DiscordArgs) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(args.api.api_url, args.token, args.channel)) {
        eprintln!("[discord bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(api_url: String, token: String, channel: String) -> Result<()> {
    let http = reqwest::Client::new();
    let chat_id = format!("discord:{}", channel);
    let (tx, rx) = mpsc::unbounded_channel::<super::Inbound>();

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
    super::relay_loop(&api_url, &chat_id, &format!("discord:{}", channel), rx, move |out: super::Outbound| {
        let (content, attachments) = (out.content, out.attachments);
        let http = http.clone();
        let url = format!("https://discord.com/api/v10/channels/{}/messages", out_channel);
        let token = out_token.clone();
        async move {
            let req = http.post(&url).header("Authorization", format!("Bot {}", token));
            let resp = if attachments.is_empty() {
                req.json(&json!({"content": content})).send().await?
            } else {
                let mut form = reqwest::multipart::Form::new()
                    .text("payload_json", json!({"content": content}).to_string());
                for (i, path) in attachments.iter().enumerate() {
                    let bytes = tokio::fs::read(path).await.with_context(|| format!("reading {}", path))?;
                    let name = std::path::Path::new(path).file_name()
                        .map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| "file".into());
                    form = form.part(format!("files[{}]", i),
                        reqwest::multipart::Part::bytes(bytes).file_name(name));
                }
                req.multipart(form).send().await?
            };
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
    tx: &mpsc::UnboundedSender<super::Inbound>,
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
                                        tx.send(text.to_string().into()).ok();
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
