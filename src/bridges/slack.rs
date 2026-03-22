use anyhow::{Context, Result};
use clap::Args;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Args)]
pub struct SlackArgs {
    /// App-level token (xapp-..., needs connections:write scope)
    #[arg(long)]
    pub app_token: String,
    /// Bot user token (xoxb-..., needs chat:write + channels:history)
    #[arg(long)]
    pub bot_token: String,
    /// Channel ID (e.g. C0123456789)
    #[arg(long)]
    pub channel: String,
    #[command(flatten)]
    pub api: super::ApiUrl,
}

pub fn run(args: SlackArgs) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(args.api.api_url, args.app_token, args.bot_token, args.channel)) {
        eprintln!("[slack bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(api_url: String, app_token: String, bot_token: String, channel: String) -> Result<()> {
    let http = reqwest::Client::new();
    let chat_id = format!("slack:{}", channel);
    let (tx, rx) = mpsc::unbounded_channel();

    // Inbound: Socket Mode websocket with reconnect loop
    let inbound_http = http.clone();
    let inbound_channel = channel.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = socket_mode_loop(&inbound_http, &app_token, &inbound_channel, &tx).await {
                eprintln!("[slack bridge] socket mode error: {:#}, reconnecting in 5s", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            if tx.is_closed() {
                return;
            }
        }
    });

    // Outbound: chat.postMessage
    let out_channel = channel.clone();
    super::relay_loop(&api_url, &chat_id, &format!("slack:{}", channel), rx, move |content| {
        let http = http.clone();
        let bot_token = bot_token.clone();
        let channel = out_channel.clone();
        async move {
            let resp: Value = http
                .post("https://slack.com/api/chat.postMessage")
                .bearer_auth(&bot_token)
                .json(&json!({"channel": channel, "text": content}))
                .send()
                .await?
                .json()
                .await?;
            if resp["ok"].as_bool() != Some(true) {
                anyhow::bail!("chat.postMessage failed: {}", resp["error"].as_str().unwrap_or("?"));
            }
            Ok(())
        }
    })
    .await
}

async fn socket_mode_loop(
    http: &reqwest::Client,
    app_token: &str,
    channel: &str,
    tx: &mpsc::UnboundedSender<String>,
) -> Result<()> {
    // Get WSS URL
    let open: Value = http
        .post("https://slack.com/api/apps.connections.open")
        .bearer_auth(app_token)
        .send()
        .await?
        .json()
        .await?;
    if open["ok"].as_bool() != Some(true) {
        anyhow::bail!("apps.connections.open failed: {}", open["error"].as_str().unwrap_or("?"));
    }
    let wss_url = open["url"].as_str().context("no WSS url in response")?;
    eprintln!("[slack bridge] connecting socket mode");

    let (ws, _) = connect_async(wss_url).await.context("websocket connect")?;
    let (mut write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        let text = match msg? {
            Message::Text(t) => t,
            Message::Ping(p) => {
                write.send(Message::Pong(p)).await?;
                continue;
            }
            Message::Close(_) => return Ok(()),
            _ => continue,
        };
        let v: Value = serde_json::from_str(&text)?;

        match v["type"].as_str() {
            Some("hello") => {
                eprintln!("[slack bridge] socket mode connected");
            }
            Some("disconnect") => {
                eprintln!("[slack bridge] server requested disconnect, will reconnect");
                return Ok(());
            }
            Some("events_api") => {
                // ACK immediately
                if let Some(envelope_id) = v["envelope_id"].as_str() {
                    let ack = json!({"envelope_id": envelope_id}).to_string();
                    write.send(Message::Text(ack.into())).await?;
                }
                let event = &v["payload"]["event"];
                if event["type"].as_str() == Some("message")
                    && event["channel"].as_str() == Some(channel)
                    && event["bot_id"].is_null()
                    && event["subtype"].is_null()
                {
                    if let Some(text) = event["text"].as_str() {
                        tx.send(text.to_string()).ok();
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
