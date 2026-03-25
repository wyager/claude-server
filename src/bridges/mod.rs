mod discord;
mod email;
mod signal;
mod slack;
mod stdio;
mod telegram;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use futures::StreamExt;
use serde_json::json;
use tokio::sync::mpsc;

fn default_api_url() -> String {
    std::env::var("CLAUDE_SERVER_BRIDGE_API")
        .unwrap_or_else(|_| "http://127.0.0.1:3000".into())
}

#[derive(Args, Clone)]
pub struct ApiUrl {
    /// Claude Server API URL (env: CLAUDE_SERVER_BRIDGE_API)
    #[arg(long, default_value_t = default_api_url())]
    pub api_url: String,
}

#[derive(Subcommand)]
pub enum BridgeCmd {
    /// CLI chat over HTTP (connect to a headless daemon)
    Stdio(ApiUrl),
    /// Relay via signal-cli (requires signal-cli installed + linked)
    Signal(signal::SignalArgs),
    /// Relay via Telegram Bot API (long-polling, no webhook)
    Telegram(telegram::TelegramArgs),
    /// Relay via Slack Socket Mode (no public callback URL)
    Slack(slack::SlackArgs),
    /// Relay via Discord Gateway websocket
    Discord(discord::DiscordArgs),
    /// Relay via IMAP IDLE (receive) + SMTP (send)
    Email(email::EmailArgs),
}

pub fn run(cmd: BridgeCmd) {
    match cmd {
        BridgeCmd::Stdio(a) => stdio::run(a),
        BridgeCmd::Signal(a) => signal::run(a),
        BridgeCmd::Telegram(a) => telegram::run(a),
        BridgeCmd::Slack(a) => slack::run(a),
        BridgeCmd::Discord(a) => discord::run(a),
        BridgeCmd::Email(a) => email::run(a),
    }
}

/// Core relay loop shared by all bridges.
///
pub struct Inbound {
    pub text: String,
    pub attachments: Vec<String>,
    /// Bridge-native message ID (Signal timestamp, Discord snowflake, etc.)
    /// for the agent to reference in reactions/replies.
    pub message_ref: Option<String>,
}

impl From<String> for Inbound {
    fn from(text: String) -> Self {
        Self { text, attachments: Vec::new(), message_ref: None }
    }
}

/// Outbound message from agent to bridge. When `react_to` is set, the bridge
/// sends a reaction (emoji in `content`) to the referenced message instead
/// of a regular message.
pub struct Outbound {
    pub content: String,
    pub attachments: Vec<String>,
    pub react_to: Option<String>,
}

/// - `inbound_rx`: messages received from the external service
/// - `outbound`: called for each agent message pulled from the SSE stream
///
/// Runs until either side closes or errors.
pub async fn relay_loop<F, Fut>(
    api_url: &str,
    chat_id: &str,
    user: &str,
    mut inbound_rx: mpsc::UnboundedReceiver<Inbound>,
    outbound: F,
) -> Result<()>
where
    F: Fn(Outbound) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let client = reqwest::Client::new();
    let msg_url = format!("{}/message", api_url);
    let sse_url = format!("{}/messages/{}/stream", api_url, chat_id);

    eprintln!("[bridge] chat_id={} api_url={}", chat_id, api_url);

    // Outbound: SSE → external service
    let resp = client
        .get(&sse_url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .with_context(|| format!("connecting to SSE stream at {}", sse_url))?;
    if !resp.status().is_success() {
        anyhow::bail!("SSE stream returned {}: {}", resp.status(), sse_url);
    }
    let mut stream = resp.bytes_stream();

    let mut sse_buf = String::new();
    let mut pending_event = String::new();
    let mut pending_data = String::new();

    loop {
        tokio::select! {
            // Inbound: external → POST /message
            maybe_msg = inbound_rx.recv() => {
                match maybe_msg {
                    Some(msg) => {
                        let body = json!({
                            "chat_id": chat_id,
                            "user": user,
                            "content": msg.text,
                            "attachments": msg.attachments,
                            "message_ref": msg.message_ref,
                        });
                        match client.post(&msg_url).json(&body).send().await {
                            Ok(r) if r.status().is_success() => {}
                            Ok(r) => eprintln!("[bridge] POST /message returned {}", r.status()),
                            Err(e) => eprintln!("[bridge] POST /message failed: {}", e),
                        }
                    }
                    None => {
                        eprintln!("[bridge] inbound channel closed, exiting");
                        return Ok(());
                    }
                }
            }

            // Outbound: SSE chunk → parse → send via external service
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        sse_buf.push_str(&String::from_utf8_lossy(&bytes));
                        // SSE events are separated by \n\n; fields by \n
                        while let Some(pos) = sse_buf.find("\n\n") {
                            let event_block: String = sse_buf.drain(..pos + 2).collect();
                            for line in event_block.lines() {
                                if let Some(rest) = line.strip_prefix("event:") {
                                    pending_event = rest.trim().to_string();
                                } else if let Some(rest) = line.strip_prefix("data:") {
                                    pending_data = rest.trim().to_string();
                                }
                            }
                            if pending_event == "message" && !pending_data.is_empty() {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&pending_data) {
                                    if let Some(content) = v.get("content").and_then(|c| c.as_str()) {
                                        let attachments: Vec<String> = v.get("attachments")
                                            .and_then(|a| a.as_array())
                                            .map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                                            .unwrap_or_default();
                                        let react_to = v.get("react_to")
                                            .and_then(|r| r.as_str())
                                            .map(String::from);
                                        let out = Outbound { content: content.to_string(), attachments, react_to };
                                        if let Err(e) = outbound(out).await {
                                            eprintln!("[bridge] outbound send failed: {}", e);
                                        }
                                    }
                                }
                            }
                            pending_event.clear();
                            pending_data.clear();
                        }
                    }
                    Some(Err(e)) => {
                        anyhow::bail!("SSE stream error: {}", e);
                    }
                    None => {
                        eprintln!("[bridge] SSE stream ended, exiting");
                        return Ok(());
                    }
                }
            }
        }
    }
}

