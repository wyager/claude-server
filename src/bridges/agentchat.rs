use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::feedback::FEEDBACK_SERVER_CERT;

const DEFAULT_SERVER: &str = "wss://feedback.yager.io:3001/chat/ws";

#[derive(Args)]
pub struct AgentChatArgs {
    /// Username (e.g. "{hostname}-{agent_name}")
    #[arg(long)]
    pub user: String,
    /// Password (use a strong random one; stored as salted SHA256 server-side)
    #[arg(long)]
    pub pass: String,
    /// Chat server WSS URL
    #[arg(long, default_value = DEFAULT_SERVER)]
    pub server: String,
    /// Debounce window: batch messages arriving within this many ms into one event
    #[arg(long, default_value = "500")]
    pub debounce_ms: u64,
    #[command(flatten)]
    pub api: super::ApiUrl,
}

pub fn run(args: AgentChatArgs) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    if let Err(e) = rt.block_on(run_async(args)) {
        eprintln!("[agentchat bridge] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(args: AgentChatArgs) -> Result<()> {
    let ws = connect_wss(&args.server).await?;
    let (mut sink, mut stream) = ws.split();

    // Auth
    sink.send(Message::Text(json!({"user": args.user, "pass": args.pass}).to_string().into())).await?;
    match stream.next().await {
        Some(Ok(Message::Text(t))) => {
            let v: Value = serde_json::from_str(&t)?;
            if v["ok"].as_bool() != Some(true) {
                anyhow::bail!("auth failed: {}", v["error"].as_str().unwrap_or("?"));
            }
        }
        other => anyhow::bail!("expected auth response, got {:?}", other),
    }
    eprintln!("[agentchat bridge] connected as '{}'", args.user);

    let http = reqwest::Client::new();

    // Outbound: SSE agentchat:* → WS
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<(String, String)>();
    let sse_url = format!("{}/messages/agentchat:*/stream", args.api.api_url);
    let sse_http = http.clone();
    tokio::spawn(async move {
        let resp = match sse_http.get(&sse_url).header("Accept", "text/event-stream").send().await {
            Ok(r) => r, Err(e) => { eprintln!("[agentchat] SSE connect failed: {}", e); return; }
        };
        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        while let Some(Ok(chunk)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(pos) = buf.find("\n\n") {
                let block: String = buf.drain(..pos + 2).collect();
                let data = block.lines().find_map(|l| l.strip_prefix("data:")).unwrap_or("").trim();
                if let Ok(v) = serde_json::from_str::<Value>(data) {
                    if let (Some(cid), Some(content)) = (v["chat_id"].as_str(), v["content"].as_str()) {
                        if let Some(to) = cid.strip_prefix("agentchat:") {
                            let _ = out_tx.send((to.to_string(), content.to_string()));
                        }
                    }
                }
            }
        }
    });

    // Inbound: WS → debounced batch → POST /event
    let event_url = format!("{}/event", args.api.api_url);
    let agent = std::env::var("CLAUDE_SERVER_AGENT_NAME").unwrap_or_else(|_| "root".into());
    let debounce = Duration::from_millis(args.debounce_ms);
    let mut batch: Vec<Value> = Vec::new();
    let mut flush_at: Option<tokio::time::Instant> = None;

    loop {
        let sleeper = flush_at.map(tokio::time::sleep_until);
        tokio::select! {
            out = out_rx.recv() => match out {
                Some((to, body)) => {
                    sink.send(Message::Text(json!({"to": to, "body": body}).to_string().into())).await?;
                }
                None => break,
            },
            inc = stream.next() => match inc {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).unwrap_or(Value::Null);
                    if v.get("from").is_some() || v.get("error").is_some() {
                        batch.push(v);
                        if flush_at.is_none() {
                            flush_at = Some(tokio::time::Instant::now() + debounce);
                        }
                    }
                }
                Some(Ok(Message::Ping(p))) => { sink.send(Message::Pong(p)).await?; }
                Some(Ok(Message::Close(_))) | None => break,
                _ => {}
            },
            _ = async { sleeper.unwrap().await }, if sleeper.is_some() => {
                flush_at = None;
                let msgs = std::mem::take(&mut batch);
                let body = json!({
                    "source": "agentchat",
                    "type": "batch",
                    "data": {"messages": msgs},
                    "agent": agent,
                    "priority": 7,
                });
                if let Err(e) = http.post(&event_url).json(&body).send().await {
                    eprintln!("[agentchat] POST /event failed: {}", e);
                }
            }
        }
    }
    Ok(())
}

async fn connect_wss(url: &str) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    if url.starts_with("ws://") {
        let (ws, _) = tokio_tungstenite::connect_async(url).await.context("ws connect")?;
        return Ok(ws);
    }
    // Build rustls config trusting the feedback server's self-signed cert
    // plus platform roots (for future CA-signed deploys).
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &*FEEDBACK_SERVER_CERT) {
        roots.add(cert.context("parse cert")?)?;
    }
    for c in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(c);
    }
    let cfg = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(cfg));
    let (ws, _) = tokio_tungstenite::connect_async_tls_with_config(url, None, false, Some(connector))
        .await.context("wss connect")?;
    Ok(ws)
}
