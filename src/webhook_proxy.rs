use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use clap::Args;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

use crate::feedback::{build_tls_acceptor, TlsListener};

type HmacSha256 = Hmac<Sha256>;

#[derive(Args)]
pub struct WebhookArgs {
    /// Listen address
    #[arg(long, default_value = "0.0.0.0:8443")]
    pub listen: String,
    /// Claude Server API URL
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    pub api_url: String,
    /// GitHub webhook secret (enables /github route)
    #[arg(long, env = "WEBHOOK_GITHUB_SECRET")]
    pub github_secret: Option<String>,
    /// Slack signing secret (enables /slack route)
    #[arg(long, env = "WEBHOOK_SLACK_SECRET")]
    pub slack_secret: Option<String>,
    /// Bearer token for /generic route
    #[arg(long, env = "WEBHOOK_GENERIC_TOKEN")]
    pub generic_token: Option<String>,
    /// TLS cert path (both --tls-cert and --tls-key required for TLS)
    #[arg(long)]
    pub tls_cert: Option<String>,
    /// TLS key path
    #[arg(long)]
    pub tls_key: Option<String>,
}

struct AppState {
    api_url: String,
    github_secret: Option<String>,
    slack_secret: Option<String>,
    generic_token: Option<String>,
    http: reqwest::Client,
}

pub fn run(args: WebhookArgs) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    if let Err(e) = rt.block_on(run_async(args)) {
        eprintln!("[webhook-proxy] error: {:#}", e);
        std::process::exit(1);
    }
}

async fn run_async(args: WebhookArgs) -> Result<()> {
    let state = Arc::new(AppState {
        api_url: args.api_url,
        github_secret: args.github_secret,
        slack_secret: args.slack_secret,
        generic_token: args.generic_token,
        http: reqwest::Client::new(),
    });

    let app = Router::new()
        .route("/github", post(github))
        .route("/slack", post(slack))
        .route("/generic", post(generic))
        .with_state(state.clone());

    let addr: SocketAddr = args.listen.parse()?;
    let tcp = tokio::net::TcpListener::bind(addr).await?;

    println!("Webhook proxy listening on {}", addr);
    println!("  /github  — {}", enabled(&state.github_secret));
    println!("  /slack   — {}", enabled(&state.slack_secret));
    println!("  /generic — {}", enabled(&state.generic_token));

    match (args.tls_cert, args.tls_key) {
        (Some(cert), Some(key)) => {
            println!("  TLS enabled");
            let acceptor = build_tls_acceptor(&cert, &key);
            let listener = TlsListener::new(tcp, acceptor);
            axum::serve(listener, app).await?;
        }
        _ => {
            axum::serve(tcp, app).await?;
        }
    }
    Ok(())
}

fn enabled(s: &Option<String>) -> &'static str {
    if s.is_some() { "enabled" } else { "disabled (no secret configured)" }
}

async fn forward(state: &AppState, source: &str, event_type: &str, data: Value, priority: u8) -> StatusCode {
    match state
        .http
        .post(format!("{}/event", state.api_url))
        .json(&json!({"source": source, "type": event_type, "data": data, "priority": priority}))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => StatusCode::OK,
        Ok(r) => {
            eprintln!("[webhook-proxy] /event returned {}", r.status());
            StatusCode::BAD_GATEWAY
        }
        Err(e) => {
            eprintln!("[webhook-proxy] forward error: {}", e);
            StatusCode::BAD_GATEWAY
        }
    }
}

// ---- GitHub ----

async fn github(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let Some(secret) = &state.github_secret else { return StatusCode::NOT_FOUND };
    let Some(sig) = headers.get("x-hub-signature-256").and_then(|v| v.to_str().ok()) else {
        return StatusCode::UNAUTHORIZED;
    };
    let Some(sig_hex) = sig.strip_prefix("sha256=") else { return StatusCode::UNAUTHORIZED };

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(&body);
    let expected = hex::encode(mac.finalize().into_bytes());
    if !constant_time_eq(sig_hex.as_bytes(), expected.as_bytes()) {
        return StatusCode::UNAUTHORIZED;
    }

    let event_type = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown");
    let data: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    forward(&state, "github", event_type, data, 6).await
}

// ---- Slack ----

async fn slack(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, StatusCode> {
    let Some(secret) = &state.slack_secret else { return Err(StatusCode::NOT_FOUND) };
    let ts = headers
        .get("x-slack-request-timestamp")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let sig = headers
        .get("x-slack-signature")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Replay protection: reject timestamps older than 5 min
    let ts_num: i64 = ts.parse().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let now = chrono::Utc::now().timestamp();
    if (now - ts_num).abs() > 300 {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let basestring = format!("v0:{}:{}", ts, std::str::from_utf8(&body).map_err(|_| StatusCode::BAD_REQUEST)?);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(basestring.as_bytes());
    let expected = format!("v0={}", hex::encode(mac.finalize().into_bytes()));
    if !constant_time_eq(sig.as_bytes(), expected.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let data: Value = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;

    // URL verification challenge
    if data["type"].as_str() == Some("url_verification") {
        return Ok(Json(json!({"challenge": data["challenge"]})));
    }

    let event_type = data["type"].as_str().unwrap_or("event").to_string();
    match forward(&state, "slack", &event_type, data, 6).await {
        StatusCode::OK => Ok(Json(json!({"ok": true}))),
        s => Err(s),
    }
}

// ---- Generic ----

#[derive(serde::Deserialize)]
struct GenericEvent {
    source: String,
    #[serde(rename = "type")]
    event_type: String,
    data: Value,
    #[serde(default = "default_prio")]
    priority: u8,
}
fn default_prio() -> u8 { 5 }

async fn generic(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(ev): Json<GenericEvent>,
) -> StatusCode {
    let Some(token) = &state.generic_token else { return StatusCode::NOT_FOUND };
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if auth != Some(token.as_str()) {
        return StatusCode::UNAUTHORIZED;
    }
    forward(&state, &ev.source, &ev.event_type, ev.data, ev.priority).await
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
