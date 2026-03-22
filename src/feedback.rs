use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::json;

const DEFAULT_FEEDBACK_URL: &str = "https://feedback.yager.io/feedback";
const RATE_LIMIT_PER_MIN: u32 = 10;

// ---- Client ----

pub fn run_client(args: &[String]) {
    let mut summary = None;
    let mut details = None;
    let mut repro = None;
    let mut url = std::env::var("CLAUDE_SERVER_FEEDBACK_URL")
        .unwrap_or_else(|_| DEFAULT_FEEDBACK_URL.to_string());
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--summary" => {
                summary = args.get(i + 1).cloned();
                i += 2;
            }
            "--details" => {
                details = args.get(i + 1).cloned();
                i += 2;
            }
            "--repro" => {
                repro = args.get(i + 1).cloned();
                i += 2;
            }
            "--url" => {
                if let Some(u) = args.get(i + 1) {
                    url = u.clone();
                }
                i += 2;
            }
            "--help" | "-h" => {
                println!("Usage: claude-server feedback --summary TEXT [--details TEXT] [--repro TEXT] [--url URL]");
                println!();
                println!("Send a harness bug report or suggestion to the feedback server.");
                println!("Default URL: {} (override with CLAUDE_SERVER_FEEDBACK_URL)", DEFAULT_FEEDBACK_URL);
                println!();
                println!("DO NOT include private user data — only harness behavior, errors, repro steps.");
                return;
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                std::process::exit(1);
            }
        }
    }

    let summary = summary.unwrap_or_else(|| {
        eprintln!("--summary is required");
        std::process::exit(1);
    });

    let body = json!({
        "summary": summary,
        "details": details,
        "repro": repro,
        "harness_version": env!("CARGO_PKG_VERSION"),
        "agent_name": std::env::var("CLAUDE_SERVER_AGENT_NAME").ok(),
    });

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let result = rt.block_on(async {
        reqwest::Client::new()
            .post(&url)
            .json(&body)
            .timeout(Duration::from_secs(10))
            .send()
            .await
    });

    match result {
        Ok(resp) if resp.status().is_success() => {
            println!("Feedback sent to {}", url);
        }
        Ok(resp) => {
            eprintln!("Feedback server returned {}: {}", resp.status(), url);
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to send feedback to {}: {}", url, e);
            std::process::exit(1);
        }
    }
}

// ---- Server ----

#[derive(Clone)]
struct ServerState {
    db: Arc<Mutex<Connection>>,
    admin_token: Option<String>,
    rate_limiter: Arc<Mutex<HashMap<IpAddr, (Instant, u32)>>>,
}

#[derive(Deserialize)]
struct FeedbackReq {
    summary: String,
    details: Option<String>,
    repro: Option<String>,
    harness_version: Option<String>,
    agent_name: Option<String>,
}

#[derive(Serialize)]
struct FeedbackRow {
    id: i64,
    timestamp: String,
    summary: String,
    details: Option<String>,
    repro: Option<String>,
    harness_version: Option<String>,
    agent_name: Option<String>,
    remote_addr: String,
}

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<u32>,
    since: Option<i64>,
}

pub fn run_server(args: &[String]) {
    let mut listen = "0.0.0.0:3001".to_string();
    let mut db_path = "feedback.db".to_string();
    let mut admin_token = std::env::var("CLAUDE_SERVER_FEEDBACK_ADMIN_TOKEN").ok();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" => {
                if let Some(v) = args.get(i + 1) {
                    listen = v.clone();
                }
                i += 2;
            }
            "--db" => {
                if let Some(v) = args.get(i + 1) {
                    db_path = v.clone();
                }
                i += 2;
            }
            "--admin-token" => {
                admin_token = args.get(i + 1).cloned();
                i += 2;
            }
            "--help" | "-h" => {
                println!("Usage: claude-server feedback-server [--listen ADDR] [--db PATH] [--admin-token TOKEN]");
                println!();
                println!("Run the harness feedback collection server.");
                println!("  POST /feedback         — public, rate-limited (10/min/IP)");
                println!("  GET  /feedback         — admin-only, requires Bearer token");
                println!();
                println!("Defaults: listen 0.0.0.0:3001, db feedback.db");
                println!("Admin token also read from CLAUDE_SERVER_FEEDBACK_ADMIN_TOKEN.");
                return;
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                std::process::exit(1);
            }
        }
    }

    let conn = Connection::open(&db_path).expect("Failed to open feedback database");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            summary TEXT NOT NULL,
            details TEXT,
            repro TEXT,
            harness_version TEXT,
            agent_name TEXT,
            remote_addr TEXT NOT NULL
        );",
    )
    .expect("Failed to create feedback table");

    let has_admin = admin_token.is_some();
    let state = ServerState {
        db: Arc::new(Mutex::new(conn)),
        admin_token,
        rate_limiter: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/feedback", post(handle_post).get(handle_get))
        .with_state(state);

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        let addr: SocketAddr = listen.parse().expect("Invalid listen address");
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("Failed to bind");
        println!("Feedback server listening on {}", addr);
        println!("  POST /feedback — public, rate-limited ({}/min/IP)", RATE_LIMIT_PER_MIN);
        println!(
            "  GET  /feedback — {}",
            if has_admin { "admin-only (Bearer token)" } else { "disabled (no admin token set)" }
        );
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
}

async fn handle_post(
    State(state): State<ServerState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(req): Json<FeedbackReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Rate limit
    {
        let mut rl = state.rate_limiter.lock().unwrap();
        let now = Instant::now();
        let entry = rl.entry(addr.ip()).or_insert((now, 0));
        if now.duration_since(entry.0) > Duration::from_secs(60) {
            *entry = (now, 0);
        }
        if entry.1 >= RATE_LIMIT_PER_MIN {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
        entry.1 += 1;
    }

    let ts = chrono::Utc::now().to_rfc3339();
    let db = state.db.lock().unwrap();
    db.execute(
        "INSERT INTO feedback (timestamp, summary, details, repro, harness_version, agent_name, remote_addr)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            ts,
            req.summary,
            req.details,
            req.repro,
            req.harness_version,
            req.agent_name,
            addr.ip().to_string()
        ],
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let id = db.last_insert_rowid();

    Ok(Json(json!({"status": "ok", "id": id})))
}

async fn handle_get(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<FeedbackRow>>, StatusCode> {
    let Some(admin_token) = &state.admin_token else {
        return Err(StatusCode::FORBIDDEN);
    };
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if auth != Some(admin_token.as_str()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let limit = q.limit.unwrap_or(100).min(1000);
    let since = q.since.unwrap_or(0);
    let db = state.db.lock().unwrap();
    let mut stmt = db
        .prepare(
            "SELECT id, timestamp, summary, details, repro, harness_version, agent_name, remote_addr
             FROM feedback WHERE id > ?1 ORDER BY id DESC LIMIT ?2",
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = stmt
        .query_map(rusqlite::params![since, limit], |r| {
            Ok(FeedbackRow {
                id: r.get(0)?,
                timestamp: r.get(1)?,
                summary: r.get(2)?,
                details: r.get(3)?,
                repro: r.get(4)?,
                harness_version: r.get(5)?,
                agent_name: r.get(6)?,
                remote_addr: r.get(7)?,
            })
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .filter_map(|r| r.ok())
        .collect();

    Ok(Json(rows))
}
