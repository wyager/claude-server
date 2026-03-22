use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::serve::{Listener, ListenerExt};
use axum::{Json, Router};
use clap::Args;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::json;

const DEFAULT_FEEDBACK_URL: &str = "https://feedback.yager.io/feedback";
const RATE_LIMIT_PER_MIN: u32 = 10;

fn default_feedback_url() -> String {
    std::env::var("CLAUDE_SERVER_FEEDBACK_URL")
        .unwrap_or_else(|_| DEFAULT_FEEDBACK_URL.into())
}

// ---- Client ----

#[derive(Args)]
pub struct FeedbackArgs {
    /// Brief summary of the issue
    #[arg(long)]
    pub summary: String,
    /// Longer description
    #[arg(long)]
    pub details: Option<String>,
    /// Reproduction steps
    #[arg(long)]
    pub repro: Option<String>,
    /// Feedback server URL (env: CLAUDE_SERVER_FEEDBACK_URL)
    #[arg(long, default_value_t = default_feedback_url())]
    pub url: String,
}

pub fn run_client(args: FeedbackArgs) {
    let url = args.url;
    let body = json!({
        "summary": args.summary,
        "details": args.details,
        "repro": args.repro,
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

#[derive(Args)]
pub struct ServerArgs {
    /// Listen address
    #[arg(long, default_value = "0.0.0.0:3001")]
    pub listen: String,
    /// SQLite database path
    #[arg(long, default_value = "feedback.db")]
    pub db: String,
    /// Admin Bearer token for GET /feedback (env: CLAUDE_SERVER_FEEDBACK_ADMIN_TOKEN)
    #[arg(long, env = "CLAUDE_SERVER_FEEDBACK_ADMIN_TOKEN")]
    pub admin_token: Option<String>,
    /// Path to TLS certificate PEM file (enables HTTPS when paired with --tls-key)
    #[arg(long, requires = "tls_key")]
    pub tls_cert: Option<String>,
    /// Path to TLS private key PEM file (enables HTTPS when paired with --tls-cert)
    #[arg(long, requires = "tls_cert")]
    pub tls_key: Option<String>,
}

pub fn run_server(args: ServerArgs) {
    let listen = args.listen;
    let admin_token = args.admin_token;
    let tls_cert = args.tls_cert;
    let tls_key = args.tls_key;
    let conn = Connection::open(&args.db).expect("Failed to open feedback database");
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
        let tcp_listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("Failed to bind");

        let protocol = if tls_cert.is_some() { "https" } else { "http" };
        println!("Feedback server listening on {}://{}", protocol, addr);
        println!("  POST /feedback — public, rate-limited ({}/min/IP)", RATE_LIMIT_PER_MIN);
        println!(
            "  GET  /feedback — {}",
            if has_admin { "admin-only (Bearer token)" } else { "disabled (no admin token set)" }
        );

        let make_service = app.into_make_service_with_connect_info::<SocketAddr>();

        if let (Some(cert_path), Some(key_path)) = (tls_cert, tls_key) {
            let tls_acceptor = build_tls_acceptor(&cert_path, &key_path);
            let listener = TlsListener { inner: tcp_listener, acceptor: tls_acceptor }
                .tap_io(|_| {});
            axum::serve(listener, make_service).await.unwrap();
        } else {
            axum::serve(tcp_listener, make_service).await.unwrap();
        }
    });
}

fn build_tls_acceptor(cert_path: &str, key_path: &str) -> tokio_rustls::TlsAcceptor {
    use rustls::ServerConfig;
    use rustls_pemfile::{certs, private_key};
    use std::fs::File;
    use std::io::BufReader;

    let cert_file = File::open(cert_path)
        .unwrap_or_else(|e| panic!("Failed to open TLS cert {}: {}", cert_path, e));
    let key_file = File::open(key_path)
        .unwrap_or_else(|e| panic!("Failed to open TLS key {}: {}", key_path, e));

    let certs: Vec<_> = certs(&mut BufReader::new(cert_file))
        .collect::<Result<_, _>>()
        .expect("Failed to parse TLS certificate PEM");
    let key = private_key(&mut BufReader::new(key_file))
        .expect("Failed to read TLS key PEM")
        .expect("No private key found in TLS key PEM");

    let config = ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("Failed to set TLS protocol versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("Failed to build TLS config (cert/key mismatch?)");

    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

// TODO WYAGER: check Claude's work
struct TlsListener {
    inner: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
}

impl Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                Ok((stream, addr)) => match self.acceptor.accept(stream).await {
                    Ok(tls_stream) => return (tls_stream, addr),
                    Err(e) => eprintln!("TLS handshake failed from {}: {}", addr, e),
                },
                Err(e) => eprintln!("TCP accept error: {}", e),
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }
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
