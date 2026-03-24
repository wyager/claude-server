use std::collections::HashMap;
use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::connect_info::Connected;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, post};
use axum::serve::{IncomingStream, Listener};
use axum::{Json, Router};
use clap::Args;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::json;

const DEFAULT_FEEDBACK_URL: &str = "https://feedback.yager.io:3001/feedback";

const FEEDBACK_SERVER_CERT: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIBqTCCAU6gAwIBAgIUZ0VsIgxcQoLptYwun/K5gCMtrL8wCgYIKoZIzj0EAwIw
HDEaMBgGA1UEAwwRZmVlZGJhY2sueWFnZXIuaW8wHhcNMjYwMzIyMjExNjMyWhcN
MzYwMzE5MjExNjMyWjAcMRowGAYDVQQDDBFmZWVkYmFjay55YWdlci5pbzBZMBMG
ByqGSM49AgEGCCqGSM49AwEHA0IABI8RsQCiRpfV4eJHTt7TmkN2MOYhcpsPCnpU
pfDSgooB3gKR0Q5PJjyomIxi+noujmhhEI6OrDyOY9eSlegLDESjbjBsMB0GA1Ud
DgQWBBQs6qE14l7MxEAES4kfbfpy3g3TFDAfBgNVHSMEGDAWgBQs6qE14l7MxEAE
S4kfbfpy3g3TFDAMBgNVHRMBAf8EAjAAMBwGA1UdEQQVMBOCEWZlZWRiYWNrLnlh
Z2VyLmlvMAoGCCqGSM49BAMCA0kAMEYCIQCubcHvBns3YWvBSL1Nks30juoWV5ne
gBiP4LMic+obyQIhAPwH7LUAe1Wc8d690c2QXKebW29J/QnBjIv8oapAdA7b
-----END CERTIFICATE-----
";
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
    /// Attach the daemon's last N API request/response pairs (full JSON,
    /// including images). Fetched from the local daemon's /api-trace endpoint.
    #[arg(long)]
    pub with_api_trace: bool,
}

pub fn run_client(args: FeedbackArgs) {
    let url = args.url;
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let api_trace = if args.with_api_trace {
        let daemon = std::env::var("CLAUDE_SERVER_EVENT_URL")
            .ok()
            .and_then(|u| u.strip_suffix("/event").map(String::from))
            .unwrap_or_else(|| "http://127.0.0.1:3000".to_string());
        rt.block_on(async {
            reqwest::Client::new()
                .get(format!("{}/api-trace", daemon))
                .timeout(Duration::from_secs(5))
                .send().await.ok()?
                .json::<serde_json::Value>().await.ok()
        })
    } else {
        None
    };

    let body = json!({
        "summary": args.summary,
        "details": args.details,
        "repro": args.repro,
        "harness_version": env!("CARGO_PKG_VERSION"),
        "agent_name": std::env::var("CLAUDE_SERVER_AGENT_NAME").ok(),
        "api_trace": api_trace,
    });

    let result = rt.block_on(async {
        let cert = reqwest::Certificate::from_pem(FEEDBACK_SERVER_CERT)
            .expect("Failed to parse embedded feedback server certificate");
        reqwest::Client::builder()
            .add_root_certificate(cert)
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build HTTP client")
            .post(&url)
            .json(&body)
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
            let mut src = e.source();
            while let Some(s) = src {
                eprintln!("  caused by: {}", s);
                src = s.source();
            }
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
    #[serde(default)]
    api_trace: Option<serde_json::Value>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    has_api_trace: Option<bool>,
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
            remote_addr TEXT NOT NULL,
            api_trace TEXT
        );",
    )
    .expect("Failed to create feedback table");
    // Migration for existing DBs — ignore error if column already exists.
    let _ = conn.execute("ALTER TABLE feedback ADD COLUMN api_trace TEXT", []);

    let has_admin = admin_token.is_some();
    let state = ServerState {
        db: Arc::new(Mutex::new(conn)),
        admin_token,
        rate_limiter: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/feedback", post(handle_post).get(handle_get))
        .route("/feedback/{id}", delete(handle_delete))
        .route("/feedback/{id}/trace", axum::routing::get(handle_get_trace))
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

        let make_service = app.into_make_service_with_connect_info::<ClientAddr>();

        if let (Some(cert_path), Some(key_path)) = (tls_cert, tls_key) {
            let tls_acceptor = build_tls_acceptor(&cert_path, &key_path);
            let listener = TlsListener::new(tcp_listener, tls_acceptor);
            axum::serve(listener, make_service).await.unwrap();
        } else {
            axum::serve(tcp_listener, make_service).await.unwrap();
        }
    });
}

pub fn build_tls_acceptor(cert_path: &str, key_path: &str) -> tokio_rustls::TlsAcceptor {
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

/// TLS listener that spawns handshakes into background tasks so a slow or
/// stalled client can't block the accept loop.
pub struct TlsListener {
    local_addr: SocketAddr,
    ready_rx: tokio::sync::mpsc::Receiver<(
        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
        SocketAddr,
    )>,
}

impl TlsListener {
    pub fn new(tcp: tokio::net::TcpListener, acceptor: tokio_rustls::TlsAcceptor) -> Self {
        let local_addr = tcp.local_addr().expect("listener local_addr");
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                match tcp.accept().await {
                    Ok((stream, addr)) => {
                        let acceptor = acceptor.clone();
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let handshake = tokio::time::timeout(
                                Duration::from_secs(10),
                                acceptor.accept(stream),
                            );
                            match handshake.await {
                                Ok(Ok(tls)) => {
                                    let _ = tx.send((tls, addr)).await;
                                }
                                Ok(Err(e)) => {
                                    eprintln!("TLS handshake failed from {}: {}", addr, e)
                                }
                                Err(_) => eprintln!("TLS handshake timeout from {}", addr),
                            }
                        });
                    }
                    Err(e) => eprintln!("TCP accept error: {}", e),
                }
            }
        });
        Self { local_addr, ready_rx: rx }
    }
}

impl Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.ready_rx.recv().await.expect("TLS accept task exited")
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

/// Local newtype so we can impl `Connected` for both the plain TCP and TLS
/// listeners without hitting orphan rules.
#[derive(Clone, Copy)]
struct ClientAddr(SocketAddr);

impl Connected<IncomingStream<'_, tokio::net::TcpListener>> for ClientAddr {
    fn connect_info(s: IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        ClientAddr(*s.remote_addr())
    }
}

impl Connected<IncomingStream<'_, TlsListener>> for ClientAddr {
    fn connect_info(s: IncomingStream<'_, TlsListener>) -> Self {
        ClientAddr(*s.remote_addr())
    }
}


async fn handle_post(
    State(state): State<ServerState>,
    ConnectInfo(ClientAddr(addr)): ConnectInfo<ClientAddr>,
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
    let trace_json = req.api_trace.as_ref().map(|v| v.to_string());
    let db = state.db.lock().unwrap();
    db.execute(
        "INSERT INTO feedback (timestamp, summary, details, repro, harness_version, agent_name, remote_addr, api_trace)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            ts,
            req.summary,
            req.details,
            req.repro,
            req.harness_version,
            req.agent_name,
            addr.ip().to_string(),
            trace_json
        ],
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let id = db.last_insert_rowid();

    Ok(Json(json!({"status": "ok", "id": id})))
}

fn check_admin(state: &ServerState, headers: &HeaderMap) -> Result<(), StatusCode> {
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
    Ok(())
}

async fn handle_get_trace(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_admin(&state, &headers)?;
    let db = state.db.lock().unwrap();
    let trace: Option<String> = db
        .query_row(
            "SELECT api_trace FROM feedback WHERE id = ?1",
            rusqlite::params![id],
            |r| r.get(0),
        )
        .map_err(|_| StatusCode::NOT_FOUND)?;
    match trace {
        Some(t) => serde_json::from_str(&t)
            .map(Json)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn handle_delete(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_admin(&state, &headers)?;
    let db = state.db.lock().unwrap();
    let n = db
        .execute("DELETE FROM feedback WHERE id = ?1", rusqlite::params![id])
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if n == 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(json!({"status": "deleted", "id": id})))
}

async fn handle_get(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<FeedbackRow>>, StatusCode> {
    check_admin(&state, &headers)?;

    let limit = q.limit.unwrap_or(100).min(1000);
    let since = q.since.unwrap_or(0);
    let db = state.db.lock().unwrap();
    let mut stmt = db
        .prepare(
            "SELECT id, timestamp, summary, details, repro, harness_version, agent_name, remote_addr,
                    api_trace IS NOT NULL
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
                has_api_trace: Some(r.get(8)?),
            })
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .filter_map(|r| r.ok())
        .collect();

    Ok(Json(rows))
}
