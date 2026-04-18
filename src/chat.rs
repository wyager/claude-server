use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderName, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use clap::Args;

use crate::tls::TlsArgs;

static CHAT_HTML: &str = include_str!("chat.html");

/// Hop-by-hop headers that must not be forwarded through a proxy (RFC 7230 §6.1)
/// plus `host`/`content-length` which we let reqwest recompute.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "host"
            | "content-length"
    )
}

#[derive(Args)]
pub struct ChatArgs {
    /// Port for the chat UI (also serves the /api reverse-proxy to the daemon).
    #[arg(short, long, default_value_t = 8080)]
    pub port: u16,

    /// Bind address. Defaults to 127.0.0.1 (loopback only).
    /// Use 0.0.0.0 to expose externally (typical with --tls-cert / --acme-domain).
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: std::net::IpAddr,

    /// Backend Claude Server API URL. The chat UI page always uses same-origin
    /// `/api/...` requests; this server reverse-proxies those to the backend.
    /// Default: loopback — the daemon and chat UI run on the same host.
    #[arg(short = 'a', long, default_value = "http://127.0.0.1:3000")]
    pub api_url: String,

    #[command(flatten)]
    pub tls: TlsArgs,
}

#[derive(Clone)]
struct ProxyState {
    backend: String,
    client: reqwest::Client,
}

pub fn run_chat_server(args: ChatArgs) {
    if let Err(e) = args.tls.validate() {
        eprintln!("error: {}", e);
        std::process::exit(2);
    }

    let scheme = if args.tls.is_enabled() { "https" } else { "http" };
    // Same-origin API: all browser requests go to /api/... on this server.
    let html = CHAT_HTML.replace("{{API_URL}}", "/api");
    let port = args.port;
    let api_url = args.api_url.trim_end_matches('/').to_string();
    let bind = args.bind;
    let tls = args.tls.clone();

    // Long-lived proxy client with no overall timeout so SSE streams don't
    // get cut mid-connection. connect_timeout still fires on dead backends.
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .build()
        .expect("build proxy client");
    let proxy_state = ProxyState { backend: api_url.clone(), client };

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        let app = Router::new()
            .route(
                "/",
                get(move || {
                    let html = html.clone();
                    async move { Html(html) }
                }),
            )
            .route("/favicon.ico", get(|| async { StatusCode::NO_CONTENT }))
            .route("/api/{*path}", any(proxy_handler))
            .with_state(proxy_state);

        let addr = std::net::SocketAddr::new(bind, port);

        println!("Chat UI running at {}://{}", scheme, addr);
        println!("Proxying /api → {}", api_url);
        println!();
        println!("Open your browser to {}://{}", scheme, addr);

        if tls.is_enabled() {
            if let Err(e) = crate::tls::serve(addr, app, tls).await {
                eprintln!("TLS server error: {:#}", e);
                std::process::exit(1);
            }
        } else {
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to bind to {}: {}", addr, e);
                    std::process::exit(1);
                });
            axum::serve(listener, app).await.unwrap();
        }
    });
}

/// Reverse-proxies every method under `/api/{*path}` to the backend daemon.
/// Streams the response body so SSE endpoints remain streaming end-to-end.
async fn proxy_handler(
    State(state): State<ProxyState>,
    Path(path): Path<String>,
    req: Request,
) -> Response {
    let (parts, body) = req.into_parts();

    let query = parts
        .uri
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();
    let url = format!("{}/{}{}", state.backend, path, query);

    let method = match reqwest::Method::from_bytes(parts.method.as_str().as_bytes()) {
        Ok(m) => m,
        Err(_) => return (StatusCode::BAD_REQUEST, "bad method").into_response(),
    };

    let mut builder = state.client.request(method, &url);
    for (k, v) in &parts.headers {
        if is_hop_by_hop(k.as_str()) {
            continue;
        }
        builder = builder.header(k.as_str(), v.as_bytes());
    }

    // Buffer the request body. API POST bodies (messages, events) are small;
    // 10 MiB is far past any realistic chat payload.
    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response(),
    };
    if !body_bytes.is_empty() {
        builder = builder.body(body_bytes.to_vec());
    }

    let resp = match builder.send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[proxy] backend error on {}: {}", url, e);
            return (StatusCode::BAD_GATEWAY, format!("proxy: {}", e)).into_response();
        }
    };

    let status_u16 = resp.status().as_u16();
    let upstream_headers = resp.headers().clone();

    // Stream the body through. `bytes_stream()` preserves chunked transfer,
    // which is what keeps SSE events flowing without buffering.
    let stream = resp.bytes_stream();
    let body = Body::from_stream(stream);

    let mut response = Response::builder().status(status_u16);
    let response_headers = response.headers_mut().expect("builder headers");
    for (k, v) in &upstream_headers {
        if is_hop_by_hop(k.as_str()) {
            continue;
        }
        if let Ok(name) = HeaderName::from_bytes(k.as_str().as_bytes()) {
            if let Ok(val) = axum::http::HeaderValue::from_bytes(v.as_bytes()) {
                response_headers.insert(name, val);
            }
        }
    }
    response.body(body).unwrap_or_else(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, "proxy response build failed").into_response()
    })
}

