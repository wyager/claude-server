use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::Json;
use axum::routing::{delete, get, post};
use axum::Router;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;

use crate::config::Config;
use crate::core_loop::HarnessEvent;
use crate::db::Database;
use crate::types::{AgentRegistry, TokenAccumulator};

/// Tracks active SSE subscription patterns so `send_message` can fail fast
/// on unroutable chat_ids and `wait_for_message_channel` can block until a
/// bridge connects. Patterns are either exact chat_ids or prefixes (trailing
/// `*` stripped before insert; see `would_reach`).
#[derive(Default)]
pub struct SubscriberRegistry {
    inner: Mutex<SubscriberInner>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct SubscriberInner {
    /// Exact-match subscriptions → count (same chat_id can have multiple subs).
    exact: std::collections::HashMap<String, usize>,
    /// Prefix subscriptions (without the `*`) → count.
    prefix: std::collections::HashMap<String, usize>,
}

impl SubscriberRegistry {
    /// Register a subscription. Returns a guard that unregisters on drop.
    /// `pattern` is the raw SSE path segment — trailing `*` means prefix match.
    pub fn register(self: &Arc<Self>, pattern: &str) -> SubscriberGuard {
        let (key, is_prefix) = match pattern.strip_suffix('*') {
            Some(p) => (p.to_owned(), true),
            None => (pattern.to_owned(), false),
        };
        {
            let mut inner = self.inner.lock().unwrap();
            let map = if is_prefix { &mut inner.prefix } else { &mut inner.exact };
            *map.entry(key.clone()).or_default() += 1;
        }
        self.notify.notify_waiters();
        SubscriberGuard { reg: self.clone(), key, is_prefix }
    }

    /// Would a `send_message(chat_id, ...)` reach at least one subscriber?
    pub fn would_reach(&self, chat_id: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.exact.contains_key(chat_id)
            || inner.prefix.keys().any(|p| chat_id.starts_with(p))
    }

    /// Block until `would_reach(chat_id)` is true or timeout. Arm-then-check
    /// closes the TOCTOU gap between checking and waiting.
    pub async fn wait_for(&self, chat_id: &str, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            if self.would_reach(chat_id) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }
}

pub struct SubscriberGuard {
    reg: Arc<SubscriberRegistry>,
    key: String,
    is_prefix: bool,
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        let mut inner = self.reg.inner.lock().unwrap();
        let map = if self.is_prefix { &mut inner.prefix } else { &mut inner.exact };
        if let Some(c) = map.get_mut(&self.key) {
            *c -= 1;
            if *c == 0 { map.remove(&self.key); }
        }
    }
}

/// Broadcast message sent from the core loop to SSE subscribers.
#[derive(Debug, Clone)]
pub enum BroadcastMsg {
    Message {
        chat_id: String,
        content: String,
        attachments: Vec<String>,
        id: i64,
        created_at: String,
        /// If set, bridges send this as a reaction to the referenced message
        /// (content is the emoji) instead of a regular message.
        react_to: Option<String>,
    },
    Status {
        status: String,
    },
}

#[derive(Clone)]
struct AppState {
    event_tx: mpsc::UnboundedSender<HarnessEvent>,
    db: Arc<Database>,
    broadcast_tx: broadcast::Sender<BroadcastMsg>,
    token_accumulator: Arc<Mutex<TokenAccumulator>>,
    config: Arc<Config>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    registry: Arc<AgentRegistry>,
    api_trace: Arc<Mutex<crate::api_client::ApiTrace>>,
    subscribers: Arc<SubscriberRegistry>,
}

// ---- Request/Response types ----

#[derive(Deserialize)]
struct MessageRequest {
    chat_id: Option<String>,
    user: String,
    content: String,
    #[serde(default)]
    attachments: Vec<String>,
    #[serde(default)]
    message_ref: Option<String>,
}

#[derive(Serialize)]
struct MessageResponse {
    status: String,
    chat_id: String,
}

#[derive(Serialize)]
struct StatusResponse {
    status: String,
    model: String,
}

// ---- Handlers ----

async fn handle_message(
    State(state): State<AppState>,
    Json(body): Json<MessageRequest>,
) -> Result<Json<MessageResponse>, StatusCode> {
    let chat_id = body
        .chat_id
        .unwrap_or_else(|| format!("{:08x}", rand_id()));

    state
        .event_tx
        .send(HarnessEvent::UserMessage {
            chat_id: chat_id.clone(),
            user: body.user,
            content: body.content,
            attachments: body.attachments,
            message_ref: body.message_ref,
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(MessageResponse {
        status: "queued".to_string(),
        chat_id,
    }))
}

async fn handle_api_trace(State(state): State<AppState>) -> Json<serde_json::Value> {
    let trace = state.api_trace.lock().unwrap();
    Json(serde_json::to_value(trace.snapshot()).unwrap_or_default())
}

async fn handle_status() -> Json<StatusResponse> {
    Json(StatusResponse {
        status: "running".to_string(),
        model: "claude-server".to_string(),
    })
}

/// Full state snapshot for all agents (root + children). Each snapshot is
/// updated once per turn by the owning AgentLoop, so this is "eventually
/// consistent" with a ~seconds lag. Cheap to serve — just clones the
/// registry's HashMap.
async fn handle_dashboard_state(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::to_value(state.registry.all_snapshots()).unwrap_or_default())
}

/// Remove a snapshot from the dashboard. If the agent is still running
/// it'll reappear on its next turn — this is for clearing completed
/// children, not hiding live agents. `:name` of `*` prunes all terminal
/// snapshots at once (saves clicking through a dozen done children).
async fn handle_dashboard_dismiss(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Json<serde_json::Value> {
    if name == "*" {
        let n = state.registry.prune_terminal_snapshots();
        Json(serde_json::json!({"removed": n}))
    } else {
        let removed = state.registry.remove_snapshot(&name);
        Json(serde_json::json!({"removed": if removed { 1 } else { 0 }}))
    }
}

/// Serve the embedded dashboard HTML. Single-file, no build step — same
/// pattern as chat.html. The page polls /dashboard/state every 2s; SSE felt
/// like overkill given snapshots only change once per turn anyway.
async fn handle_dashboard() -> axum::response::Html<String> {
    axum::response::Html(include_str!("dashboard.html").to_string())
}

/// Root landing page — just a link index so hitting the base URL in a
/// browser doesn't 404. Intentionally spartan; this is plumbing, not a
/// product surface.
async fn handle_index() -> axum::response::Html<&'static str> {
    axum::response::Html(r#"<!doctype html><meta charset=utf-8>
<title>claude-server</title>
<style>body{font:14px/1.6 ui-monospace,Menlo,monospace;background:#0d1117;color:#c9d1d9;padding:2em}
a{color:#58a6ff;text-decoration:none}a:hover{text-decoration:underline}
code{background:#161b22;padding:1px 5px;border-radius:3px}</style>
<h3>claude-server</h3>
<p><a href="/dashboard">→ dashboard</a> (live agent state)</p>
<p><a href="/status">/status</a> · <a href="/cost">/cost</a> · <a href="/api-trace">/api-trace</a> · <a href="/dashboard/state">/dashboard/state</a></p>
<p><code>POST /message</code> · <code>POST /event</code> · <code>POST /shutdown</code></p>
"#)
}

async fn handle_get_messages(
    State(state): State<AppState>,
    Path(chat_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let messages = state
        .db
        .load_outbound_messages(&chat_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({ "messages": messages })))
}

async fn handle_shutdown(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    state
        .shutdown_tx
        .send(true)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({ "status": "shutting_down" })))
}

// ---- Cost Handler ----

async fn handle_cost(
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let acc = state.token_accumulator.lock().unwrap();
    let config = &state.config;
    let cost = (acc.input_tokens as f64 * config.cost_per_m_input
        + acc.output_tokens as f64 * config.cost_per_m_output
        + acc.cache_read_tokens as f64 * config.cost_per_m_cache_read
        + acc.cache_creation_tokens as f64 * config.cost_per_m_cache_write)
        / 1_000_000.0;
    Json(serde_json::json!({
        "input_tokens": acc.input_tokens,
        "output_tokens": acc.output_tokens,
        "cache_read_tokens": acc.cache_read_tokens,
        "cache_creation_tokens": acc.cache_creation_tokens,
        "estimated_cost_usd": (cost * 100.0).round() / 100.0,
        "turns": acc.turns,
        // Unix timestamp at which the accumulator started — counts above are the
        // sum since this point. Today it resets on every daemon restart; document
        // here so callers don't mistake "$X.XX" for a per-day or lifetime figure.
        "since": acc.since,
    }))
}

// ---- Event Handler ----

#[derive(Deserialize)]
struct EventRequest {
    source: String,
    #[serde(rename = "type")]
    event_type: String,
    data: serde_json::Value,
    priority: Option<u8>,
    /// Route to a specific agent by name. Omitted/null → root.
    agent: Option<String>,
}

async fn handle_event(
    State(state): State<AppState>,
    Json(body): Json<EventRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let priority = body.priority.unwrap_or(5);
    let event = HarnessEvent::ExternalEvent {
        source: body.source,
        event_type: body.event_type,
        data: body.data,
        priority,
    };

    let routed_to = match body.agent.as_deref() {
        None | Some("root") => {
            state.event_tx.send(event).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            "root".to_string()
        }
        Some(name) => match state.registry.send_to(name, event) {
            Ok(true) => name.to_string(),
            // Completed or unknown agent → fall back to root so events aren't silently dropped.
            Ok(false) | Err(_) => {
                let fallback = HarnessEvent::ExternalEvent {
                    source: format!("routed-from:{}", name),
                    event_type: "agent-not-found".to_string(),
                    data: serde_json::json!({"original_target": name}),
                    priority,
                };
                let _ = state.event_tx.send(fallback);
                return Ok(Json(serde_json::json!({"status": "fallback", "target": name, "routed_to": "root"})));
            }
        },
    };

    Ok(Json(serde_json::json!({"status": "queued", "routed_to": routed_to})))
}

// ---- SSE Handler ----

async fn handle_sse(
    State(state): State<AppState>,
    Path(chat_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.broadcast_tx.subscribe();
    // Trailing `*` enables prefix match — lets a bridge subscribe to all
    // chat_ids under a namespace (e.g. `agentchat:*`) and parse the
    // recipient from the suffix.
    let prefix = chat_id.strip_suffix('*').map(str::to_owned);
    // Guard lives in the stream closure; drops when the stream ends (client
    // disconnect), decrementing the subscriber count.
    let guard = state.subscribers.register(&chat_id);
    let stream = BroadcastStream::new(rx)
        .filter_map(move |msg| {
            let _guard = &guard;
            match msg {
                Ok(BroadcastMsg::Message {
                    chat_id: ref msg_chat_id,
                    ref content,
                    ref attachments,
                    id,
                    ref created_at,
                    ref react_to,
                }) if prefix.as_ref().map_or(*msg_chat_id == chat_id, |p| msg_chat_id.starts_with(p)) => {
                    let data = serde_json::json!({
                        "id": id,
                        "chat_id": msg_chat_id,
                        "content": content,
                        "react_to": react_to,
                        "attachments": attachments,
                        "created_at": created_at,
                    });
                    Some(Ok(Event::default()
                        .event("message")
                        .data(data.to_string())))
                }
                Ok(BroadcastMsg::Status { ref status }) => {
                    let data = serde_json::json!({ "status": status });
                    Some(Ok(Event::default()
                        .event("status")
                        .data(data.to_string())))
                }
                _ => None,
            }
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ---- Router ----

pub fn create_router(
    event_tx: mpsc::UnboundedSender<HarnessEvent>,
    db: Arc<Database>,
    broadcast_tx: broadcast::Sender<BroadcastMsg>,
    token_accumulator: Arc<Mutex<TokenAccumulator>>,
    config: Arc<Config>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    registry: Arc<AgentRegistry>,
    api_trace: Arc<Mutex<crate::api_client::ApiTrace>>,
    subscribers: Arc<SubscriberRegistry>,
) -> Router {
    let state = AppState {
        event_tx,
        db,
        broadcast_tx,
        token_accumulator,
        config,
        shutdown_tx,
        registry,
        api_trace,
        subscribers,
    };

    Router::new()
        .route("/message", post(handle_message))
        .route("/event", post(handle_event))
        .route("/", get(handle_index))
        .route("/status", get(handle_status))
        .route("/dashboard", get(handle_dashboard))
        .route("/dashboard/state", get(handle_dashboard_state))
        .route("/dashboard/state/{name}", delete(handle_dashboard_dismiss))
        .route("/cost", get(handle_cost))
        .route("/api-trace", get(handle_api_trace))
        .route("/messages/{chat_id}", get(handle_get_messages))
        .route("/messages/{chat_id}/stream", get(handle_sse))
        .route("/shutdown", post(handle_shutdown))
        .with_state(state)
        .layer(CorsLayer::permissive())
}

fn rand_id() -> u32 {
    use std::time::SystemTime;
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_nanos() & 0xFFFFFFFF) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_registry_would_reach() {
        let reg = Arc::new(SubscriberRegistry::default());
        assert!(!reg.would_reach("signal:+1555"));

        let g1 = reg.register("signal:+1555");
        assert!(reg.would_reach("signal:+1555"));
        assert!(!reg.would_reach("signal:+1999"));
        assert!(!reg.would_reach("telegram:123"));

        let g2 = reg.register("telegram:*");
        assert!(reg.would_reach("telegram:123"));
        assert!(reg.would_reach("telegram:anything"));
        assert!(!reg.would_reach("telegraph:x"));

        drop(g1);
        assert!(!reg.would_reach("signal:+1555"));
        assert!(reg.would_reach("telegram:123"));

        drop(g2);
        assert!(!reg.would_reach("telegram:123"));
    }

    #[test]
    fn test_subscriber_registry_refcount() {
        let reg = Arc::new(SubscriberRegistry::default());
        let g1 = reg.register("signal:*");
        let g2 = reg.register("signal:*");
        assert!(reg.would_reach("signal:+1"));
        drop(g1);
        assert!(reg.would_reach("signal:+1")); // g2 still holds it
        drop(g2);
        assert!(!reg.would_reach("signal:+1"));
    }

    #[tokio::test]
    async fn test_subscriber_wait_for() {
        let reg = Arc::new(SubscriberRegistry::default());

        // Already subscribed → immediate
        let g = reg.register("x:*");
        assert!(reg.wait_for("x:1", std::time::Duration::from_millis(10)).await);
        drop(g);

        // Timeout
        assert!(!reg.wait_for("y:1", std::time::Duration::from_millis(50)).await);

        // Subscribe mid-wait
        let reg2 = reg.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            // Guard held to end of task — long enough for the waiter to observe.
            let _g = reg2.register("z:*");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });
        assert!(reg.wait_for("z:1", std::time::Duration::from_millis(200)).await);
    }
}
