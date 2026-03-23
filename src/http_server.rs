use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::Json;
use axum::routing::{get, post};
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
use crate::types::TokenAccumulator;

/// Broadcast message sent from the core loop to SSE subscribers.
#[derive(Debug, Clone)]
pub enum BroadcastMsg {
    Message {
        chat_id: String,
        content: String,
        attachments: Vec<String>,
        id: i64,
        created_at: String,
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
}

// ---- Request/Response types ----

#[derive(Deserialize)]
struct MessageRequest {
    chat_id: Option<String>,
    user: String,
    content: String,
    #[serde(default)]
    attachments: Vec<String>,
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
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(MessageResponse {
        status: "queued".to_string(),
        chat_id,
    }))
}

async fn handle_status() -> Json<StatusResponse> {
    Json(StatusResponse {
        status: "running".to_string(),
        model: "claude-server".to_string(),
    })
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
        .event_tx
        .send(HarnessEvent::Shutdown)
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
}

async fn handle_event(
    State(state): State<AppState>,
    Json(body): Json<EventRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let priority = body.priority.unwrap_or(5);

    state
        .event_tx
        .send(HarnessEvent::ExternalEvent {
            source: body.source,
            event_type: body.event_type,
            data: body.data,
            priority,
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(serde_json::json!({
        "status": "queued",
    })))
}

// ---- SSE Handler ----

async fn handle_sse(
    State(state): State<AppState>,
    Path(chat_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.broadcast_tx.subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(move |msg| {
            match msg {
                Ok(BroadcastMsg::Message {
                    chat_id: ref msg_chat_id,
                    ref content,
                    ref attachments,
                    id,
                    ref created_at,
                }) if *msg_chat_id == chat_id => {
                    let data = serde_json::json!({
                        "id": id,
                        "content": content,
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
) -> Router {
    let state = AppState {
        event_tx,
        db,
        broadcast_tx,
        token_accumulator,
        config,
    };

    Router::new()
        .route("/message", post(handle_message))
        .route("/event", post(handle_event))
        .route("/status", get(handle_status))
        .route("/cost", get(handle_cost))
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
