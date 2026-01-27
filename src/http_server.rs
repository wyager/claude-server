use std::convert::Infallible;
use std::sync::Arc;

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

use crate::core_loop::HarnessEvent;
use crate::db::Database;

/// Broadcast message sent from the core loop to SSE subscribers.
#[derive(Debug, Clone)]
pub enum BroadcastMsg {
    Message {
        chat_id: String,
        content: String,
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
}

// ---- Request/Response types ----

#[derive(Deserialize)]
struct MessageRequest {
    chat_id: Option<String>,
    user: String,
    content: String,
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

#[derive(Deserialize)]
struct HistoryQuery {
    limit: Option<usize>,
}

#[derive(Serialize)]
struct HistoryResponse {
    entries: Vec<serde_json::Value>,
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
                    id,
                    ref created_at,
                }) if *msg_chat_id == chat_id => {
                    let data = serde_json::json!({
                        "id": id,
                        "content": content,
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
) -> Router {
    let state = AppState {
        event_tx,
        db,
        broadcast_tx,
    };

    Router::new()
        .route("/message", post(handle_message))
        .route("/status", get(handle_status))
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
