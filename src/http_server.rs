use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tower_http::cors::CorsLayer;

use crate::core_loop::HarnessEvent;
use crate::db::Database;

#[derive(Clone)]
struct AppState {
    event_tx: mpsc::UnboundedSender<HarnessEvent>,
    db: Arc<Database>,
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

// ---- Router ----

pub fn create_router(
    event_tx: mpsc::UnboundedSender<HarnessEvent>,
    db: Arc<Database>,
) -> Router {
    let state = AppState { event_tx, db };

    Router::new()
        .route("/message", post(handle_message))
        .route("/status", get(handle_status))
        .route("/messages/{chat_id}", get(handle_get_messages))
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
