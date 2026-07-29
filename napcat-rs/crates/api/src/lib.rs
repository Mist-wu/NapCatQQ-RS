//! Public HTTP and WebSocket API surface.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use napcat_message::Message as NapMessage;
use napcat_protocol::{ProtocolEvent, ProtocolResult};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, RwLock},
    time::{sleep, Duration},
};

/// API state shared across HTTP/WebSocket routes.
#[derive(Clone)]
pub struct ApiState {
    events: broadcast::Sender<ProtocolEvent>,
    runtime_running: Arc<RwLock<bool>>,
}

impl ApiState {
    /// Create a default API state.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            events,
            runtime_running: Arc::new(RwLock::new(false)),
        }
    }

    /// Build application router.
    pub fn router(self) -> Router {
        Router::new()
            .route("/login/status", get(login_status))
            .route("/message/send", post(send_message))
            .route("/message/listen", get(listen_messages))
            .route("/groups", get(list_groups))
            .route("/users", get(list_users))
            .route("/ws", get(ws_upgrade))
            .with_state(self)
    }
}

/// Login status payload.
#[derive(Debug, Serialize)]
pub struct LoginStatusResponse {
    /// Whether service runtime is online.
    pub online: bool,
    /// Informational text.
    pub message: String,
}

/// Send request payload.
#[derive(Debug, Deserialize, Serialize)]
pub struct SendRequest {
    /// Message to send.
    pub message: NapMessage,
}

/// Send response payload.
#[derive(Debug, Serialize)]
pub struct SendResponse {
    /// Accepted flag.
    pub accepted: bool,
}

async fn login_status(State(state): State<ApiState>) -> Json<LoginStatusResponse> {
    let running = *state.runtime_running.read().await;
    Json(LoginStatusResponse {
        online: running,
        message: if running {
            "runtime running".to_string()
        } else {
            "runtime not started".to_string()
        },
    })
}

async fn send_message(
    State(state): State<ApiState>,
    Json(payload): Json<SendRequest>,
) -> (StatusCode, Json<SendResponse>) {
    let event = ProtocolEvent::MessageReceived {
        message: payload.message,
    };
    let _ = state.events.send(event);
    (
        StatusCode::OK,
        Json(SendResponse {
            accepted: true,
        }),
    )
}

#[derive(Debug, Serialize)]
struct Group {
    id: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct User {
    id: String,
    nickname: String,
}

async fn list_groups() -> Json<Vec<Group>> {
    Json(vec![
        Group {
            id: "g1".to_string(),
            name: "devops".to_string(),
        },
        Group {
            id: "g2".to_string(),
            name: "design".to_string(),
        },
    ])
}

async fn list_users() -> Json<Vec<User>> {
    Json(vec![
        User {
            id: "u1".to_string(),
            nickname: "alice".to_string(),
        },
        User {
            id: "u2".to_string(),
            nickname: "bob".to_string(),
        },
    ])
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| ws_handler(socket, state))
}

async fn ws_handler(mut socket: WebSocket, state: ApiState) {
    let mut rx = state.events.subscribe();
    loop {
        match rx.recv().await {
            Ok(event) => {
                if let Ok(text) = serde_json::to_string(&event) {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
            }
            Err(_error) => {
                break;
            }
        }
    }
}

async fn listen_messages(
    State(state): State<ApiState>,
) -> Json<Vec<String>> {
    let mut events: Vec<String> = Vec::new();
    let mut rx = state.events.subscribe();
    for _ in 0..2 {
        if let Ok(event) = rx.recv().await {
            if let Ok(payload) = serde_json::to_string(&event) {
                events.push(payload);
            }
        }
        sleep(Duration::from_millis(5)).await;
    }

    Json(events)
}

pub async fn run(addr: &str) -> ProtocolResult<()> {
    let state = ApiState::new();
    let app = state.router();
    let socket_addr = addr.parse::<SocketAddr>();
    let socket_addr = match socket_addr {
        Ok(value) => value,
        Err(error) => {
            return Err(napcat_protocol::ProtocolError::Transport(error.to_string()));
        }
    };
    let listener = tokio::net::TcpListener::bind(socket_addr)
        .await
        .map_err(|err| napcat_protocol::ProtocolError::Transport(err.to_string()))?;
    tracing::info!(%socket_addr, "start api server");
    axum::serve(listener, app)
        .await
        .map_err(|err| napcat_protocol::ProtocolError::Transport(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn api_status_route_returns_default_state() {
        let state = ApiState::new();
        let app = state.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/login/status")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("request should pass");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn api_send_route_returns_accepted() {
        let state = ApiState::new();
        let app = state.router();
        let req_message = SendRequest {
            message: NapMessage::text(
                "m",
                "sender",
                napcat_message::MessageRecipient::Private {
                    user_id: "u".to_string(),
                },
                "hi",
            ),
        };
        let payload = serde_json::to_string(&req_message).expect("payload serialize");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/message/send")
                    .header("content-type", "application/json")
                    .body(Body::from(payload))
                    .expect("valid request"),
            )
            .await
            .expect("request should pass");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
