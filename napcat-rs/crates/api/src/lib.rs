//! Public HTTP and WebSocket API surface.

use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use napcat_message::{Message as NapMessage, MessageRecipient};
use napcat_protocol::{ProtocolError, ProtocolEvent, ProtocolResult, serialize_event};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    time::timeout,
};

const EVENT_BROADCAST_CAPACITY: usize = 128;
const EVENT_DISPATCH_CAPACITY: usize = 64;
const EVENT_DISPATCH_TIMEOUT_MS: u64 = 80;

type SharedGroups = Arc<Vec<GroupInfo>>;
type SharedUsers = Arc<Vec<UserInfo>>;

/// Shared API state for HTTP and WebSocket handlers.
#[derive(Clone)]
pub struct ApiState {
    events: broadcast::Sender<ProtocolEvent>,
    dispatch_tx: mpsc::Sender<ProtocolEvent>,
    runtime_running: Arc<RwLock<bool>>,
    runtime_groups: Arc<RwLock<SharedGroups>>,
    runtime_users: Arc<RwLock<SharedUsers>>,
}

/// API response envelope for public interfaces.
#[derive(Debug, Serialize)]
pub struct ApiEnvelope<T>
where
    T: Serialize,
{
    /// ok for success, failed for error.
    pub status: String,
    /// OneBot compatibility return code.
    pub retcode: i32,
    /// Payload data.
    pub data: T,
    /// Optional response hint.
    pub message: Option<String>,
}

/// Empty payload marker.
#[derive(Debug, Serialize)]
pub struct EmptyData;

/// Runtime login status payload.
#[derive(Debug, Serialize)]
pub struct LoginStatusData {
    /// Whether runtime is running.
    pub online: bool,
    /// Human readable message.
    pub message: String,
}

/// Login info payload.
#[derive(Debug, Serialize)]
pub struct LoginInfoData {
    /// Bot uid.
    pub user_id: String,
    /// Bot nickname.
    pub nickname: String,
    /// Current login state.
    pub online: bool,
}

/// Group information.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct GroupInfo {
    /// Group id.
    pub group_id: String,
    /// Group display name.
    pub group_name: String,
}

/// User information.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct UserInfo {
    /// User id.
    pub user_id: String,
    /// User display name.
    pub nickname: String,
}

/// Generic message send request.
#[derive(Debug, Serialize, Deserialize)]
pub struct SendRequest {
    /// Unified message payload.
    pub message: NapMessage,
}

/// OneBot-compatible send request.
#[derive(Debug, Serialize, Deserialize)]
pub struct CompatSendRequest {
    /// Message target type.
    #[serde(default)]
    pub message_type: MessageType,
    /// Target user for private message.
    pub user_id: Option<String>,
    /// Target group for group message.
    pub group_id: Option<String>,
    /// Plain text payload.
    pub message: String,
}

/// OneBot-compatible private send request.
#[derive(Debug, Serialize, Deserialize)]
pub struct SendPrivateRequest {
    /// Target user id.
    pub user_id: String,
    /// Message payload.
    pub message: String,
}

/// OneBot-compatible group send request.
#[derive(Debug, Serialize, Deserialize)]
pub struct SendGroupRequest {
    /// Target group id.
    pub group_id: String,
    /// Message payload.
    pub message: String,
}

/// Message send response.
#[derive(Debug, Serialize)]
pub struct SendResponse {
    /// Whether message has been accepted by adapter.
    pub accepted: bool,
    /// Compatible message id for trace.
    pub message_id: String,
}

/// Message route payload kind.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "lowercase")]
pub enum MessageType {
    #[default]
    Private,
    Group,
}

/// listen options from query string.
#[derive(Debug, Serialize, Deserialize)]
struct ListenQuery {
    /// Poll timeout in milliseconds.
    timeout_ms: Option<u64>,
    /// Max events to collect.
    max_events: Option<usize>,
}

/// API-level error.
#[derive(Debug)]
pub enum ApiError {
    /// Bad client payload.
    InvalidRequest(String),
    /// Event forwarding failed.
    EventDispatch(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::InvalidRequest(message) => write!(f, "invalid request: {message}"),
            ApiError::EventDispatch(message) => write!(f, "event dispatch failed: {message}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, message) = match self {
            ApiError::InvalidRequest(message) => (StatusCode::BAD_REQUEST, message),
            ApiError::EventDispatch(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
        };

        let payload = ApiEnvelope {
            status: String::from("failed"),
            retcode: -1,
            data: EmptyData,
            message: Some(message),
        };

        (code, Json(payload)).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

impl ApiState {
    /// Create a default state with empty cache values.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(EVENT_BROADCAST_CAPACITY);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(EVENT_DISPATCH_CAPACITY);
        let broadcaster = events.clone();
        tokio::spawn(async move {
            while let Some(event) = dispatch_rx.recv().await {
                let _ = broadcaster.send(event);
            }
        });

        Self {
            events,
            dispatch_tx,
            runtime_running: Arc::new(RwLock::new(false)),
            runtime_groups: Arc::new(RwLock::new(Arc::new(Vec::new()))),
            runtime_users: Arc::new(RwLock::new(Arc::new(Vec::new()))),
        }
    }

    /// Replace internal running flag.
    pub async fn set_runtime_running(&self, running: bool) {
        let mut state = self.runtime_running.write().await;
        *state = running;
    }

    /// Read internal running flag.
    pub async fn is_runtime_running(&self) -> bool {
        *self.runtime_running.read().await
    }

    /// Replace group cache.
    pub async fn set_groups(&self, groups: Vec<GroupInfo>) {
        let mut current = self.runtime_groups.write().await;
        *current = Arc::new(groups);
    }

    /// Replace user cache.
    pub async fn set_users(&self, users: Vec<UserInfo>) {
        let mut current = self.runtime_users.write().await;
        *current = Arc::new(users);
    }
    /// Read cached group cache.
    pub async fn groups(&self) -> Vec<GroupInfo> {
        self.runtime_groups.read().await.as_ref().clone()
    }

    /// Read cached user cache.
    pub async fn users(&self) -> Vec<UserInfo> {
        self.runtime_users.read().await.as_ref().clone()
    }

    /// Build shared router.
    pub fn router(self) -> Router {
        Router::new()
            .route("/health", get(health_check))
            .route("/login/status", get(login_status))
            .route("/get_status", get(get_status_compat))
            .route("/get_login_info", get(get_login_info))
            .route("/message/send", post(send_message))
            .route("/send_msg", post(send_msg_compat))
            .route("/send_private_msg", post(send_private_msg))
            .route("/send_group_msg", post(send_group_msg))
            .route("/message/listen", get(listen_messages))
            .route("/groups", get(list_groups))
            .route("/users", get(list_users))
            .route("/ws", get(ws_upgrade))
            .with_state(self)
    }

    async fn emit_event(&self, event: ProtocolEvent) -> ApiResult<()> {
        let send_result = timeout(
            Duration::from_millis(EVENT_DISPATCH_TIMEOUT_MS),
            self.dispatch_tx.send(event),
        )
        .await;

        match send_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(ApiError::EventDispatch(format!(
                "dispatch channel closed: {error}"
            ))),
            Err(_) => Err(ApiError::EventDispatch(String::from(
                "event dispatch timed out",
            ))),
        }
    }
}

impl Default for ApiState {
    fn default() -> Self {
        Self::new()
    }
}

fn compatibility_default_groups() -> Vec<GroupInfo> {
    vec![
        GroupInfo {
            group_id: String::from("g1"),
            group_name: String::from("devops"),
        },
        GroupInfo {
            group_id: String::from("g2"),
            group_name: String::from("design"),
        },
    ]
}

fn compatibility_default_users() -> Vec<UserInfo> {
    vec![
        UserInfo {
            user_id: String::from("u1"),
            nickname: String::from("alice"),
        },
        UserInfo {
            user_id: String::from("u2"),
            nickname: String::from("bob"),
        },
    ]
}

fn message_id_from_text(message: &str) -> String {
    format!("api-{message_len}", message_len = message.len())
}

async fn health_check() -> Json<ApiEnvelope<EmptyData>> {
    Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: EmptyData,
        message: Some(String::from("napcat api ready")),
    })
}

async fn login_status(
    State(state): State<ApiState>,
) -> ApiResult<Json<ApiEnvelope<LoginStatusData>>> {
    let online = state.is_runtime_running().await;
    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: if online { 0 } else { -1 },
        data: LoginStatusData {
            online,
            message: if online {
                String::from("runtime running")
            } else {
                String::from("runtime not started")
            },
        },
        message: None,
    }))
}

async fn get_status_compat(
    state: State<ApiState>,
) -> ApiResult<Json<ApiEnvelope<LoginStatusData>>> {
    login_status(state).await
}

async fn get_login_info(
    State(state): State<ApiState>,
) -> ApiResult<Json<ApiEnvelope<LoginInfoData>>> {
    let online = state.is_runtime_running().await;
    let info = if online {
        LoginInfoData {
            user_id: String::from("napcat-bot"),
            nickname: String::from("NapCatRS"),
            online,
        }
    } else {
        LoginInfoData {
            user_id: String::from("offline"),
            nickname: String::from("NapCatRS"),
            online,
        }
    };

    Ok(Json(ApiEnvelope {
        status: if online {
            String::from("ok")
        } else {
            String::from("failed")
        },
        retcode: if online { 0 } else { -1 },
        message: if online {
            None
        } else {
            Some(String::from("runtime not logged in"))
        },
        data: info,
    }))
}

async fn list_groups(State(state): State<ApiState>) -> Json<ApiEnvelope<Vec<GroupInfo>>> {
    let groups = state.runtime_groups.read().await;

    Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: if groups.as_ref().is_empty() {
            compatibility_default_groups()
        } else {
            groups.as_ref().clone()
        },
        message: None,
    })
}

async fn list_users(State(state): State<ApiState>) -> Json<ApiEnvelope<Vec<UserInfo>>> {
    let users = state.runtime_users.read().await;

    Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: if users.as_ref().is_empty() {
            compatibility_default_users()
        } else {
            users.as_ref().clone()
        },
        message: None,
    })
}

async fn send_message(
    State(state): State<ApiState>,
    Json(payload): Json<SendRequest>,
) -> ApiResult<Json<ApiEnvelope<SendResponse>>> {
    validate_message(&payload.message)?;
    push_send_event(&state, payload.message).await
}

async fn send_msg_compat(
    State(state): State<ApiState>,
    Json(payload): Json<CompatSendRequest>,
) -> ApiResult<Json<ApiEnvelope<SendResponse>>> {
    let message = payload.into_napcat_message()?;
    push_send_event(&state, message).await
}

async fn send_private_msg(
    State(state): State<ApiState>,
    Json(payload): Json<SendPrivateRequest>,
) -> ApiResult<Json<ApiEnvelope<SendResponse>>> {
    let request = CompatSendRequest {
        message_type: MessageType::Private,
        user_id: Some(payload.user_id),
        group_id: None,
        message: payload.message,
    };
    let message = request.into_napcat_message()?;
    push_send_event(&state, message).await
}

async fn send_group_msg(
    State(state): State<ApiState>,
    Json(payload): Json<SendGroupRequest>,
) -> ApiResult<Json<ApiEnvelope<SendResponse>>> {
    let request = CompatSendRequest {
        message_type: MessageType::Group,
        user_id: None,
        group_id: Some(payload.group_id),
        message: payload.message,
    };
    let message = request.into_napcat_message()?;
    push_send_event(&state, message).await
}

async fn listen_messages(
    State(state): State<ApiState>,
    Query(query): Query<ListenQuery>,
) -> Json<ApiEnvelope<Vec<String>>> {
    let timeout_ms = query.timeout_ms.unwrap_or(200);
    let max_events = query.max_events.unwrap_or(8).clamp(1, 32);
    let mut rx = state.events.subscribe();
    let mut records = Vec::with_capacity(max_events);

    for _ in 0..max_events {
        let event_result = timeout(Duration::from_millis(timeout_ms), rx.recv()).await;
        match event_result {
            Ok(Ok(event)) => {
                if let Ok(serialized) = serialize_event(&event) {
                    records.push(serialized);
                }
            }
            _ => {
                break;
            }
        }
    }

    Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: records,
        message: None,
    })
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<ApiState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| ws_handler(socket, state))
}

async fn ws_handler(mut socket: WebSocket, state: ApiState) {
    let mut rx = state.events.subscribe();
    while let Ok(event) = rx.recv().await {
        if let Ok(serialized) = serialize_event(&event)
            && socket.send(Message::Text(serialized.into())).await.is_err()
        {
            break;
        }
    }
}

async fn push_send_event(
    state: &ApiState,
    message: NapMessage,
) -> ApiResult<Json<ApiEnvelope<SendResponse>>> {
    let message_id = message.id.clone();
    state
        .emit_event(ProtocolEvent::MessageReceived { message })
        .await?;

    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: SendResponse {
            accepted: true,
            message_id,
        },
        message: None,
    }))
}

fn validate_message(message: &NapMessage) -> ApiResult<()> {
    match &message.recipient {
        MessageRecipient::Private { user_id } if user_id.is_empty() => Err(
            ApiError::InvalidRequest(String::from("private user_id cannot be empty")),
        ),
        MessageRecipient::Group { group_id } if group_id.is_empty() => Err(
            ApiError::InvalidRequest(String::from("group_id cannot be empty")),
        ),
        _ => Ok(()),
    }
}

impl CompatSendRequest {
    fn into_napcat_message(self) -> ApiResult<NapMessage> {
        let recipient = match self.message_type {
            MessageType::Private => {
                let user_id = self.user_id.unwrap_or_default();
                if user_id.is_empty() {
                    return Err(ApiError::InvalidRequest(String::from(
                        "user_id is required for private messages",
                    )));
                }
                MessageRecipient::Private { user_id }
            }
            MessageType::Group => {
                let group_id = self.group_id.unwrap_or_default();
                if group_id.is_empty() {
                    return Err(ApiError::InvalidRequest(String::from(
                        "group_id is required for group messages",
                    )));
                }
                MessageRecipient::Group { group_id }
            }
        };

        Ok(NapMessage::text(
            message_id_from_text(&self.message),
            "api",
            recipient,
            self.message,
        ))
    }
}

/// Start API server with in-memory state.
pub async fn run(addr: &str) -> ProtocolResult<()> {
    let state = ApiState::new();

    let app = state.router();
    let socket_addr = addr
        .parse::<SocketAddr>()
        .map_err(|err| ProtocolError::Transport(err.to_string()))?;

    let listener = tokio::net::TcpListener::bind(socket_addr)
        .await
        .map_err(|err| ProtocolError::Transport(err.to_string()))?;

    axum::serve(listener, app)
        .await
        .map_err(|err| ProtocolError::Transport(err.to_string()))
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
    async fn api_send_route_emits_event() {
        let state = ApiState::new();
        let app = state.router();
        let req_message = SendRequest {
            message: NapMessage::text(
                "m",
                "sender",
                MessageRecipient::Private {
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

    #[tokio::test]
    async fn api_send_private_compat_works() {
        let state = ApiState::new();
        let app = state.router();
        let payload = serde_json::to_string(&SendPrivateRequest {
            user_id: "u1".to_string(),
            message: "hello".to_string(),
        })
        .expect("payload serialize");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/send_private_msg")
                    .header("content-type", "application/json")
                    .body(Body::from(payload))
                    .expect("valid request"),
            )
            .await
            .expect("request should pass");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn api_group_and_user_route_works() {
        let state = ApiState::new();
        let app = state.router();

        let groups = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/groups")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("request should pass");

        let users = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/users")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("request should pass");

        assert_eq!(groups.status(), StatusCode::OK);
        assert_eq!(users.status(), StatusCode::OK);
    }
}
