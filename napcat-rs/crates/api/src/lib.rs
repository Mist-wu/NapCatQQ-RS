//! Public HTTP and WebSocket API surface.

use std::{
    collections::HashSet,
    fmt,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
use napcat_event::{EventBus, EventEnvelope};
use napcat_message::{Message as NapMessage, MessageRecipient};
use napcat_plugin::{
    PluginBackendKind, PluginDefinition, PluginEvent, PluginManager, PluginMetadata,
};
use napcat_protocol::{
    ProtocolBackend, ProtocolError, ProtocolEvent, ProtocolResult,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock, broadcast, mpsc},
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
    events: EventBus<ProtocolEvent>,
    protocol_events: broadcast::Sender<ProtocolEvent>,
    plugin_manager: Arc<PluginManager>,
    dispatch_tx: mpsc::Sender<ProtocolEvent>,
    protocol: Option<Arc<dyn ProtocolBackend>>,
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

/// OneBot-compatible friend payload.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct FriendInfo {
    /// User id.
    pub user_id: String,
    /// Friend display name.
    pub nickname: String,
    /// Optional remark set by current account.
    pub remark: String,
}

/// Delete message request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteMsgRequest {
    /// Message id to delete.
    pub message_id: String,
}

/// Delete message response payload.
#[derive(Debug, Serialize)]
pub struct DeleteMsgResponse {
    /// Deleted message id.
    pub message_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PluginLoadRequest {
    /// Full plugin definition.
    pub definition: PluginDefinition,
}

#[derive(Debug, Serialize)]
pub struct PluginLoadResult {
    /// Loaded plugin name.
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PluginUnloadRequest {
    /// Plugin unique name.
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct PluginListResponse {
    /// Loaded plugin names.
    pub plugins: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PluginMetadataRequest {
    /// Plugin unique name.
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct PluginMetadataResponse {
    /// Plugin metadata.
    pub metadata: PluginMetadata,
}

#[derive(Debug, Serialize)]
pub struct PluginStatusResponse {
    /// Runtime running status.
    pub running: bool,
    /// Loaded plugin count.
    pub plugin_count: usize,
}

#[derive(Debug, Serialize)]
pub struct PluginKindItem {
    /// Plugin unique name.
    pub name: String,
    /// Runtime backend kind.
    pub kind: PluginBackendKind,
}

#[derive(Debug, Serialize)]
pub struct PluginKindsResponse {
    /// Loaded plugin runtime kinds.
    pub kinds: Vec<PluginKindItem>,
}

/// Group info request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetGroupInfoRequest {
    /// Target group id.
    pub group_id: String,
    /// Whether to ignore cached group info.
    #[serde(default)]
    pub no_cache: bool,
}

/// Group info response payload.
#[derive(Debug, Serialize)]
pub struct GroupInfoResponse {
    /// Group id.
    pub group_id: String,
    /// Group display name.
    pub group_name: String,
    /// Estimated member count.
    pub member_count: usize,
    /// Capacity value for compatibility with OneBot-like response shape.
    pub max_member_count: usize,
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
    /// Poll timeout in seconds (compatibility alias).
    timeout: Option<u64>,
    /// Max events to collect.
    max_events: Option<usize>,
    /// Max events to collect (compatibility alias).
    limit: Option<usize>,
    /// Filter by OneBot event type (comma-separated).
    #[serde(rename = "type")]
    #[serde(alias = "types")]
    type_filter: Option<String>,
    /// Compatibility alias for `type`.
    post_type: Option<String>,
}

/// API-level error.
#[derive(Debug)]
pub enum ApiError {
    /// Bad client payload.
    InvalidRequest(String),
    /// Event forwarding failed.
    EventDispatch(String),
    /// Protocol backend send failed.
    ProtocolSend(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::InvalidRequest(message) => write!(f, "invalid request: {message}"),
            ApiError::EventDispatch(message) => write!(f, "event dispatch failed: {message}"),
            ApiError::ProtocolSend(message) => write!(f, "protocol send failed: {message}"),
        }
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, message) = match self {
            ApiError::InvalidRequest(message) => (StatusCode::BAD_REQUEST, message),
            ApiError::EventDispatch(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            ApiError::ProtocolSend(message) => (StatusCode::BAD_GATEWAY, message),
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
        Self::with_protocol(None)
    }

    /// Create a state with optional protocol backend.
    pub fn with_protocol(protocol: Option<Arc<dyn ProtocolBackend>>) -> Self {
        let events = EventBus::new(EVENT_BROADCAST_CAPACITY);
        let (protocol_events, mut protocol_rx) = broadcast::channel(EVENT_BROADCAST_CAPACITY);
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel(EVENT_DISPATCH_CAPACITY);
        let plugin_manager = Arc::new(PluginManager::new());
        let broadcaster = events.clone();
        tokio::spawn(async move {
            while let Some(event) = dispatch_rx.recv().await {
                let _ = broadcaster.publish(EventEnvelope::new("api", "protocol-event", event));
            }
        });
        let protocol_relay = events.clone();
        let plugin_manager_for_events = plugin_manager.clone();
        tokio::spawn(async move {
            while let Ok(event) = protocol_rx.recv().await {
                if let ProtocolEvent::MessageReceived { message, .. } = &event {
                    let plugin_event = PluginEvent::Message {
                        payload: serde_json::to_value(message)
                            .unwrap_or_else(|_| serde_json::json!({})),
                        source: Some(String::from("protocol")),
                    };
                    let _ = plugin_manager_for_events
                        .dispatch(plugin_event)
                        .await;
                }
                let _ = protocol_relay.publish(EventEnvelope::new("protocol", "event", event));
            }
        });

        Self {
            events,
            protocol_events,
            plugin_manager,
            dispatch_tx,
            protocol,
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
            .route("/delete_msg", post(delete_msg))
            .route("/get_group_info", post(get_group_info))
            .route("/get_group_list", get(list_groups))
            .route("/get_friend_list", get(get_friend_list))
            .route("/message/listen", get(listen_messages))
            .route("/get_events", get(listen_messages))
            .route("/groups", get(list_groups))
            .route("/users", get(list_users))
            .route("/plugin/load", post(plugin_load))
            .route("/plugin/unload", post(plugin_unload))
            .route("/plugin/list", get(plugin_list))
            .route("/plugin/kinds", get(plugin_kinds))
            .route("/plugin/metadata", post(plugin_metadata))
            .route("/plugin/status", get(plugin_status))
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

    /// Broadcast sender for protocol event stream.
    fn protocol_event_sender(&self) -> broadcast::Sender<ProtocolEvent> {
        self.protocol_events.clone()
    }

    fn plugin_manager(&self) -> Arc<PluginManager> {
        self.plugin_manager.clone()
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

fn compatibility_default_friends() -> Vec<FriendInfo> {
    vec![
        FriendInfo {
            user_id: String::from("u1"),
            nickname: String::from("alice"),
            remark: String::from(""),
        },
        FriendInfo {
            user_id: String::from("u2"),
            nickname: String::from("bob"),
            remark: String::from(""),
        },
    ]
}

fn friend_payload_from_users(users: Vec<UserInfo>) -> Vec<FriendInfo> {
    users
        .into_iter()
        .map(|user| FriendInfo {
            user_id: user.user_id,
            nickname: user.nickname,
            remark: String::new(),
        })
        .collect()
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

async fn delete_msg(
    State(state): State<ApiState>,
    Json(payload): Json<DeleteMsgRequest>,
) -> ApiResult<Json<ApiEnvelope<DeleteMsgResponse>>> {
    if payload.message_id.trim().is_empty() {
        return Err(ApiError::InvalidRequest(String::from(
            "message_id is required",
        )));
    }

    state
        .emit_event(ProtocolEvent::Warning {
            message: format!("delete message requested: {}", payload.message_id),
        })
        .await?;

    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: DeleteMsgResponse {
            message_id: payload.message_id,
        },
        message: None,
    }))
}

async fn get_group_info(
    State(state): State<ApiState>,
    Json(payload): Json<GetGroupInfoRequest>,
) -> ApiResult<Json<ApiEnvelope<GroupInfoResponse>>> {
    let requested_group_id = payload.group_id.trim().to_string();
    if requested_group_id.is_empty() {
        return Err(ApiError::InvalidRequest(String::from(
            "group_id is required",
        )));
    }

    let runtime_groups = state.groups().await;
    let fallback_groups = if payload.no_cache {
        None
    } else {
        Some(compatibility_default_groups())
    };
    let candidate = runtime_groups
        .iter()
        .find(|group| group.group_id == requested_group_id)
        .or_else(|| {
            fallback_groups.as_ref().and_then(|fallback| {
                fallback
                    .iter()
                    .find(|group| group.group_id == requested_group_id)
            })
        });

    if let Some(group) = candidate {
        return Ok(Json(ApiEnvelope {
            status: String::from("ok"),
            retcode: 0,
            data: GroupInfoResponse {
                group_id: group.group_id.clone(),
                group_name: group.group_name.clone(),
                member_count: 0,
                max_member_count: 200,
            },
            message: None,
        }));
    }

    Err(ApiError::InvalidRequest(format!(
        "group not found: {requested_group_id}"
    )))
}

async fn get_friend_list(
    State(state): State<ApiState>,
) -> ApiResult<Json<ApiEnvelope<Vec<FriendInfo>>>> {
    let friends = state.users().await;
    let payload = if friends.is_empty() {
        compatibility_default_friends()
    } else {
        friend_payload_from_users(friends)
    };

    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: payload,
        message: None,
    }))
}

async fn plugin_load(
    State(state): State<ApiState>,
    Json(payload): Json<PluginLoadRequest>,
) -> ApiResult<Json<ApiEnvelope<PluginLoadResult>>> {
    let name = state
        .plugin_manager()
        .load(payload.definition)
        .await
        .map_err(|error| ApiError::ProtocolSend(error.to_string()))?;

    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: PluginLoadResult { name },
        message: None,
    }))
}

async fn plugin_unload(
    State(state): State<ApiState>,
    Json(payload): Json<PluginUnloadRequest>,
) -> ApiResult<Json<ApiEnvelope<EmptyData>>> {
    state
        .plugin_manager()
        .unload(&payload.name)
        .await
        .map_err(|error| ApiError::ProtocolSend(error.to_string()))?;

    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: EmptyData,
        message: None,
    }))
}

async fn plugin_list(State(state): State<ApiState>) -> ApiResult<Json<ApiEnvelope<PluginListResponse>>> {
    let plugins = state.plugin_manager().list().await;
    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: PluginListResponse { plugins },
        message: None,
    }))
}

async fn plugin_kinds(State(state): State<ApiState>) -> ApiResult<Json<ApiEnvelope<PluginKindsResponse>>> {
    let kinds = state
        .plugin_manager()
        .kinds()
        .await
        .into_iter()
        .map(|(name, kind)| PluginKindItem { name, kind })
        .collect();

    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: PluginKindsResponse { kinds },
        message: None,
    }))
}

async fn plugin_metadata(
    State(state): State<ApiState>,
    Json(payload): Json<PluginMetadataRequest>,
) -> ApiResult<Json<ApiEnvelope<PluginMetadataResponse>>> {
    let metadata = state
        .plugin_manager()
        .metadata(&payload.name)
        .await
        .map_err(|error| ApiError::ProtocolSend(error.to_string()))?;

    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: PluginMetadataResponse { metadata },
        message: None,
    }))
}

async fn plugin_status(State(state): State<ApiState>) -> ApiResult<Json<ApiEnvelope<PluginStatusResponse>>> {
    let plugins = state.plugin_manager().list().await;
    let running = state.is_runtime_running().await;
    Ok(Json(ApiEnvelope {
        status: String::from("ok"),
        retcode: 0,
        data: PluginStatusResponse {
            running,
            plugin_count: plugins.len(),
        },
        message: None,
    }))
}

async fn listen_messages(
    State(state): State<ApiState>,
    Query(query): Query<ListenQuery>,
) -> Json<ApiEnvelope<Vec<String>>> {
    let timeout_ms = query
        .timeout_ms
        .or_else(|| query.timeout.map(|value| value.saturating_mul(1000)))
        .unwrap_or(200);
    let max_events = query.max_events.or(query.limit).unwrap_or(8).clamp(1, 32);
    let mut rx = state.events.subscribe();
    let event_types = parse_event_types(&query);
    let mut records = Vec::with_capacity(max_events);

    for _ in 0..max_events {
        let event_result = timeout(Duration::from_millis(timeout_ms), rx.recv()).await;
        match event_result {
            Ok(Ok(envelope)) => {
                let event = envelope.payload;
                if let Some(accepted_types) = &event_types && !accepted_types.contains(onebot_event_post_type(&event)) {
                    continue;
                }
                if let Ok(serialized) = onebot_event_payload(&event) {
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

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
    Query(query): Query<ListenQuery>,
) -> impl IntoResponse {
    let event_types = parse_event_types(&query);
    ws.on_upgrade(move |socket| ws_handler(socket, state, event_types))
}

async fn ws_handler(
    mut socket: WebSocket,
    state: ApiState,
    event_types: Option<HashSet<String>>,
) {
    let mut rx = state.events.subscribe();
    while let Ok(envelope) = rx.recv().await {
        if let Some(accepted_types) = &event_types
            && !accepted_types.contains(onebot_event_post_type(&envelope.payload))
        {
            continue;
        }
        if let Ok(serialized) = onebot_event_payload(&envelope.payload)
            && socket.send(Message::Text(serialized.into())).await.is_err()
        {
            break;
        }
    }
}

fn onebot_event_payload(event: &ProtocolEvent) -> ProtocolResult<String> {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ProtocolError::Transport(error.to_string()))?
        .as_secs();

    let payload = match event {
        ProtocolEvent::MessageReceived { message } => {
            let (message_type, sub_type, target_id) = match &message.recipient {
                MessageRecipient::Private { user_id } => ("private", "friend", user_id.as_str()),
                MessageRecipient::Group { group_id } => ("group", "normal", group_id.as_str()),
            };

            serde_json::json!({
                "post_type": "message",
                "time": time,
                "self_id": "napcat-bot",
                "message_type": message_type,
                "sub_type": sub_type,
                "message_id": message.id,
                "group_id": match &message.recipient {
                    MessageRecipient::Group { group_id } => Some(group_id.as_str()),
                    MessageRecipient::Private { .. } => None,
                },
                "user_id": message.sender_id,
                "sender": {
                    "user_id": message.sender_id,
                },
                "target_id": target_id,
                "message": message.elements,
            })
        }
        ProtocolEvent::Connected { endpoint } => serde_json::json!({
            "post_type": "meta_event",
            "time": time,
            "self_id": "napcat-bot",
            "meta_event_type": "lifecycle",
            "sub_type": "connected",
            "endpoint": endpoint,
            "status": "ok",
        }),
        ProtocolEvent::Disconnected => serde_json::json!({
            "post_type": "meta_event",
            "time": time,
            "self_id": "napcat-bot",
            "meta_event_type": "lifecycle",
            "sub_type": "disconnected",
            "status": "close",
        }),
        ProtocolEvent::Warning { message } => serde_json::json!({
            "post_type": "meta_event",
            "time": time,
            "self_id": "napcat-bot",
            "meta_event_type": "warning",
            "sub_type": "protocol_warning",
            "message": message,
            "status": "warn",
        }),
    };

    serde_json::to_string(&payload).map_err(|error| ProtocolError::Serialization(error.to_string()))
}

fn parse_event_types(query: &ListenQuery) -> Option<HashSet<String>> {
    let raw = query.type_filter.as_ref().or(query.post_type.as_ref())?;

    let types = raw
        .split(',')
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<HashSet<_>>();

    if types.is_empty() {
        None
    } else {
        Some(types)
    }
}

fn onebot_event_post_type(event: &ProtocolEvent) -> &'static str {
    match event {
        ProtocolEvent::MessageReceived { .. } => "message",
        ProtocolEvent::Connected { .. } => "meta_event",
        ProtocolEvent::Disconnected => "meta_event",
        ProtocolEvent::Warning { .. } => "meta_event",
    }
}

async fn push_send_event(
    state: &ApiState,
    message: NapMessage,
) -> ApiResult<Json<ApiEnvelope<SendResponse>>> {
    if let Some(protocol) = &state.protocol {
        protocol
            .send_message(message.clone())
            .await
            .map_err(|error| ApiError::ProtocolSend(format!("protocol send failed: {error}")))?;
    }

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
    run_with_state(addr, ApiState::new()).await
}

/// Start API server with protocol backend and pre-created state.
pub async fn run_with_protocol(
    addr: &str,
    protocol: Option<Arc<dyn ProtocolBackend>>,
) -> ProtocolResult<()> {
    run_with_state(addr, ApiState::with_protocol(protocol)).await
}

/// Start API server with a pre-created state.
pub async fn run_with_state(addr: &str, state: ApiState) -> ProtocolResult<()> {
    state.set_runtime_running(true).await;
    let protocol = state.protocol.clone();
    let socket_addr = match addr.parse::<SocketAddr>() {
        Ok(socket_addr) => socket_addr,
        Err(err) => {
            state.set_runtime_running(false).await;
            return Err(ProtocolError::Transport(err.to_string()));
        }
    };

    if let Some(protocol) = protocol.clone() {
        if let Err(error) = protocol
            .listen(state.protocol_event_sender())
            .await
            .map_err(|error| {
                ProtocolError::Transport(format!("protocol listen failed: {error}"))
            })
        {
            state.set_runtime_running(false).await;
            return Err(error);
        }
    }

    let app = state.clone().router();
    let listener = match tokio::net::TcpListener::bind(socket_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            state.set_runtime_running(false).await;
            if let Some(protocol) = protocol {
                let _ = protocol.disconnect().await;
            }
            return Err(ProtocolError::Transport(err.to_string()));
        }
    };

    let serve_result = axum::serve(listener, app)
        .await
        .map_err(|err| ProtocolError::Transport(err.to_string()));
    state.set_runtime_running(false).await;
    if let Some(protocol) = protocol {
        let _ = protocol.disconnect().await;
    }
    match serve_result {
        Ok(()) => Ok(()),
        Err(error) => Err(error),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn api_status_route_returns_default_state() {
        let state = ApiState::new();
        let app = state.clone().router();

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
    async fn api_status_route_reports_runtime_running_state() {
        let state = ApiState::new();
        state.set_runtime_running(true).await;
        let app = state.clone().router();

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

        let status = response.status();
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("login status body");
        let envelope: serde_json::Value =
            serde_json::from_slice(&body).expect("login status payload");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(envelope["data"]["online"].as_bool(), Some(true));
        assert_eq!(
            envelope["data"]["message"].as_str(),
            Some("runtime running")
        );
    }

    #[tokio::test]
    async fn api_send_route_emits_event() {
        let state = ApiState::new();
        let app = state.clone().router();
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
        let app = state.clone().router();
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
    async fn api_get_events_route_is_alias_of_message_listen() {
        let state = ApiState::new();
        let app = state.clone().router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/get_events?timeout_ms=1&max_events=1")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("get_events request should pass");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("get_events body");
        let envelope: serde_json::Value =
            serde_json::from_slice(&body).expect("get_events payload");
        assert_eq!(envelope["status"], "ok");
    }

    #[tokio::test]
    async fn api_get_events_route_filters_by_type() {
        let state = ApiState::new();
        let app = state.clone().router();

        state
            .emit_event(ProtocolEvent::Connected {
                endpoint: String::from("mock://endpoint"),
            })
            .await
            .expect("emit meta event");
        state
            .emit_event(ProtocolEvent::MessageReceived {
                message: NapMessage::text(
                    "msg-filter",
                    "sender",
                    MessageRecipient::Private {
                        user_id: "u-1".to_string(),
                    },
                    "hello",
                ),
            })
            .await
            .expect("emit message event");

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/get_events?type=message&max_events=8&timeout_ms=50")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("get_events request should pass");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 2048)
            .await
            .expect("get_events body");
        let envelope: serde_json::Value =
            serde_json::from_slice(&body).expect("get_events payload");
        let data = envelope["data"].as_array().expect("event list");
        assert_eq!(data.len(), 1);
        let event = serde_json::from_str::<serde_json::Value>(data[0].as_str().expect("event body"))
            .expect("event json parse");
        assert_eq!(event["post_type"], "message");
    }

    #[tokio::test]
    async fn api_get_group_list_route_is_alias_of_groups() {
        let state = ApiState::new();
        let app = state.clone().router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/get_group_list")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("get_group_list request should pass");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("get_group_list body");
        let envelope: serde_json::Value =
            serde_json::from_slice(&body).expect("get_group_list payload");
        let data = envelope["data"].as_array().expect("groups array");
        assert!(!data.is_empty());
    }

    #[tokio::test]
    async fn api_get_events_route_supports_compatibility_limit_alias() {
        let state = ApiState::new();
        let app = state.clone().router();

        state
            .emit_event(ProtocolEvent::MessageReceived {
                message: NapMessage::text(
                    "msg-limit-1",
                    "sender",
                    MessageRecipient::Private {
                        user_id: "u-1".to_string(),
                    },
                    "hello",
                ),
            })
            .await
            .expect("emit message event");
        state
            .emit_event(ProtocolEvent::MessageReceived {
                message: NapMessage::text(
                    "msg-limit-2",
                    "sender",
                    MessageRecipient::Private {
                        user_id: "u-2".to_string(),
                    },
                    "world",
                ),
            })
            .await
            .expect("emit message event");

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/get_events?limit=1&timeout_ms=50")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("get_events request should pass");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 2048)
            .await
            .expect("get_events body");
        let envelope: serde_json::Value =
            serde_json::from_slice(&body).expect("get_events payload");
        let data = envelope["data"].as_array().expect("event list");
        assert_eq!(data.len(), 1);
    }

    #[tokio::test]
    async fn api_get_friend_list_exposes_compatibility_remark_field() {
        let state = ApiState::new();
        let app = state.clone().router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/get_friend_list")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("get_friend_list request should pass");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("get_friend_list body");
        let envelope: serde_json::Value =
            serde_json::from_slice(&body).expect("get_friend_list payload");
        let first = envelope["data"][0].clone();
        assert_eq!(first["user_id"].as_str(), Some("u1"));
        assert!(first.get("remark").is_some());
    }

    #[tokio::test]
    async fn api_plugin_load_list_and_unload_works() {
        use std::path::PathBuf;
        let state = ApiState::new();
        let app = state.clone().router();
        let load_request = PluginLoadRequest {
            definition: PluginDefinition {
                metadata: PluginMetadata::new("test-plugin", "0.1.0"),
                source: napcat_plugin::PluginSource::Rust {
                    executable: PathBuf::from("/bin/sh"),
                    args: vec![String::from("-c"), String::from("cat >/dev/null")],
                    timeout_ms: 200,
                },
                enabled: true,
            },
        };
        let load_payload = serde_json::to_string(&load_request).expect("payload serialize");

        let load_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/plugin/load")
                    .header("content-type", "application/json")
                    .body(Body::from(load_payload))
                    .expect("valid request"),
            )
            .await
            .expect("load request should pass");
        assert_eq!(load_response.status(), StatusCode::OK);

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/plugin/list")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("list request should pass");
        assert_eq!(list_response.status(), StatusCode::OK);

        let metadata_payload = serde_json::to_string(&PluginMetadataRequest {
            name: String::from("test-plugin"),
        })
        .expect("payload serialize");
        let metadata_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/plugin/metadata")
                    .header("content-type", "application/json")
                    .body(Body::from(metadata_payload))
                    .expect("valid request"),
            )
            .await
            .expect("metadata request should pass");
        assert_eq!(metadata_response.status(), StatusCode::OK);

        let status_response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/plugin/status")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("status request should pass");
        assert_eq!(status_response.status(), StatusCode::OK);

        let unload_payload = serde_json::to_string(&PluginUnloadRequest {
            name: String::from("test-plugin"),
        })
        .expect("payload serialize");
        let unload_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/plugin/unload")
                    .header("content-type", "application/json")
                    .body(Body::from(unload_payload))
                    .expect("valid request"),
            )
            .await
            .expect("unload request should pass");
        assert_eq!(unload_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn api_onebot_event_payload_from_message_event() {
        let message = napcat_message::Message::text(
            "msg-1",
            "sender-1",
            napcat_message::MessageRecipient::Group {
                group_id: String::from("group-1"),
            },
            "hello",
        );
        let payload =
            onebot_event_payload(&ProtocolEvent::MessageReceived { message }).expect("event payload");
        let parsed: serde_json::Value = serde_json::from_str(&payload).expect("json payload");

        assert_eq!(parsed["post_type"], "message");
        assert_eq!(parsed["message_type"], "group");
        assert_eq!(parsed["message_id"], "msg-1");
        assert_eq!(parsed["group_id"], "group-1");
        assert_eq!(parsed["user_id"], "sender-1");
    }

    #[tokio::test]
    async fn api_group_and_user_route_works() {
        let state = ApiState::new();
        let app = state.clone().router();

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

    #[tokio::test]
    async fn api_run_with_state_invalid_address_resets_runtime() {
        let state = ApiState::new();
        let result = run_with_state("invalid:addr", state.clone()).await;

        assert!(result.is_err());
        assert!(!state.is_runtime_running().await);
    }
}
