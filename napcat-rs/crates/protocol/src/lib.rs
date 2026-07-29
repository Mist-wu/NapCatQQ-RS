//! Protocol abstraction layer and contracts.

use async_trait::async_trait;
use napcat_message::{
    Message, MessageElement, MessageHandler, MessageRecipient, MessageResult, encode_json,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    task::JoinHandle,
    time::sleep,
};

const DEFAULT_LISTEN_TIMEOUT_MS: u64 = 600;
const DEFAULT_LISTEN_MAX_EVENTS: usize = 8;
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 1500;

/// Protocol capabilities.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProtocolCapability {
    /// Message send and receive support.
    Message,
    /// Login status query.
    Login,
    /// User and group info access.
    Meta,
}

/// Protocol-level events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolEvent {
    /// Protocol connected.
    Connected {
        /// Endpoint id.
        endpoint: String,
    },
    /// Protocol disconnected.
    Disconnected,
    /// Incoming message.
    MessageReceived {
        /// Message payload.
        message: Message,
    },
    /// Protocol-level warning.
    Warning {
        /// Human text.
        message: String,
    },
}

/// Protocol errors.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Backend specific transport issue.
    #[error("transport failure: {0}")]
    Transport(String),

    /// Message decode/encode issue.
    #[error("message serialization failure: {0}")]
    Serialization(String),
}

/// Shared protocol result.
pub type ProtocolResult<T> = std::result::Result<T, ProtocolError>;

/// Protocol backend abstraction.
#[async_trait]
pub trait ProtocolBackend: Send + Sync {
    /// Human-readable backend name.
    fn name(&self) -> &'static str;

    /// Backends capabilities list.
    fn capabilities(&self) -> Vec<ProtocolCapability>;

    /// Connect backend.
    async fn connect(&self, endpoint: &str) -> ProtocolResult<()>;

    /// Disconnect backend.
    async fn disconnect(&self) -> ProtocolResult<()>;

    /// Login state query.
    async fn is_logged_in(&self) -> ProtocolResult<bool>;

    /// Send message through protocol.
    async fn send_message(&self, message: Message) -> ProtocolResult<()>;

    /// Emit protocol events.
    async fn listen(&self, notify: broadcast::Sender<ProtocolEvent>) -> ProtocolResult<()>;
}

/// In-memory mock backend for tests and adapters that don't need network.
#[derive(Clone)]
pub struct MockProtocol {
    connected: Arc<AtomicBool>,
    sender: mpsc::UnboundedSender<Message>,
}

impl MockProtocol {
    /// Create mock protocol pair (backend + event injector).
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Message>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                connected: Arc::new(AtomicBool::new(false)),
                sender: tx,
            },
            rx,
        )
    }

    /// Inject an incoming message into mock stream.
    pub fn inject(&self, message: Message) {
        let _ = self.sender.send(message);
    }
}

#[async_trait]
impl ProtocolBackend for MockProtocol {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn capabilities(&self) -> Vec<ProtocolCapability> {
        vec![ProtocolCapability::Message, ProtocolCapability::Login]
    }

    async fn connect(&self, endpoint: &str) -> ProtocolResult<()> {
        if endpoint.is_empty() {
            return Err(ProtocolError::Transport(String::from("empty endpoint")));
        }
        self.connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&self) -> ProtocolResult<()> {
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    async fn is_logged_in(&self) -> ProtocolResult<bool> {
        Ok(self.connected.load(Ordering::Acquire))
    }

    async fn send_message(&self, message: Message) -> ProtocolResult<()> {
        encode_json(&message).map_err(|err| ProtocolError::Serialization(err.to_string()))?;
        Ok(())
    }

    async fn listen(&self, notify: broadcast::Sender<ProtocolEvent>) -> ProtocolResult<()> {
        let _ = notify.send(ProtocolEvent::Connected {
            endpoint: String::from("mock://localhost"),
        });
        Ok(())
    }
}

/// Configuration for OneBot HTTP backend.
#[derive(Debug, Clone)]
pub struct OneBotBackendConfig {
    /// HTTP base URL of compatible OneBot server.
    pub base_url: String,
    /// Optional fixed access token header.
    pub access_token: Option<String>,
    /// Listener poll timeout in milliseconds.
    pub listen_timeout_ms: u64,
    /// Maximum listener batch size.
    pub listen_max_events: usize,
}

impl OneBotBackendConfig {
    /// Create a config from raw fields with defaults.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            access_token: None,
            listen_timeout_ms: DEFAULT_LISTEN_TIMEOUT_MS,
            listen_max_events: DEFAULT_LISTEN_MAX_EVENTS,
        }
    }

    /// Set HTTP access token.
    pub fn with_access_token(mut self, token: impl Into<String>) -> Self {
        self.access_token = Some(token.into());
        self
    }

    /// Set polling behavior.
    pub fn with_listener_settings(mut self, timeout_ms: u64, max_events: usize) -> Self {
        self.listen_timeout_ms = timeout_ms;
        self.listen_max_events = max_events;
        self
    }
}

impl Default for OneBotBackendConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            access_token: None,
            listen_timeout_ms: DEFAULT_LISTEN_TIMEOUT_MS,
            listen_max_events: DEFAULT_LISTEN_MAX_EVENTS,
        }
    }
}

/// Protocol backend implementation against OneBot-compatible HTTP/JSON endpoints.
#[derive(Clone)]
pub struct OneBotHttpBackend {
    base_url: String,
    access_token: Option<String>,
    client: Client,
    listen_timeout_ms: u64,
    listen_max_events: usize,
    connected: Arc<AtomicBool>,
    listening: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    listen_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl OneBotHttpBackend {
    /// Create backend from explicit config.
    pub fn new(config: OneBotBackendConfig) -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS))
            .user_agent("napcat-rs-onebot")
            .build()
            .unwrap_or_else(|error| {
                tracing::warn!(error = %error, "falling back to default reqwest client");
                Client::new()
            });

        Self {
            base_url: config.base_url,
            access_token: config.access_token,
            client,
            listen_timeout_ms: config.listen_timeout_ms,
            listen_max_events: config.listen_max_events,
            connected: Arc::new(AtomicBool::new(false)),
            listening: Arc::new(AtomicBool::new(false)),
            stop_requested: Arc::new(AtomicBool::new(false)),
            listen_task: Arc::new(Mutex::new(None)),
        }
    }

    /// Build a sanitized endpoint URL path.
    fn endpoint_url(&self, path: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        let sub = path.trim_start_matches('/');
        format!("{base}/{sub}")
    }

    /// Build request with optional Authorization header.
    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = self.endpoint_url(path);
        let mut req = self.client.request(method, url);
        if let Some(token) = &self.access_token {
            req = req.bearer_auth(token);
        }
        req
    }

    fn message_to_plain_text(message: &Message) -> String {
        let mut output = String::new();

        for element in &message.elements {
            match element {
                MessageElement::Text { text } => output.push_str(text),
                MessageElement::Image { path, .. } => {
                    output.push_str("[Image:");
                    output.push_str(path);
                    output.push(']');
                }
                MessageElement::File { path, .. } => {
                    output.push_str("[File:");
                    output.push_str(path);
                    output.push(']');
                }
                MessageElement::At { target_user } => {
                    output.push_str("[@");
                    output.push_str(target_user);
                    output.push(']');
                }
                MessageElement::Reply { message_id } => {
                    output.push_str("[Reply:");
                    output.push_str(message_id);
                    output.push(']');
                }
                MessageElement::Json { payload } => {
                    output.push_str(&payload.to_string());
                }
            }
        }

        if output.is_empty() {
            output.push_str(&message.sender_id);
            output.push_str(": ");
            output.push_str(&message.id);
        }

        output
    }

    fn map_status_payload(raw: Value) -> ProtocolResult<bool> {
        let data = raw.get("data").or(Some(&raw));
        if let Some(data_obj) = data {
            if let Some(online) = data_obj.get("online").and_then(Value::as_bool) {
                return Ok(online);
            }
        }
        Ok(false)
    }

    async fn get_status(&self) -> ProtocolResult<bool> {
        if self.base_url.trim().is_empty() {
            return Err(ProtocolError::Transport(String::from("empty base URL")));
        }

        let response = self
            .request(reqwest::Method::GET, "/get_status")
            .send()
            .await
            .map_err(|error| ProtocolError::Transport(error.to_string()))?;

        if !response.status().is_success() {
            return Err(ProtocolError::Transport(format!(
                "status request failed with {}",
                response.status()
            )));
        }

        let raw = response
            .json::<Value>()
            .await
            .map_err(|error| ProtocolError::Serialization(error.to_string()))?;

        Self::map_status_payload(raw)
    }
}

#[derive(Debug, Serialize)]
struct OneBotSendPrivate {
    /// target user id.
    user_id: String,
    /// message text.
    message: String,
}

#[derive(Debug, Serialize)]
struct OneBotSendGroup {
    /// target group id.
    group_id: String,
    /// message text.
    message: String,
}

#[derive(Debug)]
struct OneBotListenEnvelope {
    /// payload from /message/listen.
    data: Vec<Value>,
}

#[derive(Debug)]
enum ListenPayload {
    /// String serialized events.
    Text(String),
    /// Raw event object.
    Object(Value),
}

impl OneBotListenEnvelope {
    fn from_json(raw: Value) -> Self {
        let data = raw
            .get("data")
            .and_then(Value::as_array)
            .map_or_else(Vec::new, |items| items.clone());

        Self { data }
    }
}

#[async_trait]
impl ProtocolBackend for OneBotHttpBackend {
    fn name(&self) -> &'static str {
        "onebot-http"
    }

    fn capabilities(&self) -> Vec<ProtocolCapability> {
        vec![
            ProtocolCapability::Message,
            ProtocolCapability::Login,
            ProtocolCapability::Meta,
        ]
    }

    async fn connect(&self, endpoint: &str) -> ProtocolResult<()> {
        let normalized_endpoint = endpoint.trim();
        let base_url = if normalized_endpoint.is_empty() {
            self.base_url.trim()
        } else {
            normalized_endpoint
        };

        if base_url.is_empty() {
            return Err(ProtocolError::Transport(String::from("empty onebot endpoint")));
        }

        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            return Err(ProtocolError::Transport(String::from(
                "onebot endpoint must start with http:// or https://",
            )));
        }

        let online = self.get_status().await?;
        self.connected.store(true, Ordering::Release);
        self.stop_requested.store(false, Ordering::Release);
        tracing::info!(
            endpoint = %base_url,
            online,
            "connected to onebot-compatible protocol"
        );
        Ok(())
    }

    async fn disconnect(&self) -> ProtocolResult<()> {
        self.connected.store(false, Ordering::Release);
        self.stop_requested.store(true, Ordering::Release);
        self.listening.store(false, Ordering::Release);
        Ok(())
    }

    async fn is_logged_in(&self) -> ProtocolResult<bool> {
        if !self.connected.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.get_status().await
    }

    async fn send_message(&self, message: Message) -> ProtocolResult<()> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(ProtocolError::Transport(String::from("protocol is not connected")));
        }

        let payload = OneBotHttpBackend::message_to_plain_text(&message);
        if payload.trim().is_empty() {
            return Err(ProtocolError::Transport(String::from("message content is empty")));
        }

        let request = match &message.recipient {
            MessageRecipient::Private { user_id } => {
                self.request(reqwest::Method::POST, "/send_private_msg")
                    .json(&OneBotSendPrivate {
                        user_id: user_id.clone(),
                        message: payload,
                    })
            }
            MessageRecipient::Group { group_id } => {
                self.request(reqwest::Method::POST, "/send_group_msg")
                    .json(&OneBotSendGroup {
                        group_id: group_id.clone(),
                        message: payload,
                    })
            }
        };

        let response = request
            .send()
            .await
            .map_err(|error| ProtocolError::Transport(error.to_string()))?;

        if !response.status().is_success() {
            return Err(ProtocolError::Transport(format!(
                "send request rejected: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn listen(&self, notify: broadcast::Sender<ProtocolEvent>) -> ProtocolResult<()> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(ProtocolError::Transport(String::from("protocol is not connected")));
        }

        if self
            .listening
            .swap(true, Ordering::AcqRel)
        {
            return Err(ProtocolError::Transport(String::from("listen already started")));
        }

        let backend = self.clone();
        let endpoint = self.base_url.clone();

        let mut task_guard = self.listen_task.lock().await;
        let handle = tokio::spawn(async move {
            let _ = notify.send(ProtocolEvent::Connected {
                endpoint: endpoint.clone(),
            });

            while backend.connected.load(Ordering::Acquire)
                && !backend.stop_requested.load(Ordering::Acquire)
            {
                let timeout_ms = backend.listen_timeout_ms.to_string();
                let max_events = backend.listen_max_events.to_string();
                let response = backend
                    .request(reqwest::Method::GET, "/message/listen")
                    .query(&[
                        ("timeout_ms", timeout_ms.as_str()),
                        ("max_events", max_events.as_str()),
                    ])
                    .send()
                    .await;

                match response {
                    Ok(resp) if resp.status().is_success() => {
                        match resp
                            .json::<Value>()
                            .await
                            .map(OneBotListenEnvelope::from_json)
                        {
                            Ok(envelope) => {
                                let mut delivered = false;
                                for raw in envelope.data {
                                    let payload = if let Some(raw_text) = raw.as_str() {
                                        ListenPayload::Text(raw_text.to_string())
                                    } else {
                                        ListenPayload::Object(raw)
                                    };

                                    let result = match payload {
                                        ListenPayload::Text(raw_text) => deserialize_event(&raw_text),
                                        ListenPayload::Object(raw_obj) => {
                                            serde_json::from_value(raw_obj).map_err(|error| {
                                                ProtocolError::Serialization(error.to_string())
                                            })
                                        }
                                    };

                                    match result {
                                        Ok(event) => {
                                            delivered = true;
                                            if notify.send(event).is_err() {
                                                break;
                                            }
                                        }
                                        Err(err) => {
                                            let warning = ProtocolEvent::Warning {
                                                message: format!(
                                                    "failed to decode protocol event: {}",
                                                    err
                                                ),
                                            };
                                            let _ = notify.send(warning);
                                        }
                                    }
                                }

                                if !delivered {
                                    sleep(Duration::from_millis(20)).await;
                                }
                            }
                            Err(err) => {
                                let warning = ProtocolEvent::Warning {
                                    message: format!("message/listen payload decode failed: {err}"),
                                };
                                let _ = notify.send(warning);
                                sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }
                    Ok(resp) => {
                        let warning = ProtocolEvent::Warning {
                            message: format!("message/listen non-success: {}", resp.status()),
                        };
                        let _ = notify.send(warning);
                        sleep(Duration::from_millis(200)).await;
                    }
                    Err(error) => {
                        let warning = ProtocolEvent::Warning {
                            message: format!("message/listen failed: {error}"),
                        };
                        let _ = notify.send(warning);
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }

            let _ = notify.send(ProtocolEvent::Disconnected);
            backend.listening.store(false, Ordering::Release);
        });

        *task_guard = Some(handle);
        Ok(())
    }
}

/// Build message event from raw JSON.
pub fn deserialize_event(payload: &str) -> ProtocolResult<ProtocolEvent> {
    serde_json::from_str(payload).map_err(|err| ProtocolError::Serialization(err.to_string()))
}

/// Serialize protocol event to JSON.
pub fn serialize_event(event: &ProtocolEvent) -> ProtocolResult<String> {
    serde_json::to_string(event).map_err(|err| ProtocolError::Serialization(err.to_string()))
}

/// Generic protocol-to-handler bridge.
pub async fn forward_to_handler<H>(handler: &H, event: &ProtocolEvent) -> MessageResult<String>
where
    H: MessageHandler,
{
    if let ProtocolEvent::MessageReceived { message } = event {
        let outcome = handler.handle(message).await?;
        Ok(format!("{}: {}", handler.name(), outcome.detail))
    } else {
        Ok(String::from("noop"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use napcat_message::{EchoHandler, MessageRecipient};

    #[tokio::test]
    async fn mock_protocol_has_caps() {
        let (proto, _rx) = MockProtocol::new();
        let mut joined = proto.capabilities();
        joined.sort_by_key(|cap| match cap {
            ProtocolCapability::Login => 0,
            ProtocolCapability::Message => 1,
            ProtocolCapability::Meta => 2,
        });

        assert_eq!(proto.name(), "mock");
        assert!(joined.contains(&ProtocolCapability::Message));
        assert!(joined.contains(&ProtocolCapability::Login));
        assert!(!proto.is_logged_in().await.expect("should query state"));
    }

    #[tokio::test]
    async fn mock_protocol_send_and_forward() -> ProtocolResult<()> {
        let (proto, mut rx) = MockProtocol::new();
        let (event_tx, _event_rx) = broadcast::channel(4);
        proto.connect("ws://localhost").await?;

        proto
            .listen(event_tx)
            .await
            .map_err(|error| ProtocolError::Transport(error.to_string()))?;

        let incoming = Message::text(
            "1",
            "user",
            MessageRecipient::Private {
                user_id: "friend".to_string(),
            },
            "hi",
        );
        proto.inject(incoming.clone());

        let recv = rx
            .recv()
            .await
            .ok_or_else(|| ProtocolError::Transport(String::from("no message from mock protocol")))?;
        let handler = EchoHandler;
        let event = ProtocolEvent::MessageReceived { message: recv };
        let summary = forward_to_handler(&handler, &event)
            .await
            .map_err(|error| ProtocolError::Transport(error.to_string()))?;
        assert!(summary.starts_with("echo:"));

        proto.disconnect().await?;
        assert!(!proto.is_logged_in().await?);
        Ok(())
    }
}
