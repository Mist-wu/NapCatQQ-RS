//! Protocol abstraction layer and contracts.

use async_trait::async_trait;
use napcat_message::{Message, MessageHandler, MessageResult, encode_json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, broadcast, mpsc};

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
    connected: Arc<Mutex<bool>>,
    sender: mpsc::UnboundedSender<Message>,
}

impl MockProtocol {
    /// Create mock protocol pair (backend + event injector).
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Message>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                connected: Arc::new(Mutex::new(false)),
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
            return Err(ProtocolError::Transport("empty endpoint".to_string()));
        }
        let mut connected = self.connected.lock().await;
        *connected = true;
        Ok(())
    }

    async fn disconnect(&self) -> ProtocolResult<()> {
        let mut connected = self.connected.lock().await;
        *connected = false;
        Ok(())
    }

    async fn is_logged_in(&self) -> ProtocolResult<bool> {
        Ok(*self.connected.lock().await)
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
        Ok("noop".to_string())
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
            .ok_or_else(|| ProtocolError::Transport("no message from mock protocol".to_string()))?;
        let handler = EchoHandler;
        let event = ProtocolEvent::MessageReceived { message: recv };
        let summary =
            forward_to_handler(&handler, &event)
                .await
                .map_err(|error| ProtocolError::Transport(error.to_string()))?;
        assert!(summary.starts_with("echo:"));

        proto.disconnect().await?;
        assert!(!proto.is_logged_in().await?);
        Ok(())
    }
}
