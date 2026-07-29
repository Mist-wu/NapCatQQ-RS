//! Unified message model and handler abstraction.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Delivery channel for a message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MessageChannel {
    /// One-to-one private chat.
    Private,
    /// Group chat.
    Group,
}

/// Unified recipient descriptor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MessageRecipient {
    /// Private recipient.
    Private { user_id: String },
    /// Group recipient.
    Group { group_id: String },
}

impl MessageRecipient {
    /// Convert recipient to a channel.
    pub fn channel(&self) -> MessageChannel {
        match self {
            MessageRecipient::Private { .. } => MessageChannel::Private,
            MessageRecipient::Group { .. } => MessageChannel::Group,
        }
    }
}

/// Message attachment and command elements.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageElement {
    /// Raw text content.
    Text { text: String },
    /// Image attachment metadata.
    Image { path: String, size: Option<u64> },
    /// Video attachment metadata.
    Video {
        /// Local or remote video resource path.
        path: String,
        /// Optional duration in milliseconds.
        duration_ms: Option<u64>,
    },
    /// File attachment metadata.
    File { path: String, md5: Option<String> },
    /// Mention element.
    At { target_user: String },
    /// Reply to another message id.
    Reply { message_id: String },
    /// Embedded JSON payload.
    Json { payload: serde_json::Value },
}

/// Message envelope used by all protocol/API layers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    /// Message id (provider-defined).
    pub id: String,
    /// Sender.
    pub sender_id: String,
    /// Recipient info.
    pub recipient: MessageRecipient,
    /// Message payload elements.
    pub elements: Vec<MessageElement>,
}

impl Message {
    /// Build a minimal message from one text element.
    pub fn text(
        id: impl Into<String>,
        sender: impl Into<String>,
        recipient: MessageRecipient,
        text: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            sender_id: sender.into(),
            recipient,
            elements: vec![MessageElement::Text { text: text.into() }],
        }
    }

    /// Check whether the payload contains an image.
    pub fn is_image_message(&self) -> bool {
        self.elements
            .iter()
            .any(|e| matches!(e, MessageElement::Image { .. }))
    }

    /// Check whether the payload contains a file.
    pub fn is_file_message(&self) -> bool {
        self.elements
            .iter()
            .any(|e| matches!(e, MessageElement::File { .. }))
    }

    /// Check whether the payload contains a video.
    pub fn is_video_message(&self) -> bool {
        self.elements
            .iter()
            .any(|e| matches!(e, MessageElement::Video { .. }))
    }

    /// Check whether the payload contains an @ mention.
    pub fn is_at_message(&self) -> bool {
        self.elements
            .iter()
            .any(|e| matches!(e, MessageElement::At { .. }))
    }

    /// Check whether the payload contains a reply reference.
    pub fn is_reply_message(&self) -> bool {
        self.elements
            .iter()
            .any(|e| matches!(e, MessageElement::Reply { .. }))
    }

    /// Check whether the payload contains JSON content.
    pub fn is_json_message(&self) -> bool {
        self.elements
            .iter()
            .any(|e| matches!(e, MessageElement::Json { .. }))
    }
}

/// Message handling result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleResult {
    /// Whether handling succeeded.
    pub accepted: bool,
    /// Human-readable status.
    pub detail: String,
}

/// Handler contract for business message adapters.
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// Handler identity.
    fn name(&self) -> &'static str;

    /// Whether the handler can process the message.
    fn can_handle(&self, message: &Message) -> bool;

    /// Handle message asynchronously.
    async fn handle(&self, message: &Message) -> MessageResult<HandleResult>;
}

/// Type alias for message handling.
pub type MessageResult<T> = std::result::Result<T, MessageError>;

/// Message error type.
#[derive(Debug, Error)]
pub enum MessageError {
    /// Unknown recipient channel.
    #[error("unsupported channel: {0}")]
    UnsupportedChannel(String),

    /// Serialization failed.
    #[error("serialization failed: {0}")]
    Serialization(String),
}

/// Helper for JSON conversion used by JSON element flows.
pub fn encode_json(message: &Message) -> MessageResult<String> {
    serde_json::to_string(message).map_err(|error| MessageError::Serialization(error.to_string()))
}

/// Helper for JSON decoding.
pub fn decode_json(text: &str) -> MessageResult<Message> {
    serde_json::from_str::<Message>(text)
        .map_err(|error| MessageError::Serialization(error.to_string()))
}

/// Echo handler for internal tests and loopback channels.
pub struct EchoHandler;

#[async_trait]
impl MessageHandler for EchoHandler {
    fn name(&self) -> &'static str {
        "echo"
    }

    fn can_handle(&self, _message: &Message) -> bool {
        true
    }

    async fn handle(&self, message: &Message) -> MessageResult<HandleResult> {
        match &message.recipient {
            MessageRecipient::Group { group_id } => Ok(HandleResult {
                accepted: true,
                detail: format!("group:{group_id}"),
            }),
            MessageRecipient::Private { .. } => Ok(HandleResult {
                accepted: true,
                detail: format!("private:{}", message.sender_id),
            }),
        }
    }
}

/// Dispatch message to a handler.
pub async fn dispatch(
    handler: &dyn MessageHandler,
    message: &Message,
) -> MessageResult<HandleResult> {
    if !handler.can_handle(message) {
        return Err(MessageError::UnsupportedChannel(format!(
            "{:?}",
            message.recipient.channel()
        )));
    }

    handler.handle(message).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn message_roundtrip_json() -> Result<(), MessageError> {
        let message = Message {
            id: "m1".to_string(),
            sender_id: "alice".to_string(),
            recipient: MessageRecipient::Group {
                group_id: "g1".to_string(),
            },
            elements: vec![
                MessageElement::Text {
                    text: "hello".to_string(),
                },
                MessageElement::Image {
                    path: "/tmp/x.png".to_string(),
                    size: Some(1024),
                },
                MessageElement::Video {
                    path: "/tmp/x.mp4".to_string(),
                    duration_ms: Some(12_000),
                },
                MessageElement::At {
                    target_user: "bob".to_string(),
                },
                MessageElement::Reply {
                    message_id: "m0".to_string(),
                },
                MessageElement::Json {
                    payload: serde_json::json!({"k":"v"}),
                },
            ],
        };

        assert!(message.is_image_message());
        assert!(message.is_video_message());
        assert!(message.is_at_message());
        assert!(message.is_reply_message());
        assert!(message.is_json_message());

        let encoded = encode_json(&message)?;
        let decoded = decode_json(&encoded)?;
        assert_eq!(decoded.id, message.id);
        Ok(())
    }

    #[tokio::test]
    async fn message_handler_dispatch_works() -> MessageResult<()> {
        let handler = EchoHandler;
        let message = Message::text(
            "m2",
            "alice",
            MessageRecipient::Private {
                user_id: "bob".to_string(),
            },
            "ping",
        );
        let outcome = dispatch(&handler, &message).await?;
        assert!(outcome.accepted);
        assert_eq!(outcome.detail, "private:alice");
        Ok(())
    }
}
