//! Unified message types used by API and protocol layers.

use serde::{Deserialize, Serialize};

/// Content variants for messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageContent {
    /// Plain text message.
    Text(String),
    /// Binary blob path.
    Binary(Vec<u8>),
}

/// Unified message envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Message identifier.
    pub id: String,
    /// Source user id.
    pub from_user: String,
    /// Destination user/group id.
    pub to_target: String,
    /// Message payload.
    pub content: MessageContent,
}
