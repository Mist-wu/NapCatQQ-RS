//! Protocol abstraction crate placeholder for future protocol implementations.

use serde::{Deserialize, Serialize};

/// Standard protocol message envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolFrame {
    /// Frame payload encoded as UTF-8 JSON.
    pub payload: String,
    /// Optional event tag.
    pub event: Option<String>,
}
