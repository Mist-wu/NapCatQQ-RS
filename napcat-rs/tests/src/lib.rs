//! Shared helpers for workspace-level tests.

use napcat_message::{Message, MessageRecipient};

/// Create a sample private message fixture.
pub fn sample_private_message() -> Message {
    Message::text(
        "msg-1",
        "alice",
        MessageRecipient::Private {
            user_id: String::from("bob"),
        },
        "hello",
    )
}
