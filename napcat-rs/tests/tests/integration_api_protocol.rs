//! Integration tests across API and protocol layers.

use napcat_api::{ApiState, CompatSendRequest, MessageType};
use napcat_message::MessageRecipient;
use napcat_protocol::{ProtocolEvent, ProtocolResult, deserialize_event, forward_to_handler};

#[tokio::test]
async fn api_state_group_and_user_cache_roundtrip() {
    let state = ApiState::new();

    let original_groups = state.groups().await;
    let original_users = state.users().await;
    assert!(original_groups.is_empty());
    assert!(original_users.is_empty());

    let groups = vec![napcat_api::GroupInfo {
        group_id: String::from("g1"),
        group_name: String::from("dev"),
    }];
    let users = vec![napcat_api::UserInfo {
        user_id: String::from("u1"),
        nickname: String::from("alice"),
    }];

    state.set_groups(groups.clone()).await;
    state.set_users(users.clone()).await;

    assert_eq!(state.groups().await, groups);
    assert_eq!(state.users().await, users);

    state.set_groups(vec![]).await;
    state.set_users(vec![]).await;
    assert!(state.groups().await.is_empty());
    assert!(state.users().await.is_empty());
}

#[tokio::test]
async fn protocol_event_compat_roundtrip() -> ProtocolResult<()> {
    let message = napcat_message::Message::text(
        "m-1",
        "api",
        MessageRecipient::Group {
            group_id: String::from("g1"),
        },
        "hello",
    );

    let event = ProtocolEvent::MessageReceived { message };
    let encoded = napcat_protocol::serialize_event(&event)
        .map_err(|error| napcat_protocol::ProtocolError::Serialization(error.to_string()))?;

    let decoded = deserialize_event(&encoded)
        .map_err(|error| napcat_protocol::ProtocolError::Serialization(error.to_string()))?;

    let payload = match decoded {
        ProtocolEvent::MessageReceived { message } => message,
        _ => {
            return Err(napcat_protocol::ProtocolError::Transport(String::from(
                "unexpected protocol event",
            )));
        }
    };

    assert_eq!(payload.sender_id, "api");
    Ok(())
}

#[tokio::test]
async fn protocol_forwarder_produces_handler_summary() {
    let event = ProtocolEvent::MessageReceived {
        message: napcat_message::Message::text(
            "x-1",
            "sender",
            MessageRecipient::Private {
                user_id: String::from("u2"),
            },
            "payload",
        ),
    };

    let summary = forward_to_handler(&napcat_message::EchoHandler, &event)
        .await
        .expect("handler should produce summary");
    assert!(summary.contains("echo:"));
}

#[tokio::test]
async fn api_compat_request_roundtrip_to_message_payload() {
    let request = CompatSendRequest {
        message_type: MessageType::Private,
        user_id: Some(String::from("u1")),
        group_id: None,
        message: String::from("hello"),
    };

    let payload = napcat_message::Message::text(
        "msg",
        "api",
        request
            .user_id
            .as_deref()
            .map(|id| MessageRecipient::Private {
                user_id: id.to_string(),
            })
            .expect("private user id should exist"),
        request.message,
    );

    assert_eq!(payload.sender_id, "api");
    assert_eq!(payload.id, "msg");
}
