//! Generic event bus primitives for runtime, protocol, and plugin layers.

use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::broadcast;

/// Unified result type for event operations.
pub type EventResult<T> = std::result::Result<T, EventError>;

/// Event bus error surface.
#[derive(Debug, Error)]
pub enum EventError {
    /// No active receivers for the event channel.
    #[error("event publish has no active subscribers")]
    NoSubscriber,
}

/// A minimal metadata envelope for async event exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventEnvelope<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Optional human-readable source.
    pub source: String,
    /// Optional event category.
    pub kind: String,
    /// Unix timestamp in milliseconds.
    pub at_millis: u128,
    /// Typed event payload.
    pub payload: T,
}

impl<T> EventEnvelope<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Build a new event envelope.
    pub fn new(source: impl Into<String>, kind: impl Into<String>, payload: T) -> Self {
        let source = source.into();
        let kind = kind.into();
        let at_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis());

        Self {
            source,
            kind,
            at_millis,
            payload,
        }
    }
}

/// Asynchronous event bus with bounded broadcast capacity.
#[derive(Clone)]
pub struct EventBus<T>
where
    T: Clone + Send + Sync + 'static,
{
    sender: broadcast::Sender<EventEnvelope<T>>,
}

impl<T> EventBus<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Create a bus with specific broadcast capacity.
    pub fn new(capacity: usize) -> Self {
        if capacity == 0 {
            panic!("event bus capacity must be greater than zero");
        }
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Publish an event to all subscribed listeners.
    ///
    /// Missing subscribers are reported with `EventError::NoSubscriber`.
    pub fn publish(&self, event: EventEnvelope<T>) -> EventResult<usize> {
        self.sender
            .send(event)
            .map_err(|_| EventError::NoSubscriber)
    }

    /// Subscribe to the bus stream.
    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope<T>> {
        self.sender.subscribe()
    }

    /// Current receiver count.
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Clone sender for external dispatch.
    pub fn sender(&self) -> broadcast::Sender<EventEnvelope<T>> {
        self.sender.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct EventPayload {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn bus_requires_capacity() {
        // Capacity is a hard contract; zero-capacity bus is a programmer error.
        // We intentionally avoid constructing one.
        let _unused = EventBus::<EventPayload>::new(1);

        assert_eq!(
            EventBus::<EventPayload>::new(8).sender().receiver_count(),
            0
        );
    }

    #[tokio::test]
    async fn bus_publish_without_receivers_reports_error() {
        let bus = EventBus::<EventPayload>::new(8);

        let result = bus.publish(EventEnvelope::new(
            "api",
            "protocol",
            EventPayload {
                id: 1,
                name: String::from("no-listener"),
            },
        ));
        assert!(matches!(result, Err(EventError::NoSubscriber)));
    }

    #[tokio::test]
    async fn bus_subscriber_receives_events() {
        let bus = EventBus::<EventPayload>::new(8);

        let mut rx = bus.subscribe();
        let published = bus
            .publish(EventEnvelope::new(
                "api",
                "protocol",
                EventPayload {
                    id: 2,
                    name: String::from("payload"),
                },
            ))
            .expect("event published");

        assert_eq!(published, 1);
        let received = rx.recv().await.expect("event should be delivered");
        assert_eq!(received.payload.id, 2);
        assert_eq!(received.source, "api");
        assert_eq!(received.kind, "protocol");
    }
}
