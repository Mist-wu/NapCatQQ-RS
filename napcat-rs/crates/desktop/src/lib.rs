//! Minimal desktop control surface for status dashboards and local inspection.

use napcat_event::{EventBus, EventEnvelope};
use napcat_protocol::ProtocolEvent;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
    time::{interval, Duration},
};

/// Desktop module result alias.
pub type DesktopResult<T> = std::result::Result<T, DesktopError>;

/// Desktop runtime error surface.
#[derive(Debug, Error)]
pub enum DesktopError {
    /// Runtime already stopped.
    #[error("desktop runtime already stopped")]
    AlreadyStopped,

    /// Runtime already running.
    #[error("desktop runtime already started")]
    AlreadyRunning,

    /// Runtime control channel closed.
    #[error("desktop control channel closed")]
    ControlClosed,
}

/// Runtime visibility snapshot for dashboards.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct DesktopSnapshot {
    /// Human readable module title.
    pub title: String,
    /// Total protocol events observed.
    pub total_events: u64,
    /// Most recently observed event timestamp in epoch milliseconds.
    pub last_event_ms: Option<u128>,
    /// Source name for the latest event.
    pub last_source: Option<String>,
    /// Event kind for the latest event.
    pub last_kind: Option<String>,
}

impl DesktopSnapshot {
    /// Create default snapshot for a title.
    pub fn empty(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            total_events: 0,
            last_event_ms: None,
            last_source: None,
            last_kind: None,
        }
    }

    /// Format a one-line status view.
    pub fn as_status_line(&self) -> String {
        match (self.last_source.as_deref(), self.last_kind.as_deref()) {
            (Some(source), Some(kind)) => {
                format!(
                    "{} | events={} last={} {}",
                    self.title, self.total_events, source, kind
                )
            }
            _ => format!("{} | events={}", self.title, self.total_events),
        }
    }
}

/// Dashboard-like desktop runtime with event tap.
pub struct DesktopRuntime {
    snapshot: std::sync::Arc<RwLock<DesktopSnapshot>>,
    stop_tx: Option<mpsc::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Default for DesktopRuntime {
    fn default() -> Self {
        Self::new("napcat-desktop")
    }
}

impl DesktopRuntime {
    /// Create a new runtime with default snapshot.
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            snapshot: std::sync::Arc::new(RwLock::new(DesktopSnapshot::empty(title))),
            stop_tx: None,
            task: None,
        }
    }

    /// Return a cloned latest snapshot.
    pub async fn snapshot(&self) -> DesktopSnapshot {
        self.snapshot.read().await.clone()
    }

    /// Start consuming protocol event envelopes until stop is requested.
    pub fn start(
        &mut self,
        bus: EventBus<ProtocolEvent>,
        refresh_interval_ms: u64,
    ) -> DesktopResult<()> {
        if self.task.is_some() {
            return Err(DesktopError::AlreadyRunning);
        }

        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let mut event_rx = bus.subscribe();
        let snapshot = self.snapshot.clone();

        let interval_ms = refresh_interval_ms.max(250);
        let task = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(interval_ms));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        tracing::debug!(
                            "desktop runtime heartbeat: {}",
                            snapshot.read().await.total_events
                        );
                    }
                    received = event_rx.recv() => {
                        if let Ok(EventEnvelope {
                            source,
                            kind,
                            at_millis,
                            ..
                        }) = received
                        {
                            let mut data = snapshot.write().await;
                            data.total_events = data.total_events.saturating_add(1);
                            data.last_event_ms = Some(at_millis);
                            data.last_source = Some(source);
                            data.last_kind = Some(kind);
                        }
                    }
                    _ = stop_rx.recv() => {
                        break;
                    }
                }
            }
        });

        self.stop_tx = Some(stop_tx);
        self.task = Some(task);
        Ok(())
    }

    /// Request stop and wait for the background task to exit.
    pub async fn stop(&mut self) -> DesktopResult<()> {
        let Some(stop_tx) = self.stop_tx.take() else {
            return Err(DesktopError::AlreadyStopped);
        };

        stop_tx
            .send(())
            .await
            .map_err(|_| DesktopError::ControlClosed)?;

        if let Some(task) = self.task.take() {
            let _ = task.await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_default_snapshot_is_empty_and_status_line_renders() {
        let runtime = DesktopRuntime::new("desktop");
        let snapshot = runtime.snapshot().await;

        assert_eq!(snapshot.total_events, 0);
        assert_eq!(snapshot.as_status_line(), "desktop | events=0");
    }

    #[tokio::test]
    async fn runtime_receives_protocol_events_and_stops() -> DesktopResult<()> {
        let mut runtime = DesktopRuntime::new("desktop");
        let bus = EventBus::new(4);
        runtime.start(bus.clone(), 300)?;

        bus.publish(EventEnvelope::new(
            "protocol",
            "message",
            ProtocolEvent::Connected {
                endpoint: String::from("ws://localhost"),
            },
        ))
        .expect("event should publish");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.total_events, 1);
        assert_eq!(snapshot.last_source.as_deref(), Some("protocol"));

        runtime.stop().await?;
        assert!(matches!(runtime.stop().await, Err(DesktopError::AlreadyStopped)));
        Ok(())
    }
}
