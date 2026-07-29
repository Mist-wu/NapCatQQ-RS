//! QQ client protocol abstraction and session lifecycle contract.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime},
};
use thiserror::Error;

/// Result alias used by QQ client adapters.
pub type QQClientResult<T> = std::result::Result<T, QQClientError>;

/// High-level configuration for QQ protocol connectors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QQClientConfig {
    /// Endpoint address (for mock or remote adapters).
    pub endpoint: String,
    /// Optional authentication token.
    pub token: Option<String>,
    /// Polling or reconnect timeout in milliseconds.
    pub timeout_ms: u64,
}

impl QQClientConfig {
    /// Create a default config with timeout.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: None,
            timeout_ms: 2_000,
        }
    }

    /// Override token.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Override timeout.
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// Standard client states.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectionState {
    /// Disconnected from QQ endpoint.
    Disconnected,
    /// Connected but not authenticated.
    Connected,
    /// Authenticated and sending/receiving packets.
    LoggedIn,
    /// Explicitly closed by the caller.
    Closing,
}

/// Lightweight packet abstraction for transport layers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Packet {
    /// Packet route or opcode name.
    pub route: String,
    /// Raw UTF-8 payload.
    pub payload: String,
}

impl Packet {
    /// Build a packet from route + payload.
    pub fn new(route: impl Into<String>, payload: impl Into<String>) -> Self {
        Self {
            route: route.into(),
            payload: payload.into(),
        }
    }
}

/// Error surface for QQ client adapters.
#[derive(Debug, Error)]
pub enum QQClientError {
    /// Network transport failure.
    #[error("transport failure: {0}")]
    Transport(String),

    /// Protocol decode/encode issue.
    #[error("protocol parsing failure: {0}")]
    Protocol(String),

    /// Authentication failed.
    #[error("login failure: {0}")]
    Login(String),

    /// Missing or invalid data.
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// Operation timed out.
    #[error("operation timed out: {0}")]
    Timeout(String),
}

/// QQ client runtime contract.
#[async_trait]
pub trait QQClient: Send + Sync {
    /// 建立网络连接。Build transport channel before login.
    async fn connect(&self, config: QQClientConfig) -> QQClientResult<()>;

    /// 执行账号登录。
    async fn login(&self, account: &str, password: &str) -> QQClientResult<String>;

    /// 发包。
    async fn send_packet(&self, packet: Packet) -> QQClientResult<()>;

    /// 阻塞接收下行包。
    async fn receive_packet(&self) -> QQClientResult<Option<Packet>>;

    /// 心跳 tick，允许外层服务刷新会话活性。
    async fn heartbeat(&self, interval: Duration) -> QQClientResult<()>;
}

/// In-memory mock implementation used for protocol tests and offline validation.
pub struct MockQQClient {
    endpoint: tokio::sync::Mutex<Option<String>>,
    token: tokio::sync::Mutex<Option<String>>,
    state: tokio::sync::Mutex<ConnectionState>,
    heartbeat_at: tokio::sync::Mutex<Option<SystemTime>>,
    timeout_ms: tokio::sync::Mutex<u64>,
    sent_packets: tokio::sync::Mutex<VecDeque<Packet>>,
    inbound_packets: tokio::sync::Mutex<VecDeque<Packet>>,
}

impl Default for MockQQClient {
    fn default() -> Self {
        Self {
            endpoint: tokio::sync::Mutex::new(None),
            token: tokio::sync::Mutex::new(None),
            state: tokio::sync::Mutex::new(ConnectionState::Disconnected),
            heartbeat_at: tokio::sync::Mutex::new(None),
            timeout_ms: tokio::sync::Mutex::new(2_000),
            sent_packets: tokio::sync::Mutex::new(VecDeque::new()),
            inbound_packets: tokio::sync::Mutex::new(VecDeque::new()),
        }
    }
}

#[async_trait]
impl QQClient for MockQQClient {
    async fn connect(&self, config: QQClientConfig) -> QQClientResult<()> {
        if config.endpoint.trim().is_empty() {
            return Err(QQClientError::Transport(String::from("empty endpoint")));
        }

        {
            let mut state = self.state.lock().await;
            if matches!(*state, ConnectionState::LoggedIn) {
                return Err(QQClientError::InvalidState(String::from(
                    "already logged in, disconnect first",
                )));
            }
            *state = ConnectionState::Connected;
        }

        *self.endpoint.lock().await = Some(config.endpoint);
        *self.timeout_ms.lock().await = config.timeout_ms;
        Ok(())
    }

    async fn login(&self, account: &str, password: &str) -> QQClientResult<String> {
        let connected = {
            let state = self.state.lock().await;
            matches!(*state, ConnectionState::Connected)
        };

        if !connected {
            return Err(QQClientError::InvalidState(String::from(
                "must connect before login",
            )));
        }

        if account.trim().is_empty() || password.trim().is_empty() {
            return Err(QQClientError::Login(String::from("account and password required")));
        }

        let mut token = self.token.lock().await;
        *token = Some(format!("mock-token-{account}"));

        let mut state = self.state.lock().await;
        *state = ConnectionState::LoggedIn;
        Ok(format!("{account}:logged-in"))
    }

    async fn send_packet(&self, packet: Packet) -> QQClientResult<()> {
        if !self.is_logged_in().await? {
            return Err(QQClientError::InvalidState(String::from(
                "must login before sending packets",
            )));
        }

        let mut packets = self.sent_packets.lock().await;
        packets.push_back(packet);
        Ok(())
    }

    async fn receive_packet(&self) -> QQClientResult<Option<Packet>> {
        let mut packets = self.inbound_packets.lock().await;
        Ok(packets.pop_front())
    }

    async fn heartbeat(&self, interval: Duration) -> QQClientResult<()> {
        if !self.is_logged_in().await? {
            return Err(QQClientError::InvalidState(String::from(
                "must login before heartbeat",
            )));
        }
        if interval.is_zero() {
            return Err(QQClientError::Timeout(String::from("heartbeat interval is zero")));
        }

        let interval_ms = u64::try_from(interval.as_millis())
            .map_err(|error| QQClientError::Timeout(format!("heartbeat interval too long: {error}")))?;

        *self.heartbeat_at.lock().await = Some(SystemTime::now());
        *self.timeout_ms.lock().await = interval_ms;
        Ok(())
    }
}

impl MockQQClient {
    /// Inject a packet that should be returned by the next `receive_packet` call.
    pub async fn inject_packet(&self, packet: Packet) {
        let mut inbound = self.inbound_packets.lock().await;
        inbound.push_back(packet);
    }

    /// Return all packets sent through `send_packet`.
    pub async fn sent_packets(&self) -> Vec<Packet> {
        let sent = self.sent_packets.lock().await;
        sent.iter().cloned().collect()
    }

    /// Return connection state observed by operations.
    pub async fn state(&self) -> ConnectionState {
        self.state.lock().await.clone()
    }

    async fn is_logged_in(&self) -> QQClientResult<bool> {
        let timeout_ms = *self.timeout_ms.lock().await;
        if timeout_ms == 0 {
            return Err(QQClientError::Timeout(String::from("configured timeout is zero")));
        }

        Ok(matches!(
            *self.state.lock().await,
            ConnectionState::LoggedIn
        ))
    }

    /// Disconnect and release all runtime state, keeping telemetry values.
    pub async fn disconnect(&self) -> QQClientResult<()> {
        let mut state = self.state.lock().await;
        *state = ConnectionState::Disconnected;
        Ok(())
    }

    /// Whether heartbeat has been triggered at least once.
    pub async fn has_heartbeat(&self) -> bool {
        self.heartbeat_at.lock().await.is_some()
    }

    /// Return current endpoint if connected.
    pub async fn endpoint(&self) -> Option<String> {
        self.endpoint.lock().await.clone()
    }

    /// Return current token if available.
    pub async fn token(&self) -> Option<String> {
        self.token.lock().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn mock_client_connect_and_login_updates_state() -> QQClientResult<()> {
        let client = MockQQClient::default();
        client.connect(QQClientConfig::new("mock://localhost")).await?;
        let ticket = client.login("alice", "secret").await?;
        assert_eq!(ticket, "alice:logged-in");

        client.heartbeat(Duration::from_millis(10)).await?;
        assert!(client.has_heartbeat().await);
        assert_eq!(client.state().await, ConnectionState::LoggedIn);
        assert!(client.receive_packet().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn mock_client_rejects_empty_login_payload() {
        let client = MockQQClient::default();
        let connected = client.connect(QQClientConfig::new("mock://localhost")).await;
        assert!(connected.is_ok());

        let result = client.login("", "").await;
        assert!(matches!(result, Err(QQClientError::Login(_))));
    }

    #[tokio::test]
    async fn mock_client_send_and_receive_packets() -> QQClientResult<()> {
        let client = MockQQClient::default();
        client.connect(QQClientConfig::new("mock://localhost")).await?;
        client.login("alice", "secret").await?;

        let packet = Packet::new("message.send", "{\"text\":\"hi\"}");
        client.send_packet(packet.clone()).await?;
        client.inject_packet(packet.clone()).await;

        let sent = client.sent_packets().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0], packet);

        let received = client
            .receive_packet()
            .await?
            .expect("received packet should exist");
        assert_eq!(received.route, "message.send");
        Ok(())
    }

    #[tokio::test]
    async fn mock_client_rejects_send_when_logged_out() {
        let client = MockQQClient::default();
        let connected = client.connect(QQClientConfig::new("mock://localhost")).await;
        assert!(connected.is_ok());
        let packet = Packet::new("message.send", "{\"text\":\"hi\"}");

        let result = client.send_packet(packet).await;
        assert!(matches!(result, Err(QQClientError::InvalidState(_))));
    }

    #[tokio::test]
    async fn mock_client_heartbeat_requires_positive_interval() -> QQClientResult<()> {
        let client = MockQQClient::default();
        client.connect(QQClientConfig::new("mock://localhost")).await?;
        client.login("alice", "secret").await?;
        let result = client.heartbeat(Duration::from_millis(0)).await;
        assert!(matches!(result, Err(QQClientError::Timeout(_))));
        Ok(())
    }
}
