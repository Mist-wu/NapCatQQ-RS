//! QQ client protocol abstraction and session lifecycle contract.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Default)]
pub struct MockQQClient {
    endpoint: tokio::sync::Mutex<Option<String>>,
    token: tokio::sync::Mutex<Option<String>>,
    state: tokio::sync::Mutex<ConnectionState>,
}

#[async_trait]
impl QQClient for MockQQClient {
    async fn connect(&self, config: QQClientConfig) -> QQClientResult<()> {
        if config.endpoint.trim().is_empty() {
            return Err(QQClientError::Transport(String::from("empty endpoint")));
        }

        let mut endpoint = self.endpoint.lock().await;
        *endpoint = Some(config.endpoint);

        let mut state = self.state.lock().await;
        *state = ConnectionState::Connected;
        Ok(())
    }

    async fn login(&self, account: &str, password: &str) -> QQClientResult<String> {
        if account.trim().is_empty() || password.trim().is_empty() {
            return Err(QQClientError::Login(String::from("account and password required")));
        }

        let mut token = self.token.lock().await;
        *token = Some(format!("mock-token-{account}"));

        let mut state = self.state.lock().await;
        *state = ConnectionState::LoggedIn;
        Ok(format!("{account}:logged-in"))
    }

    async fn send_packet(&self, _packet: Packet) -> QQClientResult<()> {
        Ok(())
    }

    async fn receive_packet(&self) -> QQClientResult<Option<Packet>> {
        Ok(None)
    }

    async fn heartbeat(&self, _interval: Duration) -> QQClientResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn mock_client_connect_and_login_updates_state() -> QQClientResult<()> {
        let client = MockQQClient::default();
        client
            .connect(QQClientConfig::new("mock://localhost"))
            .await?;
        let ticket = client.login("alice", "secret").await?;
        assert_eq!(ticket, "alice:logged-in");

        client.heartbeat(Duration::from_millis(10)).await?;
        assert!(client.receive_packet().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn mock_client_rejects_empty_login_payload() {
        let client = MockQQClient::default();
        client
            .connect(QQClientConfig::new("mock://localhost"))
            .await
            .unwrap();

        let result = client.login("", "").await;
        assert!(matches!(result, Err(QQClientError::Login(_))));
    }
}
