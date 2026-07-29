//! QQ client protocol abstraction and session lifecycle contract.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::timeout,
};

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

    /// 断开会话并释放底层资源。
    async fn disconnect(&self) -> QQClientResult<()>;
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
            return Err(QQClientError::Login(String::from(
                "account and password required",
            )));
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
            return Err(QQClientError::Timeout(String::from(
                "heartbeat interval is zero",
            )));
        }

        let interval_ms = u64::try_from(interval.as_millis()).map_err(|error| {
            QQClientError::Timeout(format!("heartbeat interval too long: {error}"))
        })?;

        *self.heartbeat_at.lock().await = Some(SystemTime::now());
        *self.timeout_ms.lock().await = interval_ms;
        Ok(())
    }

    async fn disconnect(&self) -> QQClientResult<()> {
        let mut state = self.state.lock().await;
        *state = ConnectionState::Disconnected;
        let mut sent_packets = self.sent_packets.lock().await;
        sent_packets.clear();
        let mut inbound_packets = self.inbound_packets.lock().await;
        inbound_packets.clear();
        *self.endpoint.lock().await = None;
        *self.token.lock().await = None;
        *self.heartbeat_at.lock().await = None;
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
            return Err(QQClientError::Timeout(String::from(
                "configured timeout is zero",
            )));
        }

        Ok(matches!(
            *self.state.lock().await,
            ConnectionState::LoggedIn
        ))
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

/// JSON line framed TCP transport client.
pub struct TcpQQClient {
    endpoint: tokio::sync::Mutex<Option<String>>,
    token: tokio::sync::Mutex<Option<String>>,
    state: tokio::sync::Mutex<ConnectionState>,
    heartbeat_at: tokio::sync::Mutex<Option<SystemTime>>,
    timeout_ms: tokio::sync::Mutex<u64>,
    stream: tokio::sync::Mutex<Option<TcpStream>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TcpWirePacket {
    route: String,
    payload: String,
}

fn normalize_tcp_endpoint(raw: &str) -> QQClientResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(QQClientError::Transport(String::from("empty tcp endpoint")));
    }

    let without_scheme = trimmed.split_once("://").map_or(trimmed, |(_, tail)| tail);
    let host = without_scheme.split('?').next().unwrap_or_default();
    let host = host.split('/').next().unwrap_or_default();
    if host.trim().is_empty() {
        return Err(QQClientError::Transport(String::from(
            "invalid tcp endpoint",
        )));
    }
    if !host.contains(':') {
        return Err(QQClientError::Transport(format!(
            "tcp endpoint requires host:port, got {host}"
        )));
    }

    Ok(host.to_string())
}

fn build_wire_line(route: impl Into<String>, payload: impl Into<String>) -> String {
    let frame = TcpWirePacket {
        route: route.into(),
        payload: payload.into(),
    };
    serde_json::to_string(&frame).expect("wire frame must serialize")
}

fn parse_wire_line(line: &str) -> QQClientResult<TcpWirePacket> {
    serde_json::from_str::<TcpWirePacket>(line)
        .map_err(|error| QQClientError::Protocol(format!("invalid wire packet format: {error}")))
}

fn effective_timeout_ms(raw: u64) -> u64 {
    if raw == 0 { 2_000 } else { raw }
}

impl Default for TcpQQClient {
    fn default() -> Self {
        Self {
            endpoint: tokio::sync::Mutex::new(None),
            token: tokio::sync::Mutex::new(None),
            state: tokio::sync::Mutex::new(ConnectionState::Disconnected),
            heartbeat_at: tokio::sync::Mutex::new(None),
            timeout_ms: tokio::sync::Mutex::new(2_000),
            stream: tokio::sync::Mutex::new(None),
        }
    }
}

impl TcpQQClient {
    /// Return current state for higher-level orchestrators.
    pub async fn state(&self) -> ConnectionState {
        self.state.lock().await.clone()
    }

    /// Return latest token value if available.
    pub async fn token(&self) -> Option<String> {
        self.token.lock().await.clone()
    }

    async fn is_session_open(&self) -> bool {
        let state = self.state.lock().await;
        matches!(
            *state,
            ConnectionState::Connected | ConnectionState::LoggedIn
        )
    }

    async fn set_state(&self, state: ConnectionState) {
        let mut current = self.state.lock().await;
        *current = state;
    }

    async fn recv_once_with_timeout(&self) -> QQClientResult<Option<Packet>> {
        if !self.is_session_open().await {
            return Ok(None);
        }

        let timeout_ms = *self.timeout_ms.lock().await;
        let stream = {
            let mut stream_slot = self.stream.lock().await;
            let stream = stream_slot
                .take()
                .ok_or_else(|| QQClientError::Transport(String::from("not connected")))?;

            let std_stream = stream
                .into_std()
                .map_err(|error| QQClientError::Transport(error.to_string()))?;
            let reader_stream = std_stream
                .try_clone()
                .map_err(|error| QQClientError::Transport(error.to_string()))?;
            let read_stream = TcpStream::from_std(reader_stream)
                .map_err(|error| QQClientError::Transport(error.to_string()))?;
            let write_stream = TcpStream::from_std(std_stream)
                .map_err(|error| QQClientError::Transport(error.to_string()))?;

            *stream_slot = Some(write_stream);
            read_stream
        };

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        let read_result = timeout(
            Duration::from_millis(timeout_ms),
            reader.read_line(&mut line),
        )
        .await
        .map_err(|_| QQClientError::Timeout(String::from("receive timeout")))?;
        let read_count = read_result
            .map_err(|error| QQClientError::Transport(format!("read packet failed: {error}")))?;

        if read_count == 0 {
            return Ok(None);
        }

        let wire = parse_wire_line(line.trim_end())?;
        Ok(Some(Packet {
            route: wire.route,
            payload: wire.payload,
        }))
    }

    async fn wait_for_login_ack(&self) -> QQClientResult<String> {
        let max_retries = 4;
        for _ in 0..max_retries {
            let packet = self
                .recv_once_with_timeout()
                .await?
                .ok_or_else(|| QQClientError::Timeout(String::from("login ack timeout")))?;

            match packet.route.as_str() {
                "auth.ok" => {
                    let payload =
                        serde_json::from_str::<Value>(&packet.payload).map_err(|error| {
                            QQClientError::Protocol(format!("invalid login payload: {error}"))
                        })?;
                    let token = payload
                        .get("token")
                        .and_then(Value::as_str)
                        .unwrap_or("session")
                        .to_string();
                    *self.token.lock().await = Some(token.clone());
                    return Ok(token);
                }
                "system.error" => {
                    return Err(QQClientError::Login(packet.payload));
                }
                _ => continue,
            }
        }

        Err(QQClientError::Timeout(String::from(
            "login ack not received",
        )))
    }
}

#[async_trait]
impl QQClient for TcpQQClient {
    async fn connect(&self, config: QQClientConfig) -> QQClientResult<()> {
        let current_state = self.state.lock().await.clone();
        if matches!(
            current_state,
            ConnectionState::Connected | ConnectionState::LoggedIn
        ) {
            return Err(QQClientError::InvalidState(String::from(
                "connection already active",
            )));
        }

        let socket_addr = normalize_tcp_endpoint(&config.endpoint)?;
        let timeout_ms = effective_timeout_ms(config.timeout_ms);
        let deadline = Duration::from_millis(timeout_ms);

        let stream = timeout(deadline, TcpStream::connect(socket_addr.clone()))
            .await
            .map_err(|_| QQClientError::Timeout(format!("connect timeout after {timeout_ms}ms")))?
            .map_err(|error| QQClientError::Transport(error.to_string()))?;

        *self.endpoint.lock().await = Some(config.endpoint.clone());
        *self.timeout_ms.lock().await = timeout_ms;
        *self.stream.lock().await = Some(stream);
        self.set_state(ConnectionState::Connected).await;
        Ok(())
    }

    async fn login(&self, account: &str, password: &str) -> QQClientResult<String> {
        if !self.is_session_open().await {
            return Err(QQClientError::InvalidState(String::from(
                "must connect before login",
            )));
        }

        if account.trim().is_empty() || password.trim().is_empty() {
            return Err(QQClientError::Login(String::from(
                "account and password required",
            )));
        }

        let request_payload = serde_json::json!({
            "account": account,
            "password": password,
        });
        let request = build_wire_line("auth.login", request_payload.to_string());
        {
            let mut stream = self.stream.lock().await;
            let stream = stream
                .as_mut()
                .ok_or_else(|| QQClientError::InvalidState(String::from("not connected")))?;

            timeout(
                Duration::from_millis(effective_timeout_ms(*self.timeout_ms.lock().await)),
                async {
                    stream.write_all(format!("{request}\n").as_bytes()).await?;
                    stream.flush().await
                },
            )
            .await
            .map_err(|_| QQClientError::Timeout(String::from("login send timeout")))?
            .map_err(|error| QQClientError::Transport(error.to_string()))?;
        }

        let token = self.wait_for_login_ack().await?;
        self.set_state(ConnectionState::LoggedIn).await;
        Ok(token)
    }

    async fn send_packet(&self, packet: Packet) -> QQClientResult<()> {
        let state = self.state.lock().await;
        if !matches!(*state, ConnectionState::LoggedIn) {
            return Err(QQClientError::InvalidState(String::from(
                "must login before sending packets",
            )));
        }
        drop(state);

        let raw = build_wire_line(&packet.route, &packet.payload);
        let mut stream = self.stream.lock().await;
        let stream = stream
            .as_mut()
            .ok_or_else(|| QQClientError::InvalidState(String::from("not connected")))?;

        timeout(
            Duration::from_millis(effective_timeout_ms(*self.timeout_ms.lock().await)),
            async {
                stream.write_all(format!("{raw}\n").as_bytes()).await?;
                stream.flush().await
            },
        )
        .await
        .map_err(|_| QQClientError::Timeout(String::from("send timeout")))?
        .map_err(|error| QQClientError::Transport(error.to_string()))?;
        Ok(())
    }

    async fn receive_packet(&self) -> QQClientResult<Option<Packet>> {
        if !self.is_session_open().await {
            return Ok(None);
        }
        self.recv_once_with_timeout().await
    }

    async fn heartbeat(&self, interval: Duration) -> QQClientResult<()> {
        if !self.is_session_open().await {
            return Err(QQClientError::InvalidState(String::from(
                "must connect before heartbeat",
            )));
        }
        if interval.is_zero() {
            return Err(QQClientError::Timeout(String::from(
                "heartbeat interval is zero",
            )));
        }

        let payload = serde_json::json!({ "interval_ms": interval.as_millis() }).to_string();
        let packet = Packet::new("system.heartbeat", payload);
        self.send_packet(packet).await?;
        *self.heartbeat_at.lock().await = Some(SystemTime::now());
        *self.timeout_ms.lock().await = u64::try_from(interval.as_millis()).map_err(|error| {
            QQClientError::Timeout(format!("heartbeat interval too long: {error}"))
        })?;
        Ok(())
    }

    async fn disconnect(&self) -> QQClientResult<()> {
        self.set_state(ConnectionState::Closing).await;
        {
            let mut stream = self.stream.lock().await;
            let stream = stream.take();
            if let Some(stream) = stream {
                let _ = stream.into_std().and_then(|stream| {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    Ok(())
                });
            }
        }

        *self.endpoint.lock().await = None;
        *self.token.lock().await = None;
        *self.heartbeat_at.lock().await = None;
        self.set_state(ConnectionState::Disconnected).await;
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
        assert!(client.has_heartbeat().await);
        assert_eq!(client.state().await, ConnectionState::LoggedIn);
        assert!(client.receive_packet().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn mock_client_rejects_empty_login_payload() {
        let client = MockQQClient::default();
        let connected = client
            .connect(QQClientConfig::new("mock://localhost"))
            .await;
        assert!(connected.is_ok());

        let result = client.login("", "").await;
        assert!(matches!(result, Err(QQClientError::Login(_))));
    }

    #[tokio::test]
    async fn mock_client_send_and_receive_packets() -> QQClientResult<()> {
        let client = MockQQClient::default();
        client
            .connect(QQClientConfig::new("mock://localhost"))
            .await?;
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
    async fn mock_client_disconnect_clears_runtime_state() -> QQClientResult<()> {
        let client = MockQQClient::default();
        client
            .connect(QQClientConfig::new("mock://localhost"))
            .await?;
        client.login("alice", "secret").await?;
        client
            .send_packet(Packet::new("message.send", r#"{"text":"hi"}"#))
            .await?;
        client
            .heartbeat(Duration::from_millis(10))
            .await
            .expect("heartbeat after login");
        client
            .inject_packet(Packet::new("message.received", r#"{"id":"m1","sender_id":"u1","recipient":{"private":{"user_id":"alice"}},"elements":[]}"#))
            .await;

        assert_eq!(client.state().await, ConnectionState::LoggedIn);
        assert!(!client.sent_packets().await.is_empty());
        assert!(client.token().await.is_some());

        client.disconnect().await?;
        assert_eq!(client.state().await, ConnectionState::Disconnected);
        assert!(client.sent_packets().await.is_empty());
        assert_eq!(client.token().await, None);
        assert!(!client.has_heartbeat().await);
        assert!(client.receive_packet().await?.is_none());

        let result = client
            .send_packet(Packet::new("message.send", r#"{"text":"hi"}"#))
            .await;
        assert!(matches!(result, Err(QQClientError::InvalidState(_))));
        Ok(())
    }

    #[tokio::test]
    async fn mock_client_rejects_send_when_logged_out() {
        let client = MockQQClient::default();
        let connected = client
            .connect(QQClientConfig::new("mock://localhost"))
            .await;
        assert!(connected.is_ok());
        let packet = Packet::new("message.send", "{\"text\":\"hi\"}");

        let result = client.send_packet(packet).await;
        assert!(matches!(result, Err(QQClientError::InvalidState(_))));
    }

    #[tokio::test]
    async fn mock_client_heartbeat_requires_positive_interval() -> QQClientResult<()> {
        let client = MockQQClient::default();
        client
            .connect(QQClientConfig::new("mock://localhost"))
            .await?;
        client.login("alice", "secret").await?;
        let result = client.heartbeat(Duration::from_millis(0)).await;
        assert!(matches!(result, Err(QQClientError::Timeout(_))));
        Ok(())
    }

    fn decode_packet(raw: &str) -> Packet {
        let value =
            serde_json::from_str::<TcpWirePacket>(raw).expect("wire packet should be valid json");
        Packet {
            route: value.route,
            payload: value.payload,
        }
    }

    #[tokio::test]
    async fn tcp_client_connect_login_and_receive_message() -> QQClientResult<()> {
        use tokio::io::{AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| QQClientError::Transport(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| QQClientError::Transport(error.to_string()))?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("mock tcp stream accepted");
            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);
            let mut line = String::new();

            let read_count = reader
                .read_line(&mut line)
                .await
                .expect("should receive login packet");
            assert!(read_count > 0);
            let login = decode_packet(line.trim_end());
            assert_eq!(login.route, "auth.login");

            let login_response =
                build_wire_line("auth.ok", r#"{"token":"server-token","uid":"alice"}"#);
            write_half
                .write_all(format!("{login_response}\n").as_bytes())
                .await
                .expect("should write login response");
            write_half
                .flush()
                .await
                .expect("should flush login response");

            let mut send_line = String::new();
            let send_count = reader
                .read_line(&mut send_line)
                .await
                .expect("should receive client message packet");
            assert!(send_count > 0);
            let send_packet = decode_packet(send_line.trim_end());
            assert_eq!(send_packet.route, "message.send");

            let echo = decode_packet(&build_wire_line(
                "message.received",
                r#"{"id":"m2","sender_id":"server","recipient":{"private":{"user_id":"alice"}},"elements":[{"type":"text","text":"reply"}]}"#,
            ));
            let frame = build_wire_line(echo.route, echo.payload);
            write_half
                .write_all(format!("{frame}\n").as_bytes())
                .await
                .expect("should write event");
            write_half.flush().await.expect("should flush event");
        });

        let client = TcpQQClient::default();
        client
            .connect(QQClientConfig::new(format!("tcp://{}", local_addr)))
            .await?;
        assert_eq!(client.state().await, ConnectionState::Connected);
        let token = client.login("alice", "secret").await?;
        assert_eq!(token, "server-token");
        assert_eq!(client.state().await, ConnectionState::LoggedIn);

        client
            .send_packet(Packet::new("message.send", r#"{"text":"hi"}"#))
            .await?;
        let incoming = client.receive_packet().await?;
        assert!(incoming.is_some());

        let inbound = incoming.expect("should get packet");
        assert_eq!(inbound.route, "message.received");

        let _ = server.await;
        Ok(())
    }

    #[tokio::test]
    async fn tcp_client_rejects_invalid_tcp_endpoint() {
        let client = TcpQQClient::default();
        let result = client.connect(QQClientConfig::new("bad-endpoint")).await;
        assert!(matches!(result, Err(QQClientError::Transport(_))));
    }

    #[tokio::test]
    async fn tcp_client_send_requires_login() {
        let client = TcpQQClient::default();
        let result = client
            .send_packet(Packet::new("message.send", r#"{"text":"hi"}"#))
            .await;
        assert!(matches!(result, Err(QQClientError::InvalidState(_))));
    }

    #[tokio::test]
    async fn tcp_client_disconnect_clears_session() -> QQClientResult<()> {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| QQClientError::Transport(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| QQClientError::Transport(error.to_string()))?;

        let accept = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("mock tcp stream accepted");
            tokio::time::sleep(Duration::from_millis(5)).await;
        });

        let client = TcpQQClient::default();
        client
            .connect(QQClientConfig::new(format!("tcp://{}", local_addr)))
            .await?;
        assert_eq!(client.state().await, ConnectionState::Connected);

        client.disconnect().await?;
        assert_eq!(client.state().await, ConnectionState::Disconnected);
        assert!(
            client
                .send_packet(Packet::new("message.send", r#"{"text":"hi"}"#))
                .await
                .is_err()
        );
        assert!(client.receive_packet().await?.is_none());

        let _ = accept.await;
        Ok(())
    }

    #[tokio::test]
    async fn tcp_client_receive_returns_none_after_disconnect() -> QQClientResult<()> {
        use std::sync::Arc;
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;
        use tokio::time::timeout;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| QQClientError::Transport(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| QQClientError::Transport(error.to_string()))?;

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("mock tcp stream accepted");

            let mut buffer = [0_u8; 1024];
            loop {
                match stream.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        let client = Arc::new(TcpQQClient::default());
        client
            .connect(QQClientConfig::new(format!("tcp://{}", local_addr)))
            .await?;
        assert_eq!(client.state().await, ConnectionState::Connected);

        let recv_fut = tokio::spawn({
            let client = client.clone();
            async move { client.receive_packet().await }
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        client.disconnect().await?;

        let got = timeout(Duration::from_millis(200), recv_fut)
            .await
            .expect("receive should finish after disconnect")
            .expect("receive task join ok")?;
        assert!(matches!(got, None));

        let _ = server.await;
        Ok(())
    }

    #[tokio::test]
    async fn tcp_client_reconnect_can_receive_again_after_disconnect() -> QQClientResult<()> {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| QQClientError::Transport(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| QQClientError::Transport(error.to_string()))?;

        let connections = Arc::new(AtomicUsize::new(0));
        let connections_server = connections.clone();

        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.expect("mock tcp stream accepted");

                let attempt = connections_server.fetch_add(1, Ordering::SeqCst);
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = BufReader::new(read_half);

                let mut line = String::new();
                let _ = reader
                    .read_line(&mut line)
                    .await
                    .expect("should receive login packet");
                let login_packet = decode_packet(line.trim_end());
                assert_eq!(login_packet.route, "auth.login");

                let login_response =
                    build_wire_line("auth.ok", r#"{"token":"server-token","uid":"alice"}"#);
                write_half
                    .write_all(format!("{login_response}\n").as_bytes())
                    .await
                    .expect("should write login response");
                write_half
                    .flush()
                    .await
                    .expect("should flush login response");

                if attempt > 0 {
                    let mut send_line = String::new();
                    let _ = reader
                        .read_line(&mut send_line)
                        .await
                        .expect("should receive client packet after reconnect");
                    let send_packet = decode_packet(send_line.trim_end());
                    assert_eq!(send_packet.route, "message.send");

                    let frame = build_wire_line(
                        "message.received",
                        r#"{"id":"m2","sender_id":"server","recipient":{"private":{"user_id":"alice"}},"elements":[{"type":"text","text":"reply"}]}"#,
                    );
                    write_half
                        .write_all(format!("{frame}\n").as_bytes())
                        .await
                        .expect("should write reconnect event");
                    write_half
                        .flush()
                        .await
                        .expect("should flush reconnect event");
                }
            }
        });

        let client = TcpQQClient::default();
        client
            .connect(QQClientConfig::new(format!("tcp://{}", local_addr)))
            .await?;
        assert_eq!(client.state().await, ConnectionState::Connected);
        let token = client.login("alice", "secret").await?;
        assert_eq!(token, "server-token");

        client.disconnect().await?;
        assert_eq!(client.state().await, ConnectionState::Disconnected);

        client
            .connect(QQClientConfig::new(format!("tcp://{}", local_addr)))
            .await?;
        assert_eq!(client.state().await, ConnectionState::Connected);
        let token = client.login("alice", "secret").await?;
        assert_eq!(token, "server-token");

        client
            .send_packet(Packet::new("message.send", r#"{"text":"again"}"#))
            .await?;
        let incoming = client.receive_packet().await?;
        let incoming = incoming.expect("should receive packet");
        assert_eq!(incoming.route, "message.received");

        client.disconnect().await?;
        let _ = server.await;
        assert_eq!(connections.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
