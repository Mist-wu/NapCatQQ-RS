//! Plugin architecture and dynamic runtime support.

use std::{collections::HashMap, io, path::Path, path::PathBuf, process::Stdio, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{io::AsyncWriteExt, process::Command, sync::RwLock, time::timeout};

/// Convenience plugin result type.
pub type PluginResult<T> = std::result::Result<T, PluginError>;

/// Supported plugin execution backends.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginBackendKind {
    /// Rust plugin implemented as an executable adapter.
    Rust,
    /// WASM module loaded through a runtime binary.
    Wasm,
    /// HTTP endpoint plugin.
    Http,
}

/// Plugin metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginMetadata {
    /// Plugin unique name.
    pub name: String,
    /// Human-readable plugin version.
    pub version: String,
    /// Optional short description.
    pub description: Option<String>,
    /// Optional maintainer contact.
    pub author: Option<String>,
}

impl PluginMetadata {
    /// Construct metadata values.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            description: None,
            author: None,
        }
    }
}

/// Plugin lifecycle events handled by backends.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginEvent {
    /// Triggered when plugin is loading.
    Load,
    /// Triggered when plugin is unloading.
    Unload,
    /// Message event from protocol layer.
    Message {
        /// Event payload.
        payload: serde_json::Value,
        /// Optional message source context.
        source: Option<String>,
    },
    /// Health check event.
    HealthCheck,
}

/// Actions that a plugin can emit.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginAction {
    /// Plugin handled event and no outbound payload is emitted.
    Continue,
    /// Plugin emits a payload through bus or callback.
    Emit {
        /// Plugin-generated payload.
        payload: serde_json::Value,
    },
    /// Plugin requests stop.
    Stop,
}

impl PluginAction {
    fn from_stdout_bytes(bytes: &[u8]) -> PluginResult<Option<Self>> {
        let body = String::from_utf8(bytes.to_vec())
            .map_err(|error| PluginError::InvalidUtf8(format!("invalid utf8 payload: {error}")))?;
        let trimmed = body.trim();

        if trimmed.is_empty() {
            return Ok(None);
        }

        let action = serde_json::from_str(trimmed).map_err(|error| {
            PluginError::Deserialize(format!("invalid plugin action payload: {error}"))
        })?;
        Ok(Some(action))
    }
}

/// Plugin source declaration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PluginSource {
    /// Native Rust adapter process.
    Rust {
        /// Absolute or relative executable path.
        executable: PathBuf,
        /// Extra arguments passed each invocation.
        args: Vec<String>,
        /// Default timeout in milliseconds.
        timeout_ms: u64,
    },
    /// Wasm module path executed by a configured runtime.
    Wasm {
        /// Absolute or relative Wasm module path.
        module: PathBuf,
        /// Exported entrypoint name.
        #[serde(default = "PluginSource::default_wasm_entrypoint")]
        entrypoint: String,
        /// Default timeout in milliseconds.
        timeout_ms: u64,
    },
    /// Remote HTTP endpoint plugin.
    Http {
        /// Endpoint base URL.
        endpoint: String,
        /// Default timeout in milliseconds.
        timeout_ms: u64,
    },
}

impl PluginSource {
    fn default_wasm_entrypoint() -> String {
        "handle_event".to_string()
    }
}

/// Plugin registration item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginDefinition {
    /// Plugin metadata.
    pub metadata: PluginMetadata,
    /// Backend source configuration.
    pub source: PluginSource,
    /// Whether plugin should be loaded on registration.
    #[serde(default = "PluginDefinition::default_enabled")]
    pub enabled: bool,
}

impl PluginDefinition {
    /// Build a new plugin definition.
    pub fn new(metadata: PluginMetadata, source: PluginSource) -> Self {
        Self {
            metadata,
            source,
            enabled: true,
        }
    }

    fn default_enabled() -> bool {
        true
    }
}

/// Errors for plugin loading and dispatch.
#[derive(Debug, Error)]
pub enum PluginError {
    /// Plugin name already exists.
    #[error("plugin already loaded: {name}")]
    AlreadyLoaded { name: String },

    /// Plugin metadata name is missing.
    #[error("plugin name cannot be empty")]
    EmptyName,

    /// Plugin was disabled and could not be loaded.
    #[error("plugin disabled: {name}")]
    Disabled { name: String },

    /// IO-level execution failure.
    #[error("io failure: {0}")]
    Io(String),

    /// JSON encode/decode failure.
    #[error("json failure: {0}")]
    Serialize(String),

    /// JSON decode failure.
    #[error("json decode failure: {0}")]
    Deserialize(String),

    /// UTF-8 decoding failure.
    #[error("invalid utf-8: {0}")]
    InvalidUtf8(String),

    /// Runtime command unavailable.
    #[error("plugin runtime unavailable: {0}")]
    RuntimeUnavailable(String),

    /// Timeout exceeded.
    #[error("plugin action timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    /// Plugin could not be found.
    #[error("plugin not found: {name}")]
    NotFound { name: String },

    /// Transport level failure.
    #[error("plugin transport failure: {0}")]
    Transport(String),
}

impl From<io::Error> for PluginError {
    fn from(err: io::Error) -> Self {
        PluginError::Io(err.to_string())
    }
}

enum PluginBackend {
    Rust(RustPlugin),
    Wasm(WasmPlugin),
    Http(HttpPlugin),
}

#[async_trait]
trait PluginBackendRuntime: Send + Sync {
    async fn load(&self) -> PluginResult<()>;
    async fn unload(&self) -> PluginResult<()>;
    async fn dispatch(&self, event: PluginEvent) -> PluginResult<Option<PluginAction>>;
    fn metadata(&self) -> &PluginMetadata;
    fn kind(&self) -> PluginBackendKind;
}

#[async_trait]
impl PluginBackendRuntime for PluginBackend {
    async fn load(&self) -> PluginResult<()> {
        match self {
            PluginBackend::Rust(plugin) => plugin.load().await,
            PluginBackend::Wasm(plugin) => plugin.load().await,
            PluginBackend::Http(plugin) => plugin.load().await,
        }
    }

    async fn unload(&self) -> PluginResult<()> {
        match self {
            PluginBackend::Rust(plugin) => plugin.unload().await,
            PluginBackend::Wasm(plugin) => plugin.unload().await,
            PluginBackend::Http(plugin) => plugin.unload().await,
        }
    }

    async fn dispatch(&self, event: PluginEvent) -> PluginResult<Option<PluginAction>> {
        match self {
            PluginBackend::Rust(plugin) => plugin.dispatch(event).await,
            PluginBackend::Wasm(plugin) => plugin.dispatch(event).await,
            PluginBackend::Http(plugin) => plugin.dispatch(event).await,
        }
    }

    fn metadata(&self) -> &PluginMetadata {
        match self {
            PluginBackend::Rust(plugin) => plugin.metadata(),
            PluginBackend::Wasm(plugin) => plugin.metadata(),
            PluginBackend::Http(plugin) => plugin.metadata(),
        }
    }

    fn kind(&self) -> PluginBackendKind {
        match self {
            PluginBackend::Rust(plugin) => plugin.kind(),
            PluginBackend::Wasm(plugin) => plugin.kind(),
            PluginBackend::Http(plugin) => plugin.kind(),
        }
    }
}

#[derive(Debug)]
struct RustPlugin {
    metadata: PluginMetadata,
    executable: PathBuf,
    args: Vec<String>,
    timeout: Duration,
}

impl RustPlugin {
    fn validate_path(path: &Path) -> PluginResult<()> {
        if !path.exists() {
            return Err(PluginError::RuntimeUnavailable(format!(
                "rust plugin executable missing: {}",
                path.display()
            )));
        }
        Ok(())
    }

    async fn run_plugin(
        &self,
        phase: &str,
        event: Option<&PluginEvent>,
    ) -> PluginResult<Option<PluginAction>> {
        let mut command = Command::new(&self.executable);
        command.env("NAPCAT_PLUGIN_PHASE", phase);
        command.args(self.args.iter().cloned());
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());

        let mut child = command.spawn().map_err(|error| {
            PluginError::RuntimeUnavailable(format!(
                "cannot spawn rust plugin {}: {error}",
                self.executable.display()
            ))
        })?;

        if let Some(event_payload) = event && let Some(mut stdin) = child.stdin.take() {
            let event_body = serde_json::to_vec(event_payload)
                .map_err(|error| PluginError::Serialize(error.to_string()))?;
            stdin.write_all(&event_body).await?;
        }

        let output = timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| PluginError::Timeout {
                timeout_ms: self.timeout.as_millis() as u64,
            })?;
        let output = output.map_err(|error| PluginError::Io(error.to_string()))?;

        if !output.status.success() {
            return Err(PluginError::Transport(String::from(
                "rust plugin returned non-zero exit code",
            )));
        }

        PluginAction::from_stdout_bytes(&output.stdout)
    }
}

#[async_trait]
impl PluginBackendRuntime for RustPlugin {
    async fn load(&self) -> PluginResult<()> {
        self.run_plugin("load", None).await?;
        Ok(())
    }

    async fn unload(&self) -> PluginResult<()> {
        self.run_plugin("unload", None).await?;
        Ok(())
    }

    async fn dispatch(&self, event: PluginEvent) -> PluginResult<Option<PluginAction>> {
        self.run_plugin("event", Some(&event)).await
    }

    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn kind(&self) -> PluginBackendKind {
        PluginBackendKind::Rust
    }
}

#[derive(Debug)]
struct WasmPlugin {
    metadata: PluginMetadata,
    module: PathBuf,
    entrypoint: String,
    timeout: Duration,
}

impl WasmPlugin {
    fn validate_path(path: &Path) -> PluginResult<()> {
        if !path.exists() {
            return Err(PluginError::RuntimeUnavailable(format!(
                "wasm module missing: {}",
                path.display()
            )));
        }

        Ok(())
    }

    async fn run_plugin(&self, event: Option<&PluginEvent>) -> PluginResult<Option<PluginAction>> {
        let mut command = Command::new("wasmtime");
        command.arg("run");
        command.arg("--invoke");
        command.arg(&self.entrypoint);
        command.arg(&self.module);

        if let Some(event_payload) = event {
            let event_body = serde_json::to_string(event_payload)
                .map_err(|error| PluginError::Serialize(error.to_string()))?;
            command.arg(event_body);
        }

        command.stdout(Stdio::piped());

        let output =
            timeout(self.timeout, command.output())
                .await
                .map_err(|_| PluginError::Timeout {
                    timeout_ms: self.timeout.as_millis() as u64,
                })?;
        let output = output.map_err(|error| PluginError::Io(error.to_string()))?;

        if !output.status.success() {
            return Err(PluginError::Transport(String::from(
                "wasm plugin runtime failed",
            )));
        }

        PluginAction::from_stdout_bytes(&output.stdout)
    }
}

#[async_trait]
impl PluginBackendRuntime for WasmPlugin {
    async fn load(&self) -> PluginResult<()> {
        self.run_plugin(None).await?;
        Ok(())
    }

    async fn unload(&self) -> PluginResult<()> {
        self.run_plugin(None).await?;
        Ok(())
    }

    async fn dispatch(&self, event: PluginEvent) -> PluginResult<Option<PluginAction>> {
        self.run_plugin(Some(&event)).await
    }

    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn kind(&self) -> PluginBackendKind {
        PluginBackendKind::Wasm
    }
}

#[derive(Debug)]
struct HttpPlugin {
    metadata: PluginMetadata,
    endpoint: String,
    timeout: Duration,
}

#[derive(Serialize)]
struct HttpPluginRequest {
    event: PluginEvent,
}

#[async_trait]
impl PluginBackendRuntime for HttpPlugin {
    async fn load(&self) -> PluginResult<()> {
        self.request("load", PluginEvent::Load).await?;
        Ok(())
    }

    async fn unload(&self) -> PluginResult<()> {
        self.request("unload", PluginEvent::Unload).await?;
        Ok(())
    }

    async fn dispatch(&self, event: PluginEvent) -> PluginResult<Option<PluginAction>> {
        self.request("event", event).await
    }

    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn kind(&self) -> PluginBackendKind {
        PluginBackendKind::Http
    }
}

impl HttpPlugin {
    async fn request(&self, path: &str, event: PluginEvent) -> PluginResult<Option<PluginAction>> {
        let mut target = self.endpoint.clone();
        if !target.ends_with('/') {
            target.push('/');
        }
        target.push_str(path);

        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(|error| PluginError::Transport(error.to_string()))?;

        let request = HttpPluginRequest { event };
        let response = client
            .post(target)
            .json(&request)
            .send()
            .await
            .map_err(|error| PluginError::Transport(error.to_string()))?;

        if !response.status().is_success() {
            return Err(PluginError::Transport(format!(
                "http plugin status {}",
                response.status()
            )));
        }

        let body = response
            .bytes()
            .await
            .map_err(|error| PluginError::Transport(error.to_string()))?;

        PluginAction::from_stdout_bytes(&body)
    }
}

#[derive(Default)]
pub struct PluginManager {
    plugins: RwLock<HashMap<String, Box<dyn PluginBackendRuntime>>>,
}

impl PluginManager {
    /// Create an empty plugin manager.
    pub fn new() -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
        }
    }

    /// Load a plugin from definition and keep it in the active registry.
    pub async fn load(&self, definition: PluginDefinition) -> PluginResult<String> {
        if definition.metadata.name.trim().is_empty() {
            return Err(PluginError::EmptyName);
        }

        if !definition.enabled {
            return Err(PluginError::Disabled {
                name: definition.metadata.name,
            });
        }

        let name = definition.metadata.name.clone();
        {
            let plugins = self.plugins.read().await;
            if plugins.contains_key(&name) {
                return Err(PluginError::AlreadyLoaded { name: name.clone() });
            }
        }

        let backend = Self::backend_from_definition(definition)?;
        backend.load().await?;

        let mut plugins = self.plugins.write().await;
        plugins.insert(name.clone(), Box::new(backend));
        Ok(name)
    }

    /// Unload a plugin and remove it from registry.
    pub async fn unload(&self, name: &str) -> PluginResult<()> {
        let plugin = {
            let mut plugins = self.plugins.write().await;
            plugins.remove(name)
        };

        if let Some(plugin) = plugin {
            plugin.unload().await?;
            Ok(())
        } else {
            Err(PluginError::NotFound {
                name: name.to_string(),
            })
        }
    }

    /// Dispatch an event to all loaded plugins.
    pub async fn dispatch(&self, event: PluginEvent) -> PluginResult<Vec<(String, PluginAction)>> {
        let plugins = self.plugins.read().await;
        let mut actions = Vec::with_capacity(plugins.len());

        for (name, plugin) in plugins.iter() {
            if let Some(action) = plugin.dispatch(event.clone()).await? {
                actions.push((name.clone(), action));
            }
        }

        Ok(actions)
    }

    /// List all loaded plugin names.
    pub async fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.plugins.read().await.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// Return runtime kinds for all loaded plugins.
    pub async fn kinds(&self) -> HashMap<String, PluginBackendKind> {
        let plugins = self.plugins.read().await;
        plugins
            .iter()
            .map(|(name, backend)| (name.clone(), backend.kind()))
            .collect()
    }

    /// Return metadata for an active plugin.
    pub async fn metadata(&self, name: &str) -> PluginResult<PluginMetadata> {
        let plugins = self.plugins.read().await;
        let plugin = plugins.get(name).ok_or_else(|| PluginError::NotFound {
            name: name.to_string(),
        })?;
        Ok(plugin.metadata().clone())
    }

    fn backend_from_definition(definition: PluginDefinition) -> PluginResult<PluginBackend> {
        let metadata = definition.metadata;

        match definition.source {
            PluginSource::Rust {
                executable,
                args,
                timeout_ms,
            } => {
                RustPlugin::validate_path(&executable)?;
                Ok(PluginBackend::Rust(RustPlugin {
                    metadata,
                    executable,
                    args,
                    timeout: Duration::from_millis(timeout_ms),
                }))
            }
            PluginSource::Wasm {
                module,
                entrypoint,
                timeout_ms,
            } => {
                WasmPlugin::validate_path(&module)?;
                Ok(PluginBackend::Wasm(WasmPlugin {
                    metadata,
                    module,
                    entrypoint,
                    timeout: Duration::from_millis(timeout_ms),
                }))
            }
            PluginSource::Http {
                endpoint,
                timeout_ms,
            } => Ok(PluginBackend::Http(HttpPlugin {
                metadata,
                endpoint,
                timeout: Duration::from_millis(timeout_ms),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plugin_definition_loader_rejects_duplicate_name() -> PluginResult<()> {
        let manager = PluginManager::new();
        let definition = PluginDefinition {
            metadata: PluginMetadata::new("dup", "0.1.0"),
            source: PluginSource::Rust {
                executable: PathBuf::from("/bin/sh"),
                args: vec![String::from("-c"), String::from("cat >/dev/null")],
                timeout_ms: 200,
            },
            enabled: true,
        };

        let name = manager.load(definition.clone()).await?;
        assert_eq!(name, "dup");

        let duplicate = manager.load(definition).await;
        assert!(matches!(duplicate, Err(PluginError::AlreadyLoaded { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn plugin_manager_requires_enabled_definition() {
        let manager = PluginManager::new();
        let definition = PluginDefinition {
            metadata: PluginMetadata::new("disabled", "0.1.0"),
            source: PluginSource::Rust {
                executable: PathBuf::from("/bin/sh"),
                args: vec![String::from("-c"), String::from("cat >/dev/null")],
                timeout_ms: 200,
            },
            enabled: false,
        };

        let result = manager.load(definition).await;
        assert!(matches!(result, Err(PluginError::Disabled { name }) if name == "disabled"));
    }

    #[tokio::test]
    async fn plugin_manager_unload_unknown_is_not_found() {
        let manager = PluginManager::new();
        let result = manager.unload("missing").await;
        assert!(matches!(result, Err(PluginError::NotFound { .. })));
    }

    #[tokio::test]
    async fn plugin_dispatch_returns_empty_for_no_response() -> PluginResult<()> {
        let manager = PluginManager::new();
        let definition = PluginDefinition {
            metadata: PluginMetadata::new("silent", "0.1.0"),
            source: PluginSource::Rust {
                executable: PathBuf::from("/bin/sh"),
                args: vec![String::from("-c"), String::from("cat >/dev/null")],
                timeout_ms: 200,
            },
            enabled: true,
        };

        manager.load(definition).await?;

        let actions = manager
            .dispatch(PluginEvent::Message {
                payload: serde_json::json!({"message":"hello"}),
                source: Some(String::from("test")),
            })
            .await?;

        assert!(actions.is_empty());
        Ok(())
    }
}
