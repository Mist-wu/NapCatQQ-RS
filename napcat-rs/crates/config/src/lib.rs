//! Configuration loading and environment/file override support.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Top-level application config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    /// HTTP bind host.
    pub host: String,
    /// HTTP listen port.
    pub port: u16,
    /// Default log level.
    pub log_level: String,
    /// Optional workspace database path.
    pub database_url: String,
    /// Protocol backend mode.
    pub protocol_mode: String,
    /// Protocol endpoint URL for real backend mode.
    pub protocol_endpoint: String,
    /// Optional protocol access token.
    pub protocol_access_token: Option<String>,
    /// QQ account identifier for real QQ login.
    pub qq_account: Option<String>,
    /// QQ account secret or auth ticket.
    pub qq_password: Option<String>,
    /// Protocol listener poll timeout.
    pub protocol_listen_timeout_ms: u64,
    /// Protocol listener max batch size.
    pub protocol_listen_max_events: usize,
}

/// Partial config used for overlay merges.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfigPatch {
    host: Option<String>,
    port: Option<u16>,
    log_level: Option<String>,
    database_url: Option<String>,
    protocol_mode: Option<String>,
    protocol_endpoint: Option<String>,
    protocol_access_token: Option<Option<String>>,
    qq_account: Option<Option<String>>,
    qq_password: Option<Option<String>>,
    protocol_listen_timeout_ms: Option<u64>,
    protocol_listen_max_events: Option<usize>,
}

impl AppConfig {
    /// Load config from environment + file with defaults.
    pub fn load() -> Result<Self> {
        let mut config = AppConfig::default();
        if let Ok(path) = env::var("NAPCAT_CONFIG_PATH") {
            let file_config = Self::load_file(Path::new(&path))?;
            config.apply_patch(file_config);
        }

        let env_config = Self::load_env()?;
        config.apply_patch(env_config);
        Ok(config)
    }

    /// Build config from JSON config path.
    pub fn load_file(path: &Path) -> Result<AppConfigPatch> {
        let safe_path = validate_config_path(path)?;
        let content = fs::read_to_string(&safe_path).map_err(|error| ConfigError::FileRead {
            path: safe_path.clone(),
            source: error,
        })?;

        let value = serde_json::from_str::<AppConfigPatch>(&content).map_err(|error| {
            ConfigError::FileParse {
                path: safe_path,
                source: error,
            }
        })?;

        Ok(value)
    }

    /// Build a patch from environment variables.
    pub fn load_env() -> Result<AppConfigPatch> {
        let mut patch = AppConfigPatch::default();

        if let Some(host) = env_var_optional("NAPCAT_HOST") {
            patch.host = Some(host);
        }

        if let Some(port_text) = env_var_optional("NAPCAT_PORT") {
            let parsed = port_text
                .parse::<u16>()
                .map_err(|error| ConfigError::EnvParse {
                    key: "NAPCAT_PORT".to_string(),
                    details: error.to_string(),
                })?;
            patch.port = Some(parsed);
        }

        if let Some(log_level) = env_var_optional("NAPCAT_LOG_LEVEL") {
            patch.log_level = Some(log_level);
        }

        if let Some(database_url) = env_var_optional("NAPCAT_DATABASE_URL") {
            patch.database_url = Some(database_url);
        }

        if let Some(protocol_mode) = env_var_optional("NAPCAT_PROTOCOL_MODE") {
            patch.protocol_mode = Some(protocol_mode);
        }

        if let Some(protocol_endpoint) = env_var_optional("NAPCAT_PROTOCOL_ENDPOINT") {
            patch.protocol_endpoint = Some(protocol_endpoint);
        }

        if let Some(protocol_access_token) = env_var_optional("NAPCAT_PROTOCOL_ACCESS_TOKEN") {
            patch.protocol_access_token = Some(Some(protocol_access_token));
        }

        if let Some(qq_account) = env_var_optional("NAPCAT_QQ_ACCOUNT") {
            patch.qq_account = Some(Some(qq_account));
        }

        if let Some(qq_password) = env_var_optional("NAPCAT_QQ_PASSWORD") {
            patch.qq_password = Some(Some(qq_password));
        }

        if let Some(timeout_text) = env_var_optional("NAPCAT_PROTOCOL_LISTEN_TIMEOUT_MS") {
            patch.protocol_listen_timeout_ms = Some(timeout_text.parse::<u64>().map_err(
                |error| ConfigError::EnvParse {
                    key: "NAPCAT_PROTOCOL_LISTEN_TIMEOUT_MS".to_string(),
                    details: error.to_string(),
                },
            )?);
        }

        if let Some(max_events_text) = env_var_optional("NAPCAT_PROTOCOL_LISTEN_MAX_EVENTS") {
            patch.protocol_listen_max_events = Some(max_events_text.parse::<usize>().map_err(
                |error| ConfigError::EnvParse {
                    key: "NAPCAT_PROTOCOL_LISTEN_MAX_EVENTS".to_string(),
                    details: error.to_string(),
                },
            )?);
        }

        Ok(patch)
    }

    fn apply_patch(&mut self, patch: AppConfigPatch) {
        if let Some(host) = patch.host {
            self.host = host;
        }
        if let Some(port) = patch.port {
            self.port = port;
        }
        if let Some(log_level) = patch.log_level {
            self.log_level = log_level;
        }
        if let Some(database_url) = patch.database_url {
            self.database_url = database_url;
        }
        if let Some(protocol_mode) = patch.protocol_mode {
            self.protocol_mode = protocol_mode;
        }
        if let Some(protocol_endpoint) = patch.protocol_endpoint {
            self.protocol_endpoint = protocol_endpoint;
        }
        if let Some(protocol_access_token) = patch.protocol_access_token {
            self.protocol_access_token = protocol_access_token;
        }
        if let Some(qq_account) = patch.qq_account {
            self.qq_account = qq_account;
        }
        if let Some(qq_password) = patch.qq_password {
            self.qq_password = qq_password;
        }
        if let Some(protocol_listen_timeout_ms) = patch.protocol_listen_timeout_ms {
            self.protocol_listen_timeout_ms = protocol_listen_timeout_ms;
        }
        if let Some(protocol_listen_max_events) = patch.protocol_listen_max_events {
            self.protocol_listen_max_events = protocol_listen_max_events;
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3000,
            log_level: "info".to_string(),
            database_url: "sqlite://./napcat.db".to_string(),
            protocol_mode: String::from("mock"),
            protocol_endpoint: String::new(),
            protocol_access_token: None,
            qq_account: None,
            qq_password: None,
            protocol_listen_timeout_ms: 600,
            protocol_listen_max_events: 8,
        }
    }
}

fn env_var_optional(name: &str) -> Option<String> {
    env::var(name).ok()
}

/// Configuration related errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Failed to read a file configuration.
    #[error("read config file `{path}` failed: {source}")]
    FileRead {
        /// config file path.
        path: PathBuf,
        /// underlying file error.
        source: std::io::Error,
    },

    /// Failed to parse config file.
    #[error("parse config file `{path}` failed: {source}")]
    FileParse {
        /// config file path.
        path: PathBuf,
        /// underlying parse error.
        source: serde_json::Error,
    },

    /// Environment variable parse failed.
    #[error("parse env `{key}` failed: {details}")]
    EnvParse {
        /// environment key.
        key: String,
        /// reason.
        details: String,
    },

    /// Invalid config path.
    #[error("invalid config path `{path}`: {details}")]
    InvalidPath {
        /// config file path.
        path: PathBuf,
        /// validation details.
        details: String,
    },
}

/// Generic config result.
pub type Result<T> = std::result::Result<T, ConfigError>;

fn validate_config_path(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).map_err(|error| ConfigError::InvalidPath {
        path: path.to_path_buf(),
        details: error.to_string(),
    })?;

    if !canonical.is_file() {
        return Err(ConfigError::InvalidPath {
            path: canonical,
            details: String::from("target is not a regular file"),
        });
    }

    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn with_env_restore(name: &str, value: Option<&str>) -> Option<String> {
        let previous = env::var(name).ok();
        match value {
            Some(next) => unsafe { env::set_var(name, next) },
            None => unsafe { env::remove_var(name) },
        }
        previous
    }

    fn restore_env(name: &str, previous: Option<String>) {
        match previous {
            Some(value) => unsafe { env::set_var(name, value) },
            None => unsafe { env::remove_var(name) },
        }
    }

    fn restore_two_envs(a: Option<String>, b: Option<String>) {
        restore_env("NAPCAT_HOST", a);
        restore_env("NAPCAT_PORT", b);
    }

    #[test]
    fn config_defaults_are_used_without_inputs() -> Result<()> {
        let backup_path = with_env_restore("NAPCAT_CONFIG_PATH", None);
        let backup_host = with_env_restore("NAPCAT_HOST", None);
        let backup_port = with_env_restore("NAPCAT_PORT", None);

        let config = AppConfig::load()?;
        let expected = AppConfig::default();

        restore_env("NAPCAT_CONFIG_PATH", backup_path);
        restore_two_envs(backup_host, backup_port);

        assert_eq!(expected.host, config.host);
        assert_eq!(expected.port, config.port);
        assert_eq!(expected.log_level, config.log_level);
        Ok(())
    }

    #[test]
    fn config_overrides_with_env() -> Result<()> {
        let backup_host = with_env_restore("NAPCAT_HOST", Some("0.0.0.0"));
        let backup_port = with_env_restore("NAPCAT_PORT", Some("8080"));
        let backup_path = with_env_restore("NAPCAT_CONFIG_PATH", None);

        let config = AppConfig::load()?;

        restore_env("NAPCAT_CONFIG_PATH", backup_path);
        restore_two_envs(backup_host, backup_port);

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8080);
        Ok(())
    }

    #[test]
    fn config_file_merges_with_defaults() -> Result<()> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| ConfigError::EnvParse {
                key: "time".to_string(),
                details: error.to_string(),
            })?
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("napcat-config-{}.json", nanos));

        {
            let mut file = fs::File::create(&path).map_err(|error| ConfigError::FileRead {
                path: path.clone(),
                source: error,
            })?;
            file.write_all(b"{\"host\":\"10.0.0.1\",\"port\":9000}\n")
                .map_err(|error| ConfigError::FileRead {
                    path: path.clone(),
                    source: error,
                })?;
        }

        let loaded = AppConfig::load_file(&path)?;
        fs::remove_file(&path).map_err(|error| ConfigError::FileRead {
            path,
            source: error,
        })?;

        assert_eq!(loaded.host, Some("10.0.0.1".to_string()));
        assert_eq!(loaded.port, Some(9000));

        Ok(())
    }
}
