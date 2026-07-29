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
}

/// Partial config used for overlay merges.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfigPatch {
    host: Option<String>,
    port: Option<u16>,
    log_level: Option<String>,
    database_url: Option<String>,
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
        let content = fs::read_to_string(path).map_err(|error| ConfigError::FileRead {
            path: path.to_path_buf(),
            source: error,
        })?;

        let value = serde_json::from_str::<AppConfigPatch>(&content).map_err(|error| {
            ConfigError::FileParse {
                path: path.to_path_buf(),
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
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3000,
            log_level: "info".to_string(),
            database_url: "sqlite://./napcat.db".to_string(),
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
}

/// Generic config result.
pub type Result<T> = std::result::Result<T, ConfigError>;

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
            path: path,
            source: error,
        })?;

        assert_eq!(loaded.host, Some("10.0.0.1".to_string()));
        assert_eq!(loaded.port, Some(9000));

        Ok(())
    }
}
