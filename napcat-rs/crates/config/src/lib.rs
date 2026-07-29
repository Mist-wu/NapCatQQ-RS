//! Configuration loading utilities.

use serde::{Deserialize, Serialize};

/// Application configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Service bind address.
    pub bind: String,
}

impl Default for AppConfig {
    /// Build default config values.
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:3000".to_string(),
        }
    }
}
