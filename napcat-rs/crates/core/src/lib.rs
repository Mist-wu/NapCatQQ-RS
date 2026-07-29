//! Core workspace primitives.
//!
//! This crate contains runtime-level building blocks.

use serde::{Deserialize, Serialize};

/// Core runtime status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoreState {
    /// Runtime is initializing.
    Initializing,
    /// Runtime is ready and running.
    Running,
    /// Runtime is shutting down.
    Shutdown,
}

/// Core runtime metadata.
#[derive(Debug, Clone)]
pub struct CoreRuntime {
    /// Human-readable service name.
    pub service_name: String,
    /// Current state of the runtime.
    pub state: CoreState,
}

impl CoreRuntime {
    /// Create a new runtime context.
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            state: CoreState::Initializing,
        }
    }
}
