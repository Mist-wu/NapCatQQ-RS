//! Plugin abstraction crate.

/// Basic plugin metadata.
#[derive(Debug, Clone)]
pub struct PluginMetadata {
    /// Plugin unique name.
    pub name: String,
}

impl PluginMetadata {
    /// Create plugin metadata with a string identifier.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
        }
    }
}
