//! Storage integration module.

/// Storage back-end type.
#[derive(Debug, Clone)]
pub enum StorageBackend {
    /// In-memory store.
    Memory,
    /// SQLite store via SQLx.
    Sqlite(String),
}
