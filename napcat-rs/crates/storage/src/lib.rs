//! Storage integration module.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::{
    collections::HashMap,
    fs as std_fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::RwLock;

/// Storage-wide result type.
pub type Result<T> = std::result::Result<T, StorageError>;

/// Storage back-end type.
#[derive(Debug, Clone)]
pub enum StorageBackend {
    /// In-memory storage backend.
    /// In-memory store.
    Memory,
    /// SQLite store via SQLx.
    Sqlite(String),
}

/// Trait describing the minimum asynchronous storage operations.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Ensure the backend is initialized and ready.
    async fn initialize(&self) -> Result<()>;

    /// Upsert a JSON value under the provided namespace and key.
    async fn put(
        &self,
        namespace: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<()>;

    /// Get stored record.
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<StoredRecord>>;

    /// Remove a single key from namespace.
    async fn remove(&self, namespace: &str, key: &str) -> Result<bool>;

    /// List all keys in one namespace.
    async fn keys(&self, namespace: &str) -> Result<Vec<String>>;

    /// Delete all entries for one namespace and return number of removed rows.
    async fn clear_namespace(&self, namespace: &str) -> Result<usize>;

    /// Count records in one namespace.
    async fn count_namespace(&self, namespace: &str) -> Result<usize>;
}

impl StorageBackend {
    /// Open the chosen backend and return a storage handle.
    pub async fn connect(self) -> Result<Arc<dyn Storage>> {
        match self {
            StorageBackend::Memory => Ok(Arc::new(MemoryStore::default())),
            StorageBackend::Sqlite(path) => {
                let store = SqliteStore::new(&path).await?;
                store.initialize().await?;
                Ok(Arc::new(store))
            }
        }
    }
}

/// Stored value with metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredRecord {
    /// Namespace to which this record belongs.
    pub namespace: String,
    /// Record key.
    pub key: String,
    /// JSON payload serialized by callers.
    pub value: serde_json::Value,
    /// Unix seconds at first write.
    pub created_at: u64,
    /// Unix seconds at latest write.
    pub updated_at: u64,
}

/// Storage related errors.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Storage input failed validation.
    #[error("invalid storage input: {0}")]
    Validation(String),

    /// File system-level operation failed.
    #[error("io error: {0}")]
    Io(String),

    /// Query or connection-level SQL error.
    #[error("database error: {0}")]
    Database(String),

    /// JSON serialize / deserialize failed.
    #[error("serde error: {0}")]
    Serde(String),

    /// Any unexpected internal failure.
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Default)]
struct MemoryStore {
    values: RwLock<HashMap<String, StoredRecord>>,
}

#[derive(Debug)]
struct SqliteStore {
    pool: SqlitePool,
    db_path: PathBuf,
}

impl MemoryStore {
    fn qualified_key(namespace: &str, key: &str) -> String {
        format!("{namespace}\0{key}")
    }

    fn make_record(
        namespace: &str,
        key: &str,
        value: serde_json::Value,
        existing: Option<(&StoredRecord, u64)>,
    ) -> StoredRecord {
        match existing {
            Some((previous, now)) => StoredRecord {
                namespace: namespace.to_string(),
                key: key.to_string(),
                value,
                created_at: previous.created_at,
                updated_at: now,
            },
            None => {
                let now = unix_seconds_now();
                StoredRecord {
                    namespace: namespace.to_string(),
                    key: key.to_string(),
                    value,
                    created_at: now,
                    updated_at: now,
                }
            }
        }
    }
}

impl SqliteStore {
    async fn new(db_url: &str) -> Result<Self> {
        let db_path = resolve_sqlite_path(db_url)?;
        let database_url = storage_url_from_path(&db_path)?;
        let pool = SqlitePool::connect(&database_url).await.map_err(|error| {
            StorageError::Database(format!("open sqlite backend at `{db_url}` failed: {error}"))
        })?;
        Ok(Self { pool, db_path })
    }

    fn record_to_entry(row: &sqlx::sqlite::SqliteRow) -> Result<StoredRecord> {
        let namespace: String = row
            .try_get("namespace")
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let key: String = row
            .try_get("record_key")
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let value_text: String = row
            .try_get("value")
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let created_at: i64 = row
            .try_get("created_at")
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let updated_at: i64 = row
            .try_get("updated_at")
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let value = serde_json::from_str(&value_text).map_err(|error| {
            StorageError::Serde(format!("deserialize stored record for `{namespace}:{key}` failed: {error}"))
        })?;

        let created_at = u64::try_from(created_at)
            .map_err(|error| StorageError::Internal(format!("invalid created_at value: {error}")))?;
        let updated_at = u64::try_from(updated_at)
            .map_err(|error| StorageError::Internal(format!("invalid updated_at value: {error}")))?;

        Ok(StoredRecord {
            namespace,
            key,
            value,
            created_at,
            updated_at,
        })
    }
}

#[async_trait]
impl Storage for MemoryStore {
    async fn initialize(&self) -> Result<()> {
        Ok(())
    }

    async fn put(&self, namespace: &str, key: &str, value: serde_json::Value) -> Result<()> {
        validate_namespace_and_key(namespace, key)?;
        let now = unix_seconds_now();
        let mut values = self.values.write().await;
        let current = values.remove(&Self::qualified_key(namespace, key));
        let record = Self::make_record(namespace, key, value, current.as_ref().map(|value| (value, now)));
        values.insert(Self::qualified_key(namespace, key), record);
        Ok(())
    }

    async fn get(&self, namespace: &str, key: &str) -> Result<Option<StoredRecord>> {
        validate_namespace_and_key(namespace, key)?;
        let values = self.values.read().await;
        Ok(values.get(&Self::qualified_key(namespace, key)).cloned())
    }

    async fn remove(&self, namespace: &str, key: &str) -> Result<bool> {
        validate_namespace_and_key(namespace, key)?;
        let mut values = self.values.write().await;
        let removed = values.remove(&Self::qualified_key(namespace, key));
        Ok(removed.is_some())
    }

    async fn keys(&self, namespace: &str) -> Result<Vec<String>> {
        validate_namespace(namespace)?;
        let values = self.values.read().await;
        let mut keys = values
            .keys()
            .filter_map(|qualified| {
                let mut split = qualified.splitn(2, '\0');
                let current_namespace = split.next()?;
                let current_key = split.next()?;
                if current_namespace == namespace {
                    Some(current_key.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        keys.sort_unstable();
        Ok(keys)
    }

    async fn clear_namespace(&self, namespace: &str) -> Result<usize> {
        validate_namespace(namespace)?;
        let mut values = self.values.write().await;
        let before = values.len();
        values.retain(|qualified, _| {
            if let Some((current_namespace, _)) = qualified.split_once('\0') {
                current_namespace != namespace
            } else {
                true
            }
        });
        Ok(before.saturating_sub(values.len()))
    }

    async fn count_namespace(&self, namespace: &str) -> Result<usize> {
        validate_namespace(namespace)?;
        let values = self.values.read().await;
        Ok(values
            .keys()
            .filter(|qualified| qualified.starts_with(&format!("{namespace}\0")))
            .count())
    }
}

#[async_trait]
impl Storage for SqliteStore {
    async fn initialize(&self) -> Result<()> {
        let create_table = "
            CREATE TABLE IF NOT EXISTS napcat_storage (
                namespace TEXT NOT NULL,
                record_key TEXT NOT NULL,
                value TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (namespace, record_key)
            )
        ";
        sqlx::query(create_table)
            .execute(&self.pool)
            .await
            .map_err(|error| {
                StorageError::Database(format!(
                    "initialize sqlite table at `{}` failed: {error}",
                    self.db_path.display()
                ))
            })?;
        Ok(())
    }

    async fn put(&self, namespace: &str, key: &str, value: serde_json::Value) -> Result<()> {
        validate_namespace_and_key(namespace, key)?;
        let now = unix_seconds_now();
        let payload = serde_json::to_string(&value)
            .map_err(|error| StorageError::Serde(error.to_string()))?;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let existing = sqlx::query(
            "SELECT created_at FROM napcat_storage WHERE namespace = ?1 AND record_key = ?2",
        )
        .bind(namespace)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;

        let created_at = if let Some(row) = existing {
            let raw: i64 = row
                .try_get("created_at")
                .map_err(|error| StorageError::Database(error.to_string()))?;
            u64::try_from(raw).map_err(|error| {
                StorageError::Internal(format!("invalid created_at value in sqlite: {error}"))
            })?
        } else {
            now
        };

        sqlx::query(
            "INSERT INTO napcat_storage (namespace, record_key, value, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(namespace, record_key)
             DO UPDATE SET value = ?3, updated_at = ?5",
        )
        .bind(namespace)
        .bind(key)
        .bind(payload)
        .bind(created_at as i64)
        .bind(now as i64)
        .execute(&mut *tx)
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;

        tx.commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(())
    }

    async fn get(&self, namespace: &str, key: &str) -> Result<Option<StoredRecord>> {
        validate_namespace_and_key(namespace, key)?;
        let row = sqlx::query(
            "SELECT namespace, record_key, value, created_at, updated_at
             FROM napcat_storage
             WHERE namespace = ?1 AND record_key = ?2",
        )
        .bind(namespace)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;

        match row {
            Some(row) => Ok(Some(SqliteStore::record_to_entry(&row)?)),
            None => Ok(None),
        }
    }

    async fn remove(&self, namespace: &str, key: &str) -> Result<bool> {
        validate_namespace_and_key(namespace, key)?;
        let result = sqlx::query(
            "DELETE FROM napcat_storage WHERE namespace = ?1 AND record_key = ?2",
        )
        .bind(namespace)
        .bind(key)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;

        Ok(result.rows_affected() > 0)
    }

    async fn keys(&self, namespace: &str) -> Result<Vec<String>> {
        validate_namespace(namespace)?;
        let rows = sqlx::query("SELECT record_key FROM napcat_storage WHERE namespace = ?1 ORDER BY record_key ASC")
            .bind(namespace)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let key: String = row
                .try_get("record_key")
                .map_err(|error| StorageError::Database(error.to_string()))?;
            keys.push(key);
        }
        Ok(keys)
    }

    async fn clear_namespace(&self, namespace: &str) -> Result<usize> {
        validate_namespace(namespace)?;
        let affected = sqlx::query("DELETE FROM napcat_storage WHERE namespace = ?1")
            .bind(namespace)
            .execute(&self.pool)
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(affected.rows_affected() as usize)
    }

    async fn count_namespace(&self, namespace: &str) -> Result<usize> {
        validate_namespace(namespace)?;
        let row = sqlx::query("SELECT COUNT(*) AS count FROM napcat_storage WHERE namespace = ?1")
            .bind(namespace)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let count: i64 = row
            .try_get("count")
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let count = usize::try_from(count)
            .map_err(|error| StorageError::Internal(format!("invalid count value: {error}")))?;
        Ok(count)
    }
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn validate_namespace_and_key(namespace: &str, key: &str) -> Result<()> {
    validate_namespace(namespace)?;
    validate_key(key)?;
    Ok(())
}

fn validate_namespace(namespace: &str) -> Result<()> {
    if namespace.trim().is_empty() {
        Err(StorageError::Validation(String::from(
            "namespace cannot be empty",
        )))
    } else {
        Ok(())
    }
}

fn validate_key(key: &str) -> Result<()> {
    if key.trim().is_empty() {
        Err(StorageError::Validation(String::from("key cannot be empty")))
    } else {
        Ok(())
    }
}

fn storage_url_from_path(path: &Path) -> Result<String> {
    if path.as_os_str() == ":memory:" {
        return Ok(String::from("sqlite::memory:"));
    }

    let path_text = path
        .to_str()
        .ok_or_else(|| {
            StorageError::Validation(String::from("database path contains non-utf8 characters"))
        })?;
    if path.is_absolute() {
        Ok(format!("sqlite:{path_text}?mode=rwc"))
    } else {
        Ok(format!("sqlite://{path_text}?mode=rwc"))
    }
}

fn resolve_sqlite_path(raw_path: &str) -> Result<PathBuf> {
    if raw_path.trim().is_empty() {
        return Err(StorageError::Validation(String::from(
            "sqlite path cannot be empty",
        )));
    }

    if raw_path == ":memory:" || raw_path == "sqlite::memory:" {
        return Ok(PathBuf::from(":memory:"));
    }

    let normalized = if raw_path == "sqlite::memory:" {
        PathBuf::from(":memory:")
    } else if raw_path.starts_with("sqlite://") {
        let path = raw_path.trim_start_matches("sqlite://");
        if let Some(rest) = path.strip_prefix('/') {
            Path::new(rest).to_owned()
        } else {
            Path::new(path).to_owned()
        }
    } else {
        Path::new(raw_path).to_owned()
    };

    if normalized.as_os_str().is_empty() {
        return Err(StorageError::Validation(String::from(
            "sqlite path cannot be empty after normalization",
        )));
    }

    if normalized.as_os_str() == ":memory:" {
        return Ok(normalized);
    }

    let absolute = if normalized.is_absolute() {
        normalized
    } else {
        std::env::current_dir()
            .map_err(|error| StorageError::Io(error.to_string()))?
            .join(normalized)
    };

    if let Some(parent) = absolute.parent() {
        std_fs::create_dir_all(parent).map_err(|error| StorageError::Io(error.to_string()))?;
    }

    Ok(absolute)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs as stdfs;
    use tempfile::tempdir;

    fn sample_record() -> serde_json::Value {
        serde_json::json!({
            "content": "hello",
            "version": 1,
            "tags": ["storage", "unit"]
        })
    }

    #[tokio::test]
    async fn memory_put_get_and_remove_flow() -> Result<()> {
        let backend = StorageBackend::Memory.connect().await?;
        backend.put("chat", "message-1", sample_record()).await?;

        let found = backend.get("chat", "message-1").await?;
        let found = found.expect("record should exist");
        assert_eq!(found.namespace, "chat");
        assert_eq!(found.key, "message-1");
        assert_eq!(found.value["content"], "hello");

        let keys = backend.keys("chat").await?;
        assert_eq!(keys, vec![String::from("message-1")]);

        let removed = backend.remove("chat", "message-1").await?;
        assert!(removed);
        assert!(backend.get("chat", "message-1").await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn memory_rejects_invalid_inputs() {
        let backend = StorageBackend::Memory.connect().await.expect("connect backend");
        let err = backend
            .put("", "empty", serde_json::json!({"a":1}))
            .await
            .expect_err("empty namespace should fail");
        assert!(matches!(err, StorageError::Validation(_)));
    }

    #[tokio::test]
    async fn sqlite_backend_roundtrip_and_namespace_isolation() -> Result<()> {
        let dir = tempdir().map_err(|error| StorageError::Io(error.to_string()))?;
        let file_path = dir.path().join("napcat_storage.sqlite");
        stdfs::write(file_path.join("placeholder"), b"").ok();
        let db_path = file_path.clone();

        let backend = StorageBackend::Sqlite(db_path.to_string_lossy().to_string())
            .connect()
            .await?;

        backend
            .put("private", "1", serde_json::json!({"channel":"private"}))
            .await?;
        backend
            .put("group", "1", serde_json::json!({"channel":"group"}))
            .await?;

        let private = backend.get("private", "1").await?;
        let private = private.expect("private record exists");
        assert_eq!(private.value["channel"], "private");

        let private_only = backend.keys("private").await?;
        assert_eq!(private_only, vec![String::from("1")]);

        let group_only = backend.keys("group").await?;
        assert_eq!(group_only, vec![String::from("1")]);

        let cleared = backend.clear_namespace("private").await?;
        assert_eq!(cleared, 1);
        assert!(backend.get("private", "1").await?.is_none());
        assert_eq!(backend.count_namespace("group").await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_rejects_invalid_key_input() -> Result<()> {
        let dir = tempdir().map_err(|error| StorageError::Io(error.to_string()))?;
        let file_path = dir.path().join("napcat_storage_bad.sqlite");
        let backend = StorageBackend::Sqlite(file_path.to_string_lossy().to_string())
            .connect()
            .await?;
        let err = backend
            .put("chat", "", serde_json::json!({"bad":true}))
            .await
            .expect_err("empty key should fail");
        assert!(matches!(err, StorageError::Validation(_)));
        Ok(())
    }
}
