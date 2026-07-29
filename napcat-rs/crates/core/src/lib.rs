//! Core runtime framework.

use std::{collections::HashMap, future::Future, mem, pin::Pin, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Mutex, RwLock, broadcast},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

/// Core error type for runtime orchestration.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// The runtime has reached an invalid state transition.
    #[error("invalid runtime state transition: {0}")]
    InvalidState(String),

    /// A runtime task reported an internal failure.
    #[error("runtime task failed: {0}")]
    TaskFailure(String),

    /// Timeout while waiting for service shutdown.
    #[error("shutdown timeout while waiting for tasks")]
    ShutdownTimeout,
}

/// Result alias used across core APIs.
pub type Result<T> = std::result::Result<T, RuntimeError>;

/// Runtime lifecycle states.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeState {
    /// Runtime created but no explicit start yet.
    Initialized,
    /// Runtime running and accepting registrations.
    Running,
    /// Runtime is stopping and notifying tasks to exit.
    Stopping,
    /// Runtime fully stopped.
    Stopped,
}

/// Configuration for runtime behavior.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Maximum time to wait for all tasks during shutdown.
    pub shutdown_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// Service lifecycle abstraction.
#[async_trait]
pub trait Service: Send + Sync {
    /// Service name.
    fn name(&self) -> &str;

    /// Start service logic. The task should observe `shutdown`.
    async fn start(&self, shutdown: broadcast::Receiver<()>) -> Result<()>;

    /// Optional explicit stop hook.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

type TaskFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

type TaskFactory = Box<dyn FnOnce(broadcast::Receiver<()>) -> TaskFuture + Send + 'static>;

struct ServiceTask {
    handle: JoinHandle<()>,
}

/// Core runtime container.
pub struct Runtime {
    state: RwLock<RuntimeState>,
    shutdown_tx: broadcast::Sender<()>,
    config: RuntimeConfig,
    tasks: Mutex<HashMap<String, ServiceTask>>,
}

impl Runtime {
    /// Create a runtime with the given config.
    pub fn new(config: RuntimeConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        Self {
            state: RwLock::new(RuntimeState::Initialized),
            shutdown_tx,
            config,
            tasks: Mutex::new(HashMap::new()),
        }
    }

    /// Query current runtime state.
    pub async fn state(&self) -> RuntimeState {
        let state = self.state.read().await;
        state.clone()
    }

    /// Start the runtime and allow services/tasks to be registered.
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        match *state {
            RuntimeState::Initialized | RuntimeState::Stopped => {
                *state = RuntimeState::Running;
                Ok(())
            }
            _ => Err(RuntimeError::InvalidState(format!(
                "cannot start from {:?} state",
                *state
            ))),
        }
    }

    /// Register and start one service in the runtime.
    pub async fn register_service<S>(&self, service: S) -> Result<()>
    where
        S: Service + 'static,
    {
        let state = self.state.read().await;
        if *state != RuntimeState::Running {
            return Err(RuntimeError::InvalidState(format!(
                "cannot register service in {:?} state",
                *state
            )));
        }
        drop(state);

        let name = service.name().to_string();
        let receiver = self.shutdown_tx.subscribe();
        let tracing_name = name.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = service.start(receiver).await {
                tracing::error!(error = %err, service = %tracing_name, "service start failed");
            }
        });

        let mut tasks = self.tasks.lock().await;
        if tasks.contains_key(&name) {
            return Err(RuntimeError::TaskFailure(format!(
                "service already registered: {name}"
            )));
        }

        tasks.insert(name, ServiceTask { handle });
        Ok(())
    }

    /// Register and start a custom async task that observes shutdown signals.
    pub async fn spawn_task(&self, name: &str, task: TaskFactory) -> Result<()> {
        let state = self.state.read().await;
        if *state != RuntimeState::Running {
            return Err(RuntimeError::InvalidState(format!(
                "cannot spawn task in {:?} state",
                *state
            )));
        }
        drop(state);

        let receiver = self.shutdown_tx.subscribe();
        let handle = tokio::spawn(task(receiver));

        let mut tasks = self.tasks.lock().await;
        if tasks.contains_key(name) {
            return Err(RuntimeError::TaskFailure(format!(
                "task already registered: {name}"
            )));
        }

        tasks.insert(name.to_string(), ServiceTask { handle });
        Ok(())
    }

    /// Trigger graceful shutdown and wait for all managed tasks.
    pub async fn shutdown(&self) -> Result<usize> {
        {
            let mut state = self.state.write().await;
            if *state == RuntimeState::Stopped {
                return Ok(0);
            }
            if !matches!(*state, RuntimeState::Running | RuntimeState::Stopping) {
                return Err(RuntimeError::InvalidState(format!(
                    "cannot shutdown from {:?} state",
                    *state
                )));
            }
            *state = RuntimeState::Stopping;
        }

        if self.shutdown_tx.receiver_count() > 0 {
            self.shutdown_tx
                .send(())
                .map_err(|error| RuntimeError::TaskFailure(error.to_string()))?;
        }

        let drained_tasks = {
            let mut tasks = self.tasks.lock().await;
            mem::take(&mut *tasks)
        };

        let mut stopped = 0usize;
        let mut join_set = JoinSet::new();
        let shutdown_timeout = self.config.shutdown_timeout;

        for (_, task) in drained_tasks {
            join_set.spawn(async move {
                timeout(shutdown_timeout, task.handle)
                    .await
                    .map_err(|_| RuntimeError::ShutdownTimeout)?
                    .map_err(|join_err| RuntimeError::TaskFailure(join_err.to_string()))
            });
        }

        while let Some(joined) = join_set.join_next().await {
            joined.map_err(|error| RuntimeError::TaskFailure(error.to_string()))??;
            stopped += 1;
        }

        let mut state = self.state.write().await;
        *state = RuntimeState::Stopped;
        Ok(stopped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};

    struct EchoService {
        id: String,
        sender: mpsc::Sender<String>,
    }

    #[async_trait]
    impl Service for EchoService {
        fn name(&self) -> &str {
            &self.id
        }

        async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
            tokio::select! {
                _ = shutdown.recv() => {
                    self.sender
                        .send(format!("{} stopped", self.id))
                        .await
                        .map_err(|err| RuntimeError::TaskFailure(err.to_string()))?;
                    Ok(())
                }
                _ = sleep(Duration::from_secs(2)) => {
                    Err(RuntimeError::TaskFailure(
                        "service timed out waiting shutdown".to_string(),
                    ))
                }
            }
        }
    }

    #[tokio::test]
    async fn runtime_start_and_shutdown_stops_service() -> Result<()> {
        let runtime = Runtime::new(RuntimeConfig::default());
        runtime.start().await?;

        let (tx, mut rx) = mpsc::channel(4);
        let service = EchoService {
            id: String::from("echo"),
            sender: tx,
        };
        runtime.register_service(service).await?;
        assert_eq!(runtime.state().await, RuntimeState::Running);

        let stopped = runtime.shutdown().await?;
        assert_eq!(stopped, 1);
        assert_eq!(runtime.state().await, RuntimeState::Stopped);

        let message = rx.recv().await;
        assert_eq!(message.as_deref(), Some("echo stopped"));

        Ok(())
    }

    #[tokio::test]
    async fn runtime_rejects_invalid_state() {
        let runtime = Runtime::new(RuntimeConfig::default());
        let result = runtime
            .register_service(EchoService {
                id: String::from("bad"),
                sender: mpsc::channel(1).0,
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn runtime_can_register_many_tasks_in_parallel() -> Result<()> {
        let runtime = Runtime::new(RuntimeConfig::default());
        runtime.start().await?;

        let mut services = Vec::new();
        for i in 0..4 {
            let (tx, _rx) = mpsc::channel(1);
            services.push(EchoService {
                id: format!("task-{i}"),
                sender: tx,
            });
        }

        for service in services {
            runtime.register_service(service).await?;
        }

        let stopped = runtime.shutdown().await?;
        assert_eq!(stopped, 4);

        Ok(())
    }
}
