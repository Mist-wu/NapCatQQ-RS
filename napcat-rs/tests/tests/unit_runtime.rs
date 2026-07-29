//! Unit tests for core runtime behavior.

use async_trait::async_trait;
use napcat_core::{
    Result as CoreResult, Runtime, RuntimeConfig, RuntimeError, RuntimeState, Service,
};
use tokio::sync::broadcast;

struct SilentService;

#[async_trait]
impl Service for SilentService {
    fn name(&self) -> &str {
        "silent"
    }

    async fn start(&self, shutdown: broadcast::Receiver<()>) -> CoreResult<()> {
        let mut shutdown = shutdown;
        let _ = shutdown.recv().await;
        Ok(())
    }
}

#[tokio::test]
async fn runtime_state_starts_initialized_then_running() {
    let runtime = Runtime::new(RuntimeConfig::default());

    assert_eq!(runtime.state().await, RuntimeState::Initialized);
    runtime.start().await.expect("runtime should start");
    assert_eq!(runtime.state().await, RuntimeState::Running);
}

#[tokio::test]
async fn runtime_rejects_second_start() {
    let runtime = Runtime::new(RuntimeConfig::default());
    runtime.start().await.expect("runtime should start");

    let err = runtime.start().await.expect_err("second start should fail");
    assert!(matches!(err, RuntimeError::InvalidState { .. }));
}

#[tokio::test]
async fn runtime_register_and_shutdown_tasks() -> CoreResult<()> {
    let runtime = Runtime::new(RuntimeConfig::default());
    runtime.start().await?;

    runtime
        .register_service(SilentService)
        .await
        .expect("register service");

    let stopped = runtime.shutdown().await?;
    assert_eq!(stopped, 1);
    assert_eq!(runtime.state().await, RuntimeState::Stopped);

    Ok(())
}
