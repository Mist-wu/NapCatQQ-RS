use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use napcat_core::{Runtime, RuntimeConfig, RuntimeError, Service};
use tokio::sync::broadcast;

struct FastService;

#[async_trait]
impl Service for FastService {
    fn name(&self) -> &str {
        "fast"
    }

    async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), RuntimeError> {
        let _ = shutdown.recv().await;
        Ok(())
    }
}

fn runtime_lifecycle(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .expect("runtime should build");

    c.bench_function("register_and_shutdown_runtime", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let service_runtime = Runtime::new(RuntimeConfig::default());
                service_runtime.start().await.expect("start runtime");
                service_runtime
                    .register_service(FastService)
                    .await
                    .expect("register service");
                service_runtime.shutdown().await.expect("shutdown runtime");
            });
        });
    });
}

fn runtime_scale(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .expect("runtime should build");

    c.bench_function("register_and_shutdown_runtime_with_8_services", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let service_runtime = Runtime::new(RuntimeConfig::default());
                service_runtime.start().await.expect("start runtime");

                for i in 0..8 {
                    let service_name = format!("svc-{i}");
                    service_runtime
                        .register_service(NamedFastService { service_name })
                        .await
                        .expect("register service");
                }

                service_runtime.shutdown().await.expect("shutdown runtime");
            });
        });
    });
}

#[derive(Clone)]
struct NamedFastService {
    service_name: String,
}

#[async_trait]
impl Service for NamedFastService {
    fn name(&self) -> &str {
        &self.service_name
    }

    async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<(), RuntimeError> {
        let _ = shutdown.recv().await;
        Ok(())
    }
}

criterion_group!(benches, runtime_lifecycle, runtime_scale);
criterion_main!(benches);
