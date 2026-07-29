# Changelog

## 2026-07-29

- feat(performance): optimize runtime and api hot path
  - Reduce runtime shutdown latency with concurrent task joining via `JoinSet` and single take-out of task map.
  - Add service/task registration duplicate prevention.
  - Introduce async API event dispatch queue with bounded capacity and timeout for backpressure control.
  - Reduce cloning overhead in message/protocol handler paths via borrowed arguments.
  - Add benchmark `register_and_shutdown_runtime_with_8_services`.
  - Update performance documentation in `docs/performance.md`.
