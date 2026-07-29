# Changelog

## 2026-07-29

- feat(performance): optimize runtime and api hot path
  - Reduce runtime shutdown latency with concurrent task joining via `JoinSet` and single take-out of task map.
  - Add service/task registration duplicate prevention.
  - Introduce async API event dispatch queue with bounded capacity and timeout for backpressure control.
  - Reduce cloning overhead in message/protocol handler paths via borrowed arguments.
  - Add benchmark `register_and_shutdown_runtime_with_8_services`.
  - Update performance documentation in `docs/performance.md`.
- fix(security): add plugin and config validation hardening
  - Add `config` file path canonicalization and regular-file checks before loading.
  - Validate plugin runtime paths with canonicalization and metadata checks.
  - Restrict plugin HTTP endpoints to `http`/`https` and use URL-join for request path building.
  - Enforce bounded plugin timeout range and add unit tests.
  - Add security findings and mitigations in `docs/security-review.md`.
- feat(plugin): expose plugin lifecycle trait
  - Add `Plugin` trait with `initialize`, `on_event`, and `shutdown` methods.
  - Map trait lifecycle to existing plugin backends via blanket implementation.
  - Add unit test verifying trait lifecycle passthrough for backend implementations.
- feat(ci): add GitHub Actions CI pipeline
  - Add workflow for `cargo fmt`, `cargo clippy`, and full test suite on push / pull request.
- fix(clippy): make config tests compile clean under stricter clippy warnings
  - Fix redundant struct field names and restore required unsafe blocks for env var mutation in tests.
- feat(storage): implement `napcat-storage` memory and sqlite backends
  - Add `Storage` abstraction with common async operations (`put`, `get`, `remove`, `keys`, `clear_namespace`, `count_namespace`).
  - Add in-memory and SQLite implementations with namespace/key validation and row metadata.
  - Add SQLite schema bootstrap + upsert/query/delete/list flows.
  - Add storage unit tests for both backends, including namespace isolation and invalid input validation.
