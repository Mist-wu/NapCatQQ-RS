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
- feat(ci): add GitHub Actions CI pipeline
  - Add workflow for `cargo fmt`, `cargo clippy`, and full test suite on push / pull request.
- fix(clippy): make config tests compile clean under stricter clippy warnings
  - Fix redundant struct field names and restore required unsafe blocks for env var mutation in tests.
