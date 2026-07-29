# Changelog

## Unreleased
- ci: add root-level GitHub Actions workflow to run fmt/clippy/test/bench in napcat-rs workspace.
- docs: refresh benchmark timing summary in `docs/test-report.md` to latest `cargo bench` output.
- docs: add root docs/performance.md and root docs/security-review.md; refresh docs/test-report.md with latest bench and test counts.
- feat(api): expose runtime running state during run lifetime and add login status runtime online test.

- Initial Rust rewrite scaffold created for `napcat-rs` workspace.
- Implement HTTP and WebSocket API module with login/message/group/user endpoints and runtime event stream, plus `SendRequest` serialization and endpoint tests.
- Finalize API compatibility layer and stabilize API send/listen behavior: add response envelope serialization, protocol error conversion, and successful compatibility responses.
- Implemented dynamic plugin architecture in `napcat-plugin`, including Rust/Wasm/HTTP plugin backends, async load/unload/dispatch APIs, plugin metadata/versioned definitions, and registry operations with unit tests.
- Add stage-6 test infrastructure under `napcat-rs/tests` with unit tests, integration tests, and Criterion benchmark harness:
  - add dedicated test crate for runtime, API/protocol integration checks, and benchmarking lifecycle overhead.
  - add `runtime_pipeline` benchmark (`register_and_shutdown_runtime`) and align plugin loading API for test-safe phase injection.
  - record benchmark result: `register_and_shutdown_runtime 9.2008 µs .. 10.352 µs` in local CI-style validation.
- docs: enrich stage-1 analysis mapping and add API/部署/测试报告文档补充 (`docs/original-analysis.md`)。
- docs: add API 文档与接口清单（`/message/send`、`/send_msg`、`/send_private_msg`、`/send_group_msg`、`/message/listen`、WebSocket 等），并说明统一响应模型与错误码。
- docs: add 部署手册（`docs/deploy.md`）与测试报告（`docs/test-report.md`）。
- test: 记录 `cargo fmt --all -- --check`、`cargo clippy`、`cargo test --workspace --all-targets`、`cargo bench` 的云端执行结果，基准：
  - `register_and_shutdown_runtime`: `9.5319 µs ~ 9.7830 µs`
  - `register_and_shutdown_runtime_with_8_services`: `31.236 µs ~ 31.955 µs`
- feat(cli): make `napcat-cli` executable with config/env overrides for host and port and debug log-level switch.
- feat(storage): add `napcat-storage` backends with memory and sqlite implementations
  - add `Storage` abstraction with async API and typed record model.
  - add input validation, namespace isolation, metadata timestamps, and clear/list/count operations.
  - add sqlite table initialization, atomic upsert, and deterministic test coverage for both backends.

## [stage-2] 2026-07-29
- Initialize workspace `napcat-rs` with crates: `core`, `protocol`, `message`, `api`, `plugin`, `storage`, `config`, `cli`.
- Add shared dependency set based on Tokio, Axum, Serde, Tracing, SQLx, Clap.
- Add baseline module skeletons and config structures.

## [stage-1] 2026-07-29
- Add `docs/original-analysis.md` summarizing upstream repo structure and migration mapping.

## [stage-3] 2026-07-29
- Implemented core runtime in `napcat-core` with:
  - runtime lifecycle state machine (`Initialized/Running/Stopping/Stopped`),
  - broadcast shutdown channel orchestration,
  - service registration, async task registration, and graceful shutdown timeout logic,
  - runtime-level unit tests for invalid state guard and shutdown behavior.

## [stage-3 config] 2026-07-29
- Implemented `AppConfig` loading pipeline in `napcat-config`:
  - default config values,
  - optional file config from `NAPCAT_CONFIG_PATH`,
  - environment overrides (`NAPCAT_HOST`, `NAPCAT_PORT`, `NAPCAT_LOG_LEVEL`, `NAPCAT_DATABASE_URL`),
  - overlay merge + unit tests.

## [stage-3 message] 2026-07-29
- Implemented unified message model in `napcat-message`:
  - support private/group recipients and elements (`Text`, `Image`, `File`, `At`, `Reply`, `Json`),
  - channel helpers and message helper predicates,
  - `MessageHandler` async trait and reusable JSON encode/decode helpers,
  - dispatcher helper and unit tests.

## [stage-3 protocol] 2026-07-29
- Introduced `ProtocolBackend` trait in `napcat-protocol` to decouple business logic from protocol implementation.
- Added protocol capabilities, event enum, mock backend, and event ↔ handler forwarding helper.
- Added JSON serialization utilities and protocol tests for capability, connect/login state, and message forwarding.
