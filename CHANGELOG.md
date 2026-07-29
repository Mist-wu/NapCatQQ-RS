# Changelog

## Unreleased

- Initial Rust rewrite scaffold created for `napcat-rs` workspace.
- Implement HTTP and WebSocket API module with login/message/group/user endpoints and runtime event stream, plus `SendRequest` serialization and endpoint tests.

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
