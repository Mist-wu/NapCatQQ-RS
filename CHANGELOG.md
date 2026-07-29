# Changelog

## Unreleased

- Initial Rust rewrite scaffold created for `napcat-rs` workspace.

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
