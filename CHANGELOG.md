# Changelog

## Unreleased

- Initial Rust rewrite scaffold created for `napcat-rs` workspace.

## [stage-2] 2026-07-29
- Initialize workspace `napcat-rs` with crates: `core`, `protocol`, `message`, `api`, `plugin`, `storage`, `config`, `cli`.
- Add shared dependency set based on Tokio, Axum, Serde, Tracing, SQLx, Clap.
- Add baseline module skeletons and config structures.

## [stage-1] 2026-07-29
- Add `docs/original-analysis.md` summarizing upstream repo structure and migration mapping.
