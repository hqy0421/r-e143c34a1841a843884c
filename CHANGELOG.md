# Changelog

All notable changes to this project will be documented in this file.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Write-readiness probe (`WRITEREADY`, opcode `0x08`) distinct from the
  liveness-only `PING`. The server reports `OK` when write-ready,
  `DRAINING` (status `0x07`) while graceful shutdown is in progress, and
  `WRITE_DISABLED` (status `0x06`) when the engine is in its sticky
  write-refusing state after an uncertain manifest publish. Unreachable
  remains a transport error with no status byte, keeping the three
  non-ready outcomes distinguishable on the client.
- `Engine.WritesDisabled()` accessor exposing the sticky
  `writesDisabled` flag for readiness probing; a fresh `Open` always
  reports `false`.
- `little-db readiness` CLI subcommand: prints `ready` and exits `0`
  when write-ready; new exit codes `6` (draining) and `7` (write
  disabled); unreachable stays exit `1`. See `docs/cli.md`.

### Changed
- `PING` semantics documented as liveness-only: it continues to return
  `pong` regardless of the drain or write-disabled flags.

## [0.1.0] - 2026-05-28

Initial public release.

### Added
- Bitcask-style storage engine: append-only segments, in-memory keydir,
  manifest + hint files, single-writer with group commit, background
  compaction.
- Length-prefixed binary wire protocol (see `docs/SPEC.md`). Supported
  ops: `PING`, `PUT`, `GET`, `DELETE`, `BATCH`, `READKEYRANGE`, `STATS`.
- TCP server (`little-db serve`) with bounded range-scan concurrency
  and structured `slog` logging (`--log-level`, `--log-format`).
- Pure-stdlib Go client library (`internal/client`).
- `little-db` CLI with subcommands: `serve`, `ping`, `put`, `get`,
  `delete`, `batch`, `range`, `stats`. See `docs/cli.md`.
- Test fixtures under `testdata/` and a generator script for bulk
  NDJSON inputs.

[Unreleased]: https://github.com/codeekage/little-db/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/codeekage/little-db/releases/tag/v0.1.0
