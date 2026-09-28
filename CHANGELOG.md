# Changelog

All notable changes to `presence-io` will be documented in this file.

## [0.1.0] - 2026-09-28

### Added
- Initial release of `presence-io`.
- Framework-agnostic Socket.io + Redis presence manager.
- Multi-socket per user tracking (multi-tab support).
- Room-scoped presence tracking and diff events (`user:online`, `user:offline`, `room:join`, `room:leave`).
- Atomic Lua scripts for all Redis state transitions.
- Background stale sweep for crash recovery without ghost users.
- Dual CJS and ESM module exports with full TypeScript definitions.
