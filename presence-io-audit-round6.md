# presence-io — Audit Round 6 (final pre-publish check)

**Verdict: ready to publish `0.1.0`.** All previously reported bugs are fixed and independently re-verified. Only optional polish remains.

## Verified on this build (real Redis 7)

| Check | Result |
|---|---|
| Unit (12) + `test:integration` (audit 29 + audit3 12 + audit4 5 + audit5 5 + real-redis 6 = 57) | All pass |
| Audit files not weakened (all four) | Confirmed: every `expect(...)` and title identical to my originals |
| Round-5 P1: `heartbeat()` Redis failure reaches the `error` event | **Fixed** (H1 passes; 1 error event) |
| Round-5 P2: throwing `error` listener (async-rejection path and sync-listener path) | **Fixed** (child process on built `dist/index.cjs`: both survive, `track()` resolves, logged via `console.error`) |
| typecheck, lint, build, `smoke:pack`, `publint` ("All good!"), `attw` ("No problems found") | Pass |
| Tarball contents | `dist/*`, `README.md`, `LICENSE`, `package.json` only (about 30 KB packed) |
| **Lowest declared peers** (`socket.io@4.0.0`, `ioredis@5.0.0`) installed from the packed tarball: multi-tab, room join/leave, alive past TTL via auto-heartbeat, clean offline | Pass |
| `examples/basic-express` | Runs, `/health` OK |
| npm name `presence-io` | **Available** (registry 404 today; re-check right before publishing) |

## Optional polish (none blocks release)

1. **Async `error` listener that rejects still exits the process (reproduced, exit 1).** `pm.on('error', async () => { throw ... })` is the same hazard as async event listeners: `emitter.emit('error', ...)` ignores the returned promise. Fix: in `reportError` / the `error` branch of `emitLocal`, iterate `rawListeners('error')`, call each in try/catch, and `.catch(...)` any thenable result (log with `console.error`).
2. **package.json:** add `author`, `repository`, `bugs`, `homepage` (npm page shows no source link without them).
3. **README:** add one line each for numeric ids being coerced to strings (empty ids rejected) and "node clocks must be NTP-synced" (staleness uses the client `Date.now()`); the cluster single-shard note already exists.
4. **CHANGELOG:** entry is fine; consider dating it on the actual publish day.
5. Deferred by design, safe for 0.1.0: Lua scripts sent via `eval` on every call (`defineCommand`/`EVALSHA` later), staleness clock source (Redis `TIME` later), every node sweeps each interval.

## Publish steps
`npm run test:all && npm run build && npm run smoke:pack && npm publish --access public` (a `prepublishOnly` script already exists; confirm you are logged in with `npm whoami`, and enable 2FA).
