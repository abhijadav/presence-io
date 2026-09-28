# presence-io — Audit Round 5 (for AI agent)

Verified the newest `presence-io.zip` on a clean install against **real Redis 7**. Result: **round-4 fixes all hold; two small issues remain, then it is ready to publish.**

## Round-4 fixes: verified independently

| Item | Result |
|---|---|
| Unit (11) + `test:integration` (audit 29 + audit3 12 + audit4 5 + real-redis 6 = 52) | **All pass**; integration suite run 3 times, no flakiness |
| Audit files not weakened | Confirmed: every `expect(...)` and title identical to my originals (only typing changes) |
| `once()` fires exactly once (G1), also with mixed `on`/`once` listeners | **Fixed** |
| Async listener rejection no longer crashes the process (child process, no error listener) | **Fixed** (exit 0) |
| Overlapping heartbeats emit a single `user:online` (G3); 5 concurrent heartbeats -> exactly `user:online, room:join` | **Fixed** (single-flight + atomic `wasOnline`) |
| Redis outage with total data loss (down 3.5 s, empty restart) | Self-heals; node B now sees exactly **one** re-`user:online` + one `room:join` (was 6 `user:online`) |
| Foreign/malformed pub-sub messages ignored (G4) | **Fixed** (event allowlist + payload validation) |
| Churn: 600 sockets track/join/disconnect in concurrent batches | 0 leaked keys, `trackedSockets.size === 0` |
| A listener that calls back into the manager (re-entrancy) | No duplication |
| typecheck, lint, build, `smoke:pack`, `publint` ("All good!"), `attw` ("No problems found") | Pass |
| README now documents `error`, `force`, `skipped`, `subRedis` | Done (does not yet mention `once` or numeric-id coercion; minor) |

## Remaining issues: `audit5.test.ts` (5 probes: 4 pass, 1 fail) plus two child-process repros

Copy `audit5.test.ts` to `tests/audit/audit5.test.ts`. **Do not weaken assertions.**

### P1-1. `heartbeat()` now swallows every Redis error silently (test H1)
The single-flight rewrite ends with `catch (_err) { return false; }` and never reports the error. Verified: with `redis.eval` rejecting, `heartbeat()` returns `false` and **0 `error` events** fire. The heartbeat timer and `onAny` path are the main way a live app touches Redis, so a Redis outage or auth failure becomes invisible (the outage test also shows 0 error reports). `false` is also ambiguous ("socket gone" vs "Redis failed").
- **Fix:** in that `catch`, call `this.reportError(err)` before returning `false` (optionally rethrow for manual callers and keep the timer/`onAny` paths catching and reporting). Keep the `finally` that clears `inFlightHeartbeat`. Document the return value.

### P2-1. A throwing `error` listener still escapes (two child-process repros)
`reportError` / `emitLocal('error')` call the app's own `error` listener unguarded:
- **Async listener rejects + `error` listener throws** -> unhandled rejection -> **process exits with code 1**.
- **Sync `user:online` listener throws + `error` listener throws** -> the exception propagates out of `emitLocal`, so **`track()` rejects** ("error listener threw") even though presence was recorded in Redis; a caller that retries will hit the "already tracked" path.
- **Fix:** wrap the `emitter.emit('error', ...)` call inside `reportError` (and the `error` branch of `emitLocal`) in try/catch; on failure log via `console.error('[presence-io] error listener threw:', ...)` and continue. Never let the library's own error reporting throw.
- **Test to add:** child process (built `dist/index.cjs`) with an `error` listener that throws, triggered by (a) an async rejecting listener and (b) a sync throwing listener; assert the process survives and `track()` resolves.

## P3 (carried over, untested; fine to ship, document or defer)
- Lua scripts still sent via `eval` on every call (0 uses of `defineCommand`/`EVALSHA`).
- Staleness uses the client's `Date.now()` (0 uses of Redis `TIME`): document that node clocks must be NTP-synced.
- All keys share one hash tag `{prefix}` (single Redis Cluster shard): document.
- Sweep lock prevents concurrent sweeps, not repeated ones (every node sweeps each interval): document.
- README: add one line each for `once()` and "numeric ids are coerced to strings, empty ids rejected".

## Definition of done
- [ ] `test:integration` green including `audit5` (H1)
- [ ] Heartbeat Redis failures reach the `error` event; a throwing `error` listener can never crash the process or reject `track()`
- [ ] Unit, smoke, publint, attw still clean -> **publish `0.1.0`**
