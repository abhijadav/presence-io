# presence-io — Audit Round 4 (for AI agent)

Verified the newest `presence-io.zip` on a clean install against **real Redis 7**.

## Round-3 fixes: verified

| Item | Result |
|---|---|
| Unit (10) + `test:integration` (audit 29 + audit3 12 + real-redis 6 = 47) | **All pass** |
| Audit files not weakened | Confirmed: every `expect(...)` and test title identical to my originals; only typing cleanups |
| P1-1 process crash with no `error` listener | **Fixed** (child process survives, exit 0; `reportError` logs via `console.error`) |
| P1-2 throwing sync listener corrupts state / suppresses events | **Fixed** (E2, E3 pass) |
| P1-3 re-register vs disconnect race | **Fixed** (150-trial fuzz: 0 ghosts, 0 event mismatches, 0 rejections; F1 exactly one `user:offline`) |
| typecheck, lint, build, `smoke:pack`, `publint` ("All good!"), `attw` ("No problems found") | Pass |
| `examples/basic-express` runs; both examples now attach an `error` listener | Pass |
| Soak (G5): 60 live sockets, 2 nodes sweeping every 200 ms, TTL 1 s, 6 s | No flaps, 0 errors |
| Redis outage with **total data loss** while a manager is live (down 3.5 s > TTL, then empty restart) | **Self-heals**: user back online and back in its room, 0 crashes |

## New findings: `audit4.test.ts` (5 probes: 2 pass, 3 fail) plus one child-process repro

Copy `audit4.test.ts` to `tests/audit/audit4.test.ts` (runs under the existing `test:integration` config). **Each failing test is a task below; do not weaken assertions.**

### P1-1. `once()` listeners fire on every event and are never removed (test G1)
The new safe-emit calls `this.emitter.listeners(event)`, which returns the **unwrapped** originals, so Node's once-wrapper (the part that removes the listener) is bypassed. Verified: `pm.once('user:online', fn)`, then two users come online -> `fn` runs **2 times** (expected 1). `off()` of a once-listener still works (G2 passes).
- **Fix:** iterate `this.emitter.rawListeners(event)` (these include the once-wrappers) and invoke each inside the existing try/catch. Keep `this` semantics.

### P1-2. An `async` listener that rejects still kills the process (reproduced, exit code 1)
`pm.on('user:online', async () => { throw ... })` is extremely common (DB writes, API calls). `emitLocal` calls the listener but ignores the returned promise, so the rejection is unhandled and Node exits.
- **Fix:** capture the listener's return value; if it is thenable, attach `.catch(err => this.reportError(err))`.
- **Test to add:** child process (built `dist/index.cjs`) with an async throwing listener and no error listener must survive.

### P1-3. Overlapping heartbeats each re-register and each emit `user:online` (test G3, plus outage repro)
`heartbeat()` decides `wasOnlineBefore` with a separate `isUserOnline()` call and ignores the atomic `wasOnline` that `addSocketToUser` returns. Any overlap makes every caller see "offline" and emit.
- G3: two sockets of one user re-registering concurrently -> **2 `user:online`** (expected 1).
- **Real-world impact (outage repro):** during a Redis outage, ioredis queues the per-socket heartbeat timers; on reconnect all of them run at once. Node B observed **6 `user:online:alice`** events for one user after a single outage (`user:online` x1, `room:join`, then `user:online` x5, `room:join`).
- **Fix (both):**
  1. Make heartbeat **single-flight per socket** (store an in-flight promise on the tracked entry; a second call while one is running returns the same promise).
  2. Use the `wasOnline` value returned by `addSocketToUser` (atomic) instead of the separate `isUserOnline` pre-check, so exactly one caller emits across sockets/nodes.
- **Tests:** G3 passes; add one that fires 5 concurrent `heartbeat()` calls for one evicted socket and asserts exactly one `user:online` and one `room:join`.

## P2

### P2-1. Any process that can publish to the Redis channel can inject events (test G4)
The pub/sub handler passes `data.event` and `data.payload` to `emitLocal` without validation. Verified: a foreign message `{"event":"error","payload":{...}}` surfaced as a local **`error`** event, and `{"event":"user:online","payload":null}` was delivered to `user:online` listeners with a `null` payload.
- **Fix:** allowlist only `user:online`, `user:offline`, `room:join`, `room:leave`; require `payload.userId` to be a string (and `roomId` a string for room events); never route a remote `error`; ignore messages that fail validation.

### P2-2. README still does not document the new behavior (carried over from round 3, P3-2)
`README.md` has **zero** mentions of: the `error` event (apps that omit it only get a `console.error` line), `sweepStale({ force })` and the `skipped` flag, `subRedis` semantics (same client is auto-duplicated and closed on `shutdown()`), numeric ids being coerced to strings / empty ids rejected, and `once`. Add an "Error handling" section and an options table.

## P3 (carried over, untested)
- Lua scripts sent with `eval` on every call (use `defineCommand` / `EVALSHA`).
- Staleness uses the client's `Date.now()` (prefer Redis `TIME` to avoid clock-skew evictions).
- All keys share one hash tag `{prefix}` (single Redis Cluster shard); document the trade-off.
- Sweep lock is released after each sweep, so it prevents concurrent sweeps, not repeated ones (every node sweeps each interval). Harmless, but document it.

## Definition of done
- [ ] `test:integration`: audit (29) + audit3 (12) + audit4 (5) + real-redis (6) all green; unit, smoke, publint, attw still clean
- [ ] `once()` fires exactly once; async listener rejection does not crash the host (child-process test)
- [ ] Exactly one `user:online` / `room:join` per real transition even with overlapping heartbeats or an outage recovery
- [ ] Foreign / malformed pub-sub messages are ignored
- [ ] README documents `error`, `force`/`skipped`, `subRedis`, id coercion
