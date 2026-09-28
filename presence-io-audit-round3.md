# presence-io — Audit Round 3 (for AI agent)

Verified the updated `presence-io.zip` independently on a clean install against **real Redis 7**.

## Verification of your claims

| Claim | Result |
|---|---|
| 29/29 audit probes + 6 integration + 10 unit pass | **Confirmed** (unit 10, integration run 35) |
| `tests/audit/audit.test.ts` not weakened | **Confirmed**: diff vs my original shows only TypeScript typing cleanup, no assertion changed |
| Crash detection 5020 ms -> ~1100 ms; ghosts 20 -> 0; heartbeat EVALs 500 -> 0 | Confirmed (1104 ms / 0 / 0) |
| typecheck, lint, build, `smoke:pack` | Pass |
| `publint` | "All good!" (`"type": "commonjs"` fixed) |
| `@arethetypeswrong/cli` | No problems found (node10, node16 CJS+ESM, bundler) |
| Real Socket.io E2E on built `dist/index.cjs` (2 servers, numeric ids, `subRedis === redis`, multi-tab, abrupt transport close, cross-node events, 0 leaked keys) | Pass |

Also confirmed working: sweep lock is released after each sweep and never deletes another owner's lock; manual `sweepStale()` returns `skipped: true` when locked and `{force:true}` bypasses it; the duplicated sub connection is closed on `shutdown()` (client count returns to baseline); `userId` migration cleans the old user fully; chatty sockets stay alive via throttled `onAny`; double `track()` is safe; `shutdown()` leaves no live timers.

## New findings (round 3 probes: `audit3.test.ts`, 12 probes: 8 pass, 4 fail)

Copy `audit3.test.ts` to `tests/audit/audit3.test.ts` (runs under the existing `test:integration` config). **Each failing test is a task below; do not weaken assertions.**

### P1-1. A background Redis error crashes the whole Node process unless the app attached an `error` listener (reproduced)
All background failures (heartbeat timer, sweep timer, auto-track, publish, disconnect handler) call `this.emitLocal('error', err)`. Node's `EventEmitter` **throws** when `'error'` is emitted with no listener, and this happens inside a `.catch` handler, so it becomes an unhandled exception. Reproduction: track a socket without `pm.on('error', ...)`, make `redis.eval` reject once, wait for the next heartbeat tick -> process exits with code 1. Neither README nor examples mention attaching an `error` listener.
- **Fix:** add a private `reportError(err)` used everywhere: emit `'error'` only if `listenerCount('error') > 0`, otherwise log once via `console.error('[presence-io] ...')` (or `process.emitWarning`). A library must never crash the host on a transient Redis failure. Document the `error` event in the README and add `pm.on('error', ...)` to both examples.
- **Test to add:** spawn a child process (built `dist/index.cjs`) that triggers the failure with no listener and assert it survives.

### P1-2. Throwing user listeners corrupt state or suppress events (tests E2, E3)
- **E2:** if a `user:online` listener throws, `track()` runs the emit inside its rollback `try/catch`, so it **rolls back local tracking (timers cleared, entry deleted) even though Redis already recorded the user online**. Result: user online in Redis, untracked locally, no heartbeat -> ghost until TTL.
- **E3:** if a `room:leave` listener throws during `handleDisconnect`, the loop aborts and **`user:offline` is never emitted**. Same in `sweepStale`, and the pub/sub handler swallows listener exceptions silently in its `try/catch`.
- **Fix:** emit through a safe wrapper that calls listeners one by one inside try/catch and routes exceptions to `reportError` (P1-1) without interrupting the remaining events or the caller. Keep emits outside the rollback `try/catch` in `track()`.

### P1-3. Heartbeat re-registration vs disconnect race: duplicate offline, divergent state, and rejections (tests E1, F1)
150-trial fuzz (evict a socket via sweep, then run `heartbeat()` concurrently with `handleDisconnect()`): **0 ghosts (good)**, but **2-5 trials end with events saying "online + in room" while Redis says offline**, and **3-11 `heartbeat()` calls reject** with `Cannot join room: socket X is disconnected or stale`. Sample event stream for one user:
`user:online, room:join, room:leave, user:offline, user:offline, user:online, room:join` while Redis is offline.
Three separate defects:
1. **Duplicate `user:offline` (F1, deterministic, no race needed):** socket evicted by a sweep (`user:offline` emitted), then the client really disconnects -> `handleDisconnect` still holds a local entry, passes the userId hint, the Lua script sees an empty hash and reports `userWentOffline = 1` again -> **second `user:offline`**. **Fix:** in `DISCONNECT_SOCKET_LUA`, if neither the socket key nor the `HEXISTS userKey socketId` field exists (and no `socket:rooms` set), return a no-op result (`userId` empty / no transition). Only report `leftRooms`/`userWentOffline` when this call actually removed something.
2. **Silent cleanup after re-register:** in `heartbeat()`, when the entry disappears mid-flight the code calls `redisStore.handleDisconnect(...)` but **ignores the result**, so a `user:online`/`room:join` that was already emitted is never balanced by `room:leave`/`user:offline`. **Fix:** emit the events from that result (room:leave per left room, user:offline if it went offline) via the normal emitting path.
3. **`heartbeat()` rejects during the race:** the re-register `addUserToRoom` throws the "stale" error. **Fix:** catch that specific case inside `heartbeat()` and return `false` (the socket is gone); never surface it as an error.
- **Test:** E1 must show `ghosts 0`, `mismatches 0`, and preferably `rejections 0`. F1 must show exactly one `user:offline`.

## P3 — Notes / documentation

- **P3-1.** Because the lock is now released after each sweep, it prevents *concurrent* sweeps but not *repeated* ones: every node still sweeps every interval (N x SCAN load). Duplicates are harmless (evictions are atomic, events fire once), but if you want one sweeper per interval, keep the lock until `~0.8 x interval` instead of releasing it. Either is fine; document the choice.
- **P3-2.** README should document: the `error` event; `subRedis` semantics (same client is auto-duplicated and closed on shutdown); `sweepStale({force})` and the `skipped` flag; numeric ids are coerced to strings and empty ids rejected; `npm run test:integration` / `test:audit` (plain `npx vitest run tests/audit` finds no files because of the split configs).
- **P3-3 (carried over, still open, untested):** scripts are sent with `eval` each call (use `defineCommand`/`EVALSHA`); staleness uses the client's `Date.now()` (prefer Redis `TIME`); all keys share one hash tag `{prefix}` (single Redis Cluster shard).

## Definition of done

- [ ] `test:integration` -> all of `audit.test.ts` (29) and `audit3.test.ts` (12) green; unit + smoke + publint + attw still clean
- [ ] Child-process test proves no crash without an `error` listener
- [ ] Throwing listeners neither corrupt local/Redis state nor suppress later events
- [ ] Exactly one `user:offline` per real offline transition; event stream always ends consistent with Redis after any heartbeat/disconnect/sweep interleaving
