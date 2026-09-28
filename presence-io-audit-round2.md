# presence-io — Audit Round 2 (for AI agent)

Reviewed the updated `presence-io.zip` against **real Redis 7** (not `ioredis-mock`), plus real Socket.io servers/clients.

## Result summary

| Check | Result |
|---|---|
| typecheck / lint (`no-explicit-any: error`) / build | Pass |
| Existing tests (16: unit + `tests/integration`) | Pass |
| `npm run smoke:pack` (CJS `require` + ESM `import` from tarball) | Pass |
| `publint` | Pass (1 suggestion, see P3-1) |
| `@arethetypeswrong/cli` | "No problems found" (node10, node16 CJS/ESM, bundler) |
| Real Socket.io E2E on built `dist/index.cjs` (2 servers, multi-tab, cross-node events, TTL survival) | Pass |
| `examples/basic-express` (`npm install` + start + HTTP) | Pass |
| `examples/multi-instance` (docker compose) | Not run (no Docker in sandbox); compose context/Dockerfile paths look consistent |
| **My audit suite `audit.test.ts` (29 probes)** | **18 pass, 11 fail (below)** |

All 8 bugs from round 1 are confirmed **fixed** (CJS path, auto-heartbeat, ghost-after-disconnect, orphaned rooms, comma/colon room IDs, duplicate handlers, real-Redis tests, no `any`). Concurrency is solid: randomized 40-batch join/leave stress keeps Redis consistent, events alternate correctly, and **zero keys leak** after full disconnect; concurrent `track`/`joinRoom` emit exactly one event; pub/sub delivers each event once to all nodes.

The 11 failures are new gaps, mostly introduced by the fixes (sweep lock, re-register logic, heartbeat).

## How to use the attached test file

Copy `audit.test.ts` to `tests/audit/audit.test.ts` (needs Redis on `localhost:6379`). Run: `npx vitest run tests/audit --reporter=verbose`. **Each failing test is a task below; the goal is all green.** Test IDs (B1, B2, ...) are referenced in each item. Do not weaken assertions to pass them.

---

## P1 — Correctness bugs

### P1-1. Sweep lock is a fixed 5s, never released, and independent of `sweepIntervalMs`/`ttlSeconds` (tests B1, B2)
`RedisStore.sweepStale` takes `SET {prefix}:sweep:lock 1 PX 5000 NX` and never releases it.
- **B1:** two `pm.sweepStale()` calls within 5s: the second silently returns `{ offlineUsers: [], roomLeaves: [] }` even though a stale user exists.
- **B2:** with `ttlSeconds: 1`, `sweepIntervalMs: 300`, a crashed node's user was detected offline after **5020 ms** instead of about 1.3s. Any `sweepIntervalMs < 5000` is effectively rounded up to 5s.
- **Fix:** derive lock TTL from the interval (e.g. `max(500, floor(sweepIntervalMs * 0.8))`), release it when the sweep finishes using a token compare-and-delete Lua script (so a slow sweep does not delete another node's lock), and make a **manual** `pm.sweepStale()` bypass the lock (or return an explicit `skipped: true`). Never return an empty result that looks like "nothing stale".

### P1-2. `heartbeat()` resurrects sockets that already disconnected (tests B3, B4)
After `handleDisconnect(socket)`, `heartbeat(socket)` finds no socket key, treats it as "evicted but alive", and **re-registers the user online**. It resolves the user from `socket.data.userId`, which persists after disconnect. `B4` shows 20/20 ghosts when a heartbeat overlaps a disconnect. Ghosts last until TTL plus a sweep, and emit a false `user:online`.
- **Fix:** in `PresenceManager.heartbeat`, only re-register when `this.trackedSockets.has(socket.id)`; otherwise return `false`. After the async re-registration, re-check that the entry still exists and, if it was removed meanwhile, undo via `redisStore.handleDisconnect(...)`.

### P1-3. Failed `track()` cannot be retried and leaks timers (test B6)
`trackedSockets.set(...)` (plus `disconnect` handler, `onAny`, heartbeat interval) is done **before** `addSocketToUser`. If Redis errors, `track()` rejects but the socket stays registered, so a retry hits the "already tracked" early return and does nothing. The user never becomes online.
- **Fix:** wrap the Redis call in try/catch; on failure remove the entry, clear the interval, call `socket.off/offAny`, then rethrow. Alternatively, register the entry only after Redis succeeds.

### P1-4. Failed `joinRoom()` poisons later re-registration (test B7)
`entry.rooms.add(roomId)` runs before `addUserToRoom`. When Redis rejects the join (stale socket -> `-1` -> throws), the room stays in `entry.rooms`; the next `heartbeat()` re-registration then **joins the user to a room they never successfully joined**.
- **Fix:** add to `entry.rooms` only after `addUserToRoom` succeeds (and delete on failure).

### P1-5. Re-registration emits inconsistent events (verified manually)
Sequence: `track` -> `joinRoom` -> TTL expiry -> sweep -> `heartbeat` (socket was alive). Events observed:
`user:online, room:join, room:leave, user:offline, user:online`. The re-register path emits `user:online` but **no `room:join`** even though Redis membership is restored (`getRoomUsers` returns the user). Consumers that build state from events end up with the user online but not in the room.
- **Fix:** in the re-register path, emit `room:join` for each room where `isFirstSocketInRoom` is true (and keep P1-2's guard). Add a test asserting the event sequence `... user:offline, user:online, room:join`.

---

## P2 — Robustness / performance

### P2-1. `onAny` heartbeat is unthrottled (test D2)
Every incoming client event runs a Redis Lua heartbeat: **500 events -> 500 EVAL calls**. A busy chat socket would hammer Redis and add latency.
- **Fix:** keep a per-socket `lastHeartbeatAt`; skip when `Date.now() - lastHeartbeatAt < heartbeatIntervalMs` (or `ttl/3`). The interval timer already covers idle sockets.

### P2-2. Passing the same client as `redis` and `subRedis` breaks silently (test B14)
`track()` then fails with `Connection in subscriber mode, only subscriber commands may be used` and no `error` event fires.
- **Fix:** in the constructor, throw a clear error if `options.subRedis === options.redis`; document that `subRedis` must be a separate connection (`redis.duplicate()`).

### P2-3. `shutdown()` leaks the pub/sub `message` listener (test B13)
The `subRedis.on('message', ...)` handler is anonymous and never removed (listener count stays +1 per manager). Also `subRedis.unsubscribe(channel)` on a shared subscriber would drop the channel for other managers using the same prefix.
- **Fix:** store the handler, `subRedis.off('message', handler)` on shutdown, and only unsubscribe when no other listener needs the channel (or document one subscriber per manager).

### P2-4. Numeric `socket.data.userId` is silently ignored by io auto-track (test B12)
`typeof socket.data.userId === 'string'` gates auto-track, so `userId: 12345` (very common with DB ids) results in no presence and no error.
- **Fix:** accept `string | number` and coerce with `String()`, or emit an `error` event explaining the id must be a string. Also validate non-empty `userId` in `track()` (an empty string currently tracks fine but emits no offline event because of the `if (userId)` truthiness check).

### P2-5. Re-tracking a socket with a different `userId` only changes local state (test B5)
`track(socket, {userId:'new'})` on an already-tracked socket updates the in-memory entry but not Redis, so the user is never online.
- **Fix:** either throw ("socket already tracked as X") or migrate atomically (disconnect old user entry, add new).

---

## P3 — Polish / notes

- **P3-1.** `publint` suggestion: add `"type": "commonjs"` to package.json (build already outputs `.cjs/.mjs`).
- **P3-2.** Plain `npm test` also runs `tests/integration` (it appears in the default run), so contributors without Redis get failures and `test:all` runs them twice. Add `exclude: ['tests/integration/**', 'tests/audit/**']` to the default vitest config and keep them in the integration config.
- **P3-3.** Scripts are still sent with `eval` (full Lua source every call). Use `redis.defineCommand` / `EVALSHA` for lower bandwidth.
- **P3-4.** Staleness uses the client's `Date.now()`. With clock skew between nodes, one node can evict another node's live sockets early. Prefer `redis.call('TIME')` inside the Lua scripts (or document that clocks must be NTP-synced). *Design observation, not tested.*
- **P3-5.** All keys use one hash tag `{prefix}`, which makes scripts cluster-safe but places all presence data on a **single Redis Cluster shard**. Document this trade-off (or shard by `{prefix:userId}` where scripts allow).
- **P3-6.** Sweep does N+1 round trips (`HGETALL` + `EXISTS` per socket, `isUserOnline` per room member). Fine at 2000 users (132 ms measured, test D3) but pipeline it before recommending large deployments.
- **P3-7.** Run `examples/multi-instance` with `docker compose up` in CI or manually; not verified here.

---

## Definition of done

- [ ] `npx vitest run tests/audit` -> all 29 pass (currently 18/29)
- [ ] Existing unit, integration, `smoke:pack` still pass; publint clean
- [ ] Crash detection time with `ttl=1s, sweepInterval=300ms` is under 3s
- [ ] `heartbeat()` never re-registers a socket the manager no longer tracks
- [ ] Event stream stays consistent through eviction and re-registration
- [ ] Move the audit tests into the repo (under `tests/integration/`) and wire them into CI against the Redis service container
