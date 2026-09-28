# presence-io — Review Findings & Fix Plan (for AI agent)

Reviewed the uploaded `presence-io.zip`. Ran `npm install`, `typecheck`, `lint`, `build`, `vitest` (all 10 tests pass) and then probed the code against a **real Redis 7** instance.

**Summary:** the tests are green, but they only run against `ioredis-mock`, which hides several real bugs. One packaging bug breaks CommonJS consumers completely, and one functional bug evicts live users. Fix in the priority order below. After each fix, add a regression test that runs against **real Redis**.

---

## Verification status

| Check | Result |
|---|---|
| `npm run typecheck` | Pass |
| `npm run lint` | Pass (but only because `no-explicit-any` is turned off, see P2-1) |
| `npm run build` | Pass (but output filenames do not match package.json, see P0-1) |
| `npm test` (10 tests) | Pass, all against `ioredis-mock` only |
| Lua scripts on real Redis 7 | Happy paths work, edge cases fail (see below) |

---

## P0 — Critical (fix before publishing)

### P0-1. `dist/index.cjs` does not exist, CJS `require()` is broken
- `package.json` sets `main` and `exports.require` to `./dist/index.cjs`.
- `tsup` (no `"type": "module"` in package.json) emits `dist/index.js` for CJS, not `.cjs`.
- Verified: `require('./dist/index.cjs')` throws `Cannot find module`.
- **Fix (pick one):**
  - In `tsup.config.ts` add `outExtension({ format }) { return { js: format === 'cjs' ? '.cjs' : '.mjs' }; }`, **or**
  - Change package.json to point at `./dist/index.js`.
  - Also make `types` resolution correct per condition: `exports["."].import.types` -> `index.d.mts`, `exports["."].require.types` -> `index.d.cts`/`index.d.ts`.
- **Add a test/CI step:** after `npm run build`, run `npm pack`, install the tarball in a temp dir, and verify both `require('presence-io')` and `import('presence-io')` work. (`publint` and `@arethetypeswrong/cli` are good CI additions.)

### P0-2. Live sockets are evicted as "stale" because nothing sends heartbeats
- Verified on real Redis: `track()` a socket, wait past `ttlSeconds` (no heartbeat), run `sweepStale()` -> the live user is marked **offline** and removed from rooms.
- `PresenceManager.heartbeat(socket)` exists, but nothing calls it automatically, and the README does not make this requirement clear. With the default TTL of 30s, every connected user would flip offline after 30 seconds.
- After eviction, `heartbeat()` returns `false` and never restores the presence state, so the user stays ghost-offline until reconnect.
- **Fix:**
  1. In `track()`, start an automatic per-socket heartbeat (e.g. `setInterval` at `ttlSeconds / 3`, `unref()`'d), cleared on disconnect and `shutdown()`. Make this configurable (`heartbeatIntervalMs`, `0` to disable for manual mode).
  2. Optionally refresh on any incoming socket activity via `socket.onAny` if available.
  3. When `heartbeat()` finds the socket key missing but the socket is still connected, **re-register** the socket (re-add to user hash and re-join its known rooms) and emit `user:online` again, instead of silently returning `false`.
  4. Document the heartbeat model clearly in the README.
- **Tests:** real-Redis test where a tracked socket stays online across 3x TTL with auto heartbeat; test that a socket with heartbeats stopped is evicted.

---

## P1 — High

### P1-1. Room join after disconnect creates a permanent ghost
- Verified: `handleDisconnect(socket)` then `joinRoom(socket, 'late')` -> `getRoomUsers('late')` returns the user while `isUserOnline` is `false`. Because the user hash no longer exists, `sweepStale` (which iterates user hashes) never cleans it up.
- **Fix:** in `addUserToRoom` Lua, only proceed if the socket key still exists (`EXISTS socketKey == 1`), otherwise return a sentinel and have `joinRoom` throw or no-op. Also add an orphan sweep for room ZSETs (iterate `presence:room:*` and remove members whose user has no live sockets).

### P1-2. `RedisStore.removeSocketFromUser` leaves orphaned room state
- Verified: after `removeSocketFromUser`, `getRoomUsers` still returns the user, and `sweepStale` never removes them.
- It is a public method (exported and reachable via `getRedisStore()`) and tested, but it only touches the socket key and user hash.
- **Fix:** delete it or make it a thin wrapper over the same Lua used by `handleDisconnect`. Do not keep two disconnect code paths with different behavior. Update its test accordingly.

### P1-3. Lua scripts contain dead code written for the `ioredis-mock` emulator (fengari), not real Redis
- `DISCONNECT_SOCKET_LUA` has JS-style argument parsing (`ARGV[0]`, `ARGV.length`) and `HEARTBEAT_LUA` calls `pcall(require, "js")`. `ioredis-mock` runs Lua through `fengari`/`fengari-interop`, which is why this cruft exists. On real Redis these branches never run and the scripts only work because of the fallthrough `else` branches.
- **Fix:**
  1. Remove all the `js`/`.length`/`ARGV[0]` branches. Use plain Redis Lua: `ARGV[1]`, `ARGV[2]`, ... and `redis.call('SMEMBERS', ...)` which already returns a Lua table.
  2. Stop returning a comma-joined string of rooms from Lua. Return a real Lua array (see P1-4).
  3. Make real Redis the source of truth for tests (see P1-5).

### P1-4. Room IDs containing `,` are corrupted on disconnect
- Verified: joining rooms `'a,b'` and `'c'` then disconnecting returns `leftRooms = ['c','a','b']` (should be `['a,b','c']`), because Lua returns `table.concat(leftRooms, ",")` and TypeScript splits on `,`.
- **Fix:** return the array directly from Lua (`return { userId, leftRooms, userWentOffline }` with `leftRooms` as a nested table) and remove the split. Add a test with room IDs containing `,` and `:`.

### P1-5. Test suite never touches real Redis
- All tests use `ioredis-mock`. CI starts a `redis:7-alpine` service container but no test uses it.
- **Fix:**
  - Add `tests/integration/*.test.ts` using real `ioredis` against `REDIS_URL` (default `redis://localhost:6379`), with a unique key prefix per test and cleanup in `afterEach`.
  - Keep the fast mock-based unit tests only for logic that does not depend on Lua semantics, or drop `ioredis-mock` entirely for Lua-dependent code.
  - Add the multi-instance test with two `PresenceManager`s and two separate `ioredis` connections.
  - Wire the CI Redis service into `npm run test:integration`.

---

## P2 — Medium

### P2-1. `any` in the public API and the lint rule that would catch it is disabled
- `.eslintrc.cjs` sets `'@typescript-eslint/no-explicit-any': 'off'`. The build plan's Definition of Done said "no `any` in the public API surface".
- Public `any`: `PresenceOptions.io?: any`, `SocketLike.data?: Record<string, any>`, `on/off(... (...args: any[]) => void)`, `PresenceManager.io`, `connectionListener`, and the `emit`/`on` casts.
- **Fix:** re-enable the rule as `error`. Type `io` as `Pick<Server, 'on' | 'off'>` (import type from `socket.io`) or a minimal structural interface. Use `Record<string, unknown>` for `data`, `unknown[]` for args. Replace `listener as any` with a properly typed emitter wrapper.

### P2-2. Duplicate tracking when `io` is passed and the user also calls `track()`
- The constructor auto-registers an `io.on('connection')` listener that calls `track()` when `socket.data.userId` is set. The README/example also call `track()` manually.
- Verified: calling `track()` twice on one socket registers **two** `disconnect` handlers.
- **Fix:** make `track()` idempotent (keep a `Set` of tracked socket IDs; skip re-registering handlers), or remove the constructor auto-track and document one clear way to use the library. Also unregister the socket `disconnect` listener in `handleDisconnect`/`shutdown`.

### P2-3. `track()` is not awaited in the example and errors are swallowed
- `examples/basic-express/server.js` calls `presence.track(socket, ...)` without `await` or `.catch`. A Redis error becomes an unhandled rejection. `socket.on('joinRoom')` handlers also lack try/catch.
- **Fix:** add error handling in the example and consider an `error` event on `PresenceManager` for background failures (sweep, heartbeat, disconnect).

### P2-4. Every instance runs its own sweep with no coordination
- N server instances all run `sweepStale` on the same keys, so `user:offline` / `room:leave` events can fire on multiple instances for the same eviction, and `SCAN` over `presence:user:*` scales with total keyspace.
- **Fix:** guard the sweep with a short Redis lock (`SET key NX PX`) so one instance sweeps per interval, and/or track active users in a dedicated index key (`SADD presence:users`) so the sweep does not need `SCAN`. Document the event semantics (see P2-5).

### P2-5. Events are local to the instance that caused them
- `user:online`, `room:join`, etc. only fire on the instance where the action happened. Other instances only see the change through queries. The README says presence is synced across instances, which is only partly true.
- **Fix:** either publish diffs via Redis Pub/Sub so all instances emit the same events, or state the limitation clearly in the README. If adding Pub/Sub, use a separate subscriber connection.

### P2-6. Lua scripts build key names inside the script (not Redis Cluster safe)
- `DISCONNECT_SOCKET_LUA` and `HEARTBEAT_LUA` construct keys from `ARGV` (`prefix .. ':room:' .. roomId`) instead of receiving them in `KEYS`. This breaks on Redis Cluster / ElastiCache cluster mode.
- **Fix:** either document "single-node or hash-tagged keys only" and use `{prefix}` hash tags in key builders (`presence:{userId}:...`), or pass all touched keys via `KEYS`.

---

## P3 — Low / polish

- **P3-1.** Use `redis.defineCommand` (or `SCRIPT LOAD` + `EVALSHA`) instead of sending full Lua source with `eval` on every call.
- **P3-2.** `addSocketToUser` reports `wasOnline` from the hash length before `HSET`. If the same socket ID is re-added, it wrongly reports `wasOnline = true`. Check `HEXISTS` first.
- **P3-3.** `examples/basic-express/` has no `package.json` (needs `express`, `socket.io`, `ioredis`, `presence-io` via `file:../..`), and the file uses ESM `import` in a `.js` file without `"type": "module"`.
- **P3-4.** `examples/multi-instance/docker-compose.yml` uses `build: .` but there is no `Dockerfile` or server entrypoint in that folder, so `docker compose up` fails. Add a Dockerfile and reuse the basic-express server with `INSTANCE_ID` logging.
- **P3-5.** `package-lock.json` and a copy of `presence-io-build-plan.md` are in the repo root. Keep the lockfile but move/remove the plan doc, and add `"repository"`, `"bugs"`, `"homepage"`, `"sideEffects": false` to package.json.
- **P3-6.** `PresenceManager.emit` is public. Consider making it internal so consumers cannot spoof events.
- **P3-7.** Add `CONTRIBUTING`/`SECURITY` notes and a `prepublishOnly` step that runs the pack-and-import smoke test from P0-1.

---

## Suggested order of work

1. P0-1 (packaging) and add pack-and-import CI smoke test.
2. P1-5 (real-Redis integration tests), so every later fix is verified against real behavior.
3. P0-2 (auto heartbeat + re-register).
4. P1-3 and P1-4 (clean Lua, array return values), then P1-1 and P1-2.
5. P2-1 through P2-6.
6. P3 items.

## Definition of done for this pass

- [ ] `require('presence-io')` and `import 'presence-io'` both work from a packed tarball in a clean directory
- [ ] A tracked socket stays online for at least 3x `ttlSeconds` without manual heartbeat calls
- [ ] All integration tests pass against real Redis 7 in CI; the Redis service container is actually used
- [ ] No `any` in public types; `no-explicit-any` enabled as `error`
- [ ] Lua scripts contain no emulator-specific code
- [ ] Room IDs with `,` and `:` work; no orphaned room entries after any disconnect path
- [ ] Examples run with documented commands (`npm install && npm start`, `docker compose up`)
