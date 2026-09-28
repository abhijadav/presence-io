# presence-io

> **A framework-agnostic, TypeScript-first Socket.io + Redis presence & room manager npm package.**

[![CI](https://github.com/abhijadav/presence-io/actions/workflows/ci.yml/badge.svg)](https://github.com/abhijadav/presence-io/actions)
[![npm version](https://img.shields.io/npm/v/presence-io.svg)](https://www.npmjs.com/package/presence-io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Why `presence-io`?

Socket.io's official Redis adapter (`@socket.io/redis-adapter`) solves **pub/sub message scaling** across server instances — it does **NOT** solve **presence** (who is online, multi-device tracking, room membership diffs, clean disconnect handling, or crash recovery).

Naive presence implementations get real-time state wrong in predictable ways:
- **Premature Offline**: Marking a user offline the instant one tab/device disconnects, even if they have another tab or app open.
- **Ghost Users**: A crashed server instance leaves online users registered forever because no clean disconnect event fired.
- **Polling Dumps**: Forcing clients to poll full room user lists instead of broadcasting fine-grained diffs (`user:online`, `room:join`).
- **Framework Lock-in**: Tying presence logic to NestJS or specific server frameworks.

`presence-io` is a lightweight, framework-agnostic server package that solves all of these on top of `socket.io` + `ioredis`.

---

## Features

- 📱 **Multi-Socket Per User**: Supports multiple tabs or devices per user. A user stays online until their *last* socket disconnects.
- 🚪 **Room-Scoped Presence**: Tracks who is in room `X` right now, supporting multi-tab room joins without duplicate events.
- ⚡ **Atomic Redis Operations**: Multi-step joins, leaves, and disconnects execute via atomic Lua scripts to prevent race conditions across horizontally scaled nodes.
- 🧹 **Stale Cleanup & Crash Recovery**: Periodic background sweeps evict sockets without active heartbeats, guaranteeing ghost user cleanup even after ungraceful server crashes.
- 🎯 **Typed Event Emitter**: Strongly-typed events (`user:online`, `user:offline`, `room:join`, `room:leave`).
- 🌐 **Zero Framework Lock-in**: Works with raw `socket.io`, Express, Fastify, NestJS, Koa, or standalone HTTP servers.

---

## Installation

```bash
npm install presence-io
# Make sure peer dependencies are installed:
npm install socket.io ioredis
```

---

## Quick Start

```typescript
import { createServer } from 'http';
import { Server } from 'socket.io';
import Redis from 'ioredis';
import { PresenceManager } from 'presence-io';

const httpServer = createServer();
const io = new Server(httpServer);
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

const presence = new PresenceManager({
  io,
  redis,
  ttlSeconds: 30,        // how long before an inactive socket is considered stale
  sweepIntervalMs: 15000 // stale sweep interval (ms)
});

// Typed presence event listeners
presence.on('user:online', ({ userId }) => {
  console.log(`User ${userId} came online`);
});

presence.on('user:offline', ({ userId }) => {
  console.log(`User ${userId} went offline`);
});

presence.on('room:join', ({ roomId, userId }) => {
  console.log(`User ${userId} joined room ${roomId}`);
});

presence.on('room:leave', ({ roomId, userId }) => {
  console.log(`User ${userId} left room ${roomId}`);
});

// Recommended: Catch background or listener errors
presence.on('error', (err) => {
  console.error('[presence-io error]', err);
});

io.on('connection', (socket) => {
  const userId = socket.handshake.query.userId as string;

  // Track socket under logical userId
  presence.track(socket, { userId });

  socket.on('joinRoom', (roomId) => presence.joinRoom(socket, roomId));
  socket.on('leaveRoom', (roomId) => presence.leaveRoom(socket, roomId));
});

httpServer.listen(3000);
```

---

## API Reference

### `new PresenceManager(options)`
- `redis`: `Redis` (ioredis instance, required)
- `subRedis?: Redis` (optional dedicated ioredis subscriber instance. If omitted, `presence-io` automatically calls `redis.duplicate()`)
- `io?: Server` (Socket.io server instance, optional)
- `prefix?: string` (Redis key prefix, default: `'presence'`)
- `ttlSeconds?: number` (stale TTL in seconds, default: `30`)
- `sweepIntervalMs?: number` (background sweep frequency in ms, default: `15000`)
- `heartbeatIntervalMs?: number` (background heartbeat interval in ms, default: `10000`)

### Methods & Events
- `track(socket, { userId })`: Track a socket connection for a user. Non-string `userId` values (such as numbers like `123`) are automatically coerced via `String(userId)`. Empty string (`""`), whitespace-only, `null`, or `undefined` values will throw a `TypeError` / `Error`.
- `joinRoom(socket, roomId)`: Register socket into a room. Emits `room:join` if user was not previously in the room.
- `leaveRoom(socket, roomId)`: Remove socket from a room. Emits `room:leave` if user has zero remaining sockets in the room.
- `heartbeat(socket)`: Refresh socket TTL in Redis. Single-flight per socket to prevent duplicate heartbeats during network or Redis outages. Returns `Promise<boolean>`.
- `isUserOnline(userId)`: Returns `Promise<boolean>`.
- `getRoomUsers(roomId)`: Returns `Promise<string[]>`.
- `getUserRooms(userId)`: Returns `Promise<string[]>`.
- `getUserSockets(userId)`: Returns `Promise<string[]>`.
- `getRoomUserCount(roomId)`: Returns `Promise<number>`.
- `sweepStale(options?: { force?: boolean })`: Manually trigger a stale socket sweep. Pass `{ force: true }` to bypass the distributed lock / debounce limit. Returns `Promise<{ offlineUsers: string[], roomLeaves: RoomLeaveEvent[], skipped?: boolean }>`. If `skipped: true`, another cluster node held the lock or the sweep was debounced.
- `on(event, listener)` / `once(event, listener)`: Subscribe to presence events (`user:online`, `user:offline`, `room:join`, `room:leave`). `once()` listeners fire exactly once and unbind automatically. Handlers (sync or async) will not crash the process or interrupt presence state if they throw or reject.
- `on('error', listener)`: Register an error handler. Handlers (sync or async) receive errors from pub/sub parsing, background sweeps, Redis failures, or throwing event listeners. If no `error` listener is attached, errors are logged safely to `console.error` without crashing the Node process.
- `shutdown()`: Clears background timers and unsubscribes event listeners.

---

## Deployment Considerations

- **NTP Clock Synchronization**: Socket heartbeat and staleness calculations rely on `Date.now()`. When running multiple server instances across different machines, ensure server system clocks are synchronized via NTP.
- **Redis Cluster Support**: Key names utilize Redis hash tags (e.g. `{presence}:user:123`) to ensure that all keys within the same prefix land on the same Redis Cluster shard, enabling safe atomic Lua script execution.

---

## Development & Testing


Requires Redis running on `localhost:6379` for integration and audit tests.

```bash
# Run unit tests (ioredis-mock, no Redis required)
npm test

# Run real-Redis integration & 52-probe audit test suite
npm run test:integration

# Run 52-probe audit test suite directly
npm run test:audit

# Run all 58 tests across unit, integration, and audit suites
npm run test:all
```

---

## License

[MIT](LICENSE)

