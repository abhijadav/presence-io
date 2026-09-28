import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Redis from 'ioredis';
import { randomUUID } from 'crypto';
import { PresenceManager } from '../../src/PresenceManager.js';
import { RedisStore } from '../../src/RedisStore.js';
import type { SocketLike } from '../../src/types.js';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

function createMockSocket(id: string): SocketLike & {
  listeners: Record<string, ((...args: unknown[]) => void)[]>;
  emitDisconnect: () => void;
  emitActivity: () => void;
} {
  const listeners: Record<string, ((...args: unknown[]) => void)[]> = {};
  let anyListener: ((event: string, ...args: unknown[]) => void) | undefined;

  return {
    id,
    data: {},
    listeners,
    on(event: string, fn: (...args: unknown[]) => void) {
      if (!listeners[event]) listeners[event] = [];
      listeners[event].push(fn);
    },
    off(event: string, fn: (...args: unknown[]) => void) {
      if (!listeners[event]) return;
      listeners[event] = listeners[event].filter((f) => f !== fn);
    },
    onAny(fn: (event: string, ...args: unknown[]) => void) {
      anyListener = fn;
    },
    offAny() {
      anyListener = undefined;
    },
    emitDisconnect() {
      if (listeners['disconnect']) {
        for (const fn of listeners['disconnect']) {
          fn();
        }
      }
    },
    emitActivity() {
      if (anyListener) {
        anyListener('ping');
      }
    },
  };
}

describe('Real Redis Integration Tests (Redis 7)', () => {
  let redis: Redis;
  let testPrefix: string;

  beforeEach(() => {
    redis = new Redis(REDIS_URL);
    testPrefix = `real-test-${randomUUID().slice(0, 8)}`;
  });

  afterEach(async () => {
    // Clean up all keys for testPrefix
    let cursor = '0';
    do {
      const res = await redis.scan(cursor, 'MATCH', `*{${testPrefix}}*`, 'COUNT', 100);
      cursor = res[0];
      if (res[1].length > 0) {
        await redis.del(...res[1]);
      }
    } while (cursor !== '0');

    await redis.quit();
  });

  it('should track socket presence and multi-tab on real Redis 7', async () => {
    const store = new RedisStore(redis, testPrefix);
    const pm = new PresenceManager({
      redis,
      prefix: testPrefix,
      ttlSeconds: 5,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });

    const socket1 = createMockSocket('sock-1');
    const socket2 = createMockSocket('sock-2');

    await pm.track(socket1, { userId: 'alice' });
    expect(await store.isUserOnline('alice')).toBe(true);
    expect(await store.getUserSockets('alice')).toEqual(['sock-1']);

    await pm.track(socket2, { userId: 'alice' });
    expect((await store.getUserSockets('alice')).sort()).toEqual(['sock-1', 'sock-2']);

    await pm.handleDisconnect(socket1);
    expect(await store.isUserOnline('alice')).toBe(true);

    await pm.handleDisconnect(socket2);
    expect(await store.isUserOnline('alice')).toBe(false);

    await pm.shutdown();
  });

  it('should handle room IDs containing commas and colons correctly', async () => {
    const pm = new PresenceManager({
      redis,
      prefix: testPrefix,
      ttlSeconds: 5,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });

    const leaveSpy = vi.fn();
    pm.on('room:leave', leaveSpy);

    const socket = createMockSocket('sock-special');
    await pm.track(socket, { userId: 'bob' });

    const complexRoom1 = 'chat:general,announcements:v1';
    const complexRoom2 = 'room:a,b,c:zone:1';

    await pm.joinRoom(socket, complexRoom1);
    await pm.joinRoom(socket, complexRoom2);

    const rooms = await pm.getUserRooms('bob');
    expect(rooms.sort()).toEqual([complexRoom1, complexRoom2].sort());

    await pm.handleDisconnect(socket);

    expect(leaveSpy).toHaveBeenCalledWith({ roomId: complexRoom1, userId: 'bob' });
    expect(leaveSpy).toHaveBeenCalledWith({ roomId: complexRoom2, userId: 'bob' });
    expect(await pm.isUserOnline('bob')).toBe(false);

    await pm.shutdown();
  });

  it('should keep tracked socket online across 3x TTL via automatic heartbeats', async () => {
    const pm = new PresenceManager({
      redis,
      prefix: testPrefix,
      ttlSeconds: 1, // 1 sec TTL
      heartbeatIntervalMs: 250, // auto heartbeat every 250ms
      sweepIntervalMs: 0,
    });

    const socket = createMockSocket('sock-auto-hb');
    await pm.track(socket, { userId: 'charlie' });

    // Wait 3 seconds (3x TTL)
    await new Promise((r) => setTimeout(r, 3000));

    // Charlie should still be online because auto heartbeat refreshed TTL
    expect(await pm.isUserOnline('charlie')).toBe(true);

    await pm.handleDisconnect(socket);
    expect(await pm.isUserOnline('charlie')).toBe(false);

    await pm.shutdown();
  });

  it('should re-register socket and user if socket key was missing on heartbeat', async () => {
    const pm = new PresenceManager({
      redis,
      prefix: testPrefix,
      ttlSeconds: 2,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });

    const socket = createMockSocket('sock-evicted');
    await pm.track(socket, { userId: 'dave' });
    await pm.joinRoom(socket, 'lobby');

    // Simulate key expiration or manual deletion in Redis
    const store = pm.getRedisStore();
    const socketKey = `{${testPrefix}}:socket:sock-evicted`;
    await redis.del(socketKey);

    // Heartbeat should detect missing key and re-register socket + rooms
    const hbRes = await pm.heartbeat(socket);
    expect(hbRes).toBe(true);

    expect(await store.isUserOnline('dave')).toBe(true);
    expect(await store.getRoomUsers('lobby')).toEqual(['dave']);

    await pm.shutdown();
  });

  it('should prevent ghost creation when trying to join room on disconnected socket (P1-1)', async () => {
    const pm = new PresenceManager({
      redis,
      prefix: testPrefix,
      ttlSeconds: 2,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });

    const socket = createMockSocket('sock-ghost');
    await pm.track(socket, { userId: 'eve' });
    await pm.handleDisconnect(socket);

    // Socket is disconnected; joining room now should throw error
    await expect(pm.joinRoom(socket, 'late-room')).rejects.toThrow();

    const roomUsers = await pm.getRoomUsers('late-room');
    expect(roomUsers).toEqual([]);

    await pm.shutdown();
  });

  it('should synchronize events across multiple instances via Pub/Sub (P2-5)', async () => {
    const subRedis1 = new Redis(REDIS_URL);
    const subRedis2 = new Redis(REDIS_URL);

    const pm1 = new PresenceManager({
      redis: new Redis(REDIS_URL),
      subRedis: subRedis1,
      prefix: testPrefix,
      ttlSeconds: 5,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });

    const pm2 = new PresenceManager({
      redis: new Redis(REDIS_URL),
      subRedis: subRedis2,
      prefix: testPrefix,
      ttlSeconds: 5,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });

    const pm2OnlineSpy = vi.fn();
    const pm2JoinSpy = vi.fn();

    pm2.on('user:online', pm2OnlineSpy);
    pm2.on('room:join', pm2JoinSpy);

    // Give pubsub subscriber time to subscribe
    await new Promise((r) => setTimeout(r, 100));

    const socketOnInstance1 = createMockSocket('sock-node-1');
    await pm1.track(socketOnInstance1, { userId: 'frank' });
    await pm1.joinRoom(socketOnInstance1, 'global');

    // Wait for pubsub message propagation
    await new Promise((r) => setTimeout(r, 200));

    expect(pm2OnlineSpy).toHaveBeenCalledWith({ userId: 'frank' });
    expect(pm2JoinSpy).toHaveBeenCalledWith({ roomId: 'global', userId: 'frank' });

    await pm1.shutdown();
    await pm2.shutdown();
    await subRedis1.quit();
    await subRedis2.quit();
  });
});
