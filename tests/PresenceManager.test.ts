import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Redis from 'ioredis';
import { randomUUID } from 'crypto';
import { PresenceManager } from '../src/PresenceManager.js';
import type { SocketLike } from '../src/types.js';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

function createMockSocket(
  id: string,
): SocketLike & {
  listeners: Record<string, ((...args: unknown[]) => void)[]>;
  emitDisconnect: () => void;
} {
  const listeners: Record<string, ((...args: unknown[]) => void)[]> = {};
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
    emitDisconnect() {
      if (listeners['disconnect']) {
        for (const fn of listeners['disconnect']) {
          fn();
        }
      }
    },
  };
}

describe('PresenceManager', () => {
  let redis: Redis;
  let pm: PresenceManager;
  let prefix: string;

  beforeEach(() => {
    redis = new Redis(REDIS_URL);
    prefix = `test-pm-${randomUUID().slice(0, 8)}`;
    pm = new PresenceManager({
      redis,
      prefix,
      ttlSeconds: 2,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });
  });

  afterEach(async () => {
    await pm.shutdown();

    let cursor = '0';
    do {
      const res = await redis.scan(cursor, 'MATCH', `*{${prefix}}*`, 'COUNT', 100);
      cursor = res[0];
      if (res[1].length > 0) {
        await redis.del(...res[1]);
      }
    } while (cursor !== '0');

    await redis.quit();
  });

  it('should emit user:online on connect and user:offline on disconnect', async () => {
    const onlineSpy = vi.fn();
    const offlineSpy = vi.fn();

    pm.on('user:online', onlineSpy);
    pm.on('user:offline', offlineSpy);

    const socket = createMockSocket('sock-1');
    await pm.track(socket, { userId: 'alice' });

    expect(onlineSpy).toHaveBeenCalledWith({ userId: 'alice' });
    expect(await pm.isUserOnline('alice')).toBe(true);

    socket.emitDisconnect();
    await new Promise((r) => setTimeout(r, 50));

    expect(offlineSpy).toHaveBeenCalledWith({ userId: 'alice' });
    expect(await pm.isUserOnline('alice')).toBe(false);
  });

  it('should handle multi-tab presence without premature offline events', async () => {
    const onlineSpy = vi.fn();
    const offlineSpy = vi.fn();

    pm.on('user:online', onlineSpy);
    pm.on('user:offline', offlineSpy);

    const tab1 = createMockSocket('tab-1');
    const tab2 = createMockSocket('tab-2');

    await pm.track(tab1, { userId: 'bob' });
    await pm.track(tab2, { userId: 'bob' });

    expect(onlineSpy).toHaveBeenCalledTimes(1);

    tab1.emitDisconnect();
    await new Promise((r) => setTimeout(r, 50));

    expect(offlineSpy).not.toHaveBeenCalled();
    expect(await pm.isUserOnline('bob')).toBe(true);

    tab2.emitDisconnect();
    await new Promise((r) => setTimeout(r, 50));

    expect(offlineSpy).toHaveBeenCalledWith({ userId: 'bob' });
    expect(await pm.isUserOnline('bob')).toBe(false);
  });

  it('should emit room:join and room:leave diff events', async () => {
    const joinSpy = vi.fn();
    const leaveSpy = vi.fn();

    pm.on('room:join', joinSpy);
    pm.on('room:leave', leaveSpy);

    const socket1 = createMockSocket('sock-1');
    const socket2 = createMockSocket('sock-2');

    await pm.track(socket1, { userId: 'user-1' });
    await pm.track(socket2, { userId: 'user-1' });

    await pm.joinRoom(socket1, 'general');
    expect(joinSpy).toHaveBeenCalledWith({ roomId: 'general', userId: 'user-1' });

    await pm.joinRoom(socket2, 'general');
    expect(joinSpy).toHaveBeenCalledTimes(1);

    expect(await pm.getRoomUsers('general')).toEqual(['user-1']);

    await pm.leaveRoom(socket1, 'general');
    expect(leaveSpy).not.toHaveBeenCalled();

    await pm.leaveRoom(socket2, 'general');
    expect(leaveSpy).toHaveBeenCalledWith({ roomId: 'general', userId: 'user-1' });
    expect(await pm.getRoomUsers('general')).toEqual([]);
  });

  it('should sync presence across multiple server instances sharing Redis', async () => {
    const redisB = new Redis(REDIS_URL);
    const pmInstanceB = new PresenceManager({
      redis: redisB,
      prefix,
      ttlSeconds: 2,
      sweepIntervalMs: 0,
      heartbeatIntervalMs: 0,
    });

    const socketA = createMockSocket('socket-server-A');
    await pm.track(socketA, { userId: 'charlie' });

    expect(await pmInstanceB.isUserOnline('charlie')).toBe(true);
    expect(await pmInstanceB.getUserSockets('charlie')).toEqual(['socket-server-A']);

    await pmInstanceB.shutdown();
    await redisB.quit();
  });

  it('should sweep stale un-disconnected crash sockets', async () => {
    const offlineSpy = vi.fn();
    const leaveSpy = vi.fn();

    pm.on('user:offline', offlineSpy);
    pm.on('room:leave', leaveSpy);

    const socket = createMockSocket('crashed-socket');
    await pm.track(socket, { userId: 'dave' });
    await pm.joinRoom(socket, 'lobby');

    const socketKey = `{${prefix}}:socket:crashed-socket`;
    await redis.del(socketKey);

    const result = await pm.sweepStale();
    expect(result.offlineUsers).toContain('dave');
    expect(result.roomLeaves).toEqual([{ roomId: 'lobby', userId: 'dave' }]);

    expect(offlineSpy).toHaveBeenCalledWith({ userId: 'dave' });
    expect(leaveSpy).toHaveBeenCalledWith({ roomId: 'lobby', userId: 'dave' });
  });

  it('should not crash child process when async listener rejects without error listener attached', async () => {
    const { execSync } = await import('child_process');
    const code = `
      import { PresenceManager } from './src/index.ts';
      import Redis from 'ioredis';

      (async () => {
        const redis = new Redis('${REDIS_URL}');
        const pm = new PresenceManager({
          redis,
          prefix: 'child-test',
          ttlSeconds: 2,
          sweepIntervalMs: 0,
          heartbeatIntervalMs: 0,
        });

        pm.on('user:online', async () => {
          throw new Error('Async DB error inside user:online');
        });

        const mockSocket = {
          id: 'child-sock-1',
          data: {},
          on() {},
          off() {},
          emit() {},
        };

        await pm.track(mockSocket, { userId: 'child-user' });
        await pm.shutdown();
        redis.quit();
        console.log('CHILD_PROCESS_SUCCESS');
      })().catch((err) => {
        console.error(err);
        process.exit(1);
      });
    `;

    const stdout = execSync(`npx tsx -e "${code.replace(/"/g, '\\"')}"`, {
      encoding: 'utf-8',
    });

    expect(stdout).toContain('CHILD_PROCESS_SUCCESS');
  });

  it('should survive and resolve track() when error listener throws for sync/async listener errors', async () => {
    const { execSync } = await import('child_process');
    execSync('npm run build', { encoding: 'utf-8' });

    const code = `
      const { PresenceManager } = require('./dist/index.cjs');
      const Redis = require('ioredis');

      (async () => {
        const redis = new Redis('${REDIS_URL}');
        const pm = new PresenceManager({
          redis,
          prefix: 'child-err-test',
          ttlSeconds: 2,
          sweepIntervalMs: 0,
          heartbeatIntervalMs: 0,
        });

        // Application error listener that throws asynchronously
        pm.on('error', async (err) => {
          throw new Error('App error listener threw: ' + err.message);
        });


        // Async listener rejects
        pm.on('user:online', async () => {
          throw new Error('Async listener failure');
        });

        // Sync listener throws
        pm.on('user:online', () => {
          throw new Error('Sync listener failure');
        });

        const mockSocket = {
          id: 'child-err-sock-1',
          data: {},
          on() {},
          off() {},
          emit() {},
        };

        // track() must resolve cleanly and presence recorded
        await pm.track(mockSocket, { userId: 'child-err-user' });

        await pm.shutdown();
        redis.quit();
        console.log('THROWING_ERROR_LISTENER_SUCCESS');
      })().catch((err) => {
        console.error(err);
        process.exit(1);
      });
    `;

    const stdout = execSync(`node -e "${code.replace(/"/g, '\\"')}"`, {
      encoding: 'utf-8',
    });

    expect(stdout).toContain('THROWING_ERROR_LISTENER_SUCCESS');
  });
});




