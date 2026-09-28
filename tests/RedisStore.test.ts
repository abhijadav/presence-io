import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import { randomUUID } from 'crypto';
import { RedisStore } from '../src/RedisStore.js';

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

describe('RedisStore', () => {
  let redis: Redis;
  let store: RedisStore;
  let prefix: string;

  beforeEach(() => {
    redis = new Redis(REDIS_URL);
    prefix = `test-rs-${randomUUID().slice(0, 8)}`;
    store = new RedisStore(redis, prefix);
  });

  afterEach(async () => {
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

  it('should track single socket addition and removal', async () => {
    const res1 = await store.addSocketToUser('user-1', 'socket-1', 10);
    expect(res1.wasOnline).toBe(false);
    expect(res1.isOnline).toBe(true);

    const isOnline = await store.isUserOnline('user-1');
    expect(isOnline).toBe(true);

    const sockets = await store.getUserSockets('user-1');
    expect(sockets).toEqual(['socket-1']);

    const res2 = await store.removeSocketFromUser('user-1', 'socket-1');
    expect(res2.remainingSockets).toBe(0);
    expect(res2.isOffline).toBe(true);

    const isOnlineAfter = await store.isUserOnline('user-1');
    expect(isOnlineAfter).toBe(false);
  });

  it('should handle multi-tab (multiple sockets per user)', async () => {
    const res1 = await store.addSocketToUser('user-1', 'socket-1', 10);
    expect(res1.wasOnline).toBe(false);

    const res2 = await store.addSocketToUser('user-1', 'socket-2', 10);
    expect(res2.wasOnline).toBe(true);

    const sockets = await store.getUserSockets('user-1');
    expect(sockets.sort()).toEqual(['socket-1', 'socket-2']);

    const rem1 = await store.removeSocketFromUser('user-1', 'socket-1');
    expect(rem1.remainingSockets).toBe(1);
    expect(rem1.isOffline).toBe(false);
    expect(await store.isUserOnline('user-1')).toBe(true);

    const rem2 = await store.removeSocketFromUser('user-1', 'socket-2');
    expect(rem2.remainingSockets).toBe(0);
    expect(rem2.isOffline).toBe(true);
    expect(await store.isUserOnline('user-1')).toBe(false);
  });

  it('should manage room users correctly', async () => {
    await store.addSocketToUser('user-1', 'socket-1', 10);
    await store.addSocketToUser('user-2', 'socket-2', 10);

    const join1 = await store.addUserToRoom('room-A', 'user-1', 'socket-1', 10);
    expect(join1.isFirstSocketInRoom).toBe(true);

    const join2 = await store.addUserToRoom('room-A', 'user-2', 'socket-2', 10);
    expect(join2.isFirstSocketInRoom).toBe(true);

    let roomUsers = await store.getRoomUsers('room-A');
    expect(roomUsers.sort()).toEqual(['user-1', 'user-2']);

    expect(await store.getRoomUserCount('room-A')).toBe(2);

    await store.addSocketToUser('user-1', 'socket-1b', 10);
    const join3 = await store.addUserToRoom('room-A', 'user-1', 'socket-1b', 10);
    expect(join3.isFirstSocketInRoom).toBe(false);

    const leave1 = await store.removeUserFromRoom('room-A', 'user-1', 'socket-1');
    expect(leave1.leftRoom).toBe(false);

    const leave2 = await store.removeUserFromRoom('room-A', 'user-1', 'socket-1b');
    expect(leave2.leftRoom).toBe(true);

    roomUsers = await store.getRoomUsers('room-A');
    expect(roomUsers).toEqual(['user-2']);
  });

  it('should disconnect socket atomically and clean up rooms', async () => {
    await store.addSocketToUser('user-1', 'socket-1', 10);
    await store.addUserToRoom('room-A', 'user-1', 'socket-1', 10);
    await store.addUserToRoom('room-B', 'user-1', 'socket-1', 10);

    const dc = await store.handleDisconnect('socket-1');
    expect(dc.userId).toBe('user-1');
    expect(dc.leftRooms.sort()).toEqual(['room-A', 'room-B']);
    expect(dc.userWentOffline).toBe(true);

    expect(await store.isUserOnline('user-1')).toBe(false);
    expect(await store.getRoomUsers('room-A')).toEqual([]);
    expect(await store.getRoomUsers('room-B')).toEqual([]);
  });

  it('should sweep stale sockets', async () => {
    await store.addSocketToUser('user-stale', 'socket-stale', 1); // 1 sec TTL
    await store.addUserToRoom('room-stale', 'user-stale', 'socket-stale', 1);

    const socketKey = `{${prefix}}:socket:socket-stale`;
    await redis.del(socketKey);

    const result = await store.sweepStale(1, 0);
    expect(result.offlineUsers).toContain('user-stale');
    expect(result.roomLeaves).toEqual([{ roomId: 'room-stale', userId: 'user-stale' }]);
  });
});
