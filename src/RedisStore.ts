import type Redis from 'ioredis';
import { randomUUID } from 'crypto';
import {
  DEFAULT_PREFIX,
  DEFAULT_TTL_SECONDS,
  formatPrefix,
  getRoomKey,
  getRoomSocketsKey,
  getSocketKey,
  getSocketRoomsKey,
  getUserKey,
  getUserRoomsKey,
} from './constants.js';
import type { StaleEvictionResult } from './types.js';

const RELEASE_LOCK_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
`;

const ADD_SOCKET_TO_USER_LUA = `
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[4]))
local prevCount = redis.call('HLEN', KEYS[2])
local socketExists = redis.call('HEXISTS', KEYS[2], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
if prevCount > 0 or socketExists == 1 then
  return 1
else
  return 0
end
`;

const ADD_USER_TO_ROOM_LUA = `
if redis.call('EXISTS', KEYS[5]) == 0 then
  return -1
end
local countBefore = redis.call('SCARD', KEYS[1])
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[2])
redis.call('SADD', KEYS[3], ARGV[4])
redis.call('SADD', KEYS[4], ARGV[4])
return countBefore
`;

const REMOVE_USER_FROM_ROOM_LUA = `
redis.call('SREM', KEYS[1], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[3])
local remainingSockets = redis.call('SCARD', KEYS[1])
if remainingSockets == 0 then
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[2])
  redis.call('SREM', KEYS[4], ARGV[3])
end
return remainingSockets
`;

const DISCONNECT_SOCKET_LUA = `
local socketExists = redis.call('EXISTS', KEYS[1])
local socketRoomsExists = redis.call('EXISTS', KEYS[2])
local userId = redis.call('GET', KEYS[1])
if (not userId or userId == false) and ARGV[3] and ARGV[3] ~= "" then
  userId = ARGV[3]
end

if not userId or userId == false then
  return { "", {}, 0 }
end

local prefix = ARGV[2]
local userKey = prefix .. ':user:' .. userId
local userRoomsKey = prefix .. ':user:' .. userId .. ':rooms'

local socketInUser = 0
if redis.call('EXISTS', userKey) == 1 then
  socketInUser = redis.call('HEXISTS', userKey, ARGV[1])
end

if socketExists == 0 and socketRoomsExists == 0 and socketInUser == 0 then
  return { "", {}, 0 }
end

local rooms = redis.call('SMEMBERS', KEYS[2])
local leftRooms = {}

for _, roomId in ipairs(rooms) do
  local roomSocketsKey = prefix .. ':room:' .. roomId .. ':sockets:' .. userId
  local roomKey = prefix .. ':room:' .. roomId
  redis.call('SREM', roomSocketsKey, ARGV[1])
  local remRoomSockets = redis.call('SCARD', roomSocketsKey)
  if remRoomSockets == 0 then
    redis.call('DEL', roomSocketsKey)
    redis.call('ZREM', roomKey, userId)
    redis.call('SREM', userRoomsKey, roomId)
    table.insert(leftRooms, roomId)
  end
end

redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('HDEL', userKey, ARGV[1])
local remUserSockets = redis.call('HLEN', userKey)
local userWentOffline = 0
if remUserSockets == 0 then
  redis.call('DEL', userKey)
  redis.call('DEL', userRoomsKey)
  userWentOffline = 1
end

return { userId, leftRooms, userWentOffline }
`;

const HEARTBEAT_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
  local rooms = redis.call('SMEMBERS', KEYS[3])
  for _, roomId in ipairs(rooms) do
    local roomKey = ARGV[5] .. ':room:' .. roomId
    redis.call('ZADD', roomKey, tonumber(ARGV[3]), ARGV[2])
  end
  return 1
end
return 0
`;

export class RedisStore {
  private redis: Redis;
  private prefix: string;

  constructor(redis: Redis, prefix = DEFAULT_PREFIX) {
    this.redis = redis;
    this.prefix = formatPrefix(prefix);
  }

  public getPrefix(): string {
    return this.prefix;
  }

  public async addSocketToUser(
    userId: string,
    socketId: string,
    ttlSeconds = DEFAULT_TTL_SECONDS,
  ): Promise<{ wasOnline: boolean; isOnline: boolean }> {
    const socketKey = getSocketKey(socketId, this.prefix);
    const userKey = getUserKey(userId, this.prefix);
    const now = Date.now().toString();

    const wasOnlineRes = (await this.redis.eval(
      ADD_SOCKET_TO_USER_LUA,
      2,
      socketKey,
      userKey,
      userId,
      socketId,
      now,
      ttlSeconds.toString(),
    )) as number;

    return {
      wasOnline: wasOnlineRes === 1,
      isOnline: true,
    };
  }

  public async removeSocketFromUser(
    userId: string,
    socketId: string,
  ): Promise<{ remainingSockets: number; isOffline: boolean }> {
    const result = await this.handleDisconnect(socketId, userId);
    const remainingSockets = (await this.getUserSockets(userId)).length;
    return {
      remainingSockets,
      isOffline: result.userWentOffline || remainingSockets === 0,
    };
  }

  public async addUserToRoom(
    roomId: string,
    userId: string,
    socketId: string,
    ttlSeconds = DEFAULT_TTL_SECONDS,
  ): Promise<{ isFirstSocketInRoom: boolean }> {
    const roomSocketsKey = getRoomSocketsKey(roomId, userId, this.prefix);
    const roomKey = getRoomKey(roomId, this.prefix);
    const socketRoomsKey = getSocketRoomsKey(socketId, this.prefix);
    const userRoomsKey = getUserRoomsKey(userId, this.prefix);
    const socketKey = getSocketKey(socketId, this.prefix);
    const now = Date.now().toString();

    // Ensure socket key has valid TTL
    await this.redis.expire(socketKey, ttlSeconds);

    const countBefore = (await this.redis.eval(
      ADD_USER_TO_ROOM_LUA,
      5,
      roomSocketsKey,
      roomKey,
      socketRoomsKey,
      userRoomsKey,
      socketKey,
      socketId,
      userId,
      now,
      roomId,
    )) as number;

    if (countBefore === -1) {
      throw new Error(`Cannot join room: socket ${socketId} is disconnected or stale`);
    }

    return {
      isFirstSocketInRoom: countBefore === 0,
    };
  }

  public async removeUserFromRoom(
    roomId: string,
    userId: string,
    socketId: string,
  ): Promise<{ remainingSocketsInRoom: number; leftRoom: boolean }> {
    const roomSocketsKey = getRoomSocketsKey(roomId, userId, this.prefix);
    const roomKey = getRoomKey(roomId, this.prefix);
    const socketRoomsKey = getSocketRoomsKey(socketId, this.prefix);
    const userRoomsKey = getUserRoomsKey(userId, this.prefix);

    const remainingSockets = (await this.redis.eval(
      REMOVE_USER_FROM_ROOM_LUA,
      4,
      roomSocketsKey,
      roomKey,
      socketRoomsKey,
      userRoomsKey,
      socketId,
      userId,
      roomId,
    )) as number;

    return {
      remainingSocketsInRoom: remainingSockets,
      leftRoom: remainingSockets === 0,
    };
  }

  public async handleDisconnect(
    socketId: string,
    userIdHint?: string,
  ): Promise<{ userId: string | null; leftRooms: string[]; userWentOffline: boolean }> {
    const socketKey = getSocketKey(socketId, this.prefix);
    const socketRoomsKey = getSocketRoomsKey(socketId, this.prefix);

    const res = (await this.redis.eval(
      DISCONNECT_SOCKET_LUA,
      2,
      socketKey,
      socketRoomsKey,
      socketId,
      this.prefix,
      userIdHint || '',
    )) as [string, string[], number] | null;

    if (!res || !res[0]) {
      return {
        userId: null,
        leftRooms: [],
        userWentOffline: false,
      };
    }

    const userId = res[0];
    const leftRooms = Array.isArray(res[1]) ? res[1] : [];
    const userWentOffline = Number(res[2]) === 1;

    return {
      userId,
      leftRooms,
      userWentOffline,
    };
  }

  public async heartbeat(
    socketId: string,
    userId: string,
    ttlSeconds = DEFAULT_TTL_SECONDS,
  ): Promise<boolean> {
    const socketKey = getSocketKey(socketId, this.prefix);
    const userKey = getUserKey(userId, this.prefix);
    const socketRoomsKey = getSocketRoomsKey(socketId, this.prefix);
    const now = Date.now().toString();

    const result = (await this.redis.eval(
      HEARTBEAT_LUA,
      3,
      socketKey,
      userKey,
      socketRoomsKey,
      socketId,
      userId,
      now,
      ttlSeconds.toString(),
      this.prefix,
    )) as number;

    return result === 1;
  }

  public async sweepStale(
    ttlSeconds = DEFAULT_TTL_SECONDS,
    sweepIntervalMs = 0,
    force = false,
  ): Promise<StaleEvictionResult> {
    const lockDurationMs =
      !force && sweepIntervalMs > 0 ? Math.max(500, Math.floor(sweepIntervalMs * 0.8)) : 0;
    let lockToken: string | null = null;
    const lockKey = `${this.prefix}:sweep:lock`;

    if (lockDurationMs > 0) {
      lockToken = randomUUID();
      const lockAcquired = await this.redis.set(
        lockKey,
        lockToken,
        'PX',
        lockDurationMs,
        'NX',
      );
      if (!lockAcquired) {
        return { offlineUsers: [], roomLeaves: [], skipped: true };
      }
    }

    try {
      const cutoff = Date.now() - ttlSeconds * 1000;
      const offlineUsers: string[] = [];
      const roomLeaves: Array<{ roomId: string; userId: string }> = [];

      const userKeyPattern = `${this.prefix}:user:*`;
      let cursor = '0';

      do {
        const scanRes = await this.redis.scan(cursor, 'MATCH', userKeyPattern, 'COUNT', 100);
        cursor = scanRes[0];
        const keys = scanRes[1];

        for (const key of keys) {
          if (key.endsWith(':rooms')) continue;

          const userId = key.substring(`${this.prefix}:user:`.length);
          const socketsWithTimestamps = await this.redis.hgetall(key);

          for (const [socketId, tsStr] of Object.entries(socketsWithTimestamps)) {
            const ts = parseInt(tsStr, 10);
            const socketKey = getSocketKey(socketId, this.prefix);
            const exists = await this.redis.exists(socketKey);

            if (isNaN(ts) || ts < cutoff || exists === 0) {
              const disconnectResult = await this.handleDisconnect(socketId, userId);
              if (disconnectResult.userId) {
                for (const roomId of disconnectResult.leftRooms) {
                  roomLeaves.push({ roomId, userId: disconnectResult.userId });
                }
                if (disconnectResult.userWentOffline) {
                  offlineUsers.push(disconnectResult.userId);
                }
              }
            }
          }
        }
      } while (cursor !== '0');

      // Sweep orphaned room ZSET entries
      const roomKeyPattern = `${this.prefix}:room:*`;
      let roomCursor = '0';

      do {
        const scanRes = await this.redis.scan(roomCursor, 'MATCH', roomKeyPattern, 'COUNT', 100);
        roomCursor = scanRes[0];
        const roomKeys = scanRes[1];

        for (const key of roomKeys) {
          if (key.includes(':sockets:')) continue;
          const members = await this.redis.zrange(key, 0, -1);
          for (const userId of members) {
            const isOnline = await this.isUserOnline(userId);
            if (!isOnline) {
              await this.redis.zrem(key, userId);
              const roomId = key.substring(`${this.prefix}:room:`.length);
              roomLeaves.push({ roomId, userId });
            }
          }
        }
      } while (roomCursor !== '0');

      return { offlineUsers, roomLeaves };
    } finally {
      if (lockToken) {
        await this.redis.eval(RELEASE_LOCK_LUA, 1, lockKey, lockToken).catch(() => {});
      }
    }
  }

  // --- Public Queries ---

  public async getRoomUsers(roomId: string): Promise<string[]> {
    const roomKey = getRoomKey(roomId, this.prefix);
    return await this.redis.zrange(roomKey, 0, -1);
  }

  public async isUserOnline(userId: string): Promise<boolean> {
    const userKey = getUserKey(userId, this.prefix);
    const count = await this.redis.hlen(userKey);
    return count > 0;
  }

  public async getUserRooms(userId: string): Promise<string[]> {
    const userRoomsKey = getUserRoomsKey(userId, this.prefix);
    return await this.redis.smembers(userRoomsKey);
  }

  public async getUserSockets(userId: string): Promise<string[]> {
    const userKey = getUserKey(userId, this.prefix);
    return await this.redis.hkeys(userKey);
  }

  public async getRoomUserCount(roomId: string): Promise<number> {
    const roomKey = getRoomKey(roomId, this.prefix);
    return await this.redis.zcard(roomKey);
  }

  public async getSocketUser(socketId: string): Promise<string | null> {
    const socketKey = getSocketKey(socketId, this.prefix);
    return await this.redis.get(socketKey);
  }
}
