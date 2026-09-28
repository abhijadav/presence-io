import type Redis from 'ioredis';

export interface PresenceOptions {
  /**
   * Socket.io Server instance (or structural equivalent with .on / .off).
   */
  io?: SocketIoServerLike;
  /**
   * Primary Redis client instance (ioredis).
   */
  redis: Redis;
  /**
   * Optional subscriber Redis client instance (ioredis) for cross-node Pub/Sub events.
   */
  subRedis?: Redis;
  /**
   * Key prefix for Redis keys.
   * @default 'presence'
   */
  prefix?: string;
  /**
   * Time in seconds before an inactive socket without a heartbeat is considered stale.
   * @default 30
   */
  ttlSeconds?: number;
  /**
   * Frequency in milliseconds to sweep for stale user/room entries.
   * @default 15000
   */
  sweepIntervalMs?: number;
  /**
   * Frequency in milliseconds to send automatic socket heartbeats.
   * Defaults to (ttlSeconds * 1000) / 3. Set to 0 to disable auto-heartbeats.
   */
  heartbeatIntervalMs?: number;
}

export interface TrackOptions {
  /**
   * Unique user identifier.
   */
  userId: string;
}

export interface UserOnlinePayload {
  userId: string;
}

export interface UserOfflinePayload {
  userId: string;
}

export interface RoomJoinPayload {
  roomId: string;
  userId: string;
}

export interface RoomLeavePayload {
  roomId: string;
  userId: string;
}

export type EventListener<T> = (payload: T) => void;

export interface PresenceEvents {
  'user:online': (payload: UserOnlinePayload) => void;
  'user:offline': (payload: UserOfflinePayload) => void;
  'room:join': (payload: RoomJoinPayload) => void;
  'room:leave': (payload: RoomLeavePayload) => void;
  'error': (err: Error) => void;
}

export interface StaleEvictionResult {
  offlineUsers: string[];
  roomLeaves: Array<{ roomId: string; userId: string }>;
  skipped?: boolean;
}

export interface SocketLike {
  id: string;
  data?: Record<string, unknown>;
  on?(event: string, listener: (...args: unknown[]) => void): unknown;
  off?(event: string, listener: (...args: unknown[]) => void): unknown;
  onAny?(listener: (event: string, ...args: unknown[]) => void): unknown;
  offAny?(listener: (event: string, ...args: unknown[]) => void): unknown;
}

export interface SocketIoServerLike {
  on(event: string, listener: (socket: SocketLike) => void): unknown;
  off?(event: string, listener: (socket: SocketLike) => void): unknown;
}
