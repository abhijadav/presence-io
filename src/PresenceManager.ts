import { EventEmitter } from 'events';
import { randomUUID } from 'crypto';
import type Redis from 'ioredis';
import { DEFAULT_PREFIX, DEFAULT_SWEEP_INTERVAL_MS, DEFAULT_TTL_SECONDS, formatPrefix } from './constants.js';
import { RedisStore } from './RedisStore.js';
import type {
  EventListener,
  PresenceEvents,
  PresenceOptions,
  SocketIoServerLike,
  SocketLike,
  StaleEvictionResult,
  TrackOptions,
} from './types.js';

interface TrackedSocketEntry {
  socket: SocketLike;
  userId: string;
  heartbeatTimer: NodeJS.Timeout | null;
  rooms: Set<string>;
  disconnectHandler: () => void;
  anyHandler?: (event: string, ...args: unknown[]) => void;
  lastHeartbeatAt?: number;
  inFlightHeartbeat?: Promise<boolean>;
}

const ALLOWED_PUBSUB_EVENTS = new Set(['user:online', 'user:offline', 'room:join', 'room:leave']);

export class PresenceManager {
  private instanceId = randomUUID();
  private redisStore: RedisStore;
  private emitter = new EventEmitter();
  private ttlSeconds: number;
  private sweepIntervalMs: number;
  private heartbeatIntervalMs: number;
  private sweepTimer: NodeJS.Timeout | null = null;
  private trackedSockets = new Map<string, TrackedSocketEntry>();
  private io?: SocketIoServerLike;
  private connectionListener?: (socket: SocketLike) => void;
  private redis: Redis;
  private subRedis?: Redis;
  private ownSubRedis = false;
  private messageHandler?: (channel: string, message: string) => void;
  private pubSubChannel: string;

  constructor(options: PresenceOptions) {
    this.redis = options.redis;
    if (options.subRedis) {
      if (options.subRedis === options.redis) {
        this.subRedis = options.redis.duplicate();
        this.ownSubRedis = true;
      } else {
        this.subRedis = options.subRedis;
      }
    }
    const prefix = formatPrefix(options.prefix ?? DEFAULT_PREFIX);
    this.redisStore = new RedisStore(this.redis, prefix);
    this.ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
    this.sweepIntervalMs = options.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS;
    this.heartbeatIntervalMs =
      options.heartbeatIntervalMs ?? Math.floor((this.ttlSeconds * 1000) / 3);
    this.io = options.io;
    this.pubSubChannel = `${prefix}:pubsub:events`;

    if (this.io && typeof this.io.on === 'function') {
      this.connectionListener = (socket: SocketLike) => {
        const rawUserId = socket.data?.userId;
        if (rawUserId !== undefined && rawUserId !== null && String(rawUserId).trim() !== '') {
          this.track(socket, { userId: String(rawUserId) }).catch((err) => {
            this.reportError(err);
          });
        }
      };
      this.io.on('connection', this.connectionListener);
    }

    if (this.subRedis) {
      this.subRedis.subscribe(this.pubSubChannel).catch((err) => {
        this.reportError(err);
      });
      this.messageHandler = (channel: string, message: string) => {
        if (channel === this.pubSubChannel) {
          try {
            const data = JSON.parse(message) as {
              instanceId?: string;
              event?: string;
              payload?: { userId?: string; roomId?: string };
            };
            if (
              data &&
              typeof data === 'object' &&
              data.instanceId &&
              data.instanceId !== this.instanceId &&
              typeof data.event === 'string' &&
              ALLOWED_PUBSUB_EVENTS.has(data.event) &&
              data.payload &&
              typeof data.payload === 'object' &&
              typeof data.payload.userId === 'string' &&
              data.payload.userId.trim() !== ''
            ) {
              if (
                (data.event === 'room:join' || data.event === 'room:leave') &&
                typeof data.payload.roomId !== 'string'
              ) {
                return;
              }
              this.emitLocal(data.event as keyof PresenceEvents, data.payload as never);
            }
          } catch (_err) {
            // Ignore malformed pubsub messages
          }
        }
      };
      this.subRedis.on('message', this.messageHandler);
    }

    this.startSweepTimer();
  }

  // --- Typed Event Emitter API ---

  public on<K extends keyof PresenceEvents>(
    event: K,
    listener: EventListener<PresenceEvents[K]>,
  ): this {
    this.emitter.on(event, listener as (...args: unknown[]) => void);
    return this;
  }

  public off<K extends keyof PresenceEvents>(
    event: K,
    listener: EventListener<PresenceEvents[K]>,
  ): this {
    this.emitter.off(event, listener as (...args: unknown[]) => void);
    return this;
  }

  public once<K extends keyof PresenceEvents>(
    event: K,
    listener: EventListener<PresenceEvents[K]>,
  ): this {
    this.emitter.once(event, listener as (...args: unknown[]) => void);
    return this;
  }

  private reportError(err: unknown): void {
    const error = err instanceof Error ? err : new Error(String(err));
    const listeners = this.emitter.rawListeners('error');
    if (listeners.length > 0) {
      for (const listener of listeners) {
        try {
          const res = (listener as (...a: unknown[]) => unknown)(error);
          if (res && typeof (res as Promise<unknown>).then === 'function') {
            (res as Promise<unknown>).catch((listenerErr) => {
              console.error('[presence-io] Error listener threw:', listenerErr);
            });
          }
        } catch (listenerErr) {
          console.error('[presence-io] Error listener threw:', listenerErr);
        }
      }
    } else {
      console.error('[presence-io] Background error:', error.message);
    }
  }

  private emitLocal<K extends keyof PresenceEvents>(
    event: K,
    ...args: Parameters<PresenceEvents[K]>
  ): boolean {
    if (event === 'error') {
      const listeners = this.emitter.rawListeners('error');
      if (listeners.length === 0) {
        const err = args[0];
        const error = err instanceof Error ? err : new Error(String(err));
        console.error('[presence-io] Background error:', error.message);
        return false;
      }
      for (const listener of listeners) {
        try {
          const res = (listener as (...a: unknown[]) => unknown)(...args);
          if (res && typeof (res as Promise<unknown>).then === 'function') {
            (res as Promise<unknown>).catch((listenerErr) => {
              console.error('[presence-io] Error listener threw:', listenerErr);
            });
          }
        } catch (listenerErr) {
          console.error('[presence-io] Error listener threw:', listenerErr);
        }
      }
      return true;
    }

    const rawListeners = this.emitter.rawListeners(event);
    if (rawListeners.length === 0) return false;

    for (const listener of rawListeners) {
      try {
        const result = (listener as (...a: unknown[]) => unknown)(...args);
        if (result && typeof (result as Promise<unknown>).then === 'function') {
          (result as Promise<unknown>).catch((err) => this.reportError(err));
        }
      } catch (err) {
        this.reportError(err);
      }
    }
    return true;
  }

  private emitBroadcasting<K extends keyof PresenceEvents>(
    event: K,
    ...args: Parameters<PresenceEvents[K]>
  ): boolean {
    const payload = args[0];
    const emitted = this.emitLocal(event, ...args);

    if (this.subRedis && event !== 'error') {
      const msg = JSON.stringify({
        instanceId: this.instanceId,
        event,
        payload,
      });
      this.redis.publish(this.pubSubChannel, msg).catch((err) => {
        this.reportError(err);
      });
    }

    return emitted;
  }

  // --- Presence Tracking Methods ---

  public async track(socket: SocketLike, options: TrackOptions): Promise<void> {
    const rawUserId = options?.userId ?? socket.data?.userId;
    if (rawUserId === undefined || rawUserId === null || String(rawUserId).trim() === '') {
      throw new Error('User ID is required and cannot be empty');
    }
    const userId = String(rawUserId);
    if (!socket.data) {
      socket.data = {};
    }
    socket.data.userId = userId;

    const existing = this.trackedSockets.get(socket.id);
    if (existing) {
      if (existing.userId !== userId) {
        await this.handleDisconnect(socket);
      } else {
        return;
      }
    }

    const disconnectHandler = () => {
      this.handleDisconnect(socket).catch((err) => {
        this.reportError(err);
      });
    };

    let anyHandler: ((event: string, ...args: unknown[]) => void) | undefined;
    if (typeof socket.onAny === 'function') {
      anyHandler = () => {
        const now = Date.now();
        const entry = this.trackedSockets.get(socket.id);
        const minInterval =
          this.heartbeatIntervalMs > 0
            ? this.heartbeatIntervalMs
            : Math.floor((this.ttlSeconds * 1000) / 3);
        if (entry) {
          if (entry.lastHeartbeatAt && now - entry.lastHeartbeatAt < minInterval) {
            return;
          }
          entry.lastHeartbeatAt = now;
        }
        this.heartbeat(socket).catch((err) => {
          this.reportError(err);
        });
      };
      socket.onAny(anyHandler);
    }

    if (typeof socket.on === 'function') {
      socket.on('disconnect', disconnectHandler);
    }

    let heartbeatTimer: NodeJS.Timeout | null = null;
    if (this.heartbeatIntervalMs > 0) {
      heartbeatTimer = setInterval(() => {
        this.heartbeat(socket).catch((err) => {
          this.reportError(err);
        });
      }, this.heartbeatIntervalMs);

      if (heartbeatTimer.unref) {
        heartbeatTimer.unref();
      }
    }

    const entry: TrackedSocketEntry = {
      socket,
      userId,
      heartbeatTimer,
      rooms: new Set(),
      disconnectHandler,
      anyHandler,
      lastHeartbeatAt: Date.now(),
    };
    this.trackedSockets.set(socket.id, entry);

    let wasOnline = false;
    try {
      const res = await this.redisStore.addSocketToUser(
        userId,
        socket.id,
        this.ttlSeconds,
      );
      wasOnline = res.wasOnline;
    } catch (err) {
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      if (typeof socket.off === 'function') socket.off('disconnect', disconnectHandler);
      if (anyHandler && typeof socket.offAny === 'function') socket.offAny(anyHandler);
      this.trackedSockets.delete(socket.id);
      throw err;
    }

    if (!wasOnline) {
      this.emitBroadcasting('user:online', { userId });
    }
  }

  public async handleDisconnect(socket: SocketLike): Promise<void> {
    const socketId = socket.id;
    const entry = this.trackedSockets.get(socketId);

    if (entry) {
      if (entry.heartbeatTimer) {
        clearInterval(entry.heartbeatTimer);
      }
      if (typeof socket.off === 'function') {
        socket.off('disconnect', entry.disconnectHandler);
      }
      if (entry.anyHandler && typeof socket.offAny === 'function') {
        socket.offAny(entry.anyHandler);
      }
      this.trackedSockets.delete(socketId);
    }

    const { userId, leftRooms, userWentOffline } = await this.redisStore.handleDisconnect(
      socketId,
      entry?.userId,
    );

    if (userId) {
      for (const roomId of leftRooms) {
        this.emitBroadcasting('room:leave', { roomId, userId });
      }
      if (userWentOffline) {
        this.emitBroadcasting('user:offline', { userId });
      }
    }
  }

  public async joinRoom(socket: SocketLike, roomId: string): Promise<void> {
    const userId = this.resolveUserId(socket);
    if (!userId) {
      throw new Error(`Cannot join room: socket ${socket.id} is not tracked with a userId`);
    }

    const { isFirstSocketInRoom } = await this.redisStore.addUserToRoom(
      roomId,
      userId,
      socket.id,
      this.ttlSeconds,
    );

    const entry = this.trackedSockets.get(socket.id);
    if (entry) {
      entry.rooms.add(roomId);
    }

    if (isFirstSocketInRoom) {
      this.emitBroadcasting('room:join', { roomId, userId });
    }
  }

  public async leaveRoom(socket: SocketLike, roomId: string): Promise<void> {
    const userId = this.resolveUserId(socket);
    if (!userId) {
      throw new Error(`Cannot leave room: socket ${socket.id} is not tracked with a userId`);
    }

    const entry = this.trackedSockets.get(socket.id);
    if (entry) {
      entry.rooms.delete(roomId);
    }

    const { leftRoom } = await this.redisStore.removeUserFromRoom(roomId, userId, socket.id);

    if (leftRoom) {
      this.emitBroadcasting('room:leave', { roomId, userId });
    }
  }

  public async heartbeat(socket: SocketLike): Promise<boolean> {
    if (!this.trackedSockets.has(socket.id)) {
      return false;
    }
    const userId = this.resolveUserId(socket);
    if (!userId) return false;

    const entry = this.trackedSockets.get(socket.id);
    if (!entry) return false;

    if (entry.inFlightHeartbeat) {
      return entry.inFlightHeartbeat;
    }

    entry.lastHeartbeatAt = Date.now();

    entry.inFlightHeartbeat = (async () => {
      try {
        const refreshed = await this.redisStore.heartbeat(socket.id, userId, this.ttlSeconds);
        if (!refreshed) {
          // Socket key was missing in Redis (evicted or stale); re-register socket & rooms
          if (!this.trackedSockets.has(socket.id)) {
            return false;
          }
          const knownRooms = Array.from(entry.rooms);

          const { wasOnline } = await this.redisStore.addSocketToUser(
            userId,
            socket.id,
            this.ttlSeconds,
          );

          if (!wasOnline && this.trackedSockets.has(socket.id)) {
            this.emitBroadcasting('user:online', { userId });
          }

          for (const roomId of knownRooms) {
            if (!this.trackedSockets.has(socket.id)) break;
            try {
              const { isFirstSocketInRoom } = await this.redisStore.addUserToRoom(
                roomId,
                userId,
                socket.id,
                this.ttlSeconds,
              );
              if (isFirstSocketInRoom && this.trackedSockets.has(socket.id)) {
                this.emitBroadcasting('room:join', { roomId, userId });
              }
            } catch (_err) {
              break;
            }
          }

          if (!this.trackedSockets.has(socket.id)) {
            const cleanup = await this.redisStore.handleDisconnect(socket.id, userId);
            if (cleanup.userId) {
              for (const roomId of cleanup.leftRooms) {
                this.emitBroadcasting('room:leave', { roomId, userId: cleanup.userId });
              }
              if (cleanup.userWentOffline) {
                this.emitBroadcasting('user:offline', { userId: cleanup.userId });
              }
            }
            return false;
          }

          return true;
        }

        if (!this.trackedSockets.has(socket.id)) {
          const cleanup = await this.redisStore.handleDisconnect(socket.id, userId);
          if (cleanup.userId) {
            for (const roomId of cleanup.leftRooms) {
              this.emitBroadcasting('room:leave', { roomId, userId: cleanup.userId });
            }
            if (cleanup.userWentOffline) {
              this.emitBroadcasting('user:offline', { userId: cleanup.userId });
            }
          }
          return false;
        }

        return true;
      } catch (err) {
        this.reportError(err);
        return false;
      } finally {
        if (entry) {
          entry.inFlightHeartbeat = undefined;
        }
      }
    })();

    return entry.inFlightHeartbeat;
  }

  public async sweepStale(options?: { force?: boolean }): Promise<StaleEvictionResult> {
    const force = options?.force ?? false;
    const result = await this.redisStore.sweepStale(
      this.ttlSeconds,
      this.sweepIntervalMs,
      force,
    );

    for (const { roomId, userId } of result.roomLeaves) {
      this.emitBroadcasting('room:leave', { roomId, userId });
    }
    for (const userId of result.offlineUsers) {
      this.emitBroadcasting('user:offline', { userId });
    }

    return result;
  }

  // --- Public Queries ---

  public async getRoomUsers(roomId: string): Promise<string[]> {
    return await this.redisStore.getRoomUsers(roomId);
  }

  public async isUserOnline(userId: string): Promise<boolean> {
    return await this.redisStore.isUserOnline(userId);
  }

  public async getUserRooms(userId: string): Promise<string[]> {
    return await this.redisStore.getUserRooms(userId);
  }

  public async getUserSockets(userId: string): Promise<string[]> {
    return await this.redisStore.getUserSockets(userId);
  }

  public async getRoomUserCount(roomId: string): Promise<number> {
    return await this.redisStore.getRoomUserCount(roomId);
  }

  public getRedisStore(): RedisStore {
    return this.redisStore;
  }

  // --- Cleanup ---

  public async shutdown(): Promise<void> {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }

    for (const entry of this.trackedSockets.values()) {
      if (entry.heartbeatTimer) {
        clearInterval(entry.heartbeatTimer);
      }
      if (typeof entry.socket.off === 'function') {
        entry.socket.off('disconnect', entry.disconnectHandler);
      }
      if (entry.anyHandler && typeof entry.socket.offAny === 'function') {
        entry.socket.offAny(entry.anyHandler);
      }
    }
    this.trackedSockets.clear();

    if (this.io && this.connectionListener && typeof this.io.off === 'function') {
      this.io.off('connection', this.connectionListener);
    }

    if (this.subRedis) {
      if (this.messageHandler) {
        this.subRedis.off('message', this.messageHandler);
      }
      this.subRedis.unsubscribe(this.pubSubChannel).catch(() => {});
      if (this.ownSubRedis) {
        this.subRedis.disconnect();
      }
    }

    this.emitter.removeAllListeners();
  }

  // --- Private Helpers ---

  private resolveUserId(socket: SocketLike): string | undefined {
    const tracked = this.trackedSockets.get(socket.id)?.userId;
    if (tracked) return tracked;
    if (socket.data && socket.data.userId !== undefined && socket.data.userId !== null) {
      const str = String(socket.data.userId);
      return str.trim() !== '' ? str : undefined;
    }
    return undefined;
  }

  private startSweepTimer(): void {
    if (this.sweepIntervalMs > 0) {
      this.sweepTimer = setInterval(() => {
        this.sweepStale().catch((err) => {
          this.emitLocal('error', err instanceof Error ? err : new Error(String(err)));
        });
      }, this.sweepIntervalMs);

      if (this.sweepTimer && typeof this.sweepTimer.unref === 'function') {
        this.sweepTimer.unref();
      }
    }
  }
}

