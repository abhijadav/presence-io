import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import Redis from 'ioredis';
import { PresenceManager } from 'presence-io';

const instanceId = process.env.INSTANCE_ID || 'node-1';
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, { cors: { origin: '*' } });

const redis = new Redis(redisUrl);
const subRedis = new Redis(redisUrl);

const presence = new PresenceManager({
  io,
  redis,
  subRedis,
  prefix: 'demo-presence',
  ttlSeconds: 30,
});

presence.on('error', (err) => {
  console.error(`[${instanceId}] Presence Error:`, err.message);
});

presence.on('user:online', ({ userId }) => {
  console.log(`[${instanceId}] Presence Event: User ${userId} is ONLINE`);
});

presence.on('user:offline', ({ userId }) => {
  console.log(`[${instanceId}] Presence Event: User ${userId} is OFFLINE`);
});

presence.on('room:join', ({ roomId, userId }) => {
  console.log(`[${instanceId}] Presence Event: User ${userId} JOINED ${roomId}`);
});

presence.on('room:leave', ({ roomId, userId }) => {
  console.log(`[${instanceId}] Presence Event: User ${userId} LEFT ${roomId}`);
});

io.on('connection', (socket) => {
  const userId = socket.handshake.query.userId || socket.id;
  console.log(`[${instanceId}] Socket connected: ${socket.id} (user: ${userId})`);

  presence.track(socket, { userId });

  socket.on('joinRoom', (roomId) => presence.joinRoom(socket, roomId));
  socket.on('leaveRoom', (roomId) => presence.leaveRoom(socket, roomId));
});

app.get('/health', (req, res) => {
  res.json({ instanceId, status: 'ok' });
});

app.get('/room/:roomId/users', async (req, res) => {
  const users = await presence.getRoomUsers(req.params.roomId);
  res.json({ instanceId, roomId: req.params.roomId, users });
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`[${instanceId}] Presence Server listening on port ${PORT}`);
});
