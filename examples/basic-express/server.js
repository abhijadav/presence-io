import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import Redis from 'ioredis';
import { PresenceManager } from 'presence-io';

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' },
});

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

const presence = new PresenceManager({
  io,
  redis,
  ttlSeconds: 30,
  sweepIntervalMs: 15000,
});

presence.on('error', (err) => {
  console.error('[Presence Error]', err.message);
});

presence.on('user:online', ({ userId }) => {
  console.log(`[Presence] User ${userId} is ONLINE`);
});

presence.on('user:offline', ({ userId }) => {
  console.log(`[Presence] User ${userId} is OFFLINE`);
});

presence.on('room:join', ({ roomId, userId }) => {
  console.log(`[Presence] User ${userId} joined room ${roomId}`);
});

presence.on('room:leave', ({ roomId, userId }) => {
  console.log(`[Presence] User ${userId} left room ${roomId}`);
});

io.on('connection', (socket) => {
  const userId = socket.handshake.query.userId || socket.id;
  presence.track(socket, { userId });

  socket.on('joinRoom', async (roomId) => {
    await presence.joinRoom(socket, roomId);
    socket.join(roomId);
  });

  socket.on('leaveRoom', async (roomId) => {
    await presence.leaveRoom(socket, roomId);
    socket.leave(roomId);
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/room/:roomId/users', async (req, res) => {
  const users = await presence.getRoomUsers(req.params.roomId);
  res.json({ roomId: req.params.roomId, users });
});

app.get('/user/:userId/online', async (req, res) => {
  const online = await presence.isUserOnline(req.params.userId);
  res.json({ userId: req.params.userId, online });
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`Basic Express + Socket.io Server listening on port ${PORT}`);
});
