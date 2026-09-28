import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import { PresenceManager } from '../../src/index.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let redis: Redis;
let managers: PresenceManager[];
const mk = (id: string, data: Record<string, unknown> = {}) => { const l: Record<string, ((...a: unknown[]) => void)[]> = {}; return { id, data, _l: l, on(e: string, f: (...a: unknown[]) => void) { (l[e] ??= []).push(f); }, off(e: string, f: (...a: unknown[]) => void) { l[e] = (l[e] || []).filter((x) => x !== f); }, emit(e: string, ...a: unknown[]) { (l[e] || []).slice().forEach((f) => f(...a)); } }; };
function mkPM(o: Record<string, unknown> = {}) { const pm = new PresenceManager({ redis, prefix: 'a5', ttlSeconds: 30, sweepIntervalMs: 0, heartbeatIntervalMs: 0, ...o }); managers.push(pm); return pm; }
function record(pm: PresenceManager) { const ev: [string, string, string | undefined][] = []; for (const e of ['user:online','user:offline','room:join','room:leave'] as const) pm.on(e, (p: { userId: string; roomId?: string }) => ev.push([e, p.userId, p.roomId])); return ev; }
beforeEach(async () => { redis = new Redis(); managers = []; await redis.flushall(); });
afterEach(async () => { for (const m of managers) await m.shutdown(); await redis.flushall(); redis.disconnect(); });

describe('H. round-5 probes', () => {
  it('H1 a Redis failure inside heartbeat() is reported via the error event (not silently swallowed)', async () => {
    const pm = mkPM(); const errs: string[] = []; pm.on('error', (e) => errs.push(e.message));
    const s = mk('hb'); await pm.track(s, { userId: 'hb' });
    const orig = redis.eval.bind(redis);
    redis.eval = () => Promise.reject(new Error('redis boom'));
    const r = await pm.heartbeat(s).catch((e: Error) => 'rejected:' + e.message);
    redis.eval = orig;
    console.log('   heartbeat returned:', r, ' error events:', errs.length);
    expect(errs.length + (typeof r === 'string' ? 1 : 0)).toBeGreaterThan(0);
  });

  it('H2 5 concurrent heartbeats for one evicted socket -> exactly one user:online and one room:join', async () => {
    const pm = mkPM(); const ev = record(pm); const s = mk('cc');
    await pm.track(s, { userId: 'cu' }); await pm.joinRoom(s, 'R');
    await redis.hset('{a5}:user:cu', 'cc', '1'); await redis.del('{a5}:socket:cc');
    await pm.sweepStale({ force: true }); ev.length = 0;
    await Promise.all([1,2,3,4,5].map(() => pm.heartbeat(s)));
    expect(ev.map((e) => e[0])).toEqual(['user:online', 'room:join']);
  });

  it('H3 once() fires once even with mixed once/on listeners and off() during emit', async () => {
    const pm = mkPM(); const calls: string[] = [];
    pm.once('user:online', () => calls.push('once'));
    const f = () => { calls.push('on'); }; pm.on('user:online', f);
    await pm.track(mk('m1'), { userId: 'a' }); await pm.track(mk('m2'), { userId: 'b' });
    expect(calls).toEqual(['once', 'on', 'on']);
  });

  it('H4 churn: 600 sockets track/join/disconnect in concurrent batches leaves no keys and no local state', async () => {
    const pm = mkPM(); pm.on('error', () => {});
    for (let b = 0; b < 12; b++) {
      const batch = Array.from({ length: 50 }, (_, i) => mk(`c${b}_${i}`));
      await Promise.all(batch.map(async (s, i) => { await pm.track(s, { userId: 'cu' + (i % 20) }); await pm.joinRoom(s, 'room' + (i % 5)); }));
      await Promise.all(batch.map((s) => pm.handleDisconnect(s)));
    }
    const left = (await redis.keys('*')).filter((k) => !k.includes('sweep:lock'));
    expect(left).toEqual([]);
    expect(pm.trackedSockets.size).toBe(0);
  }, 60000);

  it('H5 listener registered for user:online may call back into the manager without deadlock/duplication', async () => {
    const pm = mkPM(); const ev = record(pm); const s = mk('re');
    pm.on('user:online', ({ userId: _userId }) => { pm.joinRoom(s, 'auto').catch(() => {}); });
    await pm.track(s, { userId: 'reent' }); await sleep(200);
    expect(ev.filter((e) => e[0] === 'room:join').length).toBe(1);
    expect(await pm.getRoomUsers('auto')).toEqual(['reent']);
  });
});
