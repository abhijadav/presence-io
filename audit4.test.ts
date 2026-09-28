// @ts-nocheck
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import { PresenceManager } from '../../src/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let redis, managers, extra;
const mk = (id, data = {}) => { const l = {}; return { id, data, _l: l, on(e, f) { (l[e] ??= []).push(f); }, off(e, f) { l[e] = (l[e] || []).filter((x) => x !== f); }, emit(e, ...a) { (l[e] || []).slice().forEach((f) => f(...a)); } }; };
function mkPM(o = {}) { const pm = new PresenceManager({ redis, prefix: 'a4', ttlSeconds: 30, sweepIntervalMs: 0, heartbeatIntervalMs: 0, ...o }); pm.on('error', () => {}); managers.push(pm); return pm; }
function record(pm) { const ev = []; for (const e of ['user:online','user:offline','room:join','room:leave']) pm.on(e, (p) => ev.push([e, p.userId, p.roomId])); return ev; }
beforeEach(async () => { redis = new Redis(); managers = []; extra = []; await redis.flushall(); });
afterEach(async () => { for (const m of managers) await m.shutdown(); for (const c of extra) c.disconnect(); await redis.flushall(); redis.disconnect(); });

describe('G. round-4 probes', () => {
  it('G1 once() listener fires only once', async () => {
    const pm = mkPM(); let n = 0;
    pm.once('user:online', () => { n++; });
    await pm.track(mk('o1'), { userId: 'u1' }); await pm.track(mk('o2'), { userId: 'u2' });
    expect(n).toBe(1);
  });

  it('G2 off() removes a once() listener before it fires', async () => {
    const pm = mkPM(); let n = 0; const f = () => { n++; };
    pm.once('user:online', f); pm.off('user:online', f);
    await pm.track(mk('o3'), { userId: 'u3' });
    expect(n).toBe(0);
  });

  it('G3 two sockets of one user re-registering concurrently emit exactly one user:online', async () => {
    const pm = mkPM(); const ev = record(pm);
    const s1 = mk('g1'), s2 = mk('g2');
    await pm.track(s1, { userId: 'gu' }); await pm.track(s2, { userId: 'gu' });
    // age both out and sweep (emits user:offline once)
    await redis.hset('{a4}:user:gu', 'g1', '1'); await redis.hset('{a4}:user:gu', 'g2', '1');
    await redis.del('{a4}:socket:g1', '{a4}:socket:g2');
    await pm.sweepStale({ force: true });
    ev.length = 0;
    await Promise.all([pm.heartbeat(s1), pm.heartbeat(s2)]);
    console.log('   events after concurrent re-register:', ev.map((e) => e[0]).join(','));
    expect(ev.filter((e) => e[0] === 'user:online').length).toBe(1);
  });

  it('G4 malformed / foreign pub-sub messages cannot inject events or crash', async () => {
    const sub = redis.duplicate(); extra.push(sub);
    const pm = mkPM({ subRedis: sub }); await sleep(150);
    const got = []; const errs = [];
    pm.on('user:online', (p) => got.push(p)); pm.on('error', (e) => errs.push(e.message));
    const ch = [...(await redis.pubsub('CHANNELS'))][0];
    await redis.publish(ch, 'not json');
    await redis.publish(ch, JSON.stringify({ instanceId: 'x', event: 'error', payload: { userId: 'evil' } }));
    await redis.publish(ch, JSON.stringify({ instanceId: 'x', event: 'newListener', payload: {} }));
    await redis.publish(ch, JSON.stringify({ instanceId: 'x', event: 'user:online', payload: null }));
    await sleep(200);
    console.log('   channel:', ch, ' errors seen:', JSON.stringify(errs), ' online payloads:', JSON.stringify(got));
    expect(errs.length).toBe(0);                       // a foreign message must not surface as a local 'error'
    expect(got.every((p) => p && typeof p.userId === 'string')).toBe(true);   // and must not deliver malformed payloads
  });

  it('G5 soak: 60 live sockets, 2 nodes sweeping every 200ms, TTL 1s for 6s -> nobody flaps', async () => {
    const A = mkPM({ ttlSeconds: 1, heartbeatIntervalMs: 300, sweepIntervalMs: 200 });
    const B = mkPM({ ttlSeconds: 1, heartbeatIntervalMs: 300, sweepIntervalMs: 200 });
    const evA = record(A), evB = record(B); const errs = [];
    A.on('error', (e) => errs.push(e.message)); B.on('error', (e) => errs.push(e.message));
    const socks = [];
    for (let i = 0; i < 60; i++) { const s = mk('sk' + i); socks.push(s); await (i % 2 ? A : B).track(s, { userId: 'su' + (i % 30) }); await (i % 2 ? A : B).joinRoom(s, 'sr' + (i % 4)); }
    await sleep(6000);
    const offA = evA.filter((e) => e[0] === 'user:offline').length, offB = evB.filter((e) => e[0] === 'user:offline').length;
    console.log('   spurious offline events A/B:', offA, offB, ' errors:', errs.length);
    expect(offA + offB).toBe(0); expect(errs.length).toBe(0);
    for (let i = 0; i < 30; i++) expect(await A.isUserOnline('su' + i)).toBe(true);
  }, 30000);
});
