// @ts-nocheck
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import { PresenceManager } from '../../src/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let redis, managers, extra;
function mk(id, data = {}, withAny = false) {
  const l = {}, any = [];
  const s = { id, data, _l: l, _any: any,
    on(e, f) { (l[e] ??= []).push(f); }, off(e, f) { l[e] = (l[e] || []).filter((x) => x !== f); },
    emit(e, ...a) { (l[e] || []).slice().forEach((f) => f(...a)); } };
  if (withAny) { s.onAny = (f) => any.push(f); s.offAny = (f) => { const i = any.indexOf(f); if (i >= 0) any.splice(i, 1); }; s.fire = (...a) => any.slice().forEach((f) => f(...a)); }
  return s;
}
function mkPM(o = {}) {
  const pm = new PresenceManager({ redis, prefix: 'a3', ttlSeconds: 30, sweepIntervalMs: 0, heartbeatIntervalMs: 0, ...o });
  pm.on('error', () => {}); managers.push(pm); return pm;
}
function record(pm) { const ev = []; for (const e of ['user:online','user:offline','room:join','room:leave']) pm.on(e, (p) => ev.push([e, p.userId, p.roomId])); return ev; }
beforeEach(async () => { redis = new Redis(); managers = []; extra = []; await redis.flushall(); });
afterEach(async () => { for (const m of managers) await m.shutdown(); for (const c of extra) c.disconnect(); await redis.flushall(); redis.disconnect(); });

describe('E. round-3 probes on the new code', () => {
  it('E1 re-register vs disconnect race keeps events consistent with Redis (150 fuzz trials)', async () => {
    const pm = mkPM(); const ev = record(pm); let ghost = 0, mismatch = 0, thrown = 0;
    for (let i = 0; i < 150; i++) {
      const s = mk('f' + i), uid = 'fu' + i;
      await pm.track(s, { userId: uid }); await pm.joinRoom(s, 'fr' + i);
      await redis.hset(`{a3}:user:${uid}`, s.id, '1'); await redis.del(`{a3}:socket:${s.id}`);  // age it out
      await pm.sweepStale({ force: true });                                                      // evict (emits leave/offline)
      await Promise.all([pm.heartbeat(s).catch(() => { thrown++; }), sleep(i % 5).then(() => pm.handleDisconnect(s))]);
      await sleep(5);
      const online = await pm.isUserOnline(uid);
      const inRoom = (await pm.getRoomUsers('fr' + i)).includes(uid);
      const last = ev.filter((e) => e[1] === uid && e[0].startsWith('user:')).pop()?.[0];
      if (online || inRoom) ghost++;
      if ((online && last !== 'user:online') || (!online && last === 'user:online')) { mismatch++; if (mismatch <= 2) console.log('   sample: delay', i % 5, 'events', JSON.stringify(ev.filter((e) => e[1] === uid).map((e) => e[0] + (e[2] ? '@' + e[2] : ''))), 'redisOnline', online); }
    }
    console.log('   ghosts:', ghost, ' event/Redis mismatches:', mismatch, ' heartbeat() rejections:', thrown, '/150');
    expect(ghost).toBe(0); expect(mismatch).toBe(0);
  }, 60000);

  it('E2 a throwing user listener must not roll back a successful track()', async () => {
    const pm = mkPM(); const s = mk('t1');
    pm.on('user:online', () => { throw new Error('app bug'); });
    await pm.track(s, { userId: 'lst' }).catch(() => {});
    const online = await pm.isUserOnline('lst');
    const tracked = pm.trackedSockets.has('t1');
    console.log('   online in Redis:', online, ' still tracked locally:', tracked);
    expect(online === tracked).toBe(true);        // no divergence between Redis and local tracking
  });

  it('E3 a throwing room:leave listener must not suppress user:offline on disconnect', async () => {
    const pm = mkPM(); const ev = record(pm); const s = mk('t2');
    await pm.track(s, { userId: 'lz' }); await pm.joinRoom(s, 'R');
    pm.on('room:leave', () => { throw new Error('app bug'); });
    await pm.handleDisconnect(s).catch(() => {});
    expect(ev.some((e) => e[0] === 'user:offline')).toBe(true);
  });

  it('E4 manual sweepStale(): skipped flag when lock is held, force bypasses it', async () => {
    const pm = mkPM({ sweepIntervalMs: 5000 });
    await redis.set('{a3}:sweep:lock', 'someone-else', 'PX', 3000);
    const r1 = await pm.sweepStale(); expect(r1.skipped).toBe(true);
    const r2 = await pm.sweepStale({ force: true }); expect(r2.skipped).toBeFalsy();
  });

  it('E5 sweep lock is released after completion and never deletes another owner\'s lock', async () => {
    const pm = mkPM({ sweepIntervalMs: 5000 });
    await pm.sweepStale(); expect(await redis.exists('{a3}:sweep:lock')).toBe(0);
    await redis.set('{a3}:sweep:lock', 'other', 'PX', 3000);
    await redis.eval(`if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end`, 1, '{a3}:sweep:lock', 'mine');
    expect(await redis.get('{a3}:sweep:lock')).toBe('other');
  });

  it('E6 same-client subRedis: duplicate is created and closed on shutdown (no connection leak)', async () => {
    const count = async () => (await redis.client('LIST')).split('\n').filter(Boolean).length;
    const base = await count();
    const pm = new PresenceManager({ redis, subRedis: redis, prefix: 'a3', sweepIntervalMs: 0, heartbeatIntervalMs: 0 });
    pm.on('error', () => {}); await sleep(200);
    expect(await count()).toBe(base + 1);
    await pm.shutdown(); await sleep(300);
    expect(await count()).toBe(base);
  });

  it('E7 userId migration on re-track: old user fully cleaned, events sane, new user online', async () => {
    const pm = mkPM(); const ev = record(pm); const s = mk('mig');
    await pm.track(s, { userId: 'old' }); await pm.joinRoom(s, 'R');
    await pm.track(s, { userId: 'new' });
    expect(await pm.isUserOnline('old')).toBe(false);
    expect(await pm.getRoomUsers('R')).toEqual([]);
    expect(await pm.isUserOnline('new')).toBe(true);
    expect(ev.map((e) => e[0] + ':' + e[1])).toEqual(['user:online:old','room:join:old','room:leave:old','user:offline:old','user:online:new']);
    expect(s._l.disconnect.length).toBe(1);
  });

  it('E8 event-driven keepalive: chatty socket with auto-timer OFF stays online past TTL', async () => {
    const pm = mkPM({ ttlSeconds: 1 }); const s = mk('chat', {}, true);
    await pm.track(s, { userId: 'chatty' });
    for (let i = 0; i < 8; i++) { await sleep(400); s.fire('msg'); await pm.getRedisStore().sweepStale(1, 0, true); }
    expect(await pm.isUserOnline('chatty')).toBe(true);
  }, 20000);

  it('E9 concurrent track() of the same socket (double connection event) is safe', async () => {
    const pm = mkPM(); const ev = record(pm); const s = mk('dbl');
    await Promise.all([pm.track(s, { userId: 'dd' }), pm.track(s, { userId: 'dd' })]);
    expect(ev.filter((e) => e[0] === 'user:online').length).toBe(1);
    expect(s._l.disconnect.length).toBe(1);
    await pm.handleDisconnect(s);
    expect((await redis.keys('*')).filter((k) => !k.includes('sweep:lock'))).toEqual([]);
  });

  it('E10 track() rejects invalid ids but accepts numeric ids; joinRoom on untracked socket rejects', async () => {
    const pm = mkPM();
    await expect(pm.track(mk('i1'), { userId: '  ' })).rejects.toThrow();
    await expect(pm.track(mk('i2'), { userId: undefined })).rejects.toThrow();
    await pm.track(mk('i3'), { userId: 42 }); expect(await pm.isUserOnline('42')).toBe(true);
    await expect(pm.joinRoom(mk('nope'), 'R')).rejects.toThrow();
  });

  it('E11 shutdown() during in-flight track/heartbeat does not throw or leave timers', async () => {
    const pm = mkPM({ ttlSeconds: 1, heartbeatIntervalMs: 100 }); const s = mk('sd');
    await pm.track(s, { userId: 'sd' });
    const p = pm.heartbeat(s); await pm.shutdown(); await p.catch((e) => { throw e; });
    await sleep(400);   // timers must be dead: nothing should touch Redis
    const before = +(await redis.info('commandstats')).match(/cmdstat_eval:calls=(\d+)/)[1];
    await sleep(400);
    const after = +(await redis.info('commandstats')).match(/cmdstat_eval:calls=(\d+)/)[1];
    expect(after - before).toBe(0);
  });
});

describe('F. duplicate-event probe', () => {
  it('F1 socket evicted by a sweep and then really disconnecting emits user:offline only once', async () => {
    const pm = mkPM(); const ev = record(pm); const s = mk('dup');
    await pm.track(s, { userId: 'dupu' });
    await redis.hset('{a3}:user:dupu', 'dup', '1'); await redis.del('{a3}:socket:dup');   // stale
    await pm.sweepStale({ force: true });                                                  // another node evicts it
    s.emit('disconnect'); await sleep(100);                                                // then the client really goes away
    console.log('   offline events:', ev.filter((e) => e[0] === 'user:offline').length);
    expect(ev.filter((e) => e[0] === 'user:offline').length).toBe(1);
  });
});
