// @ts-nocheck
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Redis from 'ioredis';
import { PresenceManager } from '../../src/index.js';
import { RedisStore } from '../../src/RedisStore.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let redis, managers, extra;
let seq = 0;

function mk(id, data = {}) {
  const l = {};
  return {
    id, data, _l: l,
    on(e, f) { (l[e] ??= []).push(f); },
    off(e, f) { l[e] = (l[e] || []).filter((x) => x !== f); },
    emit(e, ...a) { (l[e] || []).slice().forEach((f) => f(...a)); },
  };
}
function mkPM(opts = {}) {
  const pm = new PresenceManager({ redis, prefix: 'audit', ttlSeconds: 30, sweepIntervalMs: 0, heartbeatIntervalMs: 0, ...opts });
  managers.push(pm);
  return pm;
}
function record(pm) {
  const ev = [];
  for (const e of ['user:online', 'user:offline', 'room:join', 'room:leave'])
    pm.on(e, (p) => ev.push([e, p.userId, p.roomId]));
  return ev;
}
const keysLeft = async () => (await redis.keys('*')).filter((k) => !k.endsWith(':sweep:lock'));

beforeEach(async () => {
  redis = new Redis(); managers = []; extra = [];
  await redis.flushall();
});
afterEach(async () => {
  for (const m of managers) await m.shutdown();
  for (const c of extra) c.disconnect();
  await redis.flushall(); redis.disconnect();
});

describe('A. regression of previously reported bugs', () => {
  it('A1 auto-heartbeat keeps a live socket online for 3x TTL', async () => {
    const pm = mkPM({ ttlSeconds: 1, heartbeatIntervalMs: 300 });
    const s = mk('live'); await pm.track(s, { userId: 'alice' }); await pm.joinRoom(s, 'r');
    for (let i = 0; i < 5; i++) { await sleep(700); await pm.getRedisStore().sweepStale(1, 0); }
    expect(await pm.isUserOnline('alice')).toBe(true);
    expect(await pm.getRoomUsers('r')).toEqual(['alice']);
  }, 20000);

  it('A2 without heartbeat a silent socket IS evicted (crash recovery)', async () => {
    const pm = mkPM({ ttlSeconds: 1 });
    const ev = record(pm);
    const s = mk('dead'); await pm.track(s, { userId: 'bob' }); await pm.joinRoom(s, 'r');
    await sleep(2200);
    await pm.getRedisStore().sweepStale(1, 0);
    expect(await pm.isUserOnline('bob')).toBe(false);
    expect(await pm.getRoomUsers('r')).toEqual([]);
  }, 20000);

  it('A3 heartbeat() re-registers an evicted-but-connected socket and its rooms', async () => {
    const pm = mkPM({ ttlSeconds: 1 });
    const s = mk('re'); await pm.track(s, { userId: 'carl' }); await pm.joinRoom(s, 'r1');
    await sleep(2200); await pm.getRedisStore().sweepStale(1, 0);
    expect(await pm.isUserOnline('carl')).toBe(false);
    expect(await pm.heartbeat(s)).toBe(true);
    expect(await pm.isUserOnline('carl')).toBe(true);
    expect(await pm.getRoomUsers('r1')).toEqual(['carl']);
  }, 20000);

  it('A4 join after disconnect rejects and leaves no ghost', async () => {
    const pm = mkPM(); const s = mk('gone');
    await pm.track(s, { userId: 'dan' }); await pm.handleDisconnect(s);
    await expect(pm.joinRoom(s, 'late')).rejects.toThrow();
    expect(await pm.getRoomUsers('late')).toEqual([]);
  });

  it('A5 removeSocketFromUser cleans room state', async () => {
    const pm = mkPM(); const s = mk('s5');
    await pm.track(s, { userId: 'eve' }); await pm.joinRoom(s, 'roomX');
    await pm.getRedisStore().removeSocketFromUser('eve', 's5');
    expect(await pm.getRoomUsers('roomX')).toEqual([]);
    expect(await keysLeft()).toEqual([]);
  });

  it('A6 room IDs containing , : { } and spaces round-trip on disconnect', async () => {
    const pm = mkPM(); const s = mk('s6'); const ev = record(pm);
    const rooms = ['a,b', 'c:d', 'x{y}', 'with space', 'a:sockets:b'];
    await pm.track(s, { userId: 'fay' });
    for (const r of rooms) await pm.joinRoom(s, r);
    await pm.handleDisconnect(s);
    const left = ev.filter((e) => e[0] === 'room:leave').map((e) => e[2]).sort();
    expect(left).toEqual([...rooms].sort());
    expect(await keysLeft()).toEqual([]);
  });

  it('A7 track() twice registers one disconnect handler', async () => {
    const pm = mkPM(); const s = mk('s7');
    await pm.track(s, { userId: 'gus' }); await pm.track(s, { userId: 'gus' });
    expect(s._l.disconnect.length).toBe(1);
  });

  it('A8 same-socket re-add does not falsely report wasOnline for first add', async () => {
    const st = new RedisStore(redis, 'audit');
    expect((await st.addSocketToUser('u', 's', 10)).wasOnline).toBe(false);
  });
});

describe('B. new bug hunting', () => {
  it('B1 sweep lock must not swallow a legitimate second sweep', async () => {
    const pm = mkPM({ ttlSeconds: 1 });
    await pm.track(mk('a1'), { userId: 'u1' });
    await sleep(1600);
    await pm.sweepStale();                       // first sweep (takes lock)
    await pm.track(mk('a2'), { userId: 'u2' });
    await sleep(1600);
    const r = await pm.sweepStale();             // stale u2 should be evicted now
    expect(r.offlineUsers).toContain('u2');
  }, 20000);

  it('B2 crash-detection latency with short sweepIntervalMs (lock = 5s)', async () => {
    const a = mkPM({ ttlSeconds: 1 });
    const b = mkPM({ ttlSeconds: 1, sweepIntervalMs: 300 });
    const ev = record(b);
    await sleep(400);                              // b's first sweep runs (nothing stale) and takes the lock
    await a.track(mk('crash'), { userId: 'zed' }); // "a" never heartbeats / disconnects
    const t0 = Date.now(); let dt = null;
    while (Date.now() - t0 < 8000) {
      if (ev.some((e) => e[0] === 'user:offline' && e[1] === 'zed')) { dt = Date.now() - t0; break; }
      await sleep(100);
    }
    console.log('   crash detected after ms:', dt, '(ttl=1s, sweepInterval=300ms)');
    expect(dt).not.toBeNull();
    expect(dt).toBeLessThan(3000);
  }, 20000);

  it('B3 heartbeat() on an already-disconnected socket must NOT resurrect it', async () => {
    const pm = mkPM(); const s = mk('zombie');
    await pm.track(s, { userId: 'zoe' }); await pm.handleDisconnect(s);
    await pm.heartbeat(s);  // e.g. late timer/onAny tick; socket.data.userId is still set
    expect(await pm.isUserOnline('zoe')).toBe(false);
  });

  it('B4 concurrent heartbeat vs disconnect never leaves a ghost (20 rounds)', async () => {
    const pm = mkPM(); let ghosts = 0;
    for (let i = 0; i < 20; i++) {
      const s = mk('r' + i); await pm.track(s, { userId: 'u' + i });
      await Promise.all([pm.handleDisconnect(s), pm.heartbeat(s), pm.heartbeat(s)]);
      if (await pm.isUserOnline('u' + i)) ghosts++;
    }
    console.log('   ghosts after 20 races:', ghosts);
    expect(ghosts).toBe(0);
  });

  it('B5 re-track same socket with a different userId updates Redis', async () => {
    const pm = mkPM(); const s = mk('sw');
    await pm.track(s, { userId: 'old' }); await pm.track(s, { userId: 'new' });
    expect(await pm.isUserOnline('new')).toBe(true);
  });

  it('B6 failed track() (Redis error) can be retried', async () => {
    const pm = mkPM(); const s = mk('retry');
    const orig = redis.eval.bind(redis); let n = 0;
    redis.eval = (...a) => (n++ === 0 ? Promise.reject(new Error('boom')) : orig(...a));
    await expect(pm.track(s, { userId: 'ret' })).rejects.toThrow('boom');
    redis.eval = orig;
    await pm.track(s, { userId: 'ret' });          // retry
    expect(await pm.isUserOnline('ret')).toBe(true);
  });

  it('B7 failed joinRoom must not poison later heartbeat re-registration', async () => {
    const pm = mkPM({ ttlSeconds: 1 }); const s = mk('poison');
    await pm.track(s, { userId: 'pat' });
    await sleep(2200);                              // socket key expired
    await expect(pm.joinRoom(s, 'never')).rejects.toThrow();
    await pm.heartbeat(s);                          // re-registers known rooms
    expect(await pm.getRoomUsers('never')).toEqual([]);
  }, 20000);

  it('B8 empty-string userId is rejected (else it can never be cleaned up)', async () => {
    const pm = mkPM(); const s = mk('empty');
    let threw = false; try { await pm.track(s, { userId: '' }); } catch { threw = true; }
    await pm.handleDisconnect(s);
    const left = await keysLeft();
    console.log('   threw:', threw, 'leftover keys:', left);
    expect(threw || left.length === 0).toBe(true);
  });

  it('B9 concurrent track of 2 sockets of one user emits exactly one user:online', async () => {
    const pm = mkPM(); const ev = record(pm);
    await Promise.all([pm.track(mk('c1'), { userId: 'cc' }), pm.track(mk('c2'), { userId: 'cc' })]);
    expect(ev.filter((e) => e[0] === 'user:online').length).toBe(1);
  });

  it('B10 concurrent joinRoom from 2 sockets of one user emits exactly one room:join', async () => {
    const pm = mkPM(); const ev = record(pm);
    const s1 = mk('j1'), s2 = mk('j2');
    await pm.track(s1, { userId: 'jj' }); await pm.track(s2, { userId: 'jj' });
    await Promise.all([pm.joinRoom(s1, 'R'), pm.joinRoom(s2, 'R')]);
    expect(ev.filter((e) => e[0] === 'room:join').length).toBe(1);
  });

  it('B11 sweeping evicted user with empty roomSockets leaves zero leaked keys', async () => {
    const pm = mkPM({ ttlSeconds: 1 });
    for (let i = 0; i < 5; i++) { const s = mk('k' + i); await pm.track(s, { userId: 'ku' + i }); await pm.joinRoom(s, 'rr' + (i % 2)); }
    await sleep(2200); await pm.getRedisStore().sweepStale(1, 0);
    expect(await keysLeft()).toEqual([]);
  }, 20000);

  it('B12 numeric socket.data.userId with io auto-track is not silently ignored', async () => {
    const handlers = []; const io = { on: (e, f) => handlers.push(f), off() {} };
    const pm = mkPM({ io }); const s = mk('num', { userId: 12345 });
    handlers[0](s); await sleep(100);
    expect(await pm.isUserOnline('12345')).toBe(true);
  });

  it('B13 shutdown() removes its subRedis message listener', async () => {
    const sub = redis.duplicate(); extra.push(sub);
    const before = sub.listenerCount('message');
    const pm = mkPM({ subRedis: sub }); await sleep(100);
    await pm.shutdown();
    expect(sub.listenerCount('message')).toBe(before);
  });

  it('B14 passing the same client as redis and subRedis is rejected or safe', async () => {
    const pm = mkPM({ subRedis: redis }); const errs = []; pm.on('error', (e) => errs.push(e));
    await sleep(100);
    let ok = true; try { await pm.track(mk('same'), { userId: 'x' }); } catch { ok = false; }
    console.log('   track ok with shared client:', ok, 'errors:', errs.length);
    expect(ok).toBe(true);
  });
});

describe('C. multi-instance + pub/sub', () => {
  async function trio() {
    const mkSub = () => { const c = redis.duplicate(); extra.push(c); return c; };
    const A = mkPM({ subRedis: mkSub() }), B = mkPM({ subRedis: mkSub() }), C = mkPM({ subRedis: mkSub() });
    await sleep(200);
    return [A, B, C];
  }
  it('C1 events on A are delivered to B and C exactly once', async () => {
    const [A, B, C] = await trio(); const eA = record(A), eB = record(B), eC = record(C);
    const s = mk('m1'); await A.track(s, { userId: 'ann' }); await A.joinRoom(s, 'lobby'); await A.handleDisconnect(s);
    await sleep(300);
    const names = (e) => e.map((x) => x[0]);
    const expected = ['user:online', 'room:join', 'room:leave', 'user:offline'];
    expect(names(eA)).toEqual(expected); expect(names(eB)).toEqual(expected); expect(names(eC)).toEqual(expected);
  });

  it('C2 same user on A and B: only one online and one offline event cluster-wide', async () => {
    const [A, B, C] = await trio(); const eC = record(C);
    const sa = mk('ma'), sb = mk('mb');
    await A.track(sa, { userId: 'bea' }); await B.track(sb, { userId: 'bea' });
    await A.handleDisconnect(sa); await sleep(150);
    expect(await C.isUserOnline('bea')).toBe(true);
    await B.handleDisconnect(sb); await sleep(300);
    expect(eC.filter((e) => e[0] === 'user:online').length).toBe(1);
    expect(eC.filter((e) => e[0] === 'user:offline').length).toBe(1);
  });

  it('C3 crashed node A: exactly one node sweeps, B and C each see offline once', async () => {
    const [A, B, C] = await trio(); const eB = record(B), eC = record(C);
    await A.track(mk('dead'), { userId: 'cat' });
    await A.shutdown();                       // simulate crash: no disconnect
    await sleep(31000 > 0 ? 100 : 0);
    // shorten TTL by rewinding the heartbeat timestamp & socket key
    await redis.del('{audit}:socket:dead');
    const rB = await B.sweepStale(), rC = await C.sweepStale();
    await sleep(300);
    const swept = rB.offlineUsers.length + rC.offlineUsers.length;
    expect(swept).toBe(1);
    expect(eB.filter((e) => e[0] === 'user:offline').length).toBe(1);
    expect(eC.filter((e) => e[0] === 'user:offline').length).toBe(1);
  });

  it('C4 prefixes are isolated', async () => {
    const x = mkPM({ prefix: 'tenantX' }), y = mkPM({ prefix: 'tenantY' });
    await x.track(mk('p1'), { userId: 'same' });
    expect(await y.isUserOnline('same')).toBe(false);
  });
});

describe('D. consistency / stress', () => {
  it('D1 randomized concurrent join/leave/disconnect keeps Redis consistent and leaks no keys', async () => {
    const pm = mkPM(); const ev = record(pm);
    const U = 12, R = 5; const socks = []; const model = new Map();
    for (let u = 0; u < U; u++) for (let k = 0; k < 1 + (u % 3); k++) { const s = mk(`u${u}s${k}`); socks.push([s, 'u' + u]); model.set(s.id, new Set()); }
    await Promise.all(socks.map(([s, u]) => pm.track(s, { userId: u })));
    let rnd = 42; const rand = (n) => { rnd = (rnd * 1664525 + 1013904223) % 4294967296; return rnd % n; };
    for (let batch = 0; batch < 40; batch++) {
      const pool = [...socks].sort(() => rand(3) - 1).slice(0, 10);
      await Promise.all(pool.map(([s]) => {
        const room = 'room' + rand(R); const has = model.get(s.id).has(room);
        if (has) { model.get(s.id).delete(room); return pm.leaveRoom(s, room); }
        model.get(s.id).add(room); return pm.joinRoom(s, room);
      }));
    }
    for (let r = 0; r < R; r++) {
      const want = new Set(socks.filter(([s]) => model.get(s.id).has('room' + r)).map(([, u]) => u));
      const got = new Set(await pm.getRoomUsers('room' + r));
      expect([...got].sort()).toEqual([...want].sort());
    }
    // join/leave events must alternate per (user, room)
    const seqs = {};
    for (const [e, u, r] of ev) if (e.startsWith('room:')) (seqs[u + '|' + r] ??= []).push(e);
    for (const [k, seq] of Object.entries(seqs))
      seq.forEach((e, i) => expect([k, e]).toEqual([k, i % 2 === 0 ? 'room:join' : 'room:leave']));
    await Promise.all(socks.map(([s]) => pm.handleDisconnect(s)));
    expect(await keysLeft()).toEqual([]);
  }, 30000);

  it('D2 onAny heartbeat amplification: Redis EVALs per 500 client events', async () => {
    const pm = mkPM(); const s = mk('busy'); const any = [];
    s.onAny = (f) => any.push(f); s.offAny = () => {};
    await pm.track(s, { userId: 'busy' });
    const before = +(await redis.info('commandstats')).match(/cmdstat_eval:calls=(\d+)/)[1];
    for (let i = 0; i < 500; i++) any.forEach((f) => f('msg', i));
    await sleep(500);
    const after = +(await redis.info('commandstats')).match(/cmdstat_eval:calls=(\d+)/)[1];
    console.log('   EVAL calls for 500 events:', after - before);
    expect(after - before).toBeLessThan(50);       // desirable: throttled
  });

  it('D3 sweep performance with 2000 users (informational)', async () => {
    const st = new RedisStore(redis, 'audit'); const p = redis.pipeline();
    for (let i = 0; i < 2000; i++) await st.addSocketToUser('pu' + i, 'ps' + i, 30);
    const t0 = Date.now(); await st.sweepStale(30, 0); const dt = Date.now() - t0;
    console.log('   sweepStale over 2000 healthy users took ms:', dt);
    expect(dt).toBeLessThan(2000);
  }, 30000);
});
