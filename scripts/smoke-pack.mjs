import { execSync } from 'child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join, resolve } from 'path';

const projectRoot = resolve(process.cwd());

console.log('[Smoke Test] Packing tarball...');
const packOutput = execSync('npm pack', { cwd: projectRoot, encoding: 'utf-8' }).trim();
const lines = packOutput.split('\n');
const tarballName = lines[lines.length - 1].trim();
const tarballPath = join(projectRoot, tarballName);

const tempDir = mkdtempSync(join(tmpdir(), 'presence-io-smoke-'));

try {
  console.log(`[Smoke Test] Created temp directory: ${tempDir}`);

  // Create temporary package.json with "type": "module" for ESM test
  writeFileSync(
    join(tempDir, 'package.json'),
    JSON.stringify({
      name: 'smoke-test',
      version: '1.0.0',
      dependencies: {
        'presence-io': `file:${tarballPath}`,
        'ioredis': '^5.4.1',
        'socket.io': '^4.7.5',
      },
    }),
  );

  console.log('[Smoke Test] Installing packed tarball...');
  execSync('npm install --no-audit --no-fund', { cwd: tempDir, stdio: 'inherit' });

  // Test CJS require
  console.log('[Smoke Test] Testing CommonJS require()...');
  const cjsTestScript = `
    const { PresenceManager, RedisStore } = require('presence-io');
    if (typeof PresenceManager !== 'function') throw new Error('PresenceManager class not exported in CJS');
    if (typeof RedisStore !== 'function') throw new Error('RedisStore class not exported in CJS');
    console.log('  CJS require OK');
  `;
  writeFileSync(join(tempDir, 'test-cjs.cjs'), cjsTestScript);
  execSync('node test-cjs.cjs', { cwd: tempDir, stdio: 'inherit' });

  // Test ESM import
  console.log('[Smoke Test] Testing ESM import()...');
  const esmTestScript = `
    import { PresenceManager, RedisStore } from 'presence-io';
    if (typeof PresenceManager !== 'function') throw new Error('PresenceManager class not exported in ESM');
    if (typeof RedisStore !== 'function') throw new Error('RedisStore class not exported in ESM');
    console.log('  ESM import OK');
  `;
  writeFileSync(join(tempDir, 'test-esm.mjs'), esmTestScript);
  execSync('node test-esm.mjs', { cwd: tempDir, stdio: 'inherit' });

  console.log('[Smoke Test] SUCCESS! Tarball imports cleanly in both CJS and ESM.');
} finally {
  rmSync(tempDir, { recursive: true, force: true });
  rmSync(tarballPath, { force: true });
}
