import { copyFileSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { artifact, run, source, versionOf, witVersion } from './lib/couchbase-release.mjs';

async function main([id, ...extra]) {
  if (!id || extra.length) throw new Error('Usage: build-couchbase-release.mjs <wit|http|sockets|tls>');
  const item = artifact(id);
  const version = versionOf(id);
  const revision = process.env.GITHUB_SHA ?? (await run('git', ['rev-parse', 'HEAD'])).stdout.trim();
  if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error('Invalid source revision');
  mkdirSync('dist', { recursive: true });
  const staged = mkdtempSync(join(tmpdir(), 'couchbase-release-'));
  const raw = join(staged, 'raw.wasm');
  const result = resolve('dist', `${id}.wasm`);
  try {
    if (id === 'wit') {
      // Publish the authoritative contract without committing a second copy.
      mkdirSync(join(staged, 'wit'));
      copyFileSync(`${item.directory}/couchbase.wit`, join(staged, 'wit', 'couchbase.wit'));
      await run('wash', ['wit', 'build', '--output-file', raw], { cwd: staged });
    } else {
      const cwd = resolve(item.directory);
      await run('wash', ['wit', 'fetch'], { cwd, timeout: 180_000 });
      const target = resolve(cwd, 'target', item.feature ?? 'release-build');
      const args = ['build', '--locked', '--target', 'wasm32-wasip2', '--release', '--target-dir', target];
      if (item.feature) args.push('--features', item.feature);
      await run('cargo', args, { cwd, timeout: 600_000, maxBuffer: 8 << 20 });
      copyFileSync(join(target, 'wasm32-wasip2', 'release', `${item.binary}.wasm`), raw);
    }
    await run('wasm-tools', ['metadata', 'add', raw, '--output', result,
      '--source', source, '--homepage', `${source}/tree/main/${item.directory}`,
      '--revision', revision, '--version', version, '--licenses', 'Apache-2.0', '--authors', 'wasmCloud Maintainers']);
    await run('wasm-tools', ['validate', '--features', 'all', result]);
    const { stdout: wit } = await run('wasm-tools', ['component', 'wit', result], { maxBuffer: 8 << 20 });
    if (id === 'wit') {
      if (witVersion(wit) !== version) throw new Error('Built WIT version changed');
    } else {
      const contractVersion = versionOf('wit');
      for (const name of ['types', 'sqlpp-types', 'document-read', 'document-write', 'sqlpp']) {
        if (!wit.includes(`export wasmcloud:couchbase/${name}@${contractVersion};`)) throw new Error(`Missing Couchbase ${name} export`);
      }
      if (!wit.includes('export wasmcloud:host/workload-lifecycle@0.1.6;')) throw new Error('Missing host lifecycle export');
      const hasTls = wit.includes('import wasi:tls/client@0.3.0-draft;');
      if (hasTls !== (id === 'tls')) throw new Error('TLS imports do not match the published variant');
    }
    console.log(`Built ${result} (${version})`);
  } finally {
    rmSync(staged, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
