#!/usr/bin/env node
// Builds the TLS example and exercises real component-plugin stores.
import { execFileSync } from 'node:child_process';
import { copyFile, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { buildComponents } from '../../../../tests/component-build.mjs';
import { startCouchbaseTlsFixture } from '../../couchbase-wasi-sockets-p3/verification/fixture.mjs';

async function main() {
  const { values } = parseArgs({ options: {
    'runtime-source': { type: 'string' },
    'skip-build': { type: 'boolean', default: false },
    offline: { type: 'boolean', default: false },
    'target-dir': { type: 'string' },
    couchbase: { type: 'boolean', default: false },
    help: { type: 'boolean', default: false },
  } });
  if (values.help) {
    console.log('node verification/run.mjs --runtime-source <wasmCloud checkout> [--couchbase] [--skip-build] [--offline] [--target-dir <cache>]');
    return;
  }
  if (!values['runtime-source']) throw new Error('--runtime-source is required');
  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
  const runtime = resolve(values['runtime-source'], 'crates/wash-runtime');
  if (!(await stat(join(runtime, 'Cargo.toml')).catch(() => null))?.isFile()) {
    throw new Error('--runtime-source must point to a wasmCloud checkout');
  }
  const environment = { ...process.env, TLS_EXAMPLE_ROOT: root };
  if (values.offline) environment.CARGO_NET_OFFLINE = 'true';
  if (values['target-dir']) environment.CARGO_TARGET_DIR = resolve(values['target-dir']);

  // Cargo resolves workspace-inherited manifest values; no separate TOML parser.
  const metadata = JSON.parse(execFileSync('cargo', ['metadata', '--no-deps', '--locked',
    '--format-version', '1', '--manifest-path', join(runtime, 'Cargo.toml')],
  { env: environment, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 }));
  const packages = Array.isArray(metadata.packages) ? metadata.packages : [];
  const runtimePackage = packages.find(pkg => pkg?.name === 'wash-runtime');
  const dependencies = Array.isArray(runtimePackage?.dependencies) ? runtimePackage.dependencies : [];
  if (!dependencies.some(dep => dep?.name === 'wasmtime-wasi-tls' && dep.req === '^48.0.9')) {
    throw new Error("the verifier requires the runtime's Wasmtime 48.0.9 dependency range; use the README's runtime revision");
  }

  if (!values['skip-build']) {
    buildComponents([root, join(root, 'verification/caller')], environment);
    if (values.couchbase) {
      const couchbase = resolve(root, '../couchbase-wasi-sockets-p3');
      buildComponents([resolve(root, '../couchbase/verification/scenario')], environment);
      execFileSync('wash', ['wit', 'fetch'], { cwd: couchbase, env: environment, stdio: 'inherit' });
      execFileSync('cargo', ['build', '--locked', '--target', 'wasm32-wasip2', '--release', '--features', 'tls',
        '--target-dir', join(couchbase, 'target/tls')], {
        cwd: couchbase, env: environment, stdio: 'inherit',
      });
    }
  }
  const harness = await mkdtemp(join(tmpdir(), 'tls-client-verification-'));
  let couchbaseFixture;
  try {
    if (values.couchbase) {
      couchbaseFixture = await startCouchbaseTlsFixture();
      Object.assign(environment, couchbaseFixture.environment, {
        COUCHBASE_TLS_ROOT: resolve(root, '../couchbase-wasi-sockets-p3'),
      });
    }
    await mkdir(join(harness, 'src'));
    const manifest = await readFile(join(root, 'verification/Cargo.toml.template'), 'utf8');
    await writeFile(join(harness, 'Cargo.toml'), manifest.replace('__RUNTIME_PATH__', JSON.stringify(runtime)));
    await copyFile(join(root, 'verification/Cargo.lock'), join(harness, 'Cargo.lock'));
    await copyFile(join(root, 'verification/harness.rs'), join(harness, 'src/lib.rs'));
    await copyFile(join(root, 'verification/tls.rs'), join(harness, 'src/tls.rs'));
    await copyFile(resolve(root, '../couchbase-wasi-sockets-p3/verification/tls.rs'), join(harness, 'src/couchbase.rs'));
    execFileSync('cargo', ['test', '--locked', '--manifest-path', join(harness, 'Cargo.toml'),
      '--', '--nocapture', '--test-threads=1', ...(values.couchbase ? ['--include-ignored'] : [])],
    { env: environment, stdio: 'inherit' });
  } finally {
    try { await couchbaseFixture?.close(); }
    finally { await rm(harness, { recursive: true, force: true }); }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    await main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
