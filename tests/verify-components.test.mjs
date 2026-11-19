import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { buildComponents, rawUpload } from './verify-components.mjs';

async function peer(context, accept) {
  const sockets = new Set();
  const server = net.createServer(socket => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    socket.on('data', () => {});
    accept(socket);
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  context.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
  });
  return server.address().port;
}

test('component builds replace stale local artifacts despite an inherited shared target', context => {
  const directory = mkdtempSync(join(tmpdir(), 'component-build-test-'));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const bin = join(directory, 'bin');
  mkdirSync(bin);
  // Simulate Cargo's output-directory selection, retaining an older local file.
  writeFileSync(join(bin, 'wash'), `#!/usr/bin/env node
const { mkdirSync, readFileSync, writeFileSync } = require('node:fs');
const { join } = require('node:path');
const target = process.env.CARGO_TARGET_DIR || join(process.cwd(), 'target');
mkdirSync(target, { recursive: true });
writeFileSync(join(target, 'component.wasm'), readFileSync('source.txt'));
writeFileSync('offline.txt', process.env.CARGO_NET_OFFLINE || '');
`, { mode: 0o755 });
  const projects = ['one', 'two'].map(name => {
    const project = join(directory, name);
    mkdirSync(join(project, 'target'), { recursive: true });
    writeFileSync(join(project, 'target/component.wasm'), 'stale');
    writeFileSync(join(project, 'source.txt'), `current-${name}`);
    return project;
  });
  const shared = join(directory, 'shared-target');
  const environment = { ...process.env, PATH: `${bin}:${process.env.PATH}`, CARGO_TARGET_DIR: shared, CARGO_NET_OFFLINE: 'true' };
  buildComponents(projects, environment);
  for (const [index, project] of projects.entries()) {
    assert.equal(readFileSync(join(project, 'target/component.wasm'), 'utf8'), `current-${['one', 'two'][index]}`);
    assert.equal(readFileSync(join(project, 'offline.txt'), 'utf8'), 'true');
  }
  assert(!existsSync(shared), 'build output escaped the local target directories');
  assert.equal(environment.CARGO_TARGET_DIR, shared, 'caller environment was mutated');
});

test('Cargo config cannot redirect component artifacts away from local targets', context => {
  const directory = mkdtempSync(join(tmpdir(), 'component-cargo-config-test-'));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const bin = join(directory, 'bin');
  mkdirSync(bin);
  // Use real Cargo configuration resolution with dependency-free native crates.
  writeFileSync(join(bin, 'wash'), `#!/usr/bin/env node
require('node:child_process').execFileSync('cargo', ['build', '--quiet', '--offline'], { stdio: 'inherit' });
`, { mode: 0o755 });
  const configured = join(directory, 'configured-target');
  const projects = ['plugin', 'caller'].map(name => {
    const project = join(directory, name);
    for (const child of ['.cargo', 'src', 'target/debug']) mkdirSync(join(project, child), { recursive: true });
    writeFileSync(join(project, 'Cargo.toml'), '[package]\nname = "artifact_probe"\nversion = "0.1.0"\nedition = "2021"\n[workspace]\n');
    writeFileSync(join(project, '.cargo/config.toml'), `[build]\ntarget-dir = ${JSON.stringify(configured)}\n`);
    writeFileSync(join(project, 'src/main.rs'), `fn main() { std::hint::black_box(b"current-${name}"); }\n`);
    writeFileSync(join(project, 'target/debug/artifact_probe'), 'stale');
    return project;
  });
  const environment = { ...process.env, PATH: `${bin}:${process.env.PATH}` };
  delete environment.CARGO_TARGET_DIR;
  buildComponents(projects, environment);
  for (const [index, project] of projects.entries()) {
    const artifact = readFileSync(join(project, 'target/debug/artifact_probe'));
    assert(artifact.includes(`current-${['plugin', 'caller'][index]}`), 'local artifact stayed stale');
  }
  assert(!existsSync(configured), 'Cargo config redirected the build output');
  assert.equal(environment.CARGO_TARGET_DIR, undefined, 'caller environment was mutated');
});

test('raw uploads read a fragmented status line including a split CRLF', async context => {
  const port = await peer(context, socket => {
    socket.write('HTTP/1.1');
    const middle = setTimeout(() => socket.write(' 408 Request Timeout\r'), 20);
    const ending = setTimeout(() => socket.end('\nContent-Length: 0\r\n\r\n'), 40);
    socket.on('close', () => { clearTimeout(middle); clearTimeout(ending); });
  });
  assert.equal(await rawUpload(port, '/test.Service/Upload'), 'HTTP/1.1 408 Request Timeout\r\n');
});

test('raw uploads bound an unterminated HTTP status line', async context => {
  const port = await peer(context, socket => socket.write(Buffer.alloc(9000, 'x')));
  await assert.rejects(rawUpload(port, '/test.Service/Upload'), /status line exceeds 8 KiB/);
});

test('raw uploads reject EOF in the middle of the status line', async context => {
  const port = await peer(context, socket => socket.end('HTTP/1.1 4'));
  await assert.rejects(rawUpload(port, '/test.Service/Upload'), /before a complete HTTP status line/);
});

test('incomplete uploads tolerate a peer closing without a response', async context => {
  const port = await peer(context, socket => socket.end());
  assert.equal(await rawUpload(port, '/test.Service/Upload', { incomplete: true }), '');
});

test('a trickling upload cannot extend the raw probe deadline', { timeout: 6000 }, async context => {
  const port = await peer(context, socket => {
    const guard = setTimeout(() => socket.end(), 4500);
    socket.on('close', () => clearTimeout(guard));
  });
  await assert.rejects(rawUpload(port, '/test.Service/Upload', { trickle: true }), /raw upload did not terminate/);
});
