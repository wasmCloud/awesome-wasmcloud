#!/usr/bin/env node
// Exercises built components against isolated synthetic HTTP/2, HTTP and KV peers.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { copyFileSync, cpSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const GRPC = join(ROOT, 'components/grpc-client');
const CB = join(ROOT, 'host-plugins/component/couchbase');
const KV = join(ROOT, 'host-plugins/component/couchbase-wasi-sockets-p3');
const PROBE = join(ROOT, 'tests/probe');
const HOST_IMAGE = 'ghcr.io/wasmcloud/wash:2.12.1-all-features';
const GRPC_ARTIFACT = 'target/wasm32-wasip2/release/grpc_client.wasm';

function run(command, args, options = {}) {
  return execFileSync(command, args, { encoding: 'utf8', timeout: 180_000, ...options })?.trim() ?? '';
}

function request(port, path, { method = 'POST', body = Buffer.alloc(0), headers = {} } = {}) {
  return new Promise((resolveRequest, reject) => {
    const requestHeaders = { ...headers };
    if (!Object.keys(headers).some(name => ['content-length', 'transfer-encoding'].includes(name.toLowerCase()))) {
      requestHeaders['content-length'] = body.length;
    }
    const outgoing = http.request({ hostname: '127.0.0.1', port, path, method, headers: requestHeaders }, response => {
      const chunks = [];
      let size = 0;
      response.on('data', chunk => {
        size += chunk.length;
        if (size > 5 * 1024 * 1024) response.destroy(new Error('test response exceeds 5 MiB'));
        else chunks.push(chunk);
      });
      response.on('end', () => resolveRequest({ status: response.statusCode, headers: response.headers, body: Buffer.concat(chunks) }));
      response.on('error', reject);
    });
    outgoing.setTimeout(10_000, () => outgoing.destroy(new Error('test HTTP request timed out')));
    outgoing.on('error', reject);
    outgoing.end(body);
  });
}

function rawUpload(port, path, { incomplete = false, trickle = false } = {}) {
  return new Promise((resolveUpload, reject) => {
    const socket = net.connect({ host: '127.0.0.1', port });
    let interval;
    let settled = false;
    const finish = (error, bytes = Buffer.alloc(0)) => {
      if (settled) return;
      settled = true;
      clearInterval(interval);
      socket.destroy();
      if (error) reject(error); else resolveUpload(bytes.toString());
    };
    socket.setTimeout(3000, () => finish(new Error('raw upload did not terminate')));
    socket.on('connect', () => {
      socket.write(`POST ${path} HTTP/1.1\r\nHost: test\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\nabc`);
      if (incomplete) socket.end();
      if (trickle) interval = setInterval(() => socket.write('x'), 20);
    });
    socket.once('data', bytes => finish(null, bytes));
    socket.once('end', () => finish());
    socket.once('error', error => incomplete ? finish() : finish(error));
  });
}

class Fixture {
  constructor(directory, image) {
    this.directory = directory;
    this.image = image;
    this.prefix = `component-check-${randomUUID().slice(0, 10)}`;
    this.network = this.prefix;
    this.containers = new Set();
  }

  async start() {
    run('docker', ['network', 'create', this.network]);
    const server = join(this.directory, 'server');
    mkdirSync(server);
    copyFileSync(join(ROOT, 'tests/fixtures/server.js'), join(server, 'server.js'));
    const openssl = args => run('openssl', args, { cwd: server, stdio: ['ignore', 'pipe', 'pipe'] });
    openssl(['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-keyout', 'ca.key', '-out', 'ca.crt', '-subj', '/CN=component-check-ca']);
    openssl(['req', '-new', '-newkey', 'rsa:2048', '-nodes', '-keyout', 'server.key', '-out', 'server.csr', '-subj', '/CN=backend']);
    writeFileSync(join(server, 'server.ext'), 'subjectAltName=DNS:backend\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n');
    openssl(['x509', '-req', '-in', 'server.csr', '-CA', 'ca.crt', '-CAkey', 'ca.key', '-CAcreateserial', '-out', 'server.crt', '-days', '1', '-extfile', 'server.ext']);
    const name = `${this.prefix}-backend`;
    this.containers.add(name);
    run('docker', ['run', '-d', '--name', name, '--network', this.network, '--network-alias', 'backend',
      '--network-alias', 'wrong-backend', '-p', '127.0.0.1::18099', '-v', `${server}:/fixture:ro`,
      'node:22-alpine', 'node', '/fixture/server.js']);
    this.monitor = this.port(name, '18099/tcp');
    for (let attempt = 0; attempt < 50; attempt++) {
      try { await this.requests(); return; } catch { await delay(100); }
    }
    throw new Error('synthetic backend did not start');
  }

  port(name, internal) {
    const inspected = JSON.parse(run('docker', ['inspect', name]));
    const ports = Array.isArray(inspected) ? inspected[0]?.NetworkSettings?.Ports?.[internal] : null;
    const port = Array.isArray(ports) ? Number(ports[0]?.HostPort) : NaN;
    if (!Number.isInteger(port) || port <= 0 || port > 65535) throw new Error(`missing published port ${internal}`);
    return port;
  }

  async requests() {
    const response = await request(this.monitor, '/', { method: 'GET' });
    const entries = JSON.parse(response.body.toString());
    if (!Array.isArray(entries) || entries.some(entry => !entry || typeof entry !== 'object')) {
      throw new Error('invalid synthetic peer request log');
    }
    return entries;
  }

  async host(project, artifact, options, check) {
    const { dev = {}, workload, extraFiles = {}, readyStatus = 200 } = options;
    const stage = join(this.directory, randomUUID());
    mkdirSync(join(stage, '.wash'), { recursive: true });
    copyFileSync(join(project, artifact), join(stage, 'component.wasm'));
    for (const [name, source] of Object.entries(extraFiles)) copyFileSync(source, join(stage, name));
    copyFileSync(join(project, 'Cargo.toml'), join(stage, 'Cargo.toml'));
    cpSync(join(project, 'wit'), join(stage, 'wit'), { recursive: true });
    copyFileSync(join(this.directory, 'server/ca.crt'), join(stage, 'ca.crt'));
    const config = { version: '2.0.0', build: { command: '/bin/true', component_path: 'component.wasm' },
      dev: { http_client_ca_paths: ['/fixture/ca.crt'], ...dev } };
    if (workload) config.workload = workload;
    writeFileSync(join(stage, '.wash/config.yaml'), JSON.stringify(config));
    const name = `${this.prefix}-${randomUUID().slice(0, 8)}`;
    this.containers.add(name);
    try {
      // Mount only staged public components, WIT and synthetic certificates.
      run('docker', ['run', '-d', '--name', name, '--network', this.network, '-p', '127.0.0.1::8000',
        '-v', `${stage}:/fixture`, '-w', '/fixture', this.image, 'dev', '--non-interactive']);
      const port = this.port(name, '8000/tcp');
      let ready = false;
      for (let attempt = 0; attempt < 100; attempt++) {
        try { ready = (await request(port, '/review.Service/Ready')).status === readyStatus; } catch { /* Starting up. */ }
        if (ready) break;
        await delay(100);
      }
      if (!ready) throw new Error('component did not deploy');
      return await check(port);
    } catch (error) {
      try { console.error(run('docker', ['logs', name])); } catch { /* Container startup may have failed. */ }
      throw error;
    } finally {
      this.remove(name);
    }
  }

  remove(name) {
    try { run('docker', ['rm', '-f', name], { timeout: 30_000 }); } catch { /* Best-effort cleanup. */ }
    this.containers.delete(name);
  }

  close() {
    for (const name of this.containers) this.remove(name);
    try { run('docker', ['network', 'rm', this.network], { timeout: 30_000 }); } catch { /* Best-effort cleanup. */ }
  }
}

async function verifyGrpc(fixture, secure) {
  const authority = secure ? 'backend:18443' : 'backend:18098';
  const environment = { GRPC_AUTHORITY: authority, GRPC_TIMEOUT_MS: '100', ...(!secure ? { GRPC_PLAINTEXT: '1' } : {}) };
  const workload = { allowedHosts: [authority], environment: { config: environment } };
  let count = 0;
  await fixture.host(GRPC, GRPC_ARTIFACT, { workload }, async port => {
    for (const [mode, expected, frames] of [['Ok', Buffer.from([10, 2, 111, 107]), '1'], ['Empty', Buffer.alloc(0), '1'], ['Stream', Buffer.from('one'), '2']]) {
      const response = await request(port, `/review.Service/${mode}`);
      assert.deepEqual([response.status, response.headers['grpc-status'], response.headers['grpc-frames'], response.body], [200, '0', frames, expected]);
      count++;
    }
    const rpcError = await request(port, '/review.Service/RpcError');
    assert.equal(rpcError.status, 200);
    assert.equal(rpcError.headers['grpc-status'], '5');
    count++;
    for (const mode of ['Malformed', 'ShortHeader', 'Overflow', 'Compressed', 'InvalidFlag', 'TrailingGarbage',
      'MissingStatus', 'InvalidStatus', 'ConflictingStatus', 'WrongContentType', 'HttpError', 'Slow']) {
      const start = performance.now();
      const response = await request(port, `/review.Service/${mode}`);
      assert.equal(response.status, 502, `${mode}: ${response.body}`);
      if (mode === 'Slow') assert(response.body.includes('deadline') && performance.now() - start < 1000);
      count++;
    }
    let before = (await fixture.requests()).length;
    for (const method of ['GET', 'HEAD', 'DELETE', 'OPTIONS']) {
      const response = await request(port, '/review.Service/DeleteAll', { method });
      assert.equal(response.status, 405);
      assert.equal(response.headers.allow, 'POST');
      count++;
    }
    for (const path of ['/missing', '//Method', '/Service/', '/Service/Method?x=1']) {
      assert.equal((await request(port, path)).status, 400);
      count++;
    }
    assert.equal((await fixture.requests()).length, before, 'rejected requests reached the RPC service');
    await request(port, '/review.Service/Auth', { headers: { authorization: 'Bearer synthetic' } });
    const last = (await fixture.requests()).at(-1);
    assert.equal(last.authorization, 'Bearer synthetic');
    assert(last.timeout);
    count++;
    before = (await fixture.requests()).length;
    await rawUpload(port, '/review.Service/Incomplete', { incomplete: true });
    await delay(100);
    assert.equal((await fixture.requests()).length, before, 'incomplete input invoked the RPC');
    count++;
    for (const trickle of [false, true]) {
      const start = performance.now();
      const response = await rawUpload(port, '/review.Service/StalledUpload', { trickle });
      assert.match(response, /^HTTP\/1\.1 408 /);
      assert(performance.now() - start < 1000);
      assert.equal((await fixture.requests()).length, before, 'timed-out upload invoked the RPC');
      count++;
    }
  });
  if (!secure) {
    for (const invalid of ['not-a-number', '0']) {
      const invalidWorkload = { ...workload, environment: { config: { ...environment, GRPC_TIMEOUT_MS: invalid } } };
      const before = (await fixture.requests()).length;
      await fixture.host(GRPC, GRPC_ARTIFACT, { workload: invalidWorkload, readyStatus: 500 }, async port => {
        const response = await request(port, '/review.Service/Ok');
        assert.equal(response.status, 500);
        assert(response.body.includes('GRPC_TIMEOUT_MS'));
        assert.equal((await fixture.requests()).length, before);
      });
      count++;
    }
  }
  const failures = secure ? [[workload, { http_client_ca_paths: [] }],
    [{ allowedHosts: ['wrong-backend:18443'], environment: { config: { ...environment, GRPC_AUTHORITY: 'wrong-backend:18443' } } }, {}]] :
    [[{ allowedHosts: ['elsewhere.invalid:1'], environment: workload.environment }, {}]];
  for (const [rejected, dev] of failures) {
    const configured = { ...rejected, environment: { config: { ...rejected.environment.config, GRPC_TIMEOUT_MS: '1000' } } };
    const before = (await fixture.requests()).length;
    await fixture.host(GRPC, GRPC_ARTIFACT, { workload: configured, dev, readyStatus: 502 }, async port => {
      const response = await request(port, '/review.Service/Ok');
      assert.equal(response.status, 502);
      assert(response.body.includes('transport error'));
      assert.equal((await fixture.requests()).length, before);
    });
    count++;
  }
  const boundedWorkload = { ...workload, environment: { config: { ...environment, GRPC_TIMEOUT_MS: '5000' } } };
  await fixture.host(GRPC, GRPC_ARTIFACT, { workload: boundedWorkload }, async port => {
    for (const chunked of [false, true]) {
      const before = (await fixture.requests()).length;
      const response = await request(port, '/review.Service/OversizedUpload', {
        body: Buffer.alloc(4 * 1024 * 1024 + 1), headers: chunked ? { 'transfer-encoding': 'chunked' } : {},
      });
      assert.equal(response.status, 413, response.body.toString());
      assert.equal((await fixture.requests()).length, before, 'oversized input invoked the RPC');
      count++;
    }
    for (const [mode, message] of [['HugeFrame', '4 MiB'], ['HugeBody', '32 MiB'], ['FrameFlood', '1024-frame']]) {
      const response = await request(port, `/review.Service/${mode}`);
      assert.equal(response.status, 502);
      assert(response.body.includes(message), response.body.toString());
      assert.equal((await request(port, '/review.Service/Ok')).status, 200, 'limits poisoned the instance');
      count++;
    }
    const exact = await request(port, '/review.Service/Ok', { body: Buffer.alloc(4 * 1024 * 1024) });
    assert.equal(exact.status, 200, exact.body.toString());
    count++;
  });
  console.log(`gRPC ${secure ? 'TLS' : 'plaintext'}: ${count} checks passed`);
  return count;
}

async function verifyCouchbase(fixture, sockets) {
  const plugin = sockets ? 'couchbase-wasi-sockets-p3' : 'couchbase';
  const filename = sockets ? 'couchbase_wasi_sockets_p3_plugin.wasm' : 'couchbase_plugin.wasm';
  const stage = join(fixture.directory, `${plugin}.wasm`);
  copyFileSync(join(sockets ? KV : CB, 'target/wasm32-wasip2/release', filename), stage);
  const dev = { host_plugins: [{ id: plugin, file: `/fixture/${plugin}.wasm`,
    allowedHosts: ['backend:11210', 'backend:18009'], allowedIpNameLookups: ['*'] }],
  host_interfaces: [{ namespace: 'wasmcloud', package: 'couchbase', version: '0.2.0',
    interfaces: ['types', 'sqlpp-types', 'document', 'sqlpp'], config: {
      endpoint: sockets ? 'couchbase://backend' : 'http://backend:18009', 'query-port': '18009',
      bucket: 'test.bucket', username: 'synthetic', password: 'synthetic', 'timeout-ms': '100',
    } }] };
  const options = { dev, extraFiles: { [`${plugin}.wasm`]: stage } };
  const artifact = 'target/wasm32-wasip2/release/component_fault_probe.wasm';
  const cases = sockets ? [['expiry-race', 'cas=222', '"version":2'], ['expiry-churn', '::Timeout'],
    ['expiry-error', '::Unauthorized'], ['expiry-none', 'expiry=None'], ['expiry-short', '::RequestFailed'],
    ['query-error', '::Server'], ...['kv-bad-header', 'kv-huge-header', 'kv-wrong-opcode', 'kv-wrong-opaque'].map(path => [path, '::RequestFailed']),
    ['oversized-id', '::InvalidArgument'], ['empty-id', '::InvalidArgument']] :
    [['complete', 'document=abcdefghij'], ['truncated', '::RequestFailed'], ['timeout', '::Timeout'], ['slow', '::Timeout']];
  cases.push(['query-context', 'default:`test.bucket`.`_default`'], ['query-timeout', '::Timeout'], ['query-timeout-http', '::Timeout']);
  cases.push(...['insert', 'upsert', 'replace', 'touch', 'get-touch'].map(operation => [`large-${operation}-ttl`, '::InvalidArgument']));
  if (!sockets) cases.push(...['dot-id', 'dotdot-id', 'dot-lock-id', 'dot-remove-id'].map(path => [path, '::Unsupported']));
  await fixture.host(PROBE, artifact, options, async port => {
    for (const [path, expected, extra] of cases) {
      const before = (await fixture.requests()).length;
      const response = await request(port, `/${path}`, { method: 'GET' });
      const text = response.body.toString();
      assert(text.includes(expected) && (!extra || text.includes(extra)), `${path}: ${text}`);
      if (/^(large-|dot-|dotdot-)/.test(path)) assert.equal((await fixture.requests()).length, before, `${path} reached the service`);
    }
    assert(!(await fixture.requests()).some(entry => entry.kind === 'kv-write'));
  });
  dev.host_interfaces[0].config['timeout-ms'] = '5000';
  const limits = sockets ? ['query-oversized'] : ['oversized', 'query-oversized'];
  await fixture.host(PROBE, artifact, options, async port => {
    for (const path of limits) {
      const response = await request(port, `/${path}`, { method: 'GET' });
      assert(response.body.includes('32 MiB'), `${path}: ${response.body}`);
      assert(!response.body.includes('QUERY OK') && !response.body.toString().startsWith('OK '));
    }
    const healthy = await request(port, '/query-context', { method: 'GET' });
    assert(healthy.body.includes('QUERY OK'), healthy.body.toString());
  });
  const count = cases.length + limits.length;
  console.log(`${plugin}: ${count} checks passed`);
  return count;
}

async function main() {
  const { values } = parseArgs({ options: { 'skip-build': { type: 'boolean', default: false },
    'host-image': { type: 'string', default: HOST_IMAGE }, help: { type: 'boolean', default: false } } });
  if (values.help) {
    console.log('node tests/verify-components.mjs [--skip-build] [--host-image <image>]');
    return;
  }
  if (!values['skip-build']) {
    for (const project of [GRPC, CB, KV, PROBE]) run('wash', ['build'], { cwd: project, stdio: 'inherit' });
  }
  const directory = mkdtempSync(join(tmpdir(), 'component-check-'));
  const fixture = new Fixture(directory, values['host-image']);
  try {
    await fixture.start();
    const count = await verifyGrpc(fixture, false) + await verifyGrpc(fixture, true) +
      await verifyCouchbase(fixture, false) + await verifyCouchbase(fixture, true);
    console.log(`PASS: ${count} live protocol checks`);
  } finally {
    fixture.close();
    rmSync(directory, { recursive: true, force: true });
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
