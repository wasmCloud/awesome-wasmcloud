import assert from 'node:assert/strict';
import { test } from 'node:test';
import { artifacts, releaseTag, versionOf, witVersion } from './lib/couchbase-release.mjs';
import { classify, probe, sha256 } from './publish-couchbase-release.mjs';

const sha = 'a'.repeat(40);
const digest = sha256('wasm');
const manifest = { layers: [{ digest }] };

test('release tags match each component version; WIT has its own version', () => {
  for (const id of Object.keys(artifacts)) {
    const version = versionOf(id);
    assert.equal(releaseTag(id, version, `refs/tags/couchbase-v${versionOf('http')}`, sha), version);
  }
  assert.equal(releaseTag('http', '0.1.0', 'refs/heads/main', sha), `sha-${sha}`);
  assert.equal(releaseTag('wit', '0.3.0', 'refs/heads/main', sha), '0.3.0');
  assert.throws(() => releaseTag('tls', '0.1.0', 'refs/tags/couchbase-v9.0.0', sha));
  assert.throws(() => releaseTag('http', '0.1.0', 'refs/heads/feature', sha));
  assert.throws(() => releaseTag('http', '0.1.0', 'refs/heads/main', 'bad-sha'));
  assert.throws(() => releaseTag('missing', '0.1.0', 'refs/heads/main', sha));
  assert.throws(() => releaseTag('wit', '0.3.0', 'refs/tags/other-v0.1.0', sha));
  assert.throws(() => releaseTag('wit', '0.3.0', 'refs/tags/couchbase-vbad', sha));
});

test('WIT identity and version must be valid', () => {
  assert.equal(witVersion('package wasmcloud:couchbase@0.3.0;'), '0.3.0');
  for (const wit of ['package other:couchbase@0.3.0;', 'package wasmcloud:couchbase@latest;', 'package wasmcloud:couchbase@01.3.0;']) {
    assert.throws(() => witVersion(wit));
  }
});

test('stable publication only creates absent tags or reuses identical artifacts', () => {
  assert.equal(classify(404), 'push');
  assert.equal(classify(200, manifest, digest), 'attest');
  assert.equal(classify(200, manifest, sha256('other')), 'different');
  for (const status of [401, 403, 429, 500, 503]) assert.throws(() => classify(status));
  for (const bad of [{}, { layers: [] }, { layers: [{ digest: 'invalid' }] }, { layers: [manifest.layers[0], manifest.layers[0]] }]) {
    assert.throws(() => classify(200, bad, digest));
  }
});

test('anonymous verification sends no credentials and hashes the actual manifest', async () => {
  const requests = [];
  const raw = JSON.stringify(manifest);
  const request = async (url, options) => {
    requests.push({ url, ...options });
    return requests.length === 1 ? new Response('{"token":"anonymous-token"}') : new Response(raw);
  };
  const result = await probe(artifacts.wit.repository, '0.3.0', { anonymous: true, request });
  assert.equal(result.digest, sha256(raw));
  assert.equal(requests[0].headers.Authorization, undefined);
  assert.equal(requests[1].headers.Authorization, 'Bearer anonymous-token');
  assert.equal(requests[0].redirect, 'error');
  assert.ok(requests[0].signal);
});

test('failed token requests never become permission to publish', async () => {
  for (const status of [401, 403, 404, 503]) {
    await assert.rejects(probe(artifacts.wit.repository, '0.3.0', { anonymous: true,
      request: async () => new Response('', { status }) }), /token request failed/);
  }
  await assert.rejects(probe('../bad', '0.3.0', { anonymous: true }), /Invalid registry/);
  await assert.rejects(probe(artifacts.wit.repository, 'tag\n', { anonymous: true }), /Invalid registry/);
});
