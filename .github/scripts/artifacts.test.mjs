import assert from 'node:assert/strict';
import { test } from 'node:test';
import { artifact, loadCatalog, releaseTag, repositoryOf, selectArtifacts, validateCatalog, witVersion } from './lib/artifacts.mjs';
import { classify, consumerConfig, probe, sha256 } from './publish-artifact.mjs';
import { validateWorld } from './build-artifact.mjs';
import { matrices } from './plan-artifacts.mjs';
import { verificationArgs } from './verify-attestation.mjs';

const sha = 'a'.repeat(40);
const digest = sha256('wasm');
const manifest = { layers: [{ digest }] };
const catalog = [
  { id: 'weather-wit', kind: 'wit', releaseGroup: 'weather', directory: 'wit/weather', package: 'acme:weather', smokeImports: ['forecast'], license: 'Apache-2.0' },
  { id: 'weather', kind: 'component', releaseGroup: 'weather', directory: 'components/weather', binary: 'weather_plugin', contracts: { 'weather-wit': ['forecast'], 'queue-wit': ['messages'] }, license: 'MIT' },
  { id: 'weather-tls', kind: 'component', releaseGroup: 'weather', directory: 'components/weather', binary: 'weather_plugin', features: ['tls'], imports: ['wasi:tls/client@0.3.0-draft'], license: 'MIT' },
  { id: 'queue-wit', kind: 'wit', releaseGroup: 'queue', directory: 'wit/queue', package: 'other:queue', smokeImports: ['messages'], license: 'Apache-2.0' },
  { id: 'queue', kind: 'component', releaseGroup: 'queue', directory: 'components/queue', binary: 'queue', license: 'MIT' },
  { id: 'types-wit', kind: 'wit', releaseGroup: 'types', directory: 'wit/types', package: 'acme:types', smokeImports: ['types'], license: 'Apache-2.0' },
];
const copy = () => structuredClone(catalog);

test('catalog supports unrelated packages, components and variants without workflow changes', () => {
  assert.equal(validateCatalog(copy()).length, catalog.length);
  assert.ok(loadCatalog().length);
  assert.equal(repositoryOf(catalog[0]), 'wasmcloud/interfaces/acme/weather');
  assert.equal(repositoryOf(catalog[1]), 'wasmcloud/components/weather');
  assert.equal(artifact('weather', catalog).binary, 'weather_plugin');
  assert.throws(() => artifact('absent', catalog));
  assert.equal(JSON.parse(matrices(catalog, 'refs/pull/3/merge').matrix).include.length, catalog.length);
});

test('release groups select their artifacts and shared WIT, excluding unrelated components', () => {
  assert.deepEqual(selectArtifacts(catalog, 'refs/tags/weather-v0.8.0').map(e => e.id), ['weather-wit', 'weather', 'weather-tls', 'queue-wit']);
  assert.deepEqual(selectArtifacts(catalog, 'refs/tags/unregistered-v1.0.0'), []);
  assert.equal(matrices(catalog, 'refs/tags/unregistered-v1.0.0').has_artifacts, 'false');
  const types = matrices(catalog, 'refs/tags/types-v2.0.0');
  assert.equal(types.has_wit, 'true');
  assert.equal(types.has_components, 'false');
  assert.deepEqual(JSON.parse(types.components).include, []);
  const queue = matrices(catalog.filter(e => e.id === 'queue'), 'refs/tags/queue-v1.0.0');
  assert.equal(queue.has_wit, 'false');
  assert.equal(queue.has_components, 'true');
  for (const ref of ['refs/tags/weather-vbad', 'refs/tags/weather-v01.0.0', 'refs/tags/other']) assert.throws(() => selectArtifacts(catalog, ref));
});

test('component releases match Cargo versions while WIT retains its own version', () => {
  for (const id of ['weather', 'weather-tls']) {
    assert.equal(releaseTag(artifact(id, catalog), '0.8.0', 'refs/tags/weather-v0.8.0', sha, catalog), '0.8.0');
    assert.throws(() => releaseTag(artifact(id, catalog), '0.9.0', 'refs/tags/weather-v0.8.0', sha, catalog));
  }
  assert.equal(releaseTag(catalog[0], '2.5.0', 'refs/tags/weather-v0.8.0', sha, catalog), '2.5.0');
  assert.equal(releaseTag(catalog[3], '1.2.0', 'refs/tags/weather-v0.8.0', sha, catalog), '1.2.0');
  assert.throws(() => releaseTag(catalog[4], '0.8.0', 'refs/tags/weather-v0.8.0', sha, catalog));
  assert.throws(() => releaseTag(catalog[5], '1.0.0', 'refs/tags/types-v2.0.0', sha, catalog));
  assert.equal(releaseTag(catalog[1], '0.8.0', 'refs/heads/main', sha, catalog), `sha-${sha}`);
  assert.equal(releaseTag(catalog[0], '2.5.0', 'refs/heads/main', sha, catalog), '2.5.0');
  assert.throws(() => releaseTag(catalog[1], '0.8.0', 'refs/heads/feature', sha, catalog));
  assert.throws(() => releaseTag(catalog[1], '0.8.0', 'refs/heads/main', 'bad-sha', catalog));
});

test('catalog rejects unsafe paths, command-like features, duplicate destinations and misspelled fields', () => {
  for (const directory of ['/etc', '../outside', 'components/../outside', 'a//b', 'a\\b', '']) {
    const entries = copy(); entries[0].directory = directory;
    assert.throws(() => validateCatalog(entries));
  }
  for (const change of [
    e => e.push({ ...e[0] }),
    e => e[3].package = e[0].package,
    e => e[0].misspelled = true,
    e => e[0].binary = 'unexpected',
    e => e[1].features = ['--all-features'],
    e => e[1].binary = '../escape',
    e => e[1].contracts = { missing: ['read'] },
    e => e[1].contracts = { queue: ['read'] },
    e => e[2].forbiddenImports = e[2].imports,
    e => e[0].smokeImports = ['read; export malicious'],
    e => e[0].defaultRegistry = 'true',
    e => e[0].package = 'missing-namespace',
  ]) { const entries = copy(); change(entries); assert.throws(() => validateCatalog(entries)); }
});

test('WIT package identities and semantic versions are checked without a fixed namespace', () => {
  assert.equal(witVersion('package acme:weather@2.5.0;', 'acme:weather'), '2.5.0');
  assert.equal(witVersion('package other:queue@1.0.0-rc.1;', 'other:queue'), '1.0.0-rc.1');
  for (const text of ['package other:weather@2.5.0;', 'package acme:weather@latest;', 'package acme:weather@01.0.0;', 'package acme:weather@1.0.0-01;']) assert.throws(() => witVersion(text, 'acme:weather'));
});

test('world validation uses the catalog contracts and variant import policy', () => {
  const world = 'export acme:weather/forecast@2.5.0;\nexport other:queue/messages@1.2.0;';
  const versions = item => item.id === 'weather-wit' ? '2.5.0' : '1.2.0';
  validateWorld(catalog[1], world, catalog, versions);
  assert.throws(() => validateWorld(catalog[1], world.replace('2.5.0', '2.4.0'), catalog, versions), /Missing contract export/);
  const tls = 'import wasi:tls/client@0.3.0-draft;';
  validateWorld(catalog[2], tls, catalog, versions);
  assert.throws(() => validateWorld(catalog[2], '', catalog, versions), /Missing import/);
  assert.throws(() => validateWorld(catalog[2], `// ${tls}`, catalog, versions), /Missing import/);
  assert.throws(() => validateWorld({ ...catalog[2], imports: [], forbiddenImports: catalog[2].imports }, tls, catalog, versions), /Unexpected import/);
  assert.throws(() => validateWorld({ ...catalog[2], imports: [], exports: ['acme:weather/forecast@2.5.0'] }, '', catalog, versions), /Missing export/);
});

test('stable publication only creates absent tags or reuses identical artifacts', () => {
  assert.equal(classify(404), 'push');
  assert.equal(classify(200, manifest, digest), 'attest');
  assert.equal(classify(200, manifest, sha256('other')), 'different');
  for (const status of [401, 403, 429, 500, 503]) assert.throws(() => classify(status));
  for (const bad of [{}, { layers: [] }, { layers: [{ digest: 'invalid' }] }, { layers: [manifest.layers[0], manifest.layers[0]] }]) assert.throws(() => classify(200, bad, digest));
});

test('anonymous verification sends no credentials and hashes the actual manifest', async () => {
  const requests = [];
  const raw = JSON.stringify(manifest);
  const request = async (url, options) => {
    requests.push({ url, ...options });
    return requests.length === 1 ? new Response('{"token":"anonymous-token"}') : new Response(raw);
  };
  const result = await probe(repositoryOf(catalog[0]), '2.5.0', { anonymous: true, request });
  assert.equal(result.digest, sha256(raw));
  assert.equal(requests[0].headers.Authorization, undefined);
  assert.equal(requests[1].headers.Authorization, 'Bearer anonymous-token');
  assert.equal(requests[0].redirect, 'error');
  assert.ok(requests[0].signal);
});

test('consumer registry overrides use the package repository and version, not an OCI manifest digest', () => {
  assert.deepEqual(consumerConfig(catalog[0], '2.5.0'), {
    version: '2.0.0', wit: { sources: { 'acme:weather': 'ghcr.io/wasmcloud/interfaces/acme/weather:2.5.0' } },
  });
});

test('failed token requests never become permission to publish', async () => {
  for (const status of [401, 403, 404, 503]) {
    await assert.rejects(probe(repositoryOf(catalog[0]), '2.5.0', { anonymous: true,
      request: async () => new Response('', { status }) }), /token request failed/);
  }
  await assert.rejects(probe('../bad', '2.5.0', { anonymous: true }), /Invalid registry/);
  await assert.rejects(probe(repositoryOf(catalog[0]), 'tag\n', { anonymous: true }), /Invalid registry/);
});

test('first publication requires an authenticated absence check', async () => {
  for (const status of [200, 401, 403, 429, 503]) {
    let count = 0;
    const request = async () => new Response('', { status: ++count === 1 ? 403 : status });
    await assert.rejects(probe(repositoryOf(catalog[1]), '0.8.0', { user: 'publisher', credential: 'test-only', request }), /Cannot confirm package absence/);
  }
  const requests = [];
  const request = async (url, options) => {
    requests.push({ url, ...options });
    return new Response('', { status: requests.length === 1 ? 403 : 404 });
  };
  assert.deepEqual(await probe(repositoryOf(catalog[1]), '0.8.0', { user: 'publisher', credential: 'test-only', request }), { status: 404 });
  assert.equal(requests[1].url, 'https://api.github.com/orgs/wasmcloud/packages/container/components%2Fweather');
  assert.equal(requests[1].headers.Authorization, 'Bearer test-only');
  assert.equal(requests[1].redirect, 'error');
});

test('attestation policy pins new builds to the reusable workflow and commit', () => {
  const image = 'ghcr.io/wasmcloud/components/weather';
  const args = verificationArgs(image, digest, true, 'wasmCloud/awesome-wasmcloud', sha);
  assert.equal(args[2], `oci://${image}@${digest}`);
  assert.ok(args.includes('--deny-self-hosted-runners'));
  assert.equal(args[args.indexOf('--source-digest') + 1], sha);
  assert.equal(args[args.indexOf('--signer-workflow') + 1], 'wasmCloud/awesome-wasmcloud/.github/workflows/publish-artifact.yml');
  const reused = verificationArgs(image, digest, false, 'wasmCloud/awesome-wasmcloud', sha);
  assert.ok(reused.includes('--repo'));
  assert.ok(!reused.includes('--source-digest'));
  assert.ok(!reused.includes('--signer-workflow'));
  assert.throws(() => verificationArgs(image, 'bad', true, 'wasmCloud/awesome-wasmcloud', sha));
  assert.throws(() => verificationArgs('other.example/weather', digest, true, 'wasmCloud/awesome-wasmcloud', sha));
});
