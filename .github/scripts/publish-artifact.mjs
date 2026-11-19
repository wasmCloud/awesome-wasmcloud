import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { artifact, builtVersion, output, releaseTag, repositoryOf, run } from './lib/artifacts.mjs';

export const sha256 = (data) => `sha256:${createHash('sha256').update(data).digest('hex')}`;
const digestPattern = /^sha256:[a-f0-9]{64}$/;

export function consumerConfig(item, version) {
  // wash maps an OCI repository to a WIT registry and derives the tag from
  // the import version; it does not accept a manifest digest as that mapping.
  return { version: '2.0.0', wit: { sources: {
    [item.package]: `ghcr.io/${repositoryOf(item)}:${version}`,
  } } };
}

export function classify(status, manifest, expected) {
  if (status === 404) return 'push';
  if (status !== 200) throw new Error(`Registry lookup failed (HTTP ${status}); refusing to overwrite`);
  if (!Array.isArray(manifest?.layers) || manifest.layers.length !== 1 ||
      !digestPattern.test(manifest.layers[0]?.digest ?? '')) throw new Error('Unexpected registry manifest');
  return manifest.layers[0].digest === expected ? 'attest' : 'different';
}

export async function probe(repository, tag, { anonymous = false, request = fetch,
  user = process.env.GITHUB_ACTOR, credential = process.env.REGISTRY_TOKEN } = {}) {
  if (!/^wasmcloud\/[a-z0-9/-]+$/.test(repository) || !/^[A-Za-z0-9_.:-]+$/.test(tag)) {
    throw new Error('Invalid registry reference');
  }
  const headers = {};
  if (!anonymous) {
    if (!user || !credential) throw new Error('Authenticated publication requires GITHUB_ACTOR and REGISTRY_TOKEN');
    headers.Authorization = `Basic ${Buffer.from(`${user}:${credential}`).toString('base64')}`;
  }
  const auth = await request(`https://ghcr.io/token?service=ghcr.io&scope=repository:${repository}:pull`, {
    headers, signal: AbortSignal.timeout(30_000), redirect: 'error',
  });
  if (!auth.ok) {
    // GHCR may deny a pull token before a package's first push. Require an
    // authenticated GitHub package lookup to confirm absence, rather than
    // treating every authentication/network error as permission to overwrite.
    if (!anonymous && [403, 404].includes(auth.status)) {
      const pkg = encodeURIComponent(repository.slice('wasmcloud/'.length));
      const existing = await request(`https://api.github.com/orgs/wasmcloud/packages/container/${pkg}`, {
        headers: { Authorization: `Bearer ${credential}`, Accept: 'application/vnd.github+json' },
        signal: AbortSignal.timeout(30_000), redirect: 'error',
      });
      if (existing.status === 404) return { status: 404 };
      throw new Error(`Cannot confirm package absence (GitHub HTTP ${existing.status})`);
    }
    throw Object.assign(new Error(`GHCR token request failed (HTTP ${auth.status})`), { tokenStatus: auth.status });
  }
  const token = (await auth.json()).token;
  if (typeof token !== 'string' || !token) throw new Error('GHCR returned no registry token');
  const res = await request(`https://ghcr.io/v2/${repository}/manifests/${tag}`, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/vnd.oci.image.manifest.v1+json' },
    signal: AbortSignal.timeout(30_000), redirect: 'error',
  });
  if (res.status !== 200) return { status: res.status };
  const raw = Buffer.from(await res.arrayBuffer());
  return { status: 200, manifest: JSON.parse(raw.toString()), digest: sha256(raw) };
}

async function sameWit(local, remote) {
  const options = { maxBuffer: 8 << 20 };
  const a = await run('wasm-tools', ['component', 'wit', local], options);
  const b = await run('wasm-tools', ['component', 'wit', remote], options);
  return a.stdout === b.stdout;
}

async function pullAndCompare(item, version, reference, local, expectedLayer, anonymous) {
  const temp = mkdtempSync(join(tmpdir(), 'artifact-pull-'));
  try {
    // A fresh Docker config proves the package is available without credentials.
    const env = anonymous ? { ...process.env, DOCKER_CONFIG: temp } : process.env;
    const pulled = join(temp, 'artifact.wasm');
    await run('wash', ['oci', 'pull', reference, pulled], { env, timeout: 120_000 });
    if (sha256(readFileSync(pulled)) !== expectedLayer) throw new Error('Pulled bytes do not match the manifest');
    if (item.kind === 'wit') {
      if (!await sameWit(local, pulled)) throw new Error('Published WIT differs; bump its package version');
      if (anonymous) {
        const consumer = join(temp, 'consumer');
        mkdirSync(join(consumer, 'wit'), { recursive: true });
        writeFileSync(join(consumer, 'wit', 'world.wit'),
          `package example:consumer;\nworld consumer { ${item.smokeImports.map(name => `import ${item.package}/${name}@${version};`).join(' ')} }\n`);
        if (!item.defaultRegistry) {
          // Namespaces without a default resolver can consume the same public OCI package.
          mkdirSync(join(consumer, '.wash'));
          writeFileSync(join(consumer, '.wash', 'config.yaml'), JSON.stringify(consumerConfig(item, version)));
        }
        await run('wash', ['wit', 'fetch'], { cwd: consumer, env, timeout: 120_000 });
        await run('wasm-tools', ['component', 'wit', join(consumer, 'wit')]);
      }
    } else if (sha256(readFileSync(local)) !== expectedLayer) {
      throw new Error('Published component differs; bump its Cargo version and release tag');
    }
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
}

async function main([command, id, ...extra]) {
  if (!['check', 'publish', 'verify'].includes(command) || extra.length) throw new Error('Usage: publish-artifact.mjs <check|publish|verify> <catalog-id>');
  const item = artifact(id);
  const repository = repositoryOf(item);
  const local = resolve('dist', `${id}.wasm`);
  const version = await builtVersion(id);
  if (command === 'check') {
    if (item.kind !== 'wit') throw new Error('Contract preflight only accepts WIT artifacts');
    let found;
    try { found = await probe(repository, version, { anonymous: true }); }
    catch (error) {
      if (![401, 403, 404].includes(error.tokenStatus)) throw error;
      console.log('Package is not visible anonymously; authenticated publication must check it before components publish');
      return;
    }
    if (classify(found.status, found.manifest, sha256(readFileSync(local))) !== 'push') {
      await pullAndCompare(item, version, `ghcr.io/${repository}@${found.digest}`, local, found.manifest.layers[0].digest, false);
    }
    console.log(`Contract preflight passed: ${item.package}@${version}`);
    return;
  }
  const tag = releaseTag(item, version, process.env.GITHUB_REF ?? '', process.env.GITHUB_SHA ?? '');
  const image = `ghcr.io/${repository}`;
  const reference = `${image}:${tag}`;
  const expected = sha256(readFileSync(local));
  const result = await probe(repository, tag, { anonymous: command === 'verify' });
  let action = classify(result.status, result.manifest, expected);
  if (command === 'verify') {
    if (action === 'push') throw new Error(`${reference} is not public`);
    if (!digestPattern.test(process.env.PUBLISHED_DIGEST ?? '') || result.digest !== process.env.PUBLISHED_DIGEST) {
      throw new Error('Published tag does not resolve to the expected OCI manifest digest');
    }
    await pullAndCompare(item, version, `${image}@${result.digest}`, local, result.manifest.layers[0].digest, true);
    console.log(`Verified anonymous pull: ${reference} (${result.digest})`);
    return;
  }
  let digest = result.digest;
  if (action === 'different') {
    if (item.kind !== 'wit') throw new Error(`${reference} already holds different bytes; bump its version`);
    // Metadata may change between commits while the versioned contract stays identical.
    await pullAndCompare(item, version, `${image}@${digest}`, local, result.manifest.layers[0].digest, false);
    action = 'skip';
  }
  if (action === 'push') {
    const { stdout } = await run('wash', ['-o', 'json', 'oci', 'push', reference, local], { timeout: 120_000 });
    digest = JSON.parse(stdout).data?.digest;
  }
  if (!digestPattern.test(digest ?? '')) throw new Error('Publication returned no valid OCI manifest digest');
  output('image', image);
  output('digest', digest);
  output('attest', String(action !== 'skip'));
  console.log(`${reference} (${digest})`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.error(error.message); if (error.stderr || error.stdout) console.error(error.stderr || error.stdout); process.exitCode = 1; }
}
