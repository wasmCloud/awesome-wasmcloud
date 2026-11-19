import { copyFileSync, cpSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { artifact, contractVersion, loadCatalog, projectPath, run, semver, source, witVersion } from './lib/artifacts.mjs';

export function validateWorld(item, wit, catalog, versionFor) {
  const declarations = new Set(wit.split(/\r?\n/).map(line => line.trim()));
  for (const [direction, names] of [['import', item.imports], ['export', item.exports]]) {
    for (const name of names ?? []) {
      if (!declarations.has(`${direction} ${name};`)) throw new Error(`Missing ${direction}: ${name}`);
    }
  }
  for (const name of item.forbiddenImports ?? []) {
    if (declarations.has(`import ${name};`)) throw new Error(`Unexpected import: ${name}`);
  }
  for (const [id, names] of Object.entries(item.contracts ?? {})) {
    const dependency = artifact(id, catalog);
    for (const name of names) {
      const fullName = `${dependency.package}/${name}@${versionFor(dependency)}`;
      if (!declarations.has(`export ${fullName};`)) throw new Error(`Missing contract export: ${fullName}`);
    }
  }
}

async function main([id, ...extra]) {
  if (!id || extra.length) throw new Error('Usage: build-artifact.mjs <catalog-id>');
  const catalog = loadCatalog();
  const item = artifact(id, catalog);
  const cwd = projectPath(item.directory);
  const revision = process.env.GITHUB_SHA ?? (await run('git', ['rev-parse', 'HEAD'])).stdout.trim();
  if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error('Invalid source revision');
  mkdirSync('dist', { recursive: true });
  const staged = mkdtempSync(join(tmpdir(), 'artifact-release-'));
  const raw = join(staged, 'raw.wasm');
  const result = resolve('dist', `${id}.wasm`);
  let version;
  try {
    if (item.kind === 'wit') {
      if (item.fetchFrom) await run('wash', ['wit', 'fetch'], { cwd: projectPath(item.fetchFrom), timeout: 180_000 });
      version = contractVersion(item);
      // Keep one authoritative WIT source; stage its files and fetched dependencies.
      cpSync(cwd, join(staged, 'wit'), { recursive: true });
      await run('wash', ['wit', 'build', '--output-file', raw], { cwd: staged });
    } else {
      await run('wash', ['wit', 'fetch'], { cwd, timeout: 180_000 });
      const { stdout } = await run('cargo', ['metadata', '--locked', '--no-deps', '--format-version', '1'], { cwd, maxBuffer: 8 << 20 });
      const pkg = JSON.parse(stdout).packages?.find(p => resolve(p.manifest_path) === join(cwd, 'Cargo.toml'));
      version = pkg?.version;
      if (typeof version !== 'string' || !semver.test(version)) throw new Error('Cargo did not report the project package version');
      const target = resolve(cwd, 'target', `publish-${id}`);
      const features = item.features?.length ? ['--features', item.features.join(',')] : [];
      await run('cargo', ['test', '--locked', '--target-dir', target, ...features], { cwd, timeout: 600_000, maxBuffer: 8 << 20 });
      await run('cargo', ['build', '--locked', '--target', 'wasm32-wasip2', '--release', '--target-dir', target, ...features], { cwd, timeout: 600_000, maxBuffer: 8 << 20 });
      copyFileSync(join(target, 'wasm32-wasip2', 'release', `${item.binary}.wasm`), raw);
    }
    await run('wasm-tools', ['metadata', 'add', raw, '--output', result,
      '--source', source, '--homepage', `${source}/tree/main/${item.directory}`,
      '--revision', revision, '--version', version, '--licenses', item.license, '--authors', 'wasmCloud Maintainers']);
    await run('wasm-tools', ['validate', '--features', 'all', result]);
    const { stdout: wit } = await run('wasm-tools', ['component', 'wit', result], { maxBuffer: 8 << 20 });
    if (item.kind === 'wit') {
      if (witVersion(wit, item.package) !== version) throw new Error('Built WIT version changed');
    } else {
      validateWorld(item, wit, catalog, contractVersion);
    }
    const locks = await run('git', ['status', '--porcelain', '--untracked-files=all', '--', '**/Cargo.lock', '**/wkg.lock']);
    if (locks.stdout.trim()) throw new Error(`Commit dependency lock changes before publishing:\n${locks.stdout}`);
    console.log(`Built ${result} (${version})`);
  } finally {
    rmSync(staged, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
