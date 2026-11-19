import { execFile } from 'node:child_process';
import { appendFileSync, readFileSync, readdirSync, realpathSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { promisify } from 'node:util';

export const run = promisify(execFile);
export const source = 'https://github.com/wasmCloud/awesome-wasmcloud';
const identifier = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/;
const packageId = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*:[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/;
export const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?$/;
const fields = new Set(['id', 'kind', 'releaseGroup', 'directory', 'package', 'smokeImports',
  'fetchFrom', 'defaultRegistry', 'binary', 'features', 'contracts', 'imports', 'exports', 'forbiddenImports', 'license']);
const witFields = ['package', 'smokeImports', 'fetchFrom', 'defaultRegistry'];
const componentFields = ['binary', 'features', 'contracts', 'imports', 'exports', 'forbiddenImports'];
const interfaceId = /^[a-z][a-z0-9-]*:[a-z][a-z0-9-]*\/[a-z][a-z0-9-]*@[^\s;]+$/;

function directory(value) {
  return typeof value === 'string' && value.split('/').every(p => /^[a-zA-Z0-9_.-]+$/.test(p) && p !== '.' && p !== '..');
}

function strings(value, pattern) {
  return Array.isArray(value) && value.every(s => typeof s === 'string' && pattern.test(s)) && new Set(value).size === value.length;
}

export function validateCatalog(entries) {
  if (!Array.isArray(entries) || !entries.length) throw new Error('Artifact catalog must be a nonempty array');
  const ids = new Set();
  const repositories = new Set();
  for (const item of entries) {
    if (!item || typeof item !== 'object' || Array.isArray(item) || Object.keys(item).some(k => !fields.has(k))) throw new Error('Unknown artifact catalog fields');
    if (!identifier.test(item.id ?? '') || ids.has(item.id)) throw new Error('Invalid or duplicate artifact id');
    ids.add(item.id);
    if (!['wit', 'component'].includes(item.kind) || !identifier.test(item.releaseGroup ?? '') || !directory(item.directory)) throw new Error(`Invalid artifact: ${item.id}`);
    if (typeof item.license !== 'string' || !/^[A-Za-z0-9.+() -]+$/.test(item.license) || !item.license.trim()) throw new Error('Expected an SPDX license expression');
    const inappropriate = item.kind === 'wit' ? componentFields : witFields;
    if (inappropriate.some(k => Object.hasOwn(item, k))) throw new Error(`Fields do not match artifact kind: ${item.id}`);
    if (item.kind === 'wit') {
      if (!packageId.test(item.package ?? '') || !strings(item.smokeImports, identifier) || !item.smokeImports.length ||
          (item.fetchFrom !== undefined && !directory(item.fetchFrom)) ||
          (item.defaultRegistry !== undefined && typeof item.defaultRegistry !== 'boolean')) throw new Error(`Invalid WIT package: ${item.id}`);
    } else {
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(item.binary ?? '')) throw new Error('Expected a Cargo library binary name');
      for (const key of ['features', 'imports', 'exports', 'forbiddenImports']) {
        if (item[key] !== undefined && !strings(item[key], key === 'features' ? /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/ : interfaceId)) throw new Error(`Invalid ${key}`);
      }
      if (item.contracts !== undefined && (!item.contracts || typeof item.contracts !== 'object' || Array.isArray(item.contracts))) throw new Error('Invalid contracts');
      for (const [id, names] of Object.entries(item.contracts ?? {})) {
        if (!identifier.test(id) || !strings(names, identifier) || !names.length) throw new Error('Invalid contract exports');
      }
      if ((item.imports ?? []).some(name => item.forbiddenImports?.includes(name))) throw new Error('An import cannot be both required and forbidden');
    }
    const repository = repositoryOf(item);
    if (repositories.has(repository)) throw new Error('Duplicate artifact repository');
    repositories.add(repository);
  }
  for (const item of entries) {
    for (const id of Object.keys(item.contracts ?? {})) {
      if (entries.find(e => e.id === id)?.kind !== 'wit') throw new Error(`Unknown WIT contract: ${id}`);
    }
  }
  return entries;
}

export function loadCatalog(file = '.github/artifacts.json') {
  return validateCatalog(JSON.parse(readFileSync(file, 'utf8')));
}

export function artifact(id, catalog = loadCatalog()) {
  const item = catalog.find(e => e.id === id);
  if (!item) throw new Error(`Unknown artifact: ${id}`);
  return item;
}

export function repositoryOf(item) {
  return item.kind === 'wit' ? `wasmcloud/interfaces/${item.package.replace(':', '/')}` : `wasmcloud/components/${item.id}`;
}

export function projectPath(path) {
  const root = realpathSync('.');
  const directory = realpathSync(path);
  if (directory !== root && !directory.startsWith(`${root}${sep}`)) throw new Error('Artifact source escapes the checkout');
  return directory;
}

export function witVersion(wit, expectedPackage) {
  const match = /^package\s+([^@\s]+)@([^;\s]+);/m.exec(wit);
  if (!match || !packageId.test(match[1]) || match[1] !== expectedPackage || !semver.test(match[2])) throw new Error(`Expected versioned ${expectedPackage} WIT`);
  return match[2];
}

export function contractVersion(item) {
  const dir = projectPath(item.directory);
  const versions = readdirSync(dir).filter(f => f.endsWith('.wit'))
    .map(f => readFileSync(resolve(dir, f), 'utf8')).filter(s => /^package\s/m.test(s))
    .map(s => witVersion(s, item.package));
  if (!versions.length || new Set(versions).size !== 1) throw new Error('Missing or conflicting WIT package declarations');
  return versions[0];
}

export function selectArtifacts(catalog, ref) {
  if (!ref.startsWith('refs/tags/')) return catalog;
  const match = /^refs\/tags\/(.+)-v(.+)$/.exec(ref);
  if (!match || !identifier.test(match[1]) || !semver.test(match[2])) throw new Error('Invalid artifact release tag');
  const selected = catalog.filter(e => e.releaseGroup === match[1]);
  const dependencies = new Set(selected.flatMap(e => Object.keys(e.contracts ?? {})));
  return catalog.filter(e => selected.includes(e) || dependencies.has(e.id));
}

export function releaseTag(item, version, ref, sha, catalog = loadCatalog()) {
  if (!semver.test(version)) throw new Error(`Invalid version: ${version}`);
  if (!/^[a-f0-9]{40}$/.test(sha)) throw new Error('Expected a full Git commit SHA');
  if (ref.startsWith('refs/tags/')) {
    if (!selectArtifacts(catalog, ref).some(e => e.id === item.id)) throw new Error('Release tag does not select this artifact');
    const [, group, taggedVersion] = /^refs\/tags\/(.+)-v(.+)$/.exec(ref);
    const hasComponents = catalog.some(e => e.releaseGroup === group && e.kind === 'component');
    if ((item.kind === 'component' || (!hasComponents && item.releaseGroup === group)) && taggedVersion !== version) throw new Error(`Release tag ${ref} does not match artifact version ${version}`);
    return version;
  }
  if (ref !== 'refs/heads/main') throw new Error('Publication requires main or a registered release tag');
  return item.kind === 'wit' ? version : `sha-${sha}`;
}

export async function builtVersion(id) {
  const { stdout } = await run('wasm-tools', ['metadata', 'show', '--json', resolve('dist', `${id}.wasm`)], { maxBuffer: 8 << 20 });
  const version = JSON.parse(stdout)?.component?.metadata?.version;
  if (typeof version !== 'string' || !semver.test(version)) throw new Error('Artifact has no valid component version metadata');
  return version;
}

export function output(name, value) {
  if (/[\r\n]/.test(value)) throw new Error('Invalid multiline workflow output');
  console.log(`${name}=${value}`);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}
