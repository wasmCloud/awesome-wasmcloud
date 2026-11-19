import { execFile } from 'node:child_process';
import { appendFileSync, readFileSync } from 'node:fs';
import { promisify } from 'node:util';

export const run = promisify(execFile);
export const source = 'https://github.com/wasmCloud/awesome-wasmcloud';
const plugins = 'host-plugins/component/';
export const artifacts = {
  wit: { repository: 'wasmcloud/interfaces/wasmcloud/couchbase', directory: `${plugins}couchbase/interface` },
  http: { repository: 'wasmcloud/components/couchbase', directory: `${plugins}couchbase`, binary: 'couchbase_plugin' },
  sockets: { repository: 'wasmcloud/components/couchbase-wasi-sockets-p3', directory: `${plugins}couchbase-wasi-sockets-p3`, binary: 'couchbase_wasi_sockets_p3_plugin' },
  tls: { repository: 'wasmcloud/components/couchbase-wasi-sockets-p3-tls', directory: `${plugins}couchbase-wasi-sockets-p3`, binary: 'couchbase_wasi_sockets_p3_plugin', feature: 'tls' },
};

export function artifact(id) {
  if (!Object.hasOwn(artifacts, id)) throw new Error(`Unknown artifact: ${id}`);
  return artifacts[id];
}

const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$/;
export function witVersion(wit) {
  const match = /^package wasmcloud:couchbase@([^;\s]+);/m.exec(wit);
  if (!match || !semver.test(match[1])) throw new Error('Expected versioned wasmcloud:couchbase WIT');
  return match[1];
}

export function releaseTag(id, version, ref, sha) {
  artifact(id);
  if (!semver.test(version)) throw new Error(`Invalid version: ${version}`);
  if (ref.startsWith('refs/tags/')) {
    if (!ref.startsWith('refs/tags/couchbase-v') || !semver.test(ref.slice('refs/tags/couchbase-v'.length))) {
      throw new Error('Invalid Couchbase release tag');
    }
    if (id !== 'wit' && ref !== `refs/tags/couchbase-v${version}`) {
      throw new Error(`Release tag ${ref} does not match Cargo version ${version}`);
    }
  } else if (ref !== 'refs/heads/main') {
    throw new Error('Publication requires main or a couchbase release tag');
  }
  if (!/^[a-f0-9]{40}$/.test(sha)) throw new Error('Expected a full Git commit SHA');
  return id === 'wit' || ref.startsWith('refs/tags/') ? version : `sha-${sha}`;
}

export function versionOf(id) {
  const item = artifact(id);
  if (id === 'wit') return witVersion(readFileSync(`${item.directory}/couchbase.wit`, 'utf8'));
  // These standalone projects declare their package versions literally.
  const cargo = readFileSync(`${item.directory}/Cargo.toml`, 'utf8');
  const pkg = /(?:^|\n)\[package\]\s*\n([\s\S]*?)(?=\n\[|$)/.exec(cargo)?.[1];
  const version = /^version\s*=\s*"([^"]+)"\s*$/m.exec(pkg ?? '')?.[1];
  if (!version || !semver.test(version)) throw new Error('Expected a literal Cargo package version');
  return version;
}

export function output(name, value) {
  if (/[\r\n]/.test(value)) throw new Error('Invalid multiline workflow output');
  console.log(`${name}=${value}`);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
}
