import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { artifact, repositoryOf, run } from './lib/artifacts.mjs';

export function verificationArgs(image, digest, newlyAttested, repository, sha) {
  if (!/^ghcr\.io\/wasmcloud\/[a-z0-9/-]+$/.test(image) || !/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error('Invalid attestation subject');
  if (!/^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/.test(repository) || !/^[a-f0-9]{40}$/.test(sha) || typeof newlyAttested !== 'boolean') throw new Error('Invalid attestation policy');
  const args = ['attestation', 'verify', `oci://${image}@${digest}`, '--repo', repository, '--deny-self-hosted-runners'];
  if (newlyAttested) args.push('--signer-workflow', `${repository}/.github/workflows/publish-artifact.yml`, '--source-digest', sha);
  return args;
}

async function main([id, ...extra]) {
  if (!id || extra.length || !['true', 'false'].includes(process.env.NEW_ATTESTATION)) throw new Error('Usage: verify-attestation.mjs <catalog-id>, with publication outputs in the environment');
  if (process.env.IMAGE !== `ghcr.io/${repositoryOf(artifact(id))}`) throw new Error('Attestation image does not match the catalog');
  const args = verificationArgs(process.env.IMAGE, process.env.DIGEST, process.env.NEW_ATTESTATION === 'true', process.env.GITHUB_REPOSITORY, process.env.GITHUB_SHA);
  // Check both GitHub's current repository identity and the bundle stored with the OCI artifact.
  await run('gh', args, { timeout: 120_000 });
  await run('gh', [...args, '--bundle-from-oci'], { timeout: 120_000 });
  console.log(`Verified GitHub and GHCR provenance for ${id} (${process.env.DIGEST})`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
