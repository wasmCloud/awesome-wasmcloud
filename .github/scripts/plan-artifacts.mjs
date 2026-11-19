import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadCatalog, output, selectArtifacts } from './lib/artifacts.mjs';

export function matrices(catalog, ref) {
  const selected = selectArtifacts(catalog, ref);
  const matrix = entries => JSON.stringify({ include: entries.map(({ id, kind }) => ({ id, kind })) });
  return {
    matrix: matrix(selected),
    wit: matrix(selected.filter(e => e.kind === 'wit')),
    components: matrix(selected.filter(e => e.kind === 'component')),
    has_artifacts: String(selected.length > 0),
    has_wit: String(selected.some(e => e.kind === 'wit')),
    has_components: String(selected.some(e => e.kind === 'component')),
  };
}

async function main(args) {
  if (args.length) throw new Error('Usage: plan-artifacts.mjs');
  for (const [key, value] of Object.entries(matrices(loadCatalog(), process.env.GITHUB_REF ?? ''))) output(key, value);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(process.argv.slice(2)); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
