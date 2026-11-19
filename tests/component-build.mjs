import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';

export function buildComponents(projects, environment = process.env) {
  // The verifiers load each project's configured local target path.
  // An explicit environment value overrides both inherited settings and Cargo config.
  for (const project of projects) {
    execFileSync('wash', ['build'], {
      cwd: project,
      env: { ...environment, CARGO_TARGET_DIR: resolve(project, 'target') },
      stdio: 'inherit',
      timeout: 180_000,
    });
  }
}

export function buildComponentFeature(project, feature, environment = process.env) {
  if (!/^[a-z0-9][a-z0-9_-]*$/.test(feature)) throw new Error('invalid component feature');
  const options = { cwd: project, env: environment, stdio: 'inherit', timeout: 180_000 };
  execFileSync('wash', ['wit', 'fetch'], options);
  execFileSync('cargo', ['build', '--locked', '--target', 'wasm32-wasip2', '--release',
    '--features', feature, '--target-dir', resolve(project, 'target', feature)], options);
}
