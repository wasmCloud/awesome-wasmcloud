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
