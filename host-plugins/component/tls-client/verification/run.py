#!/usr/bin/env python3
"""Build the TLS example and exercise it in a real component-plugin store."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-source", type=Path, required=True,
                        help="wasmCloud checkout with the documented TLS runtime fixes")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--target-dir", type=Path, help="optional reusable native Cargo target directory")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    runtime = args.runtime_source.resolve() / "crates/wash-runtime"
    if not (runtime / "Cargo.toml").is_file():
        parser.error("--runtime-source must point to a wasmCloud checkout")
    workspace = tomllib.loads((runtime.parents[1] / "Cargo.toml").read_text())
    version = workspace["workspace"]["dependencies"]["wasmtime-wasi-tls"]["version"]
    if version != "48.0.8":
        parser.error("the verifier is pinned to Wasmtime 48.0.8; use the README's runtime revision")
    environment = os.environ | {"TLS_EXAMPLE_ROOT": str(root)}
    if args.offline:
        environment["CARGO_NET_OFFLINE"] = "true"
    if args.target_dir:
        environment["CARGO_TARGET_DIR"] = str(args.target_dir.resolve())
    if not args.skip_build:
        # The harness loads each component from its project-local target path.
        # Keep --target-dir (and any inherited override) for native builds only.
        build_environment = environment.copy()
        build_environment.pop("CARGO_TARGET_DIR", None)
        for project in [root, root / "verification/caller"]:
            subprocess.run(["wash", "build"], cwd=project, env=build_environment, check=True)
    with tempfile.TemporaryDirectory(prefix="tls-client-verification-") as temporary:
        harness = Path(temporary)
        (harness / "src").mkdir()
        manifest = (root / "verification/Cargo.toml.template").read_text()
        (harness / "Cargo.toml").write_text(manifest.replace("__RUNTIME_PATH__", json.dumps(str(runtime))))
        shutil.copy2(root / "verification/Cargo.lock", harness / "Cargo.lock")
        shutil.copy2(root / "verification/harness.rs", harness / "src/lib.rs")
        shutil.copy2(root / "verification/tls.rs", harness / "src/tls.rs")
        subprocess.run(["cargo", "test", "--locked", "--manifest-path", str(harness / "Cargo.toml"),
                        "--", "--nocapture", "--test-threads=1"], env=environment, check=True)


if __name__ == "__main__":
    main()
