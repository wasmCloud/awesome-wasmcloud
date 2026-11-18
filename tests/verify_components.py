#!/usr/bin/env python3
"""Exercise built components against isolated synthetic HTTP/2, HTTP and KV peers."""
import argparse
import contextlib
import http.client
import json
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
GRPC = ROOT / "components/grpc-client"
CB = ROOT / "host-plugins/component/couchbase"
KV = ROOT / "host-plugins/component/couchbase-wasi-sockets-p3"
PROBE = ROOT / "tests/probe"
HOST_IMAGE = "ghcr.io/wasmcloud/wash:2.12.1-all-features"


def run(args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, timeout=180, **kwargs)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr or result.stdout}")
    return result.stdout.strip()


def request(port, path, method="POST", body=b"", headers=None):
    with contextlib.closing(http.client.HTTPConnection("127.0.0.1", port, timeout=5)) as conn:
        conn.request(method, path, body, headers or {})
        response = conn.getresponse()
        return response.status, dict((k.lower(), v) for k, v in response.getheaders()), response.read()


class Fixture:
    def __init__(self, directory, image):
        self.directory, self.image = directory, image
        self.prefix = "component-check-" + uuid.uuid4().hex[:10]
        self.network = self.prefix
        self.containers = []

    def start(self):
        run(["docker", "network", "create", self.network])
        server = self.directory / "server"
        server.mkdir()
        shutil.copy2(ROOT / "tests/fixtures/server.js", server / "server.js")
        run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
             "-keyout", "ca.key", "-out", "ca.crt", "-subj", "/CN=component-check-ca"], cwd=server)
        run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key",
             "-out", "server.csr", "-subj", "/CN=backend"], cwd=server)
        (server / "server.ext").write_text("subjectAltName=DNS:backend\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n")
        run(["openssl", "x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
             "-CAcreateserial", "-out", "server.crt", "-days", "1", "-extfile", "server.ext"], cwd=server)
        name = self.prefix + "-backend"
        self.containers.append(name)
        run(["docker", "run", "-d", "--name", name, "--network", self.network, "--network-alias", "backend",
             "--network-alias", "wrong-backend",
             "-p", "127.0.0.1::18099", "-v", f"{server}:/fixture:ro", "node:22-alpine", "node", "/fixture/server.js"])
        self.monitor = self.port(name, "18099/tcp")
        for _ in range(50):
            try:
                self.requests()
                return
            except (OSError, http.client.HTTPException):
                time.sleep(0.1)
        raise RuntimeError("synthetic backend did not start")

    def port(self, name, internal):
        data = json.loads(run(["docker", "inspect", name]))[0]
        return int(data["NetworkSettings"]["Ports"][internal][0]["HostPort"])

    def requests(self):
        return json.loads(request(self.monitor, "/", method="GET")[2])

    @contextlib.contextmanager
    def host(self, project, artifact, dev=None, workload=None, extra_files=None, ready_status=200):
        stage = self.directory / uuid.uuid4().hex
        (stage / ".wash").mkdir(parents=True)
        shutil.copy2(project / artifact, stage / "component.wasm")
        for name, source in (extra_files or {}).items():
            shutil.copy2(source, stage / name)
        shutil.copy2(project / "Cargo.toml", stage / "Cargo.toml")
        # WIT and the component are public test inputs; the checkout is never mounted.
        shutil.copytree(project / "wit", stage / "wit")
        shutil.copy2(self.directory / "server/ca.crt", stage / "ca.crt")
        config = {"version": "2.0.0", "build": {"command": "/bin/true", "component_path": "component.wasm"},
                  "dev": {"http_client_ca_paths": ["/fixture/ca.crt"], **(dev or {})}}
        if workload is not None:
            config["workload"] = workload
        (stage / ".wash/config.yaml").write_text(json.dumps(config))
        name = self.prefix + "-" + stage.name[:8]
        self.containers.append(name)
        run(["docker", "run", "-d", "--name", name, "--network", self.network, "-p", "127.0.0.1::8000",
             "-v", f"{stage}:/fixture", "-w", "/fixture", self.image, "dev", "--non-interactive"])
        port = self.port(name, "8000/tcp")
        try:
            for _ in range(100):
                try:
                    if request(port, "/review.Service/Ready")[0] == ready_status:
                        break
                except (OSError, http.client.HTTPException):
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError("component did not deploy")
            yield port
        except Exception:
            print(run(["docker", "logs", name]))
            raise
        finally:
            self.remove(name)

    def remove(self, name):
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)
        if name in self.containers:
            self.containers.remove(name)

    def close(self):
        for name in self.containers[:]:
            self.remove(name)
        subprocess.run(["docker", "network", "rm", self.network], capture_output=True, timeout=30)


def verify_grpc(fixture, secure):
    authority = "backend:18443" if secure else "backend:18098"
    environment = {"GRPC_AUTHORITY": authority, "GRPC_TIMEOUT_MS": "100"}
    if not secure:
        environment["GRPC_PLAINTEXT"] = "1"
    workload = {"allowedHosts": [authority], "environment": {"config": environment}}
    artifact = "target/wasm32-wasip2/release/grpc_client.wasm"
    count = 0
    with fixture.host(GRPC, artifact, workload=workload) as port:
        for mode, body, frames in [("Ok", bytes([10, 2, 111, 107]), "1"), ("Empty", b"", "1"), ("Stream", b"one", "2")]:
            status, headers, actual = request(port, "/review.Service/" + mode)
            assert (status, headers.get("grpc-status"), headers.get("grpc-frames"), actual) == (200, "0", frames, body)
            count += 1
        status, headers, _ = request(port, "/review.Service/RpcError")
        assert status == 200 and headers.get("grpc-status") == "5"
        count += 1
        for mode in ["Malformed", "ShortHeader", "Overflow", "Compressed", "InvalidFlag", "TrailingGarbage",
                     "MissingStatus", "InvalidStatus", "ConflictingStatus", "WrongContentType", "HttpError", "Slow"]:
            start = time.monotonic()
            status, _, body = request(port, "/review.Service/" + mode)
            assert status == 502, (mode, status, body)
            if mode == "Slow":
                assert b"deadline" in body and time.monotonic() - start < 1
            count += 1
        before = len(fixture.requests())
        for method in ["GET", "HEAD", "DELETE", "OPTIONS"]:
            status, headers, _ = request(port, "/review.Service/DeleteAll", method=method)
            assert status == 405 and headers.get("allow") == "POST"
            count += 1
        for path in ["/missing", "//Method", "/Service/", "/Service/Method?x=1"]:
            assert request(port, path)[0] == 400
            count += 1
        assert len(fixture.requests()) == before, "rejected requests reached the RPC service"
        request(port, "/review.Service/Auth", headers={"authorization": "Bearer synthetic"})
        last = fixture.requests()[-1]
        assert last["authorization"] == "Bearer synthetic" and last.get("timeout")
        count += 1
        before = len(fixture.requests())
        with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
            connection.sendall(b"POST /review.Service/Incomplete HTTP/1.1\r\nHost: test\r\nContent-Length: 10\r\n\r\nabc")
            connection.shutdown(socket.SHUT_WR)
            try:
                connection.recv(4096)
            except OSError:
                pass
        time.sleep(0.1)
        assert len(fixture.requests()) == before, "an incomplete input body invoked the RPC"
        count += 1
    if not secure:
        for invalid in ["not-a-number", "0"]:
            invalid_workload = {**workload, "environment": {"config": {**environment, "GRPC_TIMEOUT_MS": invalid}}}
            before = len(fixture.requests())
            with fixture.host(GRPC, artifact, workload=invalid_workload, ready_status=500) as port:
                status, _, body = request(port, "/review.Service/Ok")
                assert status == 500 and b"GRPC_TIMEOUT_MS" in body
                assert len(fixture.requests()) == before
            count += 1
    failures = [({"allowedHosts": ["elsewhere.invalid:1"], "environment": workload["environment"]}, {})] if not secure else [
        (workload, {"http_client_ca_paths": []}),
        ({"allowedHosts": ["wrong-backend:18443"], "environment": {"config": {
            **environment, "GRPC_AUTHORITY": "wrong-backend:18443"}}}, {}),
    ]
    for rejected, dev in failures:
        rejected = {**rejected, "environment": {"config": {
            **rejected["environment"]["config"], "GRPC_TIMEOUT_MS": "1000"}}}
        before = len(fixture.requests())
        with fixture.host(GRPC, artifact, workload=rejected, dev=dev, ready_status=502) as port:
            status, _, body = request(port, "/review.Service/Ok")
            assert status == 502 and b"transport error" in body
            assert len(fixture.requests()) == before
        count += 1
    print(f"gRPC {'TLS' if secure else 'plaintext'}: {count} checks passed", flush=True)
    return count


def verify_couchbase(fixture, sockets):
    plugin = "couchbase-wasi-sockets-p3" if sockets else "couchbase"
    filename = "couchbase_wasi_sockets_p3_plugin.wasm" if sockets else "couchbase_plugin.wasm"
    project = KV if sockets else CB
    # Stage the plugin separately; host() stages only the probe project.
    stage = fixture.directory / (plugin + ".wasm")
    shutil.copy2(project / "target/wasm32-wasip2/release" / filename, stage)
    dev = {"host_plugins": [{"id": plugin, "file": "/fixture/" + plugin + ".wasm",
              "allowedHosts": ["backend:11210", "backend:18009"], "allowedIpNameLookups": ["*"]}],
           "host_interfaces": [{"namespace": "wasmcloud", "package": "couchbase", "version": "0.2.0",
              "interfaces": ["types", "sqlpp-types", "document", "sqlpp"], "config": {
                  "endpoint": "couchbase://backend" if sockets else "http://backend:18009",
                  "query-port": "18009", "bucket": "test.bucket", "username": "synthetic", "password": "synthetic",
                  "timeout-ms": "100"}}]}
    artifact = "target/wasm32-wasip2/release/component_fault_probe.wasm"
    with fixture.host(PROBE, artifact, dev=dev, extra_files={plugin + ".wasm": stage}) as port:
        cases = [("expiry-race", 'cas=222', '"version":2'), ("expiry-churn", "::Timeout", None),
                 ("expiry-error", "::Unauthorized", None), ("expiry-none", "expiry=None", None),
                 ("expiry-short", "::RequestFailed", None), ("query-error", "::Server", None),
                 ("kv-bad-header", "::RequestFailed", None), ("kv-huge-header", "::RequestFailed", None),
                 ("kv-wrong-opcode", "::RequestFailed", None), ("kv-wrong-opaque", "::RequestFailed", None),
                 ("oversized-id", "::InvalidArgument", None), ("empty-id", "::InvalidArgument", None)] if sockets else [
                 ("complete", "document=abcdefghij", None), ("truncated", "::RequestFailed", None),
                 ("timeout", "::Timeout", None), ("slow", "::Timeout", None)]
        cases += [("query-context", "default:`test.bucket`.`_default`", None)]
        cases += [(f"large-{operation}-ttl", "::InvalidArgument", None)
                  for operation in ["insert", "upsert", "replace", "touch", "get-touch"]]
        if not sockets:
            cases += [(path, "::Unsupported", None)
                      for path in ["dot-id", "dotdot-id", "dot-lock-id", "dot-remove-id"]]
        for path, expected, extra in cases:
            before = len(fixture.requests())
            _, _, body = request(port, "/" + path, method="GET")
            text = body.decode()
            assert expected in text and (extra is None or extra in text), (path, text)
            if path.startswith(("large-", "dot-", "dotdot-")):
                assert len(fixture.requests()) == before, f"{path} reached the service"
        assert not any(entry.get("kind") == "kv-write" for entry in fixture.requests()), "invalid keys reached the server"
    print(f"{plugin} failure cases: {len(cases)} checks passed", flush=True)
    return len(cases)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--host-image", default=HOST_IMAGE)
    args = parser.parse_args()
    if not args.skip_build:
        for project in [GRPC, CB, KV, PROBE]:
            run(["wash", "build"], cwd=project)
    with tempfile.TemporaryDirectory(prefix="component-check-", dir=Path("/tmp").resolve()) as temporary:
        fixture = Fixture(Path(temporary), args.host_image)
        try:
            fixture.start()
            count = verify_grpc(fixture, False) + verify_grpc(fixture, True)
            count += verify_couchbase(fixture, False) + verify_couchbase(fixture, True)
            print(f"PASS: {count} live protocol checks", flush=True)
        finally:
            fixture.close()


if __name__ == "__main__":
    main()
