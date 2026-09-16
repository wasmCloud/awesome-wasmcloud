#!/usr/bin/env python3
"""A Couchbase Data API server, implemented over the official Couchbase SDK.

Capella's Data API is an HTTP gateway in front of an ordinary Couchbase cluster.
No Couchbase Server release ships it (verified against 7.6.4 and 8.0.2), and
there is no `couchbase/data-api` image, so end-to-end testing of anything that
speaks the Data API needs one to exist. This is that server.

It performs **native KV operations** through the SDK rather than translating to
SQL++, so the semantics under test are the cluster's own:

  * CAS is the real 64-bit value the cluster mints, not a stringified
    `META().cas`.
  * Binary documents are stored as bytes with their real common flags, via a
    pass-through transcoder -- no JSON wrapping.
  * Expiry, `touch`, counters and append/prepend are the real KV operations.
  * Errors come from real SDK exceptions (`DocumentExistsException`,
    `CasMismatchException`, ...) rather than being inferred.

Scope is deliberately limited to the endpoints Couchbase documents at
https://docs.couchbase.com/cloud/data-api-reference/. Nothing else is served:
inventing endpoints the real Data API lacks would make a harness that passes
against fiction. That is why there is no lock/unlock or replica-read route.

WHAT THIS DOES AND DOES NOT ESTABLISH: the store beneath it is real Couchbase,
so behaviour that depends on the cluster is genuine. The mapping from HTTP onto
those operations is this file's reading of the published reference, the same
reading the client under test holds. It cannot confirm that reading is right --
only a real Capella endpoint can.
"""

import base64
import json
import os
import sys
import threading
import traceback
import urllib.error
import urllib.request
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.durability import Durability, ServerDurability
from couchbase.exceptions import (
    AuthenticationException,
    CasMismatchException,
    CouchbaseException,
    DocumentExistsException,
    DocumentLockedException,
    DocumentNotFoundException,
    InvalidArgumentException,
    PathNotFoundException,
    TimeoutException,
    UnAmbiguousTimeoutException,
)
from couchbase.options import (
    ClusterOptions,
    DecrementOptions,
    GetOptions,
    IncrementOptions,
    InsertOptions,
    RemoveOptions,
    ReplaceOptions,
    TouchOptions,
    UpsertOptions,
)
from couchbase.transcoder import Transcoder

CONNSTR = os.environ.get("COUCHBASE_CONNSTR", "couchbase://127.0.0.1")
QUERY_URL = os.environ.get("COUCHBASE_QUERY_URL", "http://127.0.0.1:8093/query/service")

# The common-flags value every SDK writes for JSON. Used when a request carries
# no explicit `X-CB-Flags`, matching the reference's "values based on the
# Content-Type header".
JSON_FLAGS = 0x02000006

LOG_LOCK = threading.Lock()
REQUEST_LOG = []

_CLUSTERS = {}
_CLUSTER_LOCK = threading.Lock()


class PassthroughTranscoder(Transcoder):
    """Store and return `(bytes, flags)` exactly as given.

    The SDK's stock transcoders each pin the flags to one encoding. This server
    has to honour whatever flags the caller asked for -- that is the whole point
    of `X-CB-Flags` -- so it does no encoding of its own.
    """

    def encode_value(self, value):
        data, flags = value
        return data, flags

    def decode_value(self, value, flags):
        return value, flags


TRANSCODER = PassthroughTranscoder()


def cluster_for(username, password):
    """A connected `Cluster` per credential, cached.

    The Data API authenticates every request, so credentials are per-call rather
    than per-process; each distinct one gets its own authenticated connection.
    """
    key = (username, password)
    with _CLUSTER_LOCK:
        if key in _CLUSTERS:
            return _CLUSTERS[key]
    cluster = Cluster(CONNSTR, ClusterOptions(PasswordAuthenticator(username, password)))
    cluster.wait_until_ready(timedelta(seconds=15))
    with _CLUSTER_LOCK:
        _CLUSTERS[key] = cluster
    return cluster


def parse_go_duration(raw):
    """Parse the `Expires` header's Go duration form (`600s`, `5m`, `2h`).

    Returns a `timedelta`, or `None` for "no expiry". The reference also allows
    an HTTP date here; that form is not handled, and says so rather than
    silently storing the wrong TTL.
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    units = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1, "m": 60, "h": 3600}
    for suffix in ("ns", "us", "ms", "h", "m", "s"):
        if raw.endswith(suffix):
            number = raw[: -len(suffix)]
            try:
                seconds = float(number) * units[suffix]
            except ValueError:
                raise ValueError("not a Go duration: %r" % raw)
            return timedelta(seconds=seconds) if seconds > 0 else None
    raise ValueError("unsupported Expires value %r (Go duration only)" % raw)


DURABILITY = {
    "none": None,
    "majority": Durability.MAJORITY,
    "majorityandpersistonmaster": Durability.MAJORITY_AND_PERSIST_TO_ACTIVE,
    "persisttomajority": Durability.PERSIST_TO_MAJORITY,
}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass  # quiet; requests are recorded in REQUEST_LOG instead

    # -- plumbing --------------------------------------------------------

    def _credentials(self):
        header = self.headers.get("Authorization") or ""
        if not header.lower().startswith("basic "):
            return None
        try:
            decoded = base64.b64decode(header.split(None, 1)[1]).decode()
            username, _, password = decoded.partition(":")
            return username, password
        except Exception:
            return None

    def _body(self):
        """Read the request body, handling Content-Length and chunked framing.

        BaseHTTPRequestHandler does not decode chunked bodies, and leaving one
        unread desynchronizes the keep-alive connection.
        """
        encoding = (self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in encoding:
            chunks = []
            while True:
                line = self.rfile.readline().strip()
                if not line:
                    break
                try:
                    size = int(line.split(b";")[0], 16)
                except ValueError:
                    break
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.readline()
            return b"".join(chunks)
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _send(self, status, payload=None, raw=None, cas=None, flags=None, token=None):
        data = raw if raw is not None else (b"" if payload is None else json.dumps(payload).encode())
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        if cas is not None:
            # Capella sends the CAS as a bare, unquoted, 16-digit lower-case hex
            # ETag -- e.g. `18c86cb5894f0000` -- and rejects the quoted form.
            # Verified against a live endpoint.
            self.send_header("etag", "%016x" % cas)
        if token is not None:
            self.send_header("X-CB-MutationToken", token)
        if flags is not None:
            self.send_header("X-CB-Flags", str(flags))
        self.end_headers()
        if data:
            self.wfile.write(data)

    @staticmethod
    def _token_header(bucket, result):
        """`bucket:vbid:vbuuid-hex:seqno`, the form Capella returns."""
        token = getattr(result, "mutation_token", None)
        token = token() if callable(token) else token
        if token is None:
            return None
        # These are properties on the SDK's MutationToken, not methods.
        try:
            return "%s:%d:%x:%d" % (
                bucket,
                token.partition_id,
                token.partition_uuid,
                token.sequence_number,
            )
        except Exception:
            traceback.print_exc()
            return None

    @staticmethod
    def _parse_if_match(if_match):
        """Capella's `If-Match` is bare hex; it rejects a quoted value."""
        raw = if_match.strip()
        if raw.startswith('"'):
            raise InvalidArgumentException("Invalid etag format %r" % if_match)
        return int(raw, 16)

    def _error(self, status, code, message):
        self._send(status, {"code": code, "message": message})

    def _record(self, note=""):
        with LOG_LOCK:
            REQUEST_LOG.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "if_match": self.headers.get("If-Match"),
                    "expires": self.headers.get("Expires"),
                    "durability": self.headers.get("X-CB-DurabilityLevel"),
                    "flags": self.headers.get("X-CB-Flags"),
                    "content_type": self.headers.get("Content-Type"),
                    "content_length": self.headers.get("Content-Length"),
                    "transfer_encoding": self.headers.get("Transfer-Encoding"),
                    "authorization": bool(self.headers.get("Authorization")),
                    "note": note,
                }
            )

    def _route(self):
        """(kind, bucket, scope, collection, key, op, query)."""
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        parts = [p for p in parsed.path.split("/") if p]
        if parts[:1] == ["_p"]:
            return ("query", None, None, None, None, None, query)
        if parts == ["v1", "callerIdentity"]:
            return ("identity", None, None, None, None, None, query)
        if len(parts) >= 9 and parts[0] == "v1":
            op = parts[9] if len(parts) > 9 else None
            return ("doc", parts[2], parts[4], parts[6], unquote(parts[8]), op, query)
        return (None, None, None, None, None, None, query)

    def _collection(self, bucket, scope, collection):
        creds = self._credentials()
        if not creds:
            raise AuthenticationException("missing or malformed Authorization header")
        cluster = cluster_for(*creds)
        return cluster.bucket(bucket).scope(scope).collection(collection)

    def _flags(self):
        raw = self.headers.get("X-CB-Flags")
        if raw is None:
            return JSON_FLAGS
        try:
            return int(raw)
        except ValueError:
            raise InvalidArgumentException("X-CB-Flags must be an integer")

    def _durability(self):
        raw = (self.headers.get("X-CB-DurabilityLevel") or "").strip().lower()
        if not raw:
            return None
        if raw not in DURABILITY:
            raise InvalidArgumentException("unknown durability level %r" % raw)
        level = DURABILITY[raw]
        return ServerDurability(level) if level is not None else None

    def _expiry(self):
        return parse_go_duration(self.headers.get("Expires"))

    def _guard(self, fn):
        """Run `fn`, mapping SDK exceptions onto the documented error shapes."""
        try:
            return fn()
        except DocumentNotFoundException:
            self._error(404, "DocumentNotFound", "The document does not exist")
        except DocumentExistsException:
            self._error(409, "DocumentExists", "The document already exists")
        except CasMismatchException:
            self._error(409, "CasMismatch", "The specified CAS for the document did not match")
        except DocumentLockedException:
            self._error(409, "Locked", "The document is locked")
        except PathNotFoundException:
            self._error(404, "PathNotFound", "The path does not exist in the document")
        except AuthenticationException as e:
            self._error(401, "Unauthorized", str(e) or "authentication failed")
        except InvalidArgumentException as e:
            self._error(400, "InvalidArgument", str(e) or "invalid argument")
        except ValueError as e:
            self._error(400, "InvalidArgument", str(e))
        except (TimeoutException, UnAmbiguousTimeoutException):
            self._error(504, "Timeout", "The operation timed out")
        except CouchbaseException as e:
            self._error(502, "CouchbaseError", str(e))
        except Exception as e:  # noqa: BLE001 - a harness should say what broke
            traceback.print_exc()
            self._error(500, "InternalError", "%s: %s" % (type(e).__name__, e))

    # -- verbs -----------------------------------------------------------

    def do_GET(self):
        if self.path.startswith("/__log"):
            with LOG_LOCK:
                return self._send(200, list(REQUEST_LOG))
        kind, b, s, c, key, _op, query = self._route()
        self._record()
        if kind == "identity":
            creds = self._credentials()
            return self._send(200, {"user": creds[0] if creds else None})
        if kind != "doc":
            return self._error(404, "NotFound", "unknown endpoint")

        def run():
            collection = self._collection(b, s, c)
            project = query.get("project")
            if project:
                # A projection returns reconstructed JSON, so the stored bytes
                # and flags no longer describe it; report it as JSON.
                res = collection.get(key, GetOptions(project=project))
                return self._send(
                    200, raw=json.dumps(res.content_as[dict]).encode(),
                    cas=res.cas, flags=JSON_FLAGS,
                )
            res = collection.get(key, GetOptions(transcoder=TRANSCODER))
            data, flags = res.value
            return self._send(200, raw=data, cas=res.cas, flags=flags)

        self._guard(run)

    def do_POST(self):
        kind, b, s, c, key, op, _query = self._route()
        if kind == "query":
            return self._proxy_query()
        self._record(note=op or "insert")
        if kind != "doc":
            self._body()
            return self._error(404, "NotFound", "unknown endpoint")
        body = self._body()

        def run():
            collection = self._collection(b, s, c)
            if op == "touch":
                return self._touch(collection, key, body)
            if op in ("increment", "decrement"):
                return self._counter(collection, key, body, op)
            if op in ("append", "prepend"):
                fn = collection.binary().append if op == "append" else collection.binary().prepend
                res = fn(key, body)
                return self._send(200, {}, cas=res.cas)
            if op is not None:
                return self._error(404, "NotFound", "unknown document operation %r" % op)
            options = InsertOptions(transcoder=TRANSCODER)
            expiry, durability = self._expiry(), self._durability()
            if expiry:
                options["expiry"] = expiry
            if durability:
                options["durability"] = durability
            res = collection.insert(key, (body, self._flags()), options)
            return self._send(200, {}, cas=res.cas, token=self._token_header(b, res))

        self._guard(run)

    def do_PUT(self):
        kind, b, s, c, key, _op, _query = self._route()
        self._record(note="upsert/replace")
        if kind != "doc":
            self._body()
            return self._error(404, "NotFound", "unknown endpoint")
        # Drain before any early return: the client frames bodies chunked, and
        # leaving one unread desynchronizes the connection.
        body = self._body()
        if_match = (self.headers.get("If-Match") or "").strip()

        def run():
            collection = self._collection(b, s, c)
            expiry, durability = self._expiry(), self._durability()
            value = (body, self._flags())

            if if_match:
                # A concrete CAS, or `*` for "it must already exist" — both are
                # replace semantics; the SDK raises DocumentNotFound when absent.
                options = ReplaceOptions(transcoder=TRANSCODER)
                if expiry:
                    options["expiry"] = expiry
                if durability:
                    options["durability"] = durability
                if if_match != "*":
                    try:
                        options["cas"] = self._parse_if_match(if_match)
                    except ValueError:
                        raise InvalidArgumentException("Invalid etag format %r" % if_match)
                res = collection.replace(key, value, options)
            else:
                options = UpsertOptions(transcoder=TRANSCODER)
                if expiry:
                    options["expiry"] = expiry
                if durability:
                    options["durability"] = durability
                res = collection.upsert(key, value, options)
            return self._send(200, {}, cas=res.cas, token=self._token_header(b, res))

        self._guard(run)

    def do_DELETE(self):
        kind, b, s, c, key, _op, _query = self._route()
        self._record()
        if kind != "doc":
            self._body()
            return self._error(404, "NotFound", "unknown endpoint")
        self._body()
        if_match = (self.headers.get("If-Match") or "").strip()

        def run():
            collection = self._collection(b, s, c)
            options = RemoveOptions()
            durability = self._durability()
            if durability:
                options["durability"] = durability
            if if_match and if_match != "*":
                try:
                    options["cas"] = self._parse_if_match(if_match)
                except ValueError:
                    raise InvalidArgumentException("Invalid etag format %r" % if_match)
            res = collection.remove(key, options)
            return self._send(200, {}, cas=res.cas, token=self._token_header(b, res))

        self._guard(run)

    # -- operations ------------------------------------------------------

    def _touch(self, collection, key, body):
        spec = json.loads(body) if body else {}
        if "expiry" not in spec:
            raise InvalidArgumentException("expiry is required")
        expiry = spec["expiry"]
        # The documented body carries an ISO 8601 instant; the SDK takes a
        # duration, so convert against the server's own clock.
        from datetime import datetime, timezone

        when = datetime.fromisoformat(str(expiry).replace("Z", "+00:00"))
        delta = when - datetime.now(timezone.utc)
        if delta.total_seconds() <= 0:
            raise InvalidArgumentException("expiry is in the past")

        if spec.get("returnContent"):
            res = collection.get_and_touch(key, delta, TouchOptions(transcoder=TRANSCODER))
            data, flags = res.value
            return self._send(200, raw=data, cas=res.cas, flags=flags)
        res = collection.touch(key, delta)
        return self._send(200, {}, cas=res.cas)

    def _counter(self, collection, key, body, op):
        spec = json.loads(body) if body else {}
        delta = int(spec.get("delta", 1))
        initial = spec.get("initial")
        kwargs = {"delta": delta}
        if initial is not None:
            kwargs["initial"] = int(initial)
        binary = collection.binary()
        options = IncrementOptions(**kwargs) if op == "increment" else DecrementOptions(**kwargs)
        res = binary.increment(key, options) if op == "increment" else binary.decrement(key, options)
        return self._send(200, {"value": res.content}, cas=res.cas)

    def _proxy_query(self):
        """Forward the Query Service passthrough, as Capella's own does."""
        self._record(note="query")
        body = self._body()
        req = urllib.request.Request(QUERY_URL, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        auth = self.headers.get("Authorization")
        if auth:
            req.add_header("Authorization", auth)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data, status = resp.read(), resp.status
        except urllib.error.HTTPError as e:
            data, status = e.read(), e.code
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    server = ThreadingHTTPServer((host, port), Handler)
    print("data api on %s:%d -> %s (KV) / %s (query)" % (host, port, CONNSTR, QUERY_URL), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
