#!/usr/bin/env bash
#
# Runs one unmodified workload against BOTH implementations of
# `wasmcloud:couchbase@0.2.0` and shows that it cannot tell them apart.
#
#   - the `couchbase` plugin, over the Data API's HTTPS surface -- served here
#     by Couchbase's Cloud Native Gateway, the same gateway that fronts Capella
#   - the `couchbase-kv` plugin, over the binary KV protocol, with the official
#     Couchbase Rust SDK compiled to wasm and driven through wasi:sockets
#
# The workload component is not rebuilt between the two runs. Only the host's
# plugin declaration changes.
#
# Usage:
#   ./demo.sh                 # run both, diff them, tear down
#   ./demo.sh --keep          # leave the cluster up afterwards
#   WASH=/path/to/wash ./demo.sh
#   KV_TRANSPORT=loopback ./demo.sh    # KV over host.wasmcloud.internal
#
# Requires a `wash` built with `--features host-component-plugins`; released
# builds do not carry it.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="$HERE/scenario"
PLUGINS="$HERE/../.."
WASH="${WASH:-wash}"
KV_TRANSPORT="${KV_TRANSPORT:-lan}"
KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

DATAAPI_WASM="$PLUGINS/couchbase/target/wasm32-wasip2/release/couchbase_plugin.wasm"
KV_WASM="$PLUGINS/couchbase-kv/target/wasm32-wasip2/release/couchbase_kv_plugin.wasm"
CONFIG="$SCENARIO/.wash/config.yaml"
OUT="$(mktemp -d)"
DEVPID=""

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
note() { printf '    %s\n' "$*"; }
die()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

cleanup() {
  if [[ -n "$DEVPID" ]]; then kill "$DEVPID" 2>/dev/null || true; fi
  if [[ $KEEP -eq 0 ]]; then
    (cd "$HERE" && docker compose down -v >/dev/null 2>&1) || true
  else
    note "cluster left running; 'docker compose down -v' in $HERE stops it"
  fi
}
trap cleanup EXIT

# The build half of the workload's config, identical for both runs.
# `wasmcloud:couchbase@0.2.0` is not published, so it resolves from the repo.
write_base_config() {
  mkdir -p "$SCENARIO/.wash"
  printf '%s\n' \
    'version: 2.0.0' \
    'wit:' \
    '  sources:' \
    '    "wasmcloud:couchbase": ../../interface' \
    'build:' \
    '  command: cargo build --target wasm32-wasip2 --release' \
    '  component_path: target/wasm32-wasip2/release/cbtest.wasm' \
    > "$CONFIG"
}

# ---------------------------------------------------------------------------

step "Preflight"
command -v docker >/dev/null || die "docker is required"
command -v "$WASH" >/dev/null || die "wash not found; set WASH=/path/to/wash"
note "wash: $("$WASH" --version 2>&1 | head -1) at $(command -v "$WASH")"

# The demo addresses the cluster by LAN address, not 127.0.0.1: inside a guest
# 127.0.0.1 means the *virtual* network, so it would not reach a service
# published on the machine. Docker publishes on 0.0.0.0, so the LAN address does
# reach it, and it is an ordinary external address that plain egress permits.
HOSTADDR="$(ipconfig getifaddr en0 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}')"
[[ -n "$HOSTADDR" ]] || die "could not determine this machine's LAN address"
note "cluster will be addressed as $HOSTADDR"

step "Bringing up Couchbase Server, fronted by the Cloud Native Gateway"
note "CNG serves the Data API on :18008."
# Never swallow this: on failure `set -e` jumps straight to the cleanup trap,
# and a hidden error here looks exactly like a hang much later on.
# CNG_SAN puts the address the plugin will dial into CNG's certificate.
if ! (cd "$HERE" && CNG_SAN="IP:$HOSTADDR" docker compose up -d > "$OUT/compose.log" 2>&1); then
  sed 's/^/    /' "$OUT/compose.log"
  die "docker compose up failed"
fi

# CNG's image is FROM scratch, so it has no healthcheck; readiness is probed
# from here, through the same TLS the plugin will verify. It has to be the
# SQL++ passthrough specifically: CNG answers document reads well before it
# has loaded the cluster map, and until then `/_p/query` fails with "failed
# to select query endpoint".
cng_ready() {
  curl -fsS --max-time 5 --cacert "$HERE/tls/ca.crt" -u appuser:apppass123 \
    -H 'content-type: application/json' -d '{"statement":"SELECT 1"}' \
    "https://$HOSTADDR:18008/_p/query/query/service" >/dev/null 2>&1
}
deadline=$((SECONDS + 300))
until cng_ready; do
  if (( SECONDS > deadline )); then
    (cd "$HERE" && docker compose ps)
    docker logs cb-verify-cng 2>&1 | tail -5 | sed 's/^/    /'
    die "the Data API did not become ready within 300s"
  fi
  sleep 5
done
note "Couchbase and the Data API are ready, over TLS verified against tls/ca.crt"

step "Building both plugins"
build_plugin() {
  if ! (cd "$1" && "$WASH" build --skip-fetch > "$OUT/build-$2.log" 2>&1); then
    tail -20 "$OUT/build-$2.log" | sed 's/^/    /'
    die "$2 plugin build failed"
  fi
}
build_plugin "$PLUGINS/couchbase" couchbase
build_plugin "$PLUGINS/couchbase-kv" couchbase-kv
note "wasmcloud:wash is unpublished, hence --skip-fetch"
note "$(basename "$DATAAPI_WASM") $(du -h "$DATAAPI_WASM" | cut -f1)"
note "$(basename "$KV_WASM") $(du -h "$KV_WASM" | cut -f1)  <- embeds the Couchbase Rust SDK"

step "Resolving the workload's WIT dependencies"
# `wash dev` deliberately skips WIT fetching, and wit/deps/ is generated rather
# than committed, so a clean checkout needs this once.
write_base_config
(cd "$SCENARIO" && "$WASH" wit fetch >/dev/null 2>&1) || die "could not resolve the workload's WIT"
note "wasmcloud:couchbase from ../../interface, wasi p3 from the registry"

# ---------------------------------------------------------------------------
# One run: declare the plugin, start a dev host, drive the workload once.
# ---------------------------------------------------------------------------
run_scenario() {
  local label="$1" plugin_id="$2" wasm="$3" endpoint="$4" grants="$5" outfile="$6"

  write_base_config
  {
    # The Data API is HTTPS under a CA this stack generated; trusting it here
    # is what lets the plugin verify CNG instead of skipping verification.
    printf '%s\n' 'dev:' '  http_client_ca_paths:'
    printf '    - %s\n' "$HERE/tls/ca.crt"
    printf '%s\n' '  host_plugins:'
    printf '    - id: %s\n' "$plugin_id"
    printf '      file: %s\n' "$wasm"
    printf '%s\n' "$grants"
    printf '%s\n' '  host_interfaces:' \
                  '    - namespace: wasmcloud' \
                  '      package: couchbase' \
                  '      interfaces: [types, sqlpp-types, document, sqlpp]' \
                  '      version: "0.2.0"' \
                  '      config:'
    printf '        endpoint: %s\n' "$endpoint"
    printf '%s\n' '        bucket: testbucket' \
                  '        username: appuser' \
                  '        password: apppass123'
  } >> "$CONFIG"

  cd "$SCENARIO"
  # stdin from /dev/null: a backgrounded process that reads the terminal takes
  # SIGTTIN, which stops its whole process group.
  "$WASH" dev --non-interactive < /dev/null > "$OUT/$plugin_id.log" 2>&1 &
  DEVPID=$!
  cd "$HERE"

  # Wait for the ingress port, not for a request: GET / *runs* the scenario, so
  # probing with it would either time out or run everything twice.
  local ready=0 waited=0
  while (( waited < 300 )); do
    if ! kill -0 "$DEVPID" 2>/dev/null; then
      tail -15 "$OUT/$plugin_id.log" | sed 's/^/    /'
      die "$label: the dev host exited during startup"
    fi
    if (exec 3<>/dev/tcp/127.0.0.1/8000) 2>/dev/null; then ready=1; break; fi
    if (( waited % 30 == 0 )); then note "starting the dev host, building the workload... ${waited}s"; fi
    sleep 3
    waited=$((waited + 3))
  done
  if (( ready != 1 )); then
    tail -15 "$OUT/$plugin_id.log" | sed 's/^/    /'
    die "$label never became ready"
  fi

  # The ingress binds before the workload is deployed behind it, so the port
  # being open is not the same as the route existing — until it does, `/`
  # answers 404. Retry until it serves; the first success *is* the run, since
  # GET / executes the whole scenario.
  local attempts=0
  until curl -fsS --max-time 180 http://127.0.0.1:8000/ > "$outfile" 2>/dev/null; do
    attempts=$((attempts + 1))
    if (( attempts > 24 )); then
      tail -15 "$OUT/$plugin_id.log" | sed 's/^/    /'
      die "$label: the workload never served a request"
    fi
    sleep 5
  done

  kill "$DEVPID" 2>/dev/null || true
  wait "$DEVPID" 2>/dev/null || true
  DEVPID=""

  local ok bad
  ok="$(grep -c ': OK' "$outfile" || true)"
  bad="$(grep -cE 'UNEXPECTED|WRONG|FAIL' "$outfile" || true)"
  note "$label: $ok OK, $bad unexpected"
  if [[ "$bad" != "0" ]]; then
    grep -E 'UNEXPECTED|WRONG|FAIL' "$outfile" | sed 's/^/    /'
    die "$label had failures"
  fi
}

step "Run 1 of 2 — the same workload, over the Data API (HTTPS, via CNG)"
run_scenario "Data API" couchbase "$DATAAPI_WASM" "https://$HOSTADDR:18008" \
  "      allowedHosts: [\"$HOSTADDR:18008\"]" "$OUT/dataapi.txt"

step "Run 2 of 2 — the same workload, over the binary KV protocol"
if [[ "$KV_TRANSPORT" == "loopback" ]]; then
  # Needs wasmCloud#5577. No allowedHosts and no allowedIpNameLookups are
  # required: the *.wasmcloud.internal zone resolves inside the host, ahead of
  # the name allowlist, and the grant is checked at connect.
  note "addressing the cluster as host.wasmcloud.internal (needs wasmCloud#5577)"
  run_scenario "KV" couchbase-kv "$KV_WASM" "couchbase://host.wasmcloud.internal" \
    "      allowedHostLoopbackPorts: [\"11210\", \"8093\"]" "$OUT/kv.txt"
else
  run_scenario "KV" couchbase-kv "$KV_WASM" "couchbase://$HOSTADDR" \
    "      allowedHosts: [\"$HOSTADDR:11210\", \"$HOSTADDR:8093\"]
      allowedIpNameLookups: [\"*\"]" "$OUT/kv.txt"
fi

# ---------------------------------------------------------------------------

step "What the workload saw"
# CAS and mutation tokens are per-write, so they are masked before comparing.
mask() { sed 's/cas[0-9]*=[0-9]*/cas=./g; s/vbuuid=[0-9]*/vbuuid=./g; s/seq=[0-9]*/seq=./g' "$1"; }

# Compared with awk rather than diff: the group-format options that would do
# this in one call are GNU-only, and macOS ships BSD diff. Both files have one
# line per scenario step, in the same order, so a positional compare is exact.
compare() {
  awk -v mode="$1" '
    NR == FNR { a[FNR] = $0; next }
    {
      if ($0 == a[FNR]) { if (mode == "same") print "    " $0 }
      else if (mode == "diff") {
        printf "    Data API | %s\n         KV | %s\n\n", a[FNR], $0
      }
    }' <(mask "$OUT/dataapi.txt") <(mask "$OUT/kv.txt")
}

bold "Identical on both transports:"
compare same

echo
bold "Where the transports genuinely differ:"
note "The workload is unchanged. Each implementation reports what it can"
note "actually do, and the scenario asserts the answer is coherent either way."
echo
compare diff

step "Done"
note "Both implementations export the same WIT. The workload was byte-identical"
note "across both runs — only the host's plugin declaration changed."
note "Full output: $OUT"
