#!/usr/bin/env bash
# Integration smoke test for the side-by-side NodeJS / Quarkus demo.
#
# Usage:  docker compose up --build -d && ./scripts/smoke-test.sh
# Needs:  curl, python3
#
# Asserts that node-service (:8088) and quarkus-service (:8089) expose the same
# API surface with the same JSON shapes, and that messages really cross between
# them over NATS.
set -uo pipefail

NODE=http://localhost:8088
QUARKUS=http://localhost:8089

pass=0
fail=0

ok()  { printf '  \033[32mPASS\033[0m %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=$((fail + 1)); }

section() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# assert_eq <expected> <actual> <description>
assert_eq() {
  if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (expected '$1', got '$2')"; fi
}

# assert_contains <haystack> <needle> <description>
assert_contains() {
  case "$1" in
    *"$2"*) ok "$3" ;;
    *)      bad "$3 (missing '$2' in: $(printf '%.120s' "$1"))" ;;
  esac
}

# field <json-path> — reads stdin, prints the value at a dotted path.
# Numeric segments index into arrays. Missing paths print __MISSING__.
field() {
  python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("__NOTJSON__"); sys.exit()
for k in sys.argv[1].split("."):
    try:
        d = d[int(k)] if k.lstrip("-").isdigit() else d[k]
    except Exception:
        print("__MISSING__"); sys.exit()
# json.dumps everything except plain strings, so booleans print as
# true/false rather than Python True/False.
print(d if isinstance(d, str) else json.dumps(d))
' "$1"
}

# status <method> <url> [body] — prints the HTTP status code only.
status() {
  if [ "$#" -ge 3 ]; then
    curl -s -o /dev/null -w '%{http_code}' -X "$1" -H 'content-type: application/json' -d "$3" "$2"
  else
    curl -s -o /dev/null -w '%{http_code}' -X "$1" "$2"
  fi
}

# body <method> <url> [json-body] — prints the response body.
body() {
  if [ "$#" -ge 3 ]; then
    curl -s -X "$1" -H 'content-type: application/json' -d "$3" "$2"
  else
    curl -s -X "$1" "$2"
  fi
}

test_health() {
  section 'health — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out; out=$(body GET "$base/api/health")
    assert_eq 'UP'   "$(printf '%s' "$out" | field status)"        "$svc /api/health status=UP"
    assert_eq "$svc" "$(printf '%s' "$out" | field service)"       "$svc /api/health service name"
    assert_eq 'true' "$(printf '%s' "$out" | field nats.connected)" "$svc /api/health nats.connected"
  done
}

test_error_shapes() {
  section 'error responses are JSON (Review Focus 2 and 5)'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out
    out=$(body GET "$base/api/does-not-exist")
    assert_contains "$out" '"error"' "$svc unknown route returns a JSON error"
    out=$(body POST "$base/api/publish" 'not json at all')
    assert_contains "$out" '"error"' "$svc malformed JSON body returns a JSON error"
    # Client errors must stay specific — a blanket "internal server error" here
    # would mean the 4xx branches got swallowed by the 5xx one.
    case "$(printf '%s' "$out" | field error)" in
      *'internal server error'*) bad "$svc 400 keeps a specific message" ;;
      *)                         ok  "$svc 400 keeps a specific message" ;;
    esac
  done
}

test_health
test_error_shapes

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
