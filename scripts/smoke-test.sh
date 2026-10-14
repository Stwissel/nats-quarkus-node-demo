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

test_subscriptions() {
  section 'subscriptions — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out id

    out=$(body POST "$base/api/subscriptions" '{"subject":"demo.smoke.>"}')
    id=$(printf '%s' "$out" | field id)
    assert_eq 'demo.smoke.>' "$(printf '%s' "$out" | field subject)" "$svc POST subscription returns the subject"
    assert_eq '0'            "$(printf '%s' "$out" | field received)" "$svc new subscription has received=0"
    assert_contains "$out" 'createdAt' "$svc subscription carries createdAt"

    out=$(body GET "$base/api/subscriptions")
    assert_contains "$out" 'demo.smoke.>' "$svc GET lists the new subscription"

    assert_eq '200' "$(status DELETE "$base/api/subscriptions/$id")" "$svc DELETE removes the subscription"

    # Review Focus 4 — unknown id must be a 404 JSON error.
    assert_eq '404' "$(status DELETE "$base/api/subscriptions/does-not-exist")" "$svc DELETE unknown id is 404"
    out=$(body DELETE "$base/api/subscriptions/does-not-exist")
    assert_contains "$out" '"error"' "$svc DELETE unknown id returns a JSON error"

    # Review Focus 1 — blank subject must be a 400, not a 500.
    assert_eq '400' "$(status POST "$base/api/subscriptions" '{"subject":"  "}')" "$svc blank subject is 400"
  done
}

test_sse() {
  section 'SSE feed — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local feed; feed=$(mktemp)

    curl -sN --max-time 6 "$base/api/events" > "$feed" &
    local pid=$!
    sleep 1
    body POST "$base/api/subscriptions" '{"subject":"demo.smoke.sse"}' > /dev/null
    sleep 1
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null

    assert_contains "$(cat "$feed")" '"kind":"hello"'      "$svc SSE sends a hello frame on connect"
    assert_contains "$(cat "$feed")" '"kind":"subscribed"' "$svc SSE announces new subscriptions"
    rm -f "$feed"
  done
}

test_publish() {
  section 'publish — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out

    out=$(body POST "$base/api/publish" '{"subject":"demo.events.smoke","payload":"hi","jetstream":false}')
    assert_eq 'demo.events.smoke' "$(printf '%s' "$out" | field subject)" "$svc core publish echoes the subject"

    out=$(body POST "$base/api/publish" '{"subject":"demo.events.smoke","payload":"hi","jetstream":true}')
    local seq; seq=$(printf '%s' "$out" | field seq)
    case "$seq" in
      ''|*[!0-9]*) bad "$svc JetStream publish returns a numeric seq (got '$seq')" ;;
      *)           ok  "$svc JetStream publish returns a numeric seq" ;;
    esac

    # Review Focus 1 — blank subject must be a 400.
    assert_eq '400' "$(status POST "$base/api/publish" '{"subject":"","payload":"x"}')" "$svc blank publish subject is 400"

    # Review Focus 3 — JetStream publish outside the DEMO stream must fail cleanly.
    assert_eq '502' "$(status POST "$base/api/publish" '{"subject":"outside.the.stream","payload":"x","jetstream":true}')" \
      "$svc JetStream publish to an unmanaged subject is 502"
  done
}

test_request() {
  section 'request/reply — crossing between the services'
  local out

  # node asks, quarkus answers
  out=$(body POST "$NODE/api/request" '{"subject":"demo.greet","payload":"Ada"}')
  assert_contains "$out" 'quarkus-service' 'node -> demo.greet is answered by quarkus-service'

  # quarkus asks, node answers
  out=$(body POST "$QUARKUS/api/request" '{"subject":"demo.echo","payload":"Ada"}')
  assert_contains "$out" 'node-service' 'quarkus -> demo.echo is answered by node-service'

  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    out=$(body POST "$base/api/request" '{"subject":"demo.work","payload":"job"}')
    assert_contains "$out" 'latencyMs' "$svc request reports latencyMs"
    assert_eq '504' "$(status POST "$base/api/request" '{"subject":"demo.nobody.listening","payload":"x"}')" \
      "$svc request with no responder is 504"
  done
}

test_health
test_error_shapes
test_subscriptions
test_sse
test_publish
test_request

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
