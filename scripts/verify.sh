#!/usr/bin/env bash
# One-command check that the project builds, passes its tests and works end to end, needing only
# Docker. Every step runs even if an earlier one fails, and a PASS/FAIL table is printed at the end.
#
#   ./scripts/verify.sh              lint, unit, integration, then the live stack: up, smoke, invariants
#   ./scripts/verify.sh --no-stack   only the Go checks (no docker compose stack)
#
# The live part starts (or reuses) the Compose stack with `docker compose up -d --build --wait` and
# leaves it running. Takes ~10 minutes on the first run (image builds, module downloads), ~5 after.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 2
API_URL="${API_URL:-http://127.0.0.1:8080}"
MOCK_URL="${MOCK_URL:-http://127.0.0.1:8090}"
export API_URL MOCK_URL
STACK=1
for arg in "$@"; do
  case "$arg" in
    --no-stack) STACK=0 ;;
    *) echo "usage: $0 [--no-stack]" >&2; exit 2 ;;
  esac
done
if [[ -n "${COMPOSE:-}" ]]; then read -r -a COMPOSE_CMD <<<"$COMPOSE"
elif docker compose version >/dev/null 2>&1; then COMPOSE_CMD=(docker compose)
else COMPOSE_CMD=(docker-compose); fi
LOG_DIR="$(mktemp -d)"
RESULTS=()
FAILED=0

# step <name> <command...>: runs the command with its output in a log file, records PASS/FAIL and time.
step() {
  local name="$1" t0 rc log
  log="${LOG_DIR}/$(echo "$1" | tr -c 'a-zA-Z0-9' '_').log"
  shift
  printf '[%s] %-28s ... ' "$(date +%H:%M:%S)" "$name"
  t0=$(date +%s)
  "$@" >"$log" 2>&1
  rc=$?
  if ((rc == 0)); then
    echo "PASS ($(($(date +%s) - t0))s)"
    RESULTS+=("$name|PASS|$(($(date +%s) - t0))s")
  else
    echo "FAIL ($(($(date +%s) - t0))s), last lines of ${log}:"
    tail -n 25 "$log" | sed 's/^/    /'
    RESULTS+=("$name|FAIL|$(($(date +%s) - t0))s")
    FAILED=$((FAILED + 1))
  fi
}

stack_up() { "${COMPOSE_CMD[@]}" up -d --build --wait; }

step "lint (gofmt, vet, golangci)" ./scripts/test-docker.sh lint
step "unit tests (-race)" ./scripts/test-docker.sh unit
step "integration tests" ./scripts/test-docker.sh integration
if ((STACK)); then
  step "compose stack up (healthy)" stack_up
  step "smoke test (every endpoint)" ./scripts/smoke.sh
  step "invariants" ./scripts/invariants.sh
fi

echo
echo "=== verify summary (logs: ${LOG_DIR}) ==="
for r in "${RESULTS[@]}"; do
  IFS='|' read -r name status took <<<"$r"
  printf '%-30s %-5s %s\n' "$name" "$status" "$took"
done
if ((FAILED)); then
  echo "${FAILED} step(s) failed"
  exit 1
fi
echo "all steps passed"
