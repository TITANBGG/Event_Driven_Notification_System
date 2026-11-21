#!/usr/bin/env bash
# Runs the Go checks inside the pinned golang image, so a machine with only Docker (no Go toolchain,
# no make, no GCC for -race) can run the same suite as CI.
#
#   ./scripts/test-docker.sh            unit + integration tests (default)
#   ./scripts/test-docker.sh unit       unit tests with -race (no extra containers)
#   ./scripts/test-docker.sh integration Testcontainers suite (starts PostgreSQL 16 + Redis 7 containers)
#   ./scripts/test-docker.sh lint       gofmt, go vet (both build tags) and golangci-lint (CI's version)
#
# Integration tests start sibling containers through the host's Docker socket, and reach them through
# host.docker.internal (Docker Desktop provides it; on Linux --add-host maps it to the host gateway).
# Module, build and linter caches live in named volumes, so only the first run downloads anything.
set -euo pipefail

GO_IMAGE="golang:1.27.4"
GOLANGCI_LINT_VERSION="v2.15.0" # keep in sync with .github/workflows/ci.yml
MODE="${1:-all}"

cd "$(dirname "$0")/.."
# Git Bash on Windows rewrites /src-like arguments into C:/... paths; pass the Windows path and stop it.
SRC="$(pwd -W 2>/dev/null || pwd)"
export MSYS_NO_PATHCONV=1

go_run() { # go_run <docker args...> -- <shell command>
  local args=()
  while [[ "$1" != "--" ]]; do args+=("$1"); shift; done
  shift
  docker run --rm -v "${SRC}:/src" -w /src \
    -v notif-gomod:/go/pkg/mod -v notif-gobuild:/root/.cache/go-build -v notif-gobin:/go/bin \
    "${args[@]}" "$GO_IMAGE" sh -ec "$*"
}

unit() {
  echo "=== unit tests (-race) ==="
  go_run -- 'go test -race ./...'
}

integration() {
  echo "=== integration tests (Testcontainers) ==="
  go_run -v /var/run/docker.sock:/var/run/docker.sock \
    -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal --add-host=host.docker.internal:host-gateway \
    -- 'go test -race -tags=integration ./internal/integration/...'
}

lint() {
  echo "=== lint: gofmt, go vet, golangci-lint ${GOLANGCI_LINT_VERSION} ==="
  # Built with this image's Go (as CI does): a prebuilt linter compiled with an older Go refuses this module.
  go_run -- "
    out=\$(gofmt -l .); if [ -n \"\$out\" ]; then echo \"gofmt needed:\"; echo \"\$out\"; exit 1; fi
    go vet ./... && go vet -tags=integration ./...
    golangci-lint version --short 2>/dev/null | grep -qx 'v\\?${GOLANGCI_LINT_VERSION#v}' ||
      go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}
    golangci-lint run ./... && golangci-lint run --build-tags=integration ./internal/integration/..."
}

case "$MODE" in
  unit) unit ;;
  integration) integration ;;
  lint) lint ;;
  all) unit && integration ;;
  *) echo "usage: $0 [unit|integration|lint|all]" >&2; exit 2 ;;
esac
