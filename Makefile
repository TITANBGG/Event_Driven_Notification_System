# Use `make test-all` in WSL/Linux/CI; native Windows without GCC/make can run `go test ./...`.
COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
# Extra `go test` flags, e.g. make test GOTESTFLAGS="-coverprofile=coverage.out -covermode=atomic" (used by CI).
GOTESTFLAGS ?=
DATABASE_URL ?= postgres://notif:notif@localhost:5432/notif?sslmode=disable

.PHONY: up down logs build test test-integration test-all test-docker verify cover cover-all lint fmt migrate-up migrate-down sqlc smoke drills load run-api run-worker invariants prom-snapshot ratecheck races chaos shutdown-test rolling-test isolation priority provider-timeout stress soak ws-test fuzz

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down --remove-orphans

logs:
	$(COMPOSE) logs -f

build:
	mkdir -p bin
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/mockprovider ./cmd/mockprovider
	go build -o bin/healthcheck ./cmd/healthcheck
	go build -o bin/loadgen ./cmd/loadgen

test:
	go test -race $(GOTESTFLAGS) ./...

test-integration:
	go test -race -tags=integration $(GOTESTFLAGS) ./...

test-all: test test-integration

# Same tests with only Docker installed (no Go, make or GCC needed on the host).
test-docker:
	./scripts/test-docker.sh

# One-command health check: lint, unit, integration, then the live stack (up, smoke, invariants).
verify:
	./scripts/verify.sh

# Unit-test coverage (no Docker): coverage.html + per-package table.
cover:
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	sh scripts/coverage-by-package.sh coverage.out

# Unit + integration coverage attributed to every internal package (-coverpkg), e.g. repository SQL paths.
cover-all:
	go test -race -tags=integration -covermode=atomic -coverpkg=./internal/... -coverprofile=coverage-all.out ./...
	go tool cover -html=coverage-all.out -o coverage-all.html
	sh scripts/coverage-by-package.sh coverage-all.out

lint:
	gofmt -l .
	go vet ./...
	golangci-lint run

fmt:
	gofmt -w .

migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down 1

sqlc:
	sqlc generate

smoke:
	./scripts/smoke.sh

# Live failure drills (Redis/PostgreSQL down, worker SIGKILL, provider outage, Redis FLUSHALL).
drills:
	./scripts/drills.sh

invariants:
	./scripts/invariants.sh

prom-snapshot:
	./scripts/prom-snapshot.sh once

races:
	./scripts/races.sh

chaos:
	./scripts/chaos.sh

shutdown-test:
	./scripts/shutdown.sh

rolling-test:
	./scripts/shutdown.sh --rolling

isolation:
	./scripts/isolation.sh

priority:
	./scripts/priority.sh

provider-timeout:
	./scripts/provider-timeout.sh

# Needs the stress profile: docker compose -f docker-compose.yml -f docker-compose.stress.yml up -d --wait
stress:
	./scripts/stress.sh

soak:
	./scripts/soak.sh

ws-test:
	./scripts/ws-boundaries.sh

# Busiest sliding 1s send window per channel since SINCE (RFC3339), from DB and provider clocks.
ratecheck:
	./scripts/ratecheck.sh --since=$(SINCE)

# Run Go built-in fuzzing: SMS segmenter, validator, cursor decoder, template engine, PII masking,
# Retry-After parsing and the HTTP API (no 5xx for any client input).
FUZZTIME ?= 5m
fuzz:
	go test -fuzz=FuzzSMSInfo -fuzztime=$(FUZZTIME) ./internal/domain
	go test -fuzz=FuzzValidateNotification -fuzztime=$(FUZZTIME) ./internal/domain
	go test -fuzz=FuzzDecodeCursor -fuzztime=$(FUZZTIME) ./internal/api
	go test -fuzz=FuzzTemplateEngine -fuzztime=$(FUZZTIME) ./internal/template
	go test -fuzz=FuzzMaskRecipientNeverRevealsTheRawValue -fuzztime=$(FUZZTIME) ./internal/observability
	go test -fuzz=FuzzParseRetryAfterIsNeverNegative -fuzztime=$(FUZZTIME) ./internal/provider
	go test -fuzz=FuzzAPINeverReturns5xxForClientInput -fuzztime=$(FUZZTIME) ./internal/api

load:
	go run ./cmd/loadgen -n 2000

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
