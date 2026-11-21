# Use `make test-all` in WSL/Linux/CI; native Windows without GCC/make can run `go test ./...`.
COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
# Extra `go test` flags, e.g. make test GOTESTFLAGS="-coverprofile=coverage.out -covermode=atomic" (used by CI).
GOTESTFLAGS ?=
DATABASE_URL ?= postgres://notif:notif@localhost:5432/notif?sslmode=disable

.PHONY: up down logs build test test-integration test-all cover cover-all lint fmt migrate-up migrate-down sqlc smoke drills load run-api run-worker invariants prom-snapshot ratecheck races chaos shutdown-test fuzz

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

# Busiest sliding 1s send window per channel since SINCE (RFC3339), from DB and provider clocks.
ratecheck:
	./scripts/ratecheck.sh --since=$(SINCE)

# Run Go built-in fuzzing across SMS segmenter, validator, cursor decoder, and template engine.
FUZZTIME ?= 5m
fuzz:
	go test -fuzz=FuzzSMSInfo -fuzztime=$(FUZZTIME) ./internal/domain
	go test -fuzz=FuzzValidateNotification -fuzztime=$(FUZZTIME) ./internal/domain
	go test -fuzz=FuzzDecodeCursor -fuzztime=$(FUZZTIME) ./internal/api
	go test -fuzz=FuzzTemplateEngine -fuzztime=$(FUZZTIME) ./internal/template

load:
	go run ./cmd/loadgen -n 2000

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
