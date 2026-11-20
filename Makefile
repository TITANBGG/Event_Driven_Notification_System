# WHAT: Standardized developer and CI task runner supporting both `docker compose` and `docker-compose`.
# WHY: Provides single-command reproducibility for builds, unit/integration tests, coverage, linting,
#      migrations, sqlc codegen, smoke checks, and load generation (D-3, D-6).
# EXPECTED OUTPUT EXAMPLE (`make test`):
#   ok  github.com/TITANBGG/notification-system/internal/config         ...
#   ok  github.com/TITANBGG/notification-system/internal/observability  ...

COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
DATABASE_URL ?= postgres://notif:notif@localhost:5432/notif?sslmode=disable

.PHONY: up down logs build test test-integration test-all cover lint fmt migrate-up migrate-down sqlc smoke load run-api run-worker

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
	go test -race ./...

test-integration:
	go test -race -tags=integration ./...

test-all: test test-integration

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	go tool cover -func=coverage.out

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

load:
	go run ./cmd/loadgen -n 2000

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

