# Use `make test-all` in WSL/Linux/CI; native Windows without GCC/make can run `go test ./...`.
COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
# Extra `go test` flags, e.g. make test GOTESTFLAGS="-coverprofile=coverage.out -covermode=atomic" (used by CI).
GOTESTFLAGS ?=
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
	go test -race $(GOTESTFLAGS) ./...

test-integration:
	go test -race -tags=integration $(GOTESTFLAGS) ./...

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
