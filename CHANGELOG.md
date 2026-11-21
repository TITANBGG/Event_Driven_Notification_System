# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). The release workflow publishes the section that matches
the pushed tag as the GitHub Release notes, so every `vX.Y.Z` tag needs a `## [X.Y.Z]` section here.

## [Unreleased]

## [1.1.0] - 2026-11-21

A test program aimed at breaking the system: fuzzing, race storms, chaos, constrained resources,
2 million rows, a 60-minute soak and a clean-room clone. It found 12 bugs, all fixed below; the 11
code bugs are pinned by regression tests, the line-ending one by the clean-room check. README section 10 opens with a summary table.

### Fixed

- Rate limit: with the token taken before `Claim`, a DB pool stall bunched sends to 107/s per channel.
  The token is now taken right before the provider call, and the claim is handed back without burning
  an attempt if the wait would outlive the lease.
- A short DB blip right after the provider accepted a send left the row `processing`, and the
  reconciler re-sent it. `InsertAttempt` and `MarkSent` now retry for up to 5 s; a lost lease is never retried.
- A fixed pool of 35 connections per worker exhausted PostgreSQL at 3 replicas. Pool size is now
  `DB_MAX_CONNS` (auto: API 20, worker 10).
- One 1,000-item batch produced a 28,013-span trace (~145 MB in Jaeger) and long runs filled Docker's
  memory. Batch items now start their own trace linked to the request span; Jaeger is capped
  (`MEMORY_MAX_TRACES=20000`, `mem_limit: 1536m`).
- The WebSocket hub dropped fast readers during batch bursts (fixed 64-slot buffers). Each client now
  has a queue that grows on demand and is dropped only when it stops making progress; events are
  encoded once.
- Workers could not dequeue while Redis was at `maxmemory` (`noeviction`): `LMOVE` is a `denyoom`
  command, so a full Redis with nothing in flight could never drain again. `dequeue.lua` now uses
  `RPOP` + `LPUSH` (integration scenario `s`).
- The reconciler's expired idempotency key cleanup scanned the whole key table on every batch
  (378 ms at 2.25M keys, growing with the table); it now probes the primary key (29 ms at any size;
  integration scenario `t`).
- `/api/v1/metrics` took 1.7 s at 2M rows; window counts and percentiles now use one scan per source
  (integration scenario `r`).
- Log masking revealed 10-character phone numbers in full and 8 of 9 characters of short push tokens;
  at least 3 characters are now always hidden (found by fuzzing).
- A very large `Retry-After` header overflowed to a negative delay, so the hint was ignored; it is now
  clamped and capped at `RETRY_AFTER_MAX` (found by fuzzing).
- A non-upgrade `GET /api/v1/ws` returned a plain-text `426`; it now returns `426 upgrade_required`
  with the standard error envelope (found by fuzzing).
- A Windows clone with `core.autocrlf=true` checked out every file with CRLF and failed `make lint`;
  `.gitattributes` now forces LF for all text files (found by the clean-room clone).

### Added

- `make verify` (`scripts/verify.sh`): one command, only Docker needed: lint, unit, integration,
  Compose stack up, smoke test and invariants, with a PASS/FAIL table.
- `make test-docker` (`scripts/test-docker.sh`): the Go tests and linters in the pinned `golang`
  image, for machines without Go, `make` or GCC.
- `DB_MAX_CONNS` setting and `db_pool_*` Prometheus metrics.
- Mock provider `POST /config` to change its fault profile at runtime without losing `/stats`.

### Tests

- `goleak` goroutine-leak check in every test package; 7 fuzz targets (~70M executions).
- Integration scenarios `r`, `s`, `t` (20 in total).
- Live scenario scripts, each failing when its triggering condition did not happen:
  - `races.sh`: 6 concurrent races.
  - `chaos.sh`: kills, Redis loss and a PostgreSQL stop under sustained load.
  - `shutdown.sh`: graceful stops and `--rolling` restarts.
  - `isolation.sh` and `priority.sh`.
  - `provider-timeout.sh`.
  - `stress.sh`: 64 MB Redis.
  - `soak.sh`: 60 minutes with provider outages and 50 WebSocket readers.
  - `ws-boundaries.sh`.
- `invariants.sh` (PostgreSQL-backed, linear-time provider stats parsing) and `ratecheck.sh` (busiest
  sliding 1 s window per channel).

### Documentation

- README section 10: test program summary, and results for every scenario above.
- Quick start says the default provider is the bundled mock and how to switch to webhook.site.
- Live EXT-1 run against webhook.site for all three channels (README section 11).
- Real screenshots of Jaeger, Swagger UI, the `/demo` page and Prometheus.

## [1.0.0] - 2026-11-21

First complete release of the Event-Driven Notification System for the Insider One Software Engineer
Assessment 2026: every functional requirement (FR-1 to FR-4), the external provider contract (EXT-1),
all deliverables (D-1 to D-6) and all six bonus features (B-1 to B-6). The requirement checklist is in
README section 13.

### Run it

```bash
git clone https://github.com/TITANBGG/Event_Driven_Notification_System.git
cd Event_Driven_Notification_System
docker compose up --build        # or: docker-compose up --build
```

No `.env` is needed and migrations run automatically. The API is at `http://localhost:8080`, Swagger UI
at `http://localhost:8081`, the live demo page at `http://localhost:8080/demo`, Jaeger at
`http://localhost:16686` and Prometheus at `http://localhost:9090`.

The image is also published as `ghcr.io/titanbgg/notification-system:1.0.0` (`linux/amd64`,
`linux/arm64`) and contains the `api`, `worker`, `mockprovider` and `healthcheck` binaries.

### Added

- **Notification API (FR-1).** Single and batch create (up to 1000 items, all-or-nothing, one
  transaction with `COPY`), status by ID with delivery attempts, status by batch with per-status counts,
  cancel for one notification or a whole batch, and listing with `status` / `channel` / `priority` /
  date-range filters and keyset pagination with opaque cursors.
- **Processing engine (FR-2).** Redis priority queues with a weighted 6:3:1 dequeue that cannot starve
  `low`, a distributed GCRA rate limiter (100 messages/s per channel shared by all worker replicas,
  fail-closed when Redis is unavailable), per-channel content validation including GSM-7 / UCS-2 SMS
  segmenting, and idempotency through `Idempotency-Key` with request hashing and original-response replay.
- **Delivery and retry (FR-3).** Provider outcome classification (transient / permanent), exponential
  backoff with full jitter and `Retry-After` support, atomic and fenced PostgreSQL state transitions,
  and a stable `Idempotency-Key: <notification id>` header on every provider attempt.
- **Observability (FR-4).** Prometheus `/metrics` on the API and the workers, a JSON snapshot at
  `/api/v1/metrics` (queue depths, 1m / 5m / 15m success and failure rates, latency percentiles),
  structured JSON logs with correlation IDs and masked recipients, and `/health`, `/health/live`,
  `/health/ready` probes.
- **Failure handling (B-1).** Per-channel circuit breaker that does not burn attempts while open,
  dead-letter queue with manual retry, a distributed reconciler that rebuilds Redis from PostgreSQL,
  an inflight sweeper, heartbeat-based recovery of crashed workers, and graceful shutdown.
- **Scheduled notifications (B-2)** through `scheduledAt`, cancellable until they are sent.
- **Templates (B-3)** with `{{variable}}` substitution, preview, soft delete and render-at-create.
- **WebSocket status stream (B-4)** at `/api/v1/ws` with Redis pub/sub fan-out across API replicas,
  plus the `/demo` page.
- **Distributed tracing (B-5)** with OpenTelemetry and Jaeger: API request -> PostgreSQL -> Redis ->
  worker -> provider in one trace.
- **CI/CD (B-6).** GitHub Actions for lint, race tests with coverage, integration tests
  (testcontainers), sqlc drift, migration up/down/up, OpenAPI lint, image build and Compose v2 + v1
  validation; tag pushes publish multi-arch images to GHCR and create this GitHub Release.
- **Deliverables.** OpenAPI 3.0.3 spec with examples (`api/openapi.yaml`, served at `/openapi.yaml`),
  versioned migrations, one-command test suite (`make test-all`), smoke script (`scripts/smoke.sh`),
  load generator (`cmd/loadgen`) and live failure drills (`scripts/drills.sh`, `make drills`).

### Verified

- **Load:** 10,000 notifications across three channels reached a terminal state in 36.8s at about
  95 sends/s per channel (limit 100/s), all `sent`, with no duplicate provider calls (README section 10).
- **Failure drills:** Redis down, PostgreSQL down, worker `SIGKILL` mid-batch, full provider outage and
  Redis `FLUSHALL` mid-batch were run against the live stack, and every notification was delivered
  (README section 5).
- **Coverage:** 78.3% of statements in `internal/...` with unit and integration tests combined.

### Known limitations

- Delivery is **at-least-once**. A provider timeout after the provider has accepted a message can cause
  a duplicate when the provider ignores `Idempotency-Key`, as `webhook.site` does.
- No authentication or multi-tenancy, one Redis instance, a circuit breaker per worker replica, and no
  Turkish national language shift table for SMS. See README section 12.

[Unreleased]: https://github.com/TITANBGG/Event_Driven_Notification_System/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/TITANBGG/Event_Driven_Notification_System/releases/tag/v1.0.0
