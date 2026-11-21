# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). The release workflow publishes the section that matches
the pushed tag as the GitHub Release notes, so every `vX.Y.Z` tag needs a `## [X.Y.Z]` section here.

## [Unreleased]

### Documentation

- Live EXT-1 run against webhook.site: one SMS, one email and one push notification were delivered on
  the first attempt with the exact `{"to","channel","content"}` body and `Idempotency-Key`,
  `X-Correlation-ID` and `traceparent` headers (README section 11).
- Real screenshots of the Jaeger trace UI (replacing a rendered drawing), Swagger UI, the `/demo` page
  and a Prometheus graph of the per-channel send rate under load (README sections 7, 8 and 10).

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
