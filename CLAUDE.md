# CLAUDE.md — Event-Driven Notification System

## Purpose

The engineering specification for this service, written before the code and kept in sync with it.
Every design decision here must be production-grade, verifiable and defensible in review: clarity,
observability and explicit failure handling matter as much as throughput and correctness. Section 10
records decisions that were measured and settled; the README explains the reasoning for readers.

- **Module Path:** `github.com/TITANBGG/notification-system`

---

## 1. Assignment Scope & Requirement IDs (Insider One, Software Engineer Assessment 2026)

- **Project:** Event-Driven Notification System
- **Language:** Go `1.27.4` — pinned identically across `go.mod`, `Dockerfile`, and CI workflows.
- **Overview:** Design and implement a scalable notification system that processes and delivers messages through multiple channels (**SMS**, **Email**, **Push**). It must handle high throughput, ensure reliable delivery, and provide real-time status tracking.
- **Business Context:** Insider One sends **MILLIONS** of notifications daily. The system must handle burst traffic (flash sales, breaking news), retry failed deliveries intelligently, and provide visibility into delivery status for both internal teams (metrics, logs, traces) and API consumers (status API, WebSocket).

### Functional Requirements

- **FR-1 — Notification Management API**
  - **FR-1.1:** Create notification requests with `recipient`, `channel`, `content`, and `priority`.
  - **FR-1.2:** Batch creation (up to `1000` notifications per request).
  - **FR-1.3:** Query notification status by ID.
  - **FR-1.4:** Query notification status by batch ID.
  - **FR-1.5:** Cancel pending notifications.
  - **FR-1.6:** List notifications with filtering (`status`, `channel`, date range) and pagination.
- **FR-2 — Processing Engine**
  - **FR-2.1:** Process notifications asynchronously via queue workers.
  - **FR-2.2:** Rate limiting: maximum **100 messages per second PER CHANNEL**.
  - **FR-2.3:** Priority queue support (`high`, `normal`, `low`).
  - **FR-2.4:** Content validation (character limits, required fields).
  - **FR-2.5:** Idempotency support to prevent duplicate sends.
- **FR-3 — Delivery & Retry Logic:** Candidate-designed reliability engine (this is where the implementation must shine; see [Section 5](#5-delivery--retry-fr-3--failure-handling-b-1)).
- **FR-4 — Observability**
  - **FR-4.1:** Real-time metrics endpoint (queue depth, success/failure rates, latency).
  - **FR-4.2:** Structured logging with correlation IDs.
  - **FR-4.3:** Health check endpoint.

### External Provider Contract (`EXT-1`)

External provider = `webhook.site` (configurable via `PROVIDER_URL`; `cmd/mockprovider` mimics this contract for deterministic load/failure tests):

- **Request:**
  ```http
  POST https://webhook.site/{uuid}
  Content-Type: application/json

  {"to":"+905551234567","channel":"sms","content":"Your message"}
  ```
- **Expected Response (`HTTP 202`):**
  ```json
  {"messageId":"uuid-here","status":"accepted","timestamp":"ISO8601"}
  ```

### Bonus Features (Implement Strictly AFTER Core Is Complete)

- **B-1:** Failure Handling (circuit breaker, dead-letter queue, reconciler, manual replay)
- **B-2:** Scheduled Notifications (future delivery via `scheduledAt`)
- **B-3:** Template System with variable substitution
- **B-4:** WebSocket real-time status updates
- **B-5:** Distributed Tracing (OpenTelemetry + Jaeger)
- **B-6:** GitHub Actions CI/CD (automated testing, linting, migrations check, release)

### Deliverables (`D-1` … `D-6`)

- **D-1:** GitHub repository with clean commit history (Conventional Commits).
- **D-2:** `README.md`: setup instructions, architecture overview, API examples.
- **D-3:** Docker Compose one-command setup: **BOTH** `docker-compose up` and `docker compose up` must work with zero manual steps (no `.env` required, no pre-built images, migrations run automatically).
- **D-4:** Swagger/OpenAPI specification (`api/openapi.yaml` + Swagger UI).
- **D-5:** Versioned database migrations (`migrations/*.up.sql` / `.down.sql`).
- **D-6:** Test suite runnable with a single command (`make test-all` in WSL/Linux/CI; with only Docker: `./scripts/test-docker.sh`, or `./scripts/verify.sh` for lint + tests + live stack smoke + invariants; native Windows without GCC/make can also run `go test ./...`).

### Timeline & Non-Functional Targets

- **Timeline:** Expected 12 hours. **Execution order:** Core (`FR-*`, `EXT-1`, `D-*`) first in ~12h; bonuses (`B-1`…`B-6`) afterwards.
- **Throughput & Reliability Targets:** Millions/day (`≈ 50–500 msg/s` average, bursts capped at `100 msg/s/channel = 300 msg/s` total), **no lost notifications**, **no duplicate sends caused by our own bugs**, horizontal scaling of stateless workers.

---

## 2. Architecture (Decided: Option A = PostgreSQL + Redis)

### Core Architectural Principles

- **Source of Truth:** PostgreSQL is the **SOURCE OF TRUTH** for every notification, batch, delivery attempt, idempotency key, and template.
- **Redis Responsibilities:** Redis serves five strictly scoped roles:
  - `(a)` Priority dispatch queues (`LIST` per channel & priority)
  - `(b)` Delayed `ZSET` for backoff retries and scheduled sends
  - `(c)` Distributed GCRA rate limiter per channel
  - `(d)` Pub/Sub (`notif:events`) for WebSocket fan-out across API replicas
  - `(e)` Short-lived distributed reconciler lock (`SET NX PX`)
- **Delivery Guarantee:** **At-least-once**, made **effectively-once** in practice via:
  1. Idempotent API creation (`idempotency_keys` table + request hashing),
  2. Atomic, fenced state transitions in PostgreSQL,
  3. `Idempotency-Key: <notification id>` header sent to the external provider (stable across retries).
  - *Honesty Rule:* **Never claim exactly-once.** A provider timeout after the provider accepted the message can still cause a duplicate if the provider ignores `Idempotency-Key` (`webhook.site` does). Document this honestly in `README.md`.
- **Write Ordering Rule:** **ALWAYS write PostgreSQL first, then Redis.** Redis is only a "hint" of work to do; anything missing in Redis is repaired by the reconciler from PostgreSQL.
- **Idempotent Enqueue Rule:** An ID is considered **"tracked"** in Redis if and only if it exists in the `{notif}:msg` hash. `enqueue.lua` and `schedule.lua` are no-ops (or only update the `ZSET` score) for tracked IDs, allowing the reconciler to safely re-enqueue anything.
- **Reconciler vs. Outbox Trade-off:** The reconciler ([Section 5](#5-delivery--retry-fr-3--failure-handling-b-1)) replaces a full transactional outbox. `README.md` must document the trade-off:
  - *Transactional Outbox:* exact, requires extra outbox table + dedicated relay process.
  - *Reconciler:* simpler schema and operations, bounded repair delay (`≤ RECONCILE_INTERVAL + PENDING_STALE_AFTER`).
- **Alternatives to Document in `README.md` (Do Not Implement):**
  - **B) PostgreSQL + RabbitMQ:** native priority queues, Dead Letter Exchanges (DLX), per-message TTL.
  - **C) PostgreSQL-only with `FOR UPDATE SKIP LOCKED`:** simplest infra, DB becomes the throughput/polling bottleneck at scale.
  - **D) Kafka for very large scale:** no native priority (`→` topic per priority), ordering per partition, retry topics.
  - Include a comparison trade-off table and a **"How we would migrate to Kafka / scale to 100M/day"** section.

### Binaries

A single multi-stage `Dockerfile` builds all binaries (`CGO_ENABLED=0`, `-trimpath`, `-ldflags "-s -w"`; final stage `gcr.io/distroless/static-debian12:nonroot`):

| Binary | Responsibility |
|---|---|
| `cmd/api` | HTTP API + WebSocket + `/metrics` + queue-depth sampler (every 5s) + `/openapi.yaml` + `/demo` |
| `cmd/worker` | Queue consumers, delayed-set mover, reconciler, heartbeat, `/metrics` + health endpoints on `:9091` |
| `cmd/mockprovider` | Configurable fake provider (latency, jitter, failure rate, 429 rate, permanent rate, stats/reset) for load & failure tests; `webhook.site` remains selectable via `PROVIDER_URL` |
| `cmd/healthcheck` | Tiny static HTTP GET probe for Docker healthchecks (distroless has no `curl`/`wget`). Usage: `/app/healthcheck http://localhost:8080/health/ready` → exit `0` on `2xx`, `1` otherwise, `2s` timeout |
| `cmd/loadgen` | Go-based load generator (implemented in Phase 6, extended in Phase 13) |

### Docker Compose Services (`docker-compose.yml`)

- **Compatibility Rules:** File name `docker-compose.yml`, **NO** top-level `version:` key, every image tag **pinned**, **NO** `deploy.replicas` (legacy `docker-compose` ignores it).
- **Zero-Config Defaults:** All env vars define defaults inside compose via `${VAR:-default}`, so `.env` is optional. `.env.example` documents all variables.
- **Services:**
  - `postgres:16-alpine` (healthcheck: `pg_isready`)
  - `redis:7-alpine` (`--appendonly yes --appendfsync everysec`, healthcheck: `redis-cli ping`)
  - `migrate` (`migrate/migrate` pinned tag; mounts `./migrations`, runs `up` then exits; `api`, `worker-1`, `worker-2` depend on `service_completed_successfully`)
  - `api` (port `8080`; owns `build:` for `notification-system:local`)
  - `worker-1` and `worker-2` (identical template via YAML anchor `x-worker: &worker` + shared env anchor, distinct `WORKER_ID`; proves the distributed rate limiter across replicas)
  - `mockprovider` (port `8090`)
  - `jaeger` all-in-one (`16686` UI, `4318` OTLP HTTP; `MEMORY_MAX_TRACES` + `mem_limit` so in-memory traces cannot exhaust the host)
  - `prometheus` (`9090`)
  - `swagger-ui` (`8081`, `./api` mounted as `/spec`)

### Package Layout

```text
cmd/
  api/
  worker/
  mockprovider/
  healthcheck/
  loadgen/
internal/
  config/           # env-based config with defaults + startup validation
  domain/           # Notification, Channel, Priority, Status, state machine, validation, SMS segmenting, errors, request hashing
  repository/       # Postgres access (sqlc-generated code in internal/repository/sqlc + thin domain wrapper)
  queue/            # Redis priority queues, inflight lists, delayed set, DLQ, Lua scripts (embedded via go:embed)
  ratelimit/        # Redis GCRA limiter per channel
  provider/         # HTTP client to external provider + outcome classification
  worker/           # consumer loop, retry policy, circuit breaker, delayed mover, reconciler, heartbeat
  api/              # router, handlers, DTOs, pagination cursor, central error mapping
  api/middleware/   # recoverer, correlation id, request logger, body limit, timeout, CORS, API key auth + scopes
  template/         # safe template renderer (Phase 9)
  realtime/         # WebSocket hub + Redis pub/sub (Phase 10)
  observability/    # slog logger, Prometheus metrics, OpenTelemetry tracing
migrations/         # golang-migrate SQL files (000001_init.up.sql / .down.sql, ...)
db/queries/         # sqlc queries; sqlc.yaml at repo root
api/openapi.yaml    # OpenAPI 3.0.3 spec (kept in sync every phase)
deploy/             # prometheus.yml
scripts/            # smoke.sh (curl examples for every endpoint)
```

### Pre-Approved Libraries

- Routing: `github.com/go-chi/chi/v5`
- PostgreSQL driver & pool: `github.com/jackc/pgx/v5` (`pgxpool`)
- SQL codegen: `sqlc` (CLI tool)
- Migrations: `github.com/golang-migrate/migrate/v4`
- Redis & rate limiting: `github.com/redis/go-redis/v9`, `github.com/go-redis/redis_rate/v10`
- Circuit breaker: `github.com/sony/gobreaker/v2`
- IDs: `github.com/google/uuid` (UUIDv7 for time-ordered IDs)
- Metrics: `github.com/prometheus/client_golang`
- Logging: `log/slog` (stdlib JSON handler)
- Tracing: `go.opentelemetry.io/otel` (`+ otlptracehttp exporter, otelhttp`), `github.com/exaring/otelpgx`
- WebSockets: `github.com/coder/websocket`
- Testing: `github.com/testcontainers/testcontainers-go` (`+ postgres and redis modules`), `github.com/alicebob/miniredis/v2` (unit tests including Lua), `github.com/stretchr/testify` (`assert`/`require` only), `go.uber.org/goleak` (`TestMain` goroutine-leak check in every test package; approved 2026-11-21)
- **Dependency Policy:** Prefer the Go standard library otherwise. Ask before adding any other external dependency.

---

## 3. Domain Rules

### Core Types

- **Channels:** `sms`, `email`, `push`
- **Priorities:** `high`, `normal`, `low` (default `normal`; unknown value → validation error)
- **Statuses:**
  - `scheduled` = accepted, waiting for `scheduled_at` (Bonus `B-2`)
  - `pending` = accepted, waiting in a queue for a worker (**"pending"** in the PDF spec sense)
  - `processing` = claimed by a worker, provider call in progress
  - `retrying` = last attempt failed transiently, waiting for `next_attempt_at`
  - `sent` = provider accepted (**terminal**)
  - `failed` = permanent error or attempts exhausted (**terminal** unless manually retried)
  - `cancelled` = cancelled by API (**terminal**)
- **Cancellable ("pending" in `FR-1.5`):** `scheduled`, `pending`, `retrying`.
  - Attempting to cancel `processing`, `sent`, `failed`, or `cancelled` → `409 Conflict` including current status.

### State Machine (`internal/domain/statemachine.go`)

Allowed transitions are enforced in **ONE** place (`internal/domain/statemachine.go`), and SQL queries mirror the exact same `allowed_from` sets:

| From | Allowed To | Trigger / Guard |
|---|---|---|
| `scheduled` | `processing`, `cancelled` | Worker claim (`scheduled_at <= now + skew`) or API cancel |
| `pending` | `processing`, `cancelled` | Worker claim or API cancel |
| `retrying` | `processing`, `cancelled` | Worker claim (`next_attempt_at <= now + skew`) or API cancel |
| `processing` | `sent`, `retrying`, `failed`, `pending` | Worker fenced outcome (`sent`/`retrying`/`failed`) or Reconciler only after visibility timeout (`pending`) |
| `failed` | `pending` | Manual retry endpoint only (resets `attempts=0`, `deferrals=0`) |
| `sent` | *(none)* | Terminal |
| `cancelled` | *(none)* | Terminal |

- **Atomic Conditional Transition Rule:** Every transition in PostgreSQL uses:
  ```sql
  UPDATE ... SET status=$new ...
  WHERE id=$id AND status = ANY($allowed_from) [AND extra guards]
  RETURNING ...;
  ```
  Zero returned rows = transition rejected. This resolves cancel-vs-worker races cleanly: whichever `UPDATE` commits first wins; the other observes `0` rows.

### Claim & Fencing SQL (Exact Contracts)

- **Claim (only way a worker takes ownership — `attempts` increments HERE):**
  ```sql
  UPDATE notifications
  SET status='processing', attempts=attempts+1, processing_started_at=now(), updated_at=now()
  WHERE id=$1 AND (
        status='pending'
     OR (status='retrying'  AND next_attempt_at <= now() + $skew)
     OR (status='scheduled' AND scheduled_at    <= now() + $skew))
  RETURNING *;
  ```
  - `$skew = CLOCK_SKEW_TOLERANCE` (default `2s`), because the delayed mover uses the worker clock while Claim checks the DB clock.
- **Fencing (every update a worker makes AFTER Claim: `sent` / `retrying` / `failed` / circuit deferral):**
  ```sql
  WHERE id=$1 AND status='processing' AND attempts=$claimedAttempts
  ```
  - `0` rows updated → **"lease lost"** (the reconciler already reclaimed or failed the row): log `warn`, increment `lease_lost_total`, `ack` the Redis item, do nothing else.

### Validation Rules (Return ALL Field Errors at Once)

Return every validation error in a single response using dot/bracket paths such as `notifications[3].recipient`:

- **Common:**
  - `recipient` required (validated without silent whitespace trimming so leading/trailing spaces fail uniformly across all channels).
  - `content` required (non-empty after `strings.TrimSpace`) unless `templateId` is used (Phase 9).
  - `channel` required and known (`sms | email | push`); input normalized lowercase (`Normalize()`) before persistence and hashing.
  - `priority` known (`high | normal | low`) if present; normalized lowercase and defaults to `normal`.
  - Channel-inappropriate optional fields (`subject` on `sms`/`push`, `title` on `sms`/`email`) → `422 unsupported_field`.
- **SMS (`channel == "sms"`):**
  - `recipient`: E.164 regex `^\+[1-9]\d{7,14}$` (e.g., `"+905551234567"` valid; `"05551234567"`, `"+0555..."` invalid).
  - **Encoding:** `GSM-7` if every rune is in the GSM 03.38 basic set or extension table; otherwise `UCS-2`.
    - Extension table characters `^ { } \ [ ~ ] | €` and form feed (`\f`) cost **2 septets** (`ESC + char`).
    - Turkish characters: `ö ü Ö Ü Ç` **ARE** in GSM-7 basic; `ç ğ Ğ ı İ ş Ş` are **NOT** → message switches to `UCS-2`. *(Known limitation to document: Turkish national language shift table is not implemented.)*
    - `UCS-2` length is counted in **UTF-16 code units** (e.g., an emoji like `😀` counts as `2`).
  - **Segments:**
    - `GSM-7`: single part `≤ 160` septets; multipart `153` septets per part.
    - `UCS-2`: single part `≤ 70` units; multipart `67` units per part.
    - **Split safety:** Never split an `ESC` pair or a UTF-16 surrogate pair across segment boundaries.
  - **Maximum limit:** `10` segments (`= 1530` GSM-7 septets or `670` UCS-2 units).
    - Examples: `160` plain ASCII chars → `1` segment; `161` ASCII chars → `2` segments; `"Şifreniz: 1234"` → `UCS-2`, `1` segment; `71` UCS-2 units → `2` segments.
- **Email (`channel == "email"`):**
  - `recipient`: `net/mail.ParseAddress` must succeed **AND** the parsed address must equal the raw input (no display names like `"Ali <a@b.com>"`), max `254` chars.
  - `subject`: **OPTIONAL** (spec does not require it); if present max `200` chars, no `\r` or `\n` (header-injection guard).
  - `content`: max `100 KB` (`102400` bytes).
- **Push (`channel == "push"`):**
  - `recipient`: device token, `1..4096` chars, no whitespace.
  - `title`: optional, max `100` chars.
  - `content`: max `2000` chars.
  - Serialized provider payload (`{to, channel, content}` or with extended fields): max `4096` bytes.
- **`scheduledAt` (Phase 8):** RFC3339 timestamp, `> now - CLOCK_SKEW_TOLERANCE`, `<= now + SCHEDULE_MAX_AHEAD` (default `720h`).
- **Idempotency keys:** `1..255` printable ASCII characters.

### Idempotency (`FR-2.5`)

- **API Storage:** Table `idempotency_keys(key PK, request_hash, resource_type 'notification'|'batch', resource_id, response_status, created_at, expires_at)`, TTL `IDEMPOTENCY_TTL` (default `24h`).
  - Expired keys are deleted by the reconciler and atomically overwritten on insert (`ON CONFLICT (key) DO UPDATE ... WHERE expires_at <= now()`) even if cleanup has not run yet; after expiry, reusing the same key creates a new resource (documented).
  - Keys are global (no multi-tenant model; multi-tenancy would prefix keys with `tenant_id` — documented).
- **Single Create (`POST /api/v1/notifications`):** Optional `Idempotency-Key` header.
  - `request_hash` = SHA-256 hex of **canonical JSON** (sorted keys, lowercase normalized channel/priority, defaults applied e.g. omitted priority = `"normal"`, `scheduledAt` normalized to UTC) for both single and batch wrapper requests.
  - Same key + same hash → return **ORIGINAL** resource with **ORIGINAL** status code (`202`) + header `Idempotent-Replayed: true`.
  - Same key + different hash → `409 idempotency_conflict`.
  - **Concurrency:** Insert/reclaim the key row in the **SAME** transaction as the notification (`INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE expires_at <= now() RETURNING *`); the unique PK serializes concurrent duplicate transactions; 0 rows returned → rollback, re-read active key, then replay or return `409`.
- **Batch Create (`POST /api/v1/notifications/batch`):**
  - Optional `Idempotency-Key` header for the **WHOLE** batch (hash of the whole canonical normalized `BatchCreateInput`) → replay returns the same `batchId` and `notificationIds` (`Idempotent-Replayed: true`).
  - Optional per-item `idempotencyKey`:
    - Duplicates inside the same batch → `422 duplicate_idempotency_key` with conflicting item indexes.
    - An item key that already exists in `idempotency_keys` → **WHOLE** batch rejected with `409 item_idempotency_key_exists`, details list item index + existing notification ID (preserves all-or-nothing; clients retry whole batches using the batch-level header).
- **Worker Side:** Atomic claim prevents two workers from processing the same row concurrently; fenced updates prevent stale writes; `Idempotency-Key: <notification id>` is sent to the provider on every attempt.

### Batch Policy & Pagination

- **Batch Policy:**
  - `BATCH_MAX_ITEMS = 1000` (more → `422 batch_too_large`).
  - Body `> BATCH_MAX_BODY_BYTES` (`16 MB` = `16777216` bytes) → `413 payload_too_large` with hint to split.
  - **All-or-nothing validation** (`422` with per-index field errors).
  - Inserted in **ONE** transaction using `pgx.CopyFrom`.
  - Returns `202 {"batchId": ..., "count": ..., "notificationIds": [...]}`.
  - Document the alternative (`207 Multi-Status` partial success) in `README.md`.
- **Pagination:**
  - Keyset pagination on `(created_at DESC, id DESC)`.
  - Opaque cursor = `base64url(JSON {"t": created_at RFC3339Nano, "id": uuid})`.
  - Invalid cursor → `400 invalid_cursor`.
  - `limit`: default `50`, max `200` (`> 200` → `422`).
  - Date range filter on `created_at`: `from` inclusive, `to` exclusive, RFC3339.

---

## 4. Redis Data Model & Queues

All keys share the hash tag `{notif}` so every Lua script touches a **single hash slot** (Redis Cluster compatible if ever scaled to Cluster; still document in `README.md` that dynamic keys inside Lua are a Cluster caveat).

### Redis Keys & Channels

| Key / Channel | Type | Purpose & Lifecycle |
|---|---|---|
| `{notif}:q:{channel}:{priority}` | `LIST` | Queue of notification IDs (`LPUSH` in, `RPOP`+`LPUSH` to inflight out = FIFO) |
| `{notif}:msg` | `HASH` | `id -> meta JSON` (`{"id","channel","priority","correlationId","traceparent"}`); presence = **"tracked"** |
| `{notif}:inflight:{workerId}` | `LIST` | IDs a specific worker dequeued (atomically, inside `dequeue.lua`) and has not yet acked |
| `{notif}:hb:{workerId}` | `STRING` | Worker liveness heartbeat, `PX = HEARTBEAT_TTL` (`15s`), refreshed every `5s` |
| `{notif}:delayed` | `ZSET` | Member = **notification ID ONLY**, score = due time in Unix ms (retries + scheduled sends) |
| `{notif}:dlq` | `LIST` | IDs of failed notifications (`LTRIM 0 99999` caps at `100k`; PostgreSQL keeps full truth) |
| `{notif}:reconciler:lock` | `STRING` | `SET NX PX 30000` (ensures one reconciler cycle runs across replicas at a time) |
| `{notif}:rl:{channel}` | `redis_rate` | Managed by `go-redis/redis_rate` GCRA rate limiter |
| `notif:events` | `PUB/SUB` | Real-time status events channel for WebSocket fan-out (Phase 10) |

### Embedded Lua Scripts (`internal/queue/lua/*.lua`, `go:embed`, `redis.NewScript`)

1. `enqueue.lua(id, meta, queueKey)`: if `HEXISTS msg id` → return `0`; else `HSET msg id meta` + `LPUSH queueKey id` → return `1`.
2. `schedule.lua(id, meta, dueMs, [inflightKey])`: `HSET msg id meta`; `ZADD delayed dueMs id`; if `inflightKey` provided: `LREM inflightKey 1 id`.
3. `dequeue.lua(inflightKey, key1, key2, key3)`: for each key in the given order: `id = RPOP key`; if `id` → `LPUSH inflightKey id` and return `{id, HGET msg id}` (= `LMOVE RIGHT LEFT`, split so the script's first write is not `denyoom`: under `maxmemory` + `noeviction` Redis refuses a script whose first write is `denyoom` but lets a script that already wrote finish, so workers keep draining a full Redis; decided 2026-11-21 after the 64 MB stress test). Return `nil` if all empty. *(Non-blocking; caller waits with idle backoff `50ms → 1s` using `select` on `ctx.Done()` and a timer — never `time.Sleep`.)*
4. `ack.lua(inflightKey, id)`: `LREM inflightKey 1 id`; `HDEL msg id`.
5. `deadletter.lua(inflightKey, id)`: `LREM inflightKey 1 id`; `HDEL msg id`; `LPUSH dlq id`; `LTRIM dlq 0 99999`.
6. `requeue.lua(inflightKey)`: move every ID in `inflightKey` back to its `{notif}:q:{channel}:{priority}` (parsed from `{notif}:msg` meta) — used on graceful shutdown and for orphaned workers.
7. `move_due.lua(nowMs, limit=500)`: `ZRANGEBYSCORE delayed -inf nowMs LIMIT 0 limit`; for each ID: `ZREM delayed id`; `meta = HGET msg id`; if `meta == nil` → skip (cancelled & acked); `LPUSH` to its priority queue. Atomic in Lua, so safe with multiple worker replicas running the mover concurrently. **The mover does NOT touch PostgreSQL** (the worker `Claim` enforces the DB due time).
8. `force_enqueue.lua(id, meta, queueKey)`: unconditional `HSET msg id meta` + `LPUSH queueKey id` — reconciler only: step `(a)` for rows already filtered as untracked, step `(c)` after `processing -> pending` (the ID may still be tracked inside a live worker's inflight list).
9. `requeue_ids.lua(inflightKey, prefix, ids...)`: for each ID, only if `LREM inflightKey 1 id` removed it → `RPUSH` back to its queue (or `HDEL` on bad meta) — used by the live worker's inflight sweeper; an ID acked concurrently is never resurrected.
10. `unlock.lua(lockKey, ownerToken)`: `DEL` only if `GET lockKey == ownerToken` — releases the reconciler lease at the end of a cycle without deleting another replica's lease.

### Weighted Priority with Fall-Through (`FR-2.3`, Anti-Starvation)

- Each consumer goroutine cycles a deterministic **10-slot schedule**: `[h, h, h, h, h, h, n, n, n, l]` (**6:3:1**).
- For slot `s`, the key order passed to `dequeue.lua` places the slot's preferred priority first, then remaining priorities in `high > normal > low` order.
- **Guarantee:** `low` priority receives `≥ 10%` of slots under continuous `high`/`normal` contention and `100%` capacity when higher-priority queues are empty.

---

## 5. Delivery & Retry (`FR-3`) + Failure Handling (`B-1`)

### Worker Loop (per channel, `WORKER_CONCURRENCY_PER_CHANNEL` goroutines per replica)

1. `item := queue.Dequeue(ctx, channel, workerID)` (`RPOP`+`LPUSH` into worker's inflight list). If none → idle backoff (`select` on `ctx.Done()` / timer) → `continue`.
2. `n, ok := repo.Claim(ctx, id)` (atomically increments `attempts`).
   - If `!ok` (cancelled / already terminal / not due yet) → `queue.Ack` → `continue`.
   - On DB error → `queue.Schedule(id, now+1s, inflight)` (moves the item out of the inflight list and retries it via the delayed mover) → `continue`.
3. `limiter.Wait(drainCtx, channel)` — acquire the GCRA token **AFTER** `Claim`, immediately before the provider call, so the limiter spaces actual sends (decided 2026-11-21 after a stress test: with the token taken before `Claim`, a DB pool stall bunched sends to 107/s per channel with 6 replicas). The wait is bounded by `VISIBILITY_TIMEOUT - 2*PROVIDER_TIMEOUT - 5s` (≥ 10s with valid config, worst observed wait ~1.5s) so a claimed row is never held past its lease; on timeout or limiter outage the claim is handed back with `repo.DeferCircuitOpen` (`attempts-1`, `deferrals+1`, no attempt burned) + `queue.Schedule(now+1s)`.
4. `outcome := breaker[channel].Execute(provider.Send(sendCtx, n))` (`sendCtx` timeout = `PROVIDER_TIMEOUT`).
5. `repo.InsertAttempt(...)` — every attempt is recorded in `delivery_attempts`. `InsertAttempt` and `MarkSent` retry non-lease DB errors for up to `5s` (backoff `100ms → 1s`): after the provider accepted a message, a short DB blip must not leave the row in `processing` for the reconciler to re-send.
6. Outcome dispatch (all DB updates **fenced** on `WHERE id=$1 AND status='processing' AND attempts=$claimedAttempts`):
   - `Sent` → `repo.MarkSent` (`provider_message_id`, `sent_at`) → `queue.Ack`
   - `Transient && attempts < max_attempts` → `repo.ScheduleRetry` (`status='retrying'`, `next_attempt_at`, `last_error`) → `queue.Schedule(id, due, inflight)`
   - `Transient && attempts >= max_attempts` → `repo.MarkFailed` → `queue.DeadLetter`
   - `Permanent` → `repo.MarkFailed` → `queue.DeadLetter`
   - `CircuitOpen` (`ErrOpenState` / `ErrTooManyRequests`) → `repo.DeferCircuitOpen` (`status='retrying'`, `attempts=attempts-1`, `deferrals=deferrals+1`, `next_attempt_at=now + CB_OPEN_TIMEOUT + jitter(up to 5s)`) → `queue.Schedule`; `delivery_attempts` row has `outcome='circuit_open'`, `counted=false`. If `deferrals > MAX_CIRCUIT_DEFERRALS` (`120 ≈ 1h`) → `MarkFailed("circuit open too long")` + `DeadLetter`.
7. Publish status event (Phase 10) and update Prometheus metrics (Phase 6).

Each loop iteration is wrapped in `recover()` so any panic logs stack + `correlation_id`, increments `worker_panics_total`, and keeps the goroutine alive.

### Provider Outcome Classification

| Provider Result | Outcome | Rationale & Breaker Treatment |
|---|---|---|
| `202` (expected), `200`, `201` | **Sent** | Store `messageId`; non-202 `2xx` or missing/invalid body → log `warn`, still `Sent` |
| `3xx` (redirects are **NOT** followed) | **Permanent** | Misconfigured provider URL |
| `408` Request Timeout, `425` Too Early, `429` Too Many Requests | **Transient** | Honor `Retry-After` header (seconds or HTTP-date) |
| Other `4xx` (`400`, `401`, `403`, `404`, `405`, `409`, `410`, `413`, `415`, `422`, …) | **Permanent** | Retrying the exact same payload cannot succeed |
| `500`, `502`, `503`, `504`, and any other `5xx` | **Transient** | Honor `Retry-After` on `503` |
| Timeout, connection refused/reset, DNS error, `EOF` | **Transient** | Note: timeout may hide a successful send → possible duplicate, mitigated by `Idempotency-Key` |
| Request build failure (bad URL) / invalid TLS cert | **Permanent** | Configuration error |

- **Breaker Success/Failure Rule:** Only **Transient** outcomes count as circuit-breaker failures. **Permanent** outcomes (`4xx`) count as breaker successes (`IsSuccessful` = true), because a `400` indicates bad client input while the provider itself is healthy.
- **Breaker Exclusion Rule:** Only the worker's own drain/shutdown cancellation (`Send` returning a non-nil error) is excluded via `IsExcluded`. Provider HTTP timeouts are **Transient failures** even though they wrap `context.DeadlineExceeded` — excluding them would stop the breaker from ever opening on a slow provider (regression test: `TestChannelBreakerProviderTimeoutsTripButDrainAbortsDoNot`).

### Exponential Backoff with Full Jitter

- Formula (deterministic in tests via injected `rand` source and clock):
  $$\text{delay} = \max\Big(\text{RETRY\_MIN},\; \text{rand}\big[0,\, \min(\text{RETRY\_CAP},\; \text{RETRY\_BASE} \times 2^{\text{attempt}-1})\big)\Big)$$
- With defaults (`RETRY_MIN = 200ms`, `RETRY_BASE = 2s`, `RETRY_CAP = 5m`):
  - Upper bounds: attempt 1 `→ 2s`, attempt 2 `→ 4s`, attempt 3 `→ 8s`, attempt 4 `→ 16s`.
  - `MAX_ATTEMPTS = 5` allows `1` initial send + up to `4` retries.
- With `Retry-After`: `delay = max(delay, retryAfter)`, capped at `RETRY_AFTER_MAX` (`15m`).

### Circuit Breaker (Per Channel, Per Worker Replica)

- Library: `sony/gobreaker/v2`, instantiated per channel **in-memory per worker replica** (documented trade-off: no shared Redis breaker state; each replica trips independently — acceptable because all replicas hit the same external provider and observe the same outage).
- Settings:
  - `ReadyToTrip`: `ConsecutiveFailures >= 10` OR (`Requests >= 20` AND failure ratio `> 0.5`)
  - `Interval`: `60s`
  - `Timeout` (open duration): `30s`
  - `MaxRequests` (half-open probes): `3`
- `OnStateChange` → structured log + `circuit_breaker_state` gauge update.

### Reconciler (Worker Binary, Every `RECONCILE_INTERVAL = 15s`)

Guarded by distributed lock `{notif}:reconciler:lock` (`SET NX PX 30000`). Each step processes at most `1000` rows per cycle (keyset scan, up to 5 pages) to bound DB/Redis load; tracked checks use pipelined `HEXISTS`. Keyset cursors **persist across cycles** (in memory, per replica) and wrap to the start after the last page, so a large tracked backlog cannot hide untracked rows beyond one cycle's budget. Execution order is `(d)` first (so requeued orphans are tracked again before `(a)` runs), then `(a)`, `(b)`, `(c)`, `(e)`; the lease is released with `unlock.lua` at the end of the cycle:

- `(a)` `pending` rows with `updated_at < now - PENDING_STALE_AFTER` (`30s`) and **NOT tracked** in Redis → `force_enqueue.lua` (repairs DB-commit-ok / Redis-enqueue-failed or API crash).
- `(b)` `retrying` / `scheduled` rows due before `now + 5m`, `updated_at < now - 30s`, and **NOT tracked** → `schedule.lua` at their due timestamp (repairs Redis data loss).
- `(c)` `processing` rows with `processing_started_at < now - VISIBILITY_TIMEOUT` (`60s`):
  - If `attempts >= max_attempts` → `failed` (`"visibility timeout; worker presumed dead"`) + `DeadLetter`.
  - Else → transition `processing -> pending` + `force_enqueue.lua`.
  - Always insert `delivery_attempts` row with `outcome='abandoned'` and log `warn "worker likely crashed"`.
- `(d)` Orphan inflight lists: `SCAN {notif}:inflight:*`; if matching `{notif}:hb:{workerId}` key is missing → `requeue.lua` then `DEL`.

### Inflight Sweeper (Every Worker Replica, Every `RECONCILE_INTERVAL`)

Covers the gap the reconciler cannot see: a DB transition succeeded but the follow-up Redis write (`Schedule` / `Ack` / `DeadLetter`) failed, or a panic was recovered mid-item. The ID is then **tracked** in `{notif}:msg` (so `(a)`/`(b)` skip it) and parked in a **live** worker's inflight list (so `(d)` skips it). Each worker keeps an in-memory set of IDs it is actively handling; IDs in its own inflight list that are not active in **two consecutive sweeps** (covers the dequeue → mark-active window) are moved back with `requeue_ids.lua`. The normal path then converges: `Claim` succeeds, or `Claim` refuses → `Ack` untracks the ID → the reconciler repairs it from PostgreSQL.
- `(e)` Delete expired `idempotency_keys` in batches of `10000`.
- **Startup Config Guard:** `VISIBILITY_TIMEOUT >= 2*PROVIDER_TIMEOUT + 15s` — startup **fails** if violated (prevents reclaiming rows from slow-but-alive workers).

### Graceful Shutdown (Worker)

On `SIGINT` / `SIGTERM`: stop dequeuing immediately → allow in-flight provider sends to finish within `SHUTDOWN_TIMEOUT` (`15s`) → run `requeue.lua` for remaining items in this worker's inflight list → delete `{notif}:hb:{workerId}` → close DB and Redis pools cleanly.

### Failure Scenarios Matrix (Document in `README.md`)

| Scenario | What Happens | Recovery |
|---|---|---|
| Redis down during create | Row committed as `pending`, enqueue fails → still returns `202` (durable), logs `warn` | Reconciler `(a)` when Redis recovers; `/health/ready` returns `503` |
| Redis down for workers | Dequeue errors → idle backoff; limiter unavailable → **FAIL CLOSED** (no sends) to honor `100/s` cap | Resumes automatically on Redis recovery |
| Redis full (`maxmemory`, `noeviction`) | API enqueue fails with `OOM` → still `202` (row committed); dequeue and ack keep working (memory-neutral / freeing) | Queues drain and free memory; reconciler `(a)` re-enqueues the untracked rows |
| Redis loses last ~1s (`AOF everysec`) | Some IDs missing from queues / `delayed` ZSET | Reconciler `(a)` and `(b)` |
| PostgreSQL down | API create → `503`; worker `Claim` errors → `requeue` + backoff | Automatic on DB recovery |
| PostgreSQL refuses connections after the provider accepted a send | `InsertAttempt`/`MarkSent` retried for up to `5s` | If still failing: row stays `processing`, `(c)` resets it after `VISIBILITY_TIMEOUT` and it is re-sent (at-least-once duplicate) |
| Worker crash mid-send | Row stays `processing`; ID stays in dead worker's inflight list | `(d)` requeues ID; `(c)` resets row to `pending` after `VISIBILITY_TIMEOUT`; possible duplicate send (at-least-once) |
| Redis write fails after a DB transition (worker alive) | ID stays tracked and parked in the live worker's inflight list; reconciler `(a)`/`(b)`/`(d)` cannot see it | Inflight sweeper requeues it after two sweeps; `Claim` then delivers it or `Ack` untracks it for reconciler repair |
| API crash after commit, before enqueue | Row stays `pending`, untracked in Redis | Reconciler `(a)` |
| Provider slow / down | Timeouts → transient retries with backoff; breaker opens; no attempts burned while open | Half-open probes after `30s` |
| Provider `202` lost (timeout) | Treated as transient → retried with identical `Idempotency-Key` | Provider-side dedupe; otherwise duplicate (documented) |
| Client retries `POST` | `Idempotency-Key` replay returns original `202` | — |
| Cancel vs. worker race | Conditional `UPDATE`; whichever commits first wins | Worker skips cancelled row at `Claim` and acks Redis item |

### Distributed Rate Limiting (`FR-2.2`) & Little's Law Sizing

- **Algorithm:** GCRA via `go-redis/redis_rate/v10`, key `{notif}:rl:{channel}`, `Limit{Rate: 100, Period: time.Second, Burst: 1}`, shared across all worker replicas.
- **`Burst = 1` Math:** Requests are spaced `≥ 10ms` apart, guaranteeing any 1-second sliding window contains at most `100` sends (`+1` at exact bucket boundary).
- **`Wait(ctx, channel)`:** Loops on `Allow`; if denied, waits `RetryAfter` via `select` respecting `ctx.Done()`. Retries consume tokens too (they are real provider HTTP requests). Redis error → **fail closed**.
- **Little's Law Sizing:** $\text{Concurrency} \approx \text{Rate} \times p_{99}\text{ Latency}$. For `100 msg/s × 0.3s = 30` concurrent sends per channel total → default `WORKER_CONCURRENCY_PER_CHANNEL = 25` per replica × `2` replicas (`= 50` headroom). `REDIS_POOL_SIZE` default (`0 = auto`) = `3 * WORKER_CONCURRENCY_PER_CHANNEL + 20`.
- **PostgreSQL Pool Sizing:** every process opens its own `pgxpool`, so `replicas × worker pool + API pool ≤ max_connections (100) − margin`. `DB_MAX_CONNS` (`0 = auto`: API `20`, worker `10`) keeps 6 worker replicas + API at 80. A send holds a connection for a few ms only (300 sends/s × ~4 queries × ~5ms ≈ 6 busy connections cluster-wide); the former fixed worker pool of 35 exhausted PostgreSQL at 3 replicas. Beyond that, put PgBouncer (transaction mode) in front of PostgreSQL.

---

## 6. Observability (`FR-4`)

### Structured Logging (`slog` JSON to `stdout`)

- Every log line carries `correlation_id` (from `X-Correlation-ID` header, validated `≤ 128` chars matching `^[A-Za-z0-9._-]+$`, or generated UUIDv7), plus `notification_id`, `batch_id`, `channel`, `attempt`, `worker_id` when relevant, and `trace_id`/`span_id` (Phase 11).
- `correlation_id` is persisted on the notification/batch row, carried in Redis `{notif}:msg` meta, forwarded to the provider in `X-Correlation-ID`, and echoed in every API response header and JSON error body.
- **PII Hygiene:** NEVER log raw `content` or unmasked `recipient` (mask examples: `+90555***4567`, `a***@example.com`, masked device token).

### Prometheus `/metrics`

Exposed on a **custom** `prometheus.Registry` (never global default) at `api :8080/metrics` and `worker :9091/metrics`, including Go/process collectors:

- `notifications_created_total{channel, priority}` (Counter)
- `notifications_processed_total{channel, status}` (`status = sent | failed | retrying | cancelled`, Counter)
- `provider_requests_total{channel, code}` (Counter)
- `delivery_duration_seconds{channel}` (Histogram — provider HTTP call)
- `end_to_end_latency_seconds{channel}` (Histogram — `created_at` → `sent_at`)
- `queue_depth{channel, priority}` (Gauge — sampled every `5s` by the **API process ONLY**, avoiding duplicate series across worker replicas)
- `delayed_depth` (Gauge — sampled by API)
- `dlq_depth` (Gauge — sampled by API)
- `inflight_depth` (Gauge — sampled by API)
- `rate_limited_total{channel}` (Counter)
- `rate_limit_wait_seconds{channel}` (Histogram)
- `circuit_breaker_state{channel}` (Gauge: `0` closed, `1` half-open, `2` open)
- `retries_scheduled_total{channel}` (Counter)
- `lease_lost_total` (Counter)
- `reconciler_actions_total{action}` (Counter)
- `worker_panics_total` (Counter)
- `http_requests_total{route, method, code}` (Counter — `route` = chi route pattern, **never** raw URL path)
- `http_request_duration_seconds{route, method}` (Histogram)
- `http_auth_failures_total{reason}` (Counter — `reason = missing | invalid | forbidden`, never the key name)
- `db_pool_acquired_conns`, `db_pool_idle_conns`, `db_pool_total_conns`, `db_pool_max_conns` (Gauge) and `db_pool_empty_acquire_total` (Counter) — `pgxpool.Stat()` read at scrape time, api and worker
- **Cardinality Rule:** **NEVER** label metrics by `notification_id`, `batch_id`, `recipient`, or `correlation_id`.

### Real-Time JSON Snapshot (`GET /api/v1/metrics`)

Computed from PostgreSQL (`sent_at` / `failed_at` partial indexes, `percentile_cont` over the last `15m`, protected by a `2s` in-memory cache) + live Redis queue lengths. Explain this design in `README.md`.

```json
{
  "generatedAt": "2026-11-21T10:00:00Z",
  "queues": {
    "sms": {"high": 12, "normal": 340, "low": 5},
    "email": {"high": 0, "normal": 10, "low": 2},
    "push": {"high": 4, "normal": 22, "low": 0},
    "delayed": 17,
    "dlq": 3,
    "inflight": 40
  },
  "windows": {
    "1m": {"sms": {"sent": 5800, "failed": 12, "retrying": 40, "successRate": 0.998, "failureRate": 0.002}},  // rates are null when a channel had no sent or failed notification in the window
    "5m": {},
    "15m": {}
  },
  "latencyMs": {
    "endToEnd": {"p50": 420, "p95": 1800, "p99": 4200},
    "providerCall": {"p50": 95, "p95": 310, "p99": 800}
  }
}
```

### Health Checks (`FR-4.3`)

- `GET /health` — alias of `/health/ready`
- `GET /health/live` — process alive → `200 {"status":"ok"}`
- `GET /health/ready` — PostgreSQL ping (`1s` timeout) + Redis `PING` (`1s` timeout); `503` if any dependency fails:
  ```json
  {
    "status": "ok",
    "checks": {
      "postgres": {"status": "ok", "latencyMs": 2},
      "redis": {"status": "ok", "latencyMs": 1}
    }
  }
  ```

---

## 7. HTTP API Contract

- **Base Path:** `/api/v1` (except root probes `/health*`, `/metrics`, `/openapi.yaml`, `/demo`).
- **Conventions:** JSON `camelCase`, timestamps `RFC3339` UTC, IDs `UUIDv7`. Unknown JSON fields rejected (`400`, via `DisallowUnknownFields`). Responses MUST map through explicit public DTOs (never serialize `domain.Notification` directly so internal fields like `processingStartedAt`, `traceparent`, `idempotencyKey`, and `deferrals` stay private).

### Authentication & Authorization (added 2026-11-22 after a security review)

- `API_KEYS` = `name:secret:scope1|scope2,...`; scopes `read`, `write`, `admin` (admin implies all). Empty → auth disabled + startup `WARN`; `docker-compose.yml` sets local-dev keys so the default stack is never open.
- Clients send `Authorization: Bearer <secret>`; browsers open `/api/v1/ws` with subprotocols `["bearer", "<secret>"]` and the server selects only `bearer` (no query-string tokens).
- Every `/api/v1` route requires a scope: `read` = GET routes, template preview, `/ws`, `/api/v1/metrics`; `write` = create/cancel/template mutations; `admin` = manual `/retry`. Probes, `/metrics`, `/openapi.yaml`, `/demo` stay public.
- `401 unauthorized` (+ `WWW-Authenticate`) / `403 forbidden` in the standard error envelope. Only SHA-256 digests of secrets are held; lookup is constant-time over all keys; logs carry `api_key_name`, never the secret.
- No tenant isolation yet (documented next step).

### Endpoints

| Method & Path | Response & Behavior |
|---|---|
| `POST /api/v1/notifications` | `202` resource \| `400` malformed JSON / unknown fields \| `415` wrong `Content-Type` \| `422` validation \| `409` idempotency conflict \| `413` body too large |
| `POST /api/v1/notifications/batch` | `202 {"batchId","count","notificationIds"}` \| `422` `batch_too_large` / validation / duplicate item key \| `409` conflict \| `413` `payload_too_large` |
| `GET /api/v1/notifications/{id}` | `200` resource + `"deliveryAttempts":[...]` (alongside integer `"attempts"`) \| `400` bad UUID \| `404` not found |
| `GET /api/v1/notifications` | Filters: `?status=pending,failed&channel=sms&priority=high&from=&to=&cursor=&limit=` → `200 {"data":[...],"nextCursor":"..." \| null}` |
| `POST /api/v1/notifications/{id}/cancel` | `200` updated resource \| `409 {"error":{...,"details":[{"field":"status","code":"current_status","message":"sent"}]}}` \| `404` |
| `POST /api/v1/notifications/{id}/retry` | `failed → pending` (manual DLQ replay; resets `attempts=0`, `deferrals=0`, clears `last_error`) → `200` \| `409` otherwise \| `404` |
| `GET /api/v1/batches/{id}` | `200 {"batchId","total","countsByStatus":{"pending":10,"sent":990,...},"createdAt","items":[...],"nextCursor"}` |
| `POST /api/v1/batches/{id}/cancel` | Cancels every cancellable item in one atomic `UPDATE` → `200 {"batchId","cancelled":N,"notCancellable":M}` |
| `GET /metrics` | Prometheus text exposition format |
| `GET /api/v1/metrics` | Human-readable JSON snapshot (queues, 1m/5m/15m windows, latency percentiles) |
| `GET /health`, `/health/live`, `/health/ready` | Liveness and dependency readiness probes (`200` / `503`) |
| `GET /openapi.yaml` | Serves `api/openapi.yaml` (embedded via `go:embed`) |
| **Bonus Routes** | `/api/v1/templates` (+ `/{id}`, `/{id}/preview`), `/api/v1/ws` (WebSocket), `/demo` (static HTML via `go:embed`) |

### Example Request & Response (`POST /api/v1/notifications`)

**Request:**
```http
POST /api/v1/notifications
Content-Type: application/json
Idempotency-Key: order-42-shipped
X-Correlation-ID: 7d3c...

{"recipient":"+905551234567","channel":"sms","content":"Siparişiniz kargoya verildi","priority":"high"}
```

**Response (`202 Accepted`):**
```json
{
  "id": "01936f7e-...",
  "batchId": null,
  "recipient": "+905551234567",
  "channel": "sms",
  "priority": "high",
  "status": "pending",
  "content": "Siparişiniz kargoya verildi",
  "attempts": 0,
  "maxAttempts": 5,
  "scheduledAt": null,
  "nextAttemptAt": null,
  "providerMessageId": null,
  "lastError": null,
  "createdAt": "...",
  "updatedAt": "...",
  "sentAt": null,
  "cancelledAt": null
}
```

### Standard Error Format (ALL Errors)

```json
{
  "error": {
    "code": "validation_failed",
    "message": "request has 2 invalid fields",
    "details": [
      {"field": "recipient", "code": "invalid_e164", "message": "must match ^\\+[1-9]\\d{7,14}$"},
      {"field": "content", "code": "too_many_segments", "message": "12 segments (UCS-2), max 10"}
    ]
  },
  "correlationId": "7d3c..."
}
```

### CORS Policy

- Allow origins configured via `CORS_ALLOWED_ORIGINS` (default `http://localhost:8081` so Swagger UI **"Try it out"** works cleanly).
- Expose `Idempotent-Replayed` and `X-Correlation-ID` headers.

---

## 8. Configuration (Environment Variables, All with Defaults)

| Variable | Default | Notes / Validation |
|---|---|---|
| `HTTP_ADDR` | `:8080` | API HTTP bind address |
| `WORKER_METRICS_ADDR` | `:9091` | Worker health & `/metrics` HTTP bind address |
| `DATABASE_URL` | `postgres://notif:notif@postgres:5432/notif?sslmode=disable` | Mask password in all logs |
| `REDIS_ADDR` | `redis:6379` | Redis `host:port` |
| `REDIS_POOL_SIZE` | `0` (`auto`) | `0` resolves to `3 * WORKER_CONCURRENCY_PER_CHANNEL + 20` |
| `DB_MAX_CONNS` | `0` (`auto`) | PostgreSQL pool per process; `0` = API `20`, worker `10`; `0..500`. Keep `replicas × pool + API ≤ max_connections` |
| `PROVIDER_URL` | `http://mockprovider:8090/` | Switch to `https://webhook.site/<uuid>` for live `EXT-1` proof |
| `PROVIDER_TIMEOUT` | `10s` | Per-attempt HTTP request context timeout |
| `PROVIDER_EXTENDED_FIELDS` | `false` | When `true`, adds optional `subject`/`title` to provider JSON |
| `RATE_LIMIT_PER_SEC` | `100` | Must be `> 0`; per-channel GCRA rate limit |
| `RATE_LIMIT_BURST` | `1` | Strict `10ms` pacing between sends per channel |
| `WORKER_ID` | `(hostname)` | Defaults to `os.Hostname()` when unset |
| `WORKER_CONCURRENCY_PER_CHANNEL` | `25` | Must be `> 0`; `25 × 2 replicas = 50` concurrent per channel |
| `MAX_ATTEMPTS` | `5` | Must be `>= 1` (`1` initial attempt + up to `4` retries) |
| `RETRY_BASE` | `2s` | Exponential backoff base |
| `RETRY_CAP` | `5m` | Exponential backoff cap |
| `RETRY_MIN` | `200ms` | Full-jitter floor |
| `RETRY_AFTER_MAX` | `15m` | Upper bound for honored `Retry-After` headers |
| `CB_CONSECUTIVE_FAILURES` | `10` | Consecutive transient failures to trip breaker |
| `CB_MIN_REQUESTS` | `20` | Minimum requests in interval before ratio trip applies |
| `CB_FAILURE_RATIO` | `0.5` | Transient failure ratio (`> 0.5`) to trip breaker |
| `CB_INTERVAL` | `60s` | Cyclic window to clear closed-state counts |
| `CB_OPEN_TIMEOUT` | `30s` | Open-state duration before half-open probes |
| `CB_HALF_OPEN_MAX` | `3` | Max probe requests in half-open state |
| `MAX_CIRCUIT_DEFERRALS` | `120` | Max circuit deferrals (`120 ≈ 1h`) before failing permanently |
| `VISIBILITY_TIMEOUT` | `60s` | Must satisfy `VISIBILITY_TIMEOUT >= 2*PROVIDER_TIMEOUT + 15s` |
| `RECONCILE_INTERVAL` | `15s` | Distributed reconciler cycle cadence |
| `PENDING_STALE_AFTER` | `30s` | Age before untracked `pending` row is re-enqueued |
| `DELAYED_MOVER_INTERVAL` | `500ms` | Cadence for moving due ZSET IDs to priority queues |
| `HEARTBEAT_INTERVAL` | `5s` | Worker heartbeat refresh cadence |
| `HEARTBEAT_TTL` | `15s` | Worker heartbeat key expiration (`PX`) |
| `CLOCK_SKEW_TOLERANCE` | `2s` | `$skew` tolerance between worker/API clock and Postgres `now()` |
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful drain window on `SIGINT`/`SIGTERM` |
| `MAX_BODY_BYTES` | `524288` | Single-request JSON limit (`512 KB`) |
| `BATCH_MAX_BODY_BYTES` | `16777216` | Batch-request JSON limit (`16 MB`) |
| `BATCH_MAX_ITEMS` | `1000` | Must be `<= 1000` |
| `PAGE_LIMIT_DEFAULT` | `50` | Default list/batch page size |
| `PAGE_LIMIT_MAX` | `200` | Maximum allowed `limit` query param |
| `IDEMPOTENCY_TTL` | `24h` | Expiration window for API `idempotency_keys` rows |
| `SCHEDULE_MAX_AHEAD` | `720h` | Maximum future scheduling horizon (`30` days) |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:8081` | Comma-separated origins (Swagger UI default) |
| `API_KEYS` | *(empty = auth off)* | `name:secret:scope1\|scope2` list; scopes `read`/`write`/`admin`; secrets 16..256 RFC 7230 token chars; compose default: local-dev admin + read keys |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4318` | OTLP HTTP collector endpoint (empty disables exporter) |
| `OTEL_SAMPLER_RATIO` | `1.0` | ParentBased trace sampling ratio |
| `LOG_LEVEL` | `info` | `debug \| info \| warn \| error` |

*(Note: Bonus Phase 10 adds `MAX_WS_CLIENTS=1000` for WebSocket connection capping.)*

---

## 9. Code Conventions

- **Context & State:** `context.Context` is the **first parameter** of every I/O function. **No global mutable state**; inject all dependencies explicitly via `New...` constructors.
- **Errors:** Wrap contextual errors with `fmt.Errorf("doing x: %w", err)`. Domain errors are strongly typed (`ErrNotFound`, `ErrInvalidTransition`, `ErrIdempotencyConflict`, `ErrLeaseLost`, `ValidationError`) and mapped to HTTP status codes in **ONE** place (`internal/api/errors.go`).
- **Interfaces:** Declare interfaces where they are **consumed** (consumer package), enabling lightweight fakes in unit tests.
- **Testing:** Table-driven unit tests everywhere. Unit tests must **not** require Docker (`miniredis` allowed); integration tests use build tag `//go:build integration` and `testcontainers-go`.
- **Comments:** Comments explain **WHY** (non-obvious invariants, concurrency/fencing rationale), not what the code literally does.
- **Formatting & Linting:** Code must be `gofmt`'d / `goimports`'d and pass `go vet ./...` and `golangci-lint` (v2).
- **Concurrency & Shutdown:** Graceful shutdown everywhere via `signal.NotifyContext`; drain in-flight work within `SHUTDOWN_TIMEOUT`. **No `time.Sleep` in production code paths** — always use `select` with `ctx.Done()` and timers.
- **Time & SQL:** **All times in UTC.** **All SQL through `sqlc`.** Zero string-concatenated SQL anywhere.

---

## 10. Decisions Already Made (Do Not Re-Open)

- **Status & Channel Columns:** PostgreSQL `text` + `CHECK` constraints (not PostgreSQL `ENUM`) — adding a channel like `whatsapp` later is a clean migration without `ALTER TYPE ... ADD VALUE` transaction caveats.
- **`updated_at` Maintenance:** Maintained automatically by a PostgreSQL trigger `set_updated_at()` (invariant holds even for manual SQL); application code still never relies on the client clock for state ordering.
- **Redis Unit vs. Integration Tests:** Unit tests use `alicebob/miniredis/v2` (supports Lua); integration tests use real `redis:7-alpine` via `testcontainers-go`.
- **Load Generator:** Custom Go program `cmd/loadgen` (no `k6`); `cmd/wsflood` drives the WebSocket flood and API boundary tests.
- **WebSocket Backpressure:** per-client queue that grows on demand; a client is dropped when > 1,024 frames are queued and it delivered none for 5s, or when > 8,192 are queued; events are encoded once and shared by all clients (decided 2026-11-21: a fixed 64/1024-slot channel dropped fast readers on every 1,000-item batch burst).
- **Database Tracing:** `github.com/exaring/otelpgx` on `pgxpool.Config`.
- **Templates:** Small safe custom renderer (`regex` scan + `strings.Builder`, no `text/template`), soft delete (`deleted_at`), rendered at **create time** (Phase 9).
- **Worker Tracing Relationship:** For a single notification the worker span is a **CHILD** of the API request span (clean end-to-end demo). For **batch items** the worker starts a **new trace linked** to the batch request span (`queue.Meta.TraceLink`, also derived from `batch_id` when the reconciler rebuilds metadata) — decided 2026-11-21 after measuring one 1,000-item batch trace at 28,013 spans (~145 MB in Jaeger), which made long load tests exhaust Docker Desktop's memory. Jaeger runs with `MEMORY_MAX_TRACES=20000` and `mem_limit: 1536m`.

---

## 11. Development Workflow

1. **Verify every change:** `gofmt -l .`, `go vet ./...` (and `-tags=integration`), `golangci-lint run`,
   `go test -race ./...`, and the integration suite when behavior across PostgreSQL/Redis changes.
   With only Docker, `./scripts/verify.sh` runs all of it plus a live smoke test. Report actual output;
   never claim something works without running it.
2. **API & smoke sync:** any change to an HTTP endpoint updates `api/openapi.yaml` and `scripts/smoke.sh`
   in the same change (`TestOpenAPISpecMatchesRouter` and Redocly enforce the spec).
3. **Requirement traceability:** every change states which requirement IDs (`FR-*`, `EXT-1`, `B-*`,
   `D-*`) it covers and how it was tested; README section 13 is the map.
4. **Commits:** small Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`,
   `refactor:`, `perf:`). Never commit `.env` or secrets.
5. **Language:** code, comments, commit messages, OpenAPI and `README.md` are written in English.
