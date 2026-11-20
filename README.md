# Event-Driven Notification System

A production-grade, horizontally scalable multi-channel notification management and delivery service (**SMS**, **Email**, **Push**) written in **Go 1.27.4**, backed by **PostgreSQL 16** (authoritative system of record) and **Redis 7** (weighted priority queues, delayed retry/schedule set, distributed GCRA rate limiter, and reconciler lease).

---

## Table of Contents

1. [Features & Core Capabilities](#1-features--core-capabilities)
2. [Quick Start (One Command)](#2-quick-start-one-command)
3. [Service URLs & Ports](#3-service-urls--ports)
4. [Architecture Overview](#4-architecture-overview)
5. [Notification State Machine & Fencing](#5-notification-state-machine--fencing)
6. [Design Decisions & Architecture Trade-Offs](#6-design-decisions--architecture-trade-offs)
7. [Failure Scenarios & Recovery Matrix](#7-failure-scenarios--recovery-matrix)
8. [HTTP API Reference & `curl` Examples](#8-http-api-reference--curl-examples)
9. [Configuration Reference](#9-configuration-reference)
10. [Testing & Load Generation](#10-testing--load-generation)
11. [Live `webhook.site` (`EXT-1`) Setup](#11-live-webhooksite-ext-1-setup)
12. [Known Limitations](#12-known-limitations)
13. [Requirement Traceability Matrix (`FR-1.1` … `D-6`)](#13-requirement-traceability-matrix-fr-11--d-6)

---

## 1. Features & Core Capabilities

- **Multi-Channel Validation & Delivery (`FR-1.1`, `FR-2.4`, `EXT-1`):**
  - **SMS:** E.164 recipient validation (`^\+[1-9]\d{7,14}$`), automatic **GSM-7** (including 2-septet extension table characters `^ { } \ [ ~ ] | €`) vs **UCS-2** (UTF-16 code unit counting for non-GSM runes and emojis) detection, and segment limit enforcement (single-part `160` GSM-7 / `70` UCS-2; multi-part `153` GSM-7 / `67` UCS-2; max `10` segments).
  - **Email:** Strict `net/mail.ParseAddress` validation (no display-name wrapping, max `254` chars), optional subject (`<= 200` chars, CR/LF header-injection guard), content up to `100 KB`.
  - **Push:** Device token (`1..4096` chars, no whitespace), optional title (`<= 100` chars), content up to `2000` chars, serialized provider JSON payload capped at `4096` bytes.
- **High-Throughput Batch Submission (`FR-1.2`):** Atomic all-or-nothing validation (returning every field error with dot/bracket paths such as `notifications[3].recipient`) and single-transaction persistence up to `1000` notifications per request via `pgx.CopyFrom`.
- **End-to-End Idempotency (`FR-2.5`):**
  - **API Layer:** Canonical SHA-256 request hashing stored in `idempotency_keys` (`24h` TTL) inside the same PostgreSQL transaction as notification/batch creation. Matching replays return the original resource with `Idempotent-Replayed: true`; payload mismatches return `409 idempotency_conflict`.
  - **Provider Layer:** Every delivery attempt sends `Idempotency-Key: <notification.id>` and `X-Correlation-ID`.
- **Weighted Priority Queues with Anti-Starvation (`FR-2.3`):** Per-channel Redis queues (`high`, `normal`, `low`) popped via an atomic Lua `LMOVE` script following a deterministic **6:3:1** 10-slot schedule (`[h,h,h,h,h,h,n,n,n,l]`) with fall-through, guaranteeing `low` priority receives `>= 10%` of capacity under sustained `high`/`normal` load and `100%` when higher queues are empty.
- **Distributed Per-Channel Rate Limiting (`FR-2.2`):** Shared Redis GCRA limiter (`go-redis/redis_rate/v10`, `Rate=100/s`, `Burst=1` = strict `10ms` spacing) enforced across all worker replicas **before** claiming a row in PostgreSQL, failing closed if Redis is unreachable.
- **Resilient Retry, Per-Channel Circuit Breaker & 5-Step Reconciler (`FR-3`, `B-1`):**
  - Full-jitter exponential backoff honoring provider `Retry-After` headers (`429`/`503`).
  - Per-channel `sony/gobreaker/v2` circuit breaker (trips on `10` consecutive transient failures or `> 50%` failure ratio over `>= 20` requests; permanent `4xx` responses count as breaker successes; open-circuit deferrals decrement `attempts` and record `counted=false, outcome='circuit_open'` so outages never burn retry budgets).
  - Lock-guarded 5-step distributed reconciler + live worker inflight sweeper automatically repairing Redis data loss, API crashes after DB commit, and worker crashes (`SIGKILL`).
- **Full Observability (`FR-4`):** Custom-registry Prometheus `/metrics` on API (`:8080`) and workers (`:9091`), real-time cached JSON snapshot (`GET /api/v1/metrics`), readiness probes (`GET /health/ready`, `/health`), liveness probes (`GET /health/live`), structured JSON logs (`log/slog`) with `correlation_id` and PII masking, and embedded OpenAPI 3.0.3 + Swagger UI.

---

## 2. Quick Start (One Command)

No `.env` file or manual migration step is required. Both `docker compose` (v2) and `docker-compose` (v1) work out of the box:

```bash
docker compose up --build -d
# or:
docker-compose up --build -d
```

Run the end-to-end smoke test suite once containers are healthy:

```bash
./scripts/smoke.sh
```

Generate a 2,000-notification load test and watch completion:

```bash
make load
# or directly with Go:
go run ./cmd/loadgen -n 2000 -batch 100 -concurrency 4
```

---

## 3. Service URLs & Ports

| Service | URL | Purpose |
|---|---|---|
| **HTTP API** | http://localhost:8080 | REST API, `/health/ready`, `/metrics`, `/api/v1/metrics`, `/openapi.yaml` |
| **Swagger UI** | http://localhost:8081 | Interactive OpenAPI 3.0.3 documentation (**"Try it out"** enabled via CORS) |
| **Mock Provider** | http://localhost:8090 | Configurable `EXT-1` provider (`/stats`, `/reset`, `/health/live`) |
| **Prometheus** | http://localhost:9090 | Scrapes `api:8080`, `worker-1:9091`, and `worker-2:9091` every `5s` |
| **Jaeger UI** | http://localhost:16686 | Distributed tracing UI (`:4318` OTLP HTTP collector) |
| **PostgreSQL 16** | `localhost:5432` | `postgres://notif:notif@localhost:5432/notif?sslmode=disable` |
| **Redis 7** | `localhost:6379` | AOF persistence (`--appendonly yes --appendfsync everysec`) |

---

## 4. Architecture Overview

```mermaid
flowchart LR
    Client["API Client / Swagger UI / loadgen"] -->|"HTTP REST + Idempotency-Key"| API["cmd/api (:8080)"]
    API -->|"1. Atomic Tx / CopyFrom (Source of Truth)"| PG[("PostgreSQL 16\nnotifications, batches,\ndelivery_attempts,\nidempotency_keys")]
    API -->|"2. Best-Effort Pipelined enqueue.lua"| Redis[("Redis 7 ({notif})\nPriority Lists, Delayed ZSET,\nInflight Lists, DLQ, GCRA")]

    subgraph Workers["Horizontally Scaled Stateless Workers (worker-1, worker-2)"]
        Dequeue["6:3:1 Weighted Dequeue\n(dequeue.lua LMOVE)"]
        Limiter["Distributed GCRA Limiter\n(100 msg/s/ch, Burst=1)"]
        Claim["Atomic DB Claim & Fencing\n(attempts = attempts + 1)"]
        Breaker["Per-Channel Circuit Breaker\n(sony/gobreaker/v2)"]
        Mover["Delayed Mover (500ms)\n(move_due.lua ZSET -> LIST)"]
        Reconciler["5-Step Reconciler (15s)\n+ Inflight Sweeper"]
    end

    Redis --> Dequeue --> Limiter --> Claim
    Claim <-->|"Fenced UPDATE (WHERE attempts=$claimed)"| PG
    Claim --> Breaker -->|"POST + Idempotency-Key + X-Correlation-ID"| Provider["External Provider\n(mockprovider:8090 / webhook.site)"]
    Mover <--> Redis
    Reconciler <-->|"Repair untracked / abandoned"| PG
    Reconciler <-->|"unlock.lua / force_enqueue.lua"| Redis
    Prom["Prometheus (:9090)"] -->|"Scrape /metrics (5s)"| API
    Prom -->|"Scrape :9091/metrics (5s)"| Workers
```

### Write Ordering & Source of Truth

1. **PostgreSQL First, Always:** Every create, batch create, cancel, manual retry, claim, retry schedule, circuit deferral, and terminal status update commits to PostgreSQL **before** touching Redis.
2. **Redis as an Ephemeral Hint:** Redis holds only compact tracking metadata (`{notif}:msg` hash: `id`, `channel`, `priority`, `correlationId`, `traceparent`) and 36-byte UUIDs in queue lists. If Redis restarts, loses its last 1 second of AOF, or an API replica crashes after DB commit before `enqueue.lua`, the **Distributed Reconciler** repairs Redis state from PostgreSQL within `PENDING_STALE_AFTER + RECONCILE_INTERVAL` (`<= 45s`).

---

## 5. Notification State Machine & Fencing

```mermaid
stateDiagram-v2
    [*] --> pending : POST /notifications (immediate)
    [*] --> scheduled : POST /notifications (scheduledAt)
    scheduled --> processing : Worker Claim (scheduled_at <= now + skew)
    scheduled --> cancelled : POST /cancel
    pending --> processing : Worker Claim
    pending --> cancelled : POST /cancel
    retrying --> processing : Worker Claim (next_attempt_at <= now + skew)
    retrying --> cancelled : POST /cancel
    processing --> sent : Fenced MarkSent (2xx)
    processing --> retrying : Fenced ScheduleRetry / DeferCircuitOpen
    processing --> failed : Fenced MarkFailed (Permanent 4xx or max_attempts)
    processing --> pending : Reconciler RequeueAbandoned (> VISIBILITY_TIMEOUT)
    failed --> pending : POST /retry (resets attempts=0, deferrals=0)
    sent --> [*]
    cancelled --> [*]
```

### Atomic Claim & Fencing Token Contract

- **Claim (`attempts` increments here and only here):**
  ```sql
  UPDATE notifications
  SET status = 'processing', attempts = attempts + 1, processing_started_at = now(), updated_at = now()
  WHERE id = $1 AND (
        status = 'pending'
     OR (status = 'retrying'  AND next_attempt_at <= now() + $skew)
     OR (status = 'scheduled' AND scheduled_at    <= now() + $skew))
  RETURNING *;
  ```
  If a notification was concurrently cancelled or already claimed by another worker, `Claim` updates `0` rows; the worker immediately runs `ack.lua` and moves on.
- **Fenced Post-Send Transitions:**
  Every subsequent worker update (`MarkSent`, `ScheduleRetry`, `DeferCircuitOpen`, `MarkFailed`) and reconciler recovery (`RequeueAbandoned`, `FailAbandoned`) fences on:
  ```sql
  WHERE id = $1 AND status = 'processing' AND attempts = $claimedAttempts
  ```
  If a slow worker returns after `VISIBILITY_TIMEOUT` (`60s`) when the reconciler has already reclaimed the row, its fenced update affects `0` rows (`ErrLeaseLost`), increments `lease_lost_total`, logs a warning, and never clobbers the newer state.

---

## 6. Design Decisions & Architecture Trade-Offs

### A. Delivery Guarantee: At-Least-Once (Effectively-Once)
Distributed systems that communicate over HTTP with external providers cannot guarantee strict mathematical **exactly-once** delivery (for example, if the provider accepts a request with `202` but the TCP connection drops before the worker receives the response bytes, or a worker suffers `SIGKILL` after the HTTP call completes but before `MarkSent` commits).
We achieve **at-least-once** delivery that behaves as **effectively-once** via three layers:
1. Transactionally persisted API `idempotency_keys` (`24h` TTL) with canonical request SHA-256 hashing.
2. Atomic PostgreSQL `Claim` and fenced `attempts = $claimedAttempts` transitions.
3. Sending a deterministic `Idempotency-Key: <notification.id>` header on every provider HTTP attempt so providers supporting idempotency deduplicate retries. *(Note: `webhook.site` does not deduplicate by `Idempotency-Key`, so timeouts against `webhook.site` can produce duplicate deliveries.)*

### B. Reconciler vs. Transactional Outbox
| Approach | Pros | Trade-Offs |
|---|---|---|
| **PostgreSQL State + Periodic Reconciler (Chosen)** | No extra `outbox` table write amplification; normal happy path has sub-millisecond direct Redis pipelined `enqueue.lua` latency; single 5-step reconciler repairs API crashes, Redis AOF loss, and dead workers alike. | If Redis is unreachable during `POST`, delivery is delayed until the reconciler scans stale rows (`PENDING_STALE_AFTER + RECONCILE_INTERVAL <= 45s`). |
| **Transactional Outbox + Relay** | Strict ordering from an append-only `outbox` table without waiting for a staleness threshold. | Doubles DB writes on every notification (`INSERT notifications` + `INSERT outbox` + `DELETE/UPDATE outbox`), and still requires a separate visibility-timeout reconciler for crashed workers. |

### C. Queue & Broker Alternatives Comparison
| Option | Architecture | Pros | Cons / Why Not Chosen for This Scope |
|---|---|---|---|
| **Option A (Chosen)** | **PostgreSQL 16 + Redis 7 (Lua + ZSET + GCRA)** | Atomic 6:3:1 weighted dequeue, sub-ms pipelined batch enqueue, built-in distributed GCRA rate limiting and delayed `ZSET` in one dependency, simple local Compose footprint. | Redis is ephemeral (mitigated by PostgreSQL source-of-truth + reconciler); Lua scripts use `{notif}` hash tag (single hash slot in Redis Cluster). |
| **Option B** | **PostgreSQL + RabbitMQ** | Native consumer acknowledgements, Dead Letter Exchanges (DLX), priority queues (`x-max-priority`). | No native distributed GCRA rate limiter (still needs Redis or token coordinator); per-message TTL in RabbitMQ only expires at queue head unless delayed-exchange plugins are added. |
| **Option C** | **PostgreSQL-only (`SELECT ... FOR UPDATE SKIP LOCKED`)** | Zero extra infrastructure dependencies; single transactional boundary. | Polling contention and index bloat under high concurrency (`50+` workers polling across `3` channels × `3` priorities + rate-limit pacing). |
| **Option D** | **PostgreSQL + Apache Kafka** | Massive horizontal throughput (`100M+/day`), durable segmented log replay, consumer group partitioning. | Heavy operational footprint (KRaft/brokers); no native priority queues or arbitrary per-message backoff timers without multi-topic retry tiers. |

#### How We Would Scale to 100M+ Notifications/Day (Kafka Migration Path)
To scale from millions/day (`~300 msg/s`) to `100M+/day` (`~1,200–5,000 msg/s` sustained, `25,000+ msg/s` bursts):
1. Replace Redis dispatch lists with partitioned **Kafka topics per `(channel, priority)`** (e.g. `notif.sms.high`, `notif.sms.normal`, `notif.sms.low`) and tiered backoff retry topics (`notif.retry.5s`, `notif.retry.30s`, `notif.retry.5m`) plus `notif.dlq`.
2. Partition `notifications` and `delivery_attempts` in PostgreSQL by daily/weekly range on `created_at` (using `pg_partman`), and batch status updates or move cold attempt audit logs to ClickHouse / BigQuery.
3. Partition rate limiting by provider sub-account / shard using local token buckets synchronized with Redis GCRA leases (`burst` reservation batches of e.g. 10 tokens per replica).

### D. Other Key Architectural Trade-Offs
- **Batch All-or-Nothing (`202` / `422`) vs. `207 Multi-Status`:** Batch creation validates all items upfront and inserts the entire batch in one transaction with `pgx.CopyFrom`. If any item is invalid or collides with an existing `idempotencyKey`, the whole request is rejected with per-index errors (`notifications[i].field`). This makes whole-batch retries via the batch-level `Idempotency-Key` header deterministic and race-free, whereas `207 Multi-Status` partial success forces clients to parse mixed results and stitch partial retries.
- **Per-Replica In-Memory Circuit Breaker vs. Shared Redis Breaker:** Each worker replica maintains an in-memory `sony/gobreaker/v2` instance per channel (`sms`, `email`, `push`). Because all worker replicas dispatch to the same external provider endpoint, they observe provider outages simultaneously without adding an extra Redis round-trip to every send or failing when Redis itself is degraded.
- **Redis `{notif}` Hash Tag & Cluster Caveat:** All Redis keys (`{notif}:msg`, `{notif}:q:<ch>:<pr>`, `{notif}:delayed`, `{notif}:inflight:<workerId>`, `{notif}:dlq`, `{notif}:reconciler:lock`, `{notif}:rl:<ch>`) share the `{notif}` hash tag so multi-key Lua scripts execute atomically within a single Redis Cluster hash slot. In a multi-shard Redis Cluster, pinning all `{notif}` keys to one slot means queue memory/CPU is bound to one primary shard (to shard across slots, use `{notif:sms}`, `{notif:email}`, `{notif:push}` per channel).
- **Queue-Depth Sampler in API Only & 2s Cached `/api/v1/metrics`:** Only `cmd/api` samples Redis queue depths into Prometheus gauges (`queue_depth`, `delayed_depth`, `dlq_depth`, `inflight_depth`) every `5s`, preventing duplicate gauge series from multiple worker replicas. `GET /api/v1/metrics` pairs live sub-millisecond Redis queue lengths with a **2-second in-memory cache** over PostgreSQL window aggregations and `percentile_cont` queries so frequent dashboard polling cannot overload PostgreSQL.

---

## 7. Failure Scenarios & Recovery Matrix

| Scenario | System Behavior | Automatic Recovery |
|---|---|---|
| **Redis down during `POST /notifications`** | Row commits to PostgreSQL as `pending`; Redis enqueue logs `warn`; API still returns `202 Accepted` (durable). `/health/ready` returns `503`. | Reconciler step `(a)` enqueues untracked stale `pending` rows once Redis recovers. |
| **Redis down for workers** | Dequeue backs off with context-aware timer; GCRA rate limiter **fails closed** (no sends) to protect the `100/s` provider SLA. | Workers resume automatically as soon as Redis responds. |
| **Redis loses last ~1s (`AOF everysec`)** | Some IDs vanish from ready lists or `{notif}:delayed` ZSET. | Reconciler steps `(a)` (stale `pending`) and `(b)` (untracked `retrying`/`scheduled` due within 5m) restore them from PostgreSQL. |
| **PostgreSQL down** | API create returns `503 service_unavailable`; `/health/ready` returns `503`; worker `Claim` errors schedule the item `now+1s` out of inflight. | Resumes automatically when PostgreSQL recovers. |
| **Worker crash (`SIGKILL`) mid-send** | Row stays `processing` in DB; ID stays in `{notif}:inflight:<deadWorkerId>`. | Step `(d)` requeues orphaned inflight list after `HEARTBEAT_TTL` (`15s`); step `(c)` resets `processing -> pending` (or `failed` if attempts exhausted) after `VISIBILITY_TIMEOUT` (`60s`) and records `outcome='abandoned'`. |
| **Redis write fails after DB transition (worker alive)** | Row transitioned in DB, but follow-up `Ack`/`Schedule`/`DeadLetter` failed; ID remains in live worker's inflight list. | Live worker's **Inflight Sweeper** detects IDs inactive across two consecutive sweeps and requeues them via `requeue_ids.lua`. |
| **API crash after DB commit, before Redis enqueue** | Row is `pending` in PostgreSQL and absent from `{notif}:msg`. | Reconciler step `(a)` (`force_enqueue.lua`) after `PENDING_STALE_AFTER` (`30s`). |
| **Provider slow / `5xx` outage** | Timeouts & `5xx` trigger full-jitter exponential backoff; after `10` consecutive failures (or `> 50%` of `20` requests), channel circuit breaker opens for `30s`. | Open-circuit items defer via `DeferCircuitOpen` (`attempts-1`, `deferrals+1`, `counted=false`); after `30s`, `3` half-open probes close the circuit on `2xx`. |
| **Provider `202` lost in network timeout** | Worker classifies timeout as `Transient` and retries with identical `Idempotency-Key: <notification.id>`. | Provider-side deduplication (or duplicate delivery if provider ignores `Idempotency-Key`). |
| **Client retries `POST` after network drop** | `idempotency_keys` lookup matches key + SHA-256 request hash. | Returns original `202` resource with `Idempotent-Replayed: true` header. |
| **Cancel vs. Worker race** | Both execute atomic conditional SQL `UPDATE ... WHERE id=$1 AND status IN (...)`. | Whichever transaction commits first wins; loser sees `0` rows (`409 Conflict` for API cancel, or `claimed=false -> Ack` for worker). |

---

## 8. HTTP API Reference & `curl` Examples

### 1. Create a Single Notification (`POST /api/v1/notifications`)

```bash
curl -i -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-42-shipped" \
  -H "X-Correlation-ID: corr-order-42" \
  -d '{
    "recipient": "+905551234567",
    "channel": "sms",
    "priority": "high",
    "content": "Siparişiniz kargoya verildi"
  }'
```

**Response (`202 Accepted`):**
```json
{
  "id": "01936f7e-8a1b-7c20-9d3e-4f5a6b7c8d9e",
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
  "createdAt": "2026-11-20T19:00:00Z",
  "updatedAt": "2026-11-20T19:00:00Z",
  "sentAt": null,
  "failedAt": null,
  "cancelledAt": null
}
```

### 2. Create a Batch (`POST /api/v1/notifications/batch`)

```bash
curl -i -X POST http://localhost:8080/api/v1/notifications/batch \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: flash-sale-batch-1" \
  -d '{
    "notifications": [
      {"recipient": "+905551234501", "channel": "sms", "priority": "high", "content": "Flash sale started!"},
      {"recipient": "user@example.com", "channel": "email", "priority": "normal", "subject": "Flash Sale", "content": "50% off today."},
      {"recipient": "fcm-device-token-12345", "channel": "push", "priority": "low", "title": "Sale", "content": "Tap to view."}
    ]
  }'
```

**Response (`202 Accepted`):**
```json
{
  "batchId": "01936f7f-1122-7334-8445-556677889900",
  "count": 3,
  "notificationIds": [
    "01936f7f-1122-7334-8445-556677889901",
    "01936f7f-1122-7334-8445-556677889902",
    "01936f7f-1122-7334-8445-556677889903"
  ]
}
```

### 3. Get Notification & Delivery Attempts (`GET /api/v1/notifications/{id}`)

```bash
curl -s http://localhost:8080/api/v1/notifications/01936f7e-8a1b-7c20-9d3e-4f5a6b7c8d9e | jq .
```

**Response (`200 OK`):**
```json
{
  "id": "01936f7e-8a1b-7c20-9d3e-4f5a6b7c8d9e",
  "batchId": null,
  "recipient": "+905551234567",
  "channel": "sms",
  "priority": "high",
  "status": "sent",
  "content": "Siparişiniz kargoya verildi",
  "attempts": 1,
  "maxAttempts": 5,
  "scheduledAt": null,
  "nextAttemptAt": null,
  "providerMessageId": "mock-01936f7e-...",
  "lastError": null,
  "createdAt": "2026-11-20T19:00:00Z",
  "updatedAt": "2026-11-20T19:00:00.045Z",
  "sentAt": "2026-11-20T19:00:00.045Z",
  "failedAt": null,
  "cancelledAt": null,
  "deliveryAttempts": [
    {
      "id": 1,
      "attemptNo": 1,
      "counted": true,
      "startedAt": "2026-11-20T19:00:00.012Z",
      "durationMs": 31,
      "httpStatus": 202,
      "outcome": "sent",
      "error": null,
      "providerMessageId": "mock-01936f7e-...",
      "workerId": "worker-1"
    }
  ]
}
```

### 4. List Notifications with Filters & Keyset Cursor (`GET /api/v1/notifications`)

```bash
curl -s "http://localhost:8080/api/v1/notifications?status=sent,failed&channel=sms&priority=high&from=2026-11-20T00:00:00Z&to=2026-11-21T00:00:00Z&limit=50" | jq .
```

### 5. Cancel a Pending Notification (`POST /api/v1/notifications/{id}/cancel`)

```bash
curl -i -X POST http://localhost:8080/api/v1/notifications/01936f7e-8a1b-7c20-9d3e-4f5a6b7c8d9e/cancel
```

### 6. Manually Retry a Failed Notification (`POST /api/v1/notifications/{id}/retry`)

```bash
curl -i -X POST http://localhost:8080/api/v1/notifications/01936f7e-8a1b-7c20-9d3e-4f5a6b7c8d9e/retry
```

### 7. Query & Cancel a Batch (`GET /api/v1/batches/{id}`, `POST /api/v1/batches/{id}/cancel`)

```bash
curl -s "http://localhost:8080/api/v1/batches/01936f7f-1122-7334-8445-556677889900?limit=20" | jq .
curl -i -X POST http://localhost:8080/api/v1/batches/01936f7f-1122-7334-8445-556677889900/cancel
```

### 8. Real-Time Metrics, Prometheus & Health Probes

```bash
curl -s http://localhost:8080/api/v1/metrics | jq .
curl -s http://localhost:8080/health/ready | jq .
curl -s http://localhost:8080/health/live | jq .
curl -s http://localhost:8080/metrics | head -n 30
```

---

## 9. Configuration Reference

Every variable carries a production-safe default; `.env` is optional (see `.env.example`):

| Variable | Default | Description & Validation |
|---|---|---|
| `HTTP_ADDR` | `:8080` | API HTTP listen address |
| `WORKER_METRICS_ADDR` | `:9091` | Worker health & Prometheus `/metrics` listen address |
| `DATABASE_URL` | `postgres://notif:notif@postgres:5432/notif?sslmode=disable` | PostgreSQL connection string (password masked in logs) |
| `REDIS_ADDR` | `redis:6379` | Redis `host:port` |
| `REDIS_POOL_SIZE` | `0` (`auto`) | `0` auto-sizes to `3 * WORKER_CONCURRENCY_PER_CHANNEL + 20` |
| `PROVIDER_URL` | `http://mockprovider:8090/` | External `EXT-1` provider endpoint (`http`/`https`) |
| `PROVIDER_TIMEOUT` | `10s` | Per-attempt HTTP client timeout (`> 0`) |
| `PROVIDER_EXTENDED_FIELDS` | `false` | Include optional `subject`/`title` in provider JSON body |
| `RATE_LIMIT_PER_SEC` | `100` | Distributed GCRA rate limit per channel (`> 0`) |
| `RATE_LIMIT_BURST` | `1` | GCRA burst (`1` enforces strict `10ms` pacing at `100/s`) |
| `WORKER_ID` | `(hostname)` | Unique replica identifier for inflight & heartbeat keys |
| `WORKER_CONCURRENCY_PER_CHANNEL` | `25` | Consumer goroutines per channel per replica (`> 0`) |
| `MAX_ATTEMPTS` | `5` | Max counted delivery attempts (`>= 1`) |
| `RETRY_BASE` | `2s` | Exponential backoff base duration (`>= RETRY_MIN`) |
| `RETRY_CAP` | `5m` | Maximum exponential backoff ceiling (`>= RETRY_BASE`) |
| `RETRY_MIN` | `200ms` | Full-jitter minimum backoff floor (`> 0`) |
| `RETRY_AFTER_MAX` | `15m` | Cap for honored provider `Retry-After` headers (`>= RETRY_CAP`) |
| `CB_CONSECUTIVE_FAILURES` | `10` | Consecutive transient failures to open breaker (`>= 1`) |
| `CB_MIN_REQUESTS` | `20` | Minimum requests in window before failure ratio trip (`>= 1`) |
| `CB_FAILURE_RATIO` | `0.5` | Transient failure ratio (`(0, 1]`) to open breaker |
| `CB_INTERVAL` | `60s` | Closed-state counter reset interval (`> 0`) |
| `CB_OPEN_TIMEOUT` | `30s` | Open state duration before half-open probing (`> 0`) |
| `CB_HALF_OPEN_MAX` | `3` | Consecutive successful half-open probes to close (`>= 1`) |
| `MAX_CIRCUIT_DEFERRALS` | `120` | Max open-circuit deferrals (`120 ≈ 1h`) before failing (`>= 1`) |
| `VISIBILITY_TIMEOUT` | `60s` | Must satisfy `>= 2*PROVIDER_TIMEOUT + 15s` |
| `RECONCILE_INTERVAL` | `15s` | Distributed reconciler cycle interval (`> 0`) |
| `PENDING_STALE_AFTER` | `30s` | Age before untracked `pending` row is repaired (`> 0`) |
| `DELAYED_MOVER_INTERVAL` | `500ms` | Delayed ZSET due-item mover cadence (`> 0`) |
| `HEARTBEAT_INTERVAL` | `5s` | Worker liveness refresh interval (`> 0`) |
| `HEARTBEAT_TTL` | `15s` | Worker liveness key TTL (`> HEARTBEAT_INTERVAL`) |
| `CLOCK_SKEW_TOLERANCE` | `2s` | Allowed skew between worker clock and DB `now()` (`>= 0`) |
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful drain timeout on `SIGINT`/`SIGTERM` (`> 0`) |
| `MAX_BODY_BYTES` | `524288` | Max body size (`512 KB`) for single requests |
| `BATCH_MAX_BODY_BYTES` | `16777216` | Max body size (`16 MB`) for batch requests |
| `BATCH_MAX_ITEMS` | `1000` | Max notifications per batch (`1..1000`) |
| `PAGE_LIMIT_DEFAULT` | `50` | Default pagination limit (`1..PAGE_LIMIT_MAX`) |
| `PAGE_LIMIT_MAX` | `200` | Maximum pagination limit (`1..1000`) |
| `IDEMPOTENCY_TTL` | `24h` | TTL for API `idempotency_keys` (`> 0`) |
| `SCHEDULE_MAX_AHEAD` | `720h` | Maximum future `scheduledAt` horizon (`30` days) |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:8081` | Comma-separated allowed CORS origins |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4318` | OpenTelemetry OTLP HTTP endpoint |
| `OTEL_SAMPLER_RATIO` | `1.0` | Trace sampling ratio (`[0, 1]`) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |

---

## 10. Testing & Load Generation

The test suite separates fast unit tests (using `miniredis` and in-memory fakes, zero Docker dependency) from full container-backed integration tests (`//go:build integration` using `testcontainers-go` PostgreSQL 16 + Redis 7 and `golang-migrate`):

```bash
# Run unit tests only (no Docker required):
make test
# or on native Windows without GCC/make:
go test ./...

# Run Testcontainers integration suite (PostgreSQL 16 + Redis 7):
make test-integration
# or:
go test -v -tags=integration ./internal/integration/...

# Single command to run ALL unit + integration tests with race detector (D-6):
make test-all

# Run static analysis & linter:
make lint

# Run 2,000-notification batch load test against running stack:
make load
```

---

## 11. Live `webhook.site` (`EXT-1`) Setup

By default, `docker-compose.yml` points `PROVIDER_URL` at the in-cluster `http://mockprovider:8090/` service, which implements the exact `EXT-1` contract (`202 Accepted` with `{"messageId":"...","status":"accepted","timestamp":"..."}`). To test against a live `webhook.site` URL:

1. Open https://webhook.site and copy your unique endpoint URL.
2. Configure `webhook.site` response status to `202` and response body to:
   ```json
   {"messageId":"webhook-site-live-1","status":"accepted","timestamp":"2026-11-20T19:00:00Z"}
   ```
3. Restart the workers with your `PROVIDER_URL`:
   ```bash
   PROVIDER_URL="https://webhook.site/<your-uuid>" docker compose up -d worker-1 worker-2
   ```

---

## 12. Known Limitations

1. **SMS Turkish National Language Shift Table:** GSM 03.38 basic/extension tables include `ö ü Ö Ü Ç`, whereas `ç ğ Ğ ı İ ş Ş` require the 3GPP TS 23.038 Turkish Locking/Single Shift table. When those characters appear, our segmenter automatically switches the message to `UCS-2` (`70`/`67` code units per segment), matching conservative carrier gateway behavior.
2. **Global Idempotency Key Namespace:** `idempotency_keys.key` is currently a global primary key (single-tenant model). In a multi-tenant deployment, the primary key would be composite `(tenant_id, key)`.
3. **In-Memory Per-Replica Circuit Breaker:** Circuit breaker state is tracked per channel in memory on each worker replica rather than shared in Redis, trading a small per-replica detection warm-up (`10` failures per replica) for zero Redis overhead and resilience during Redis outages.

---

## 13. Requirement Traceability Matrix (`FR-1.1` … `D-6`)

| ID | Requirement | Implementation File(s) | Automated Test(s) | Status |
|---|---|---|---|---|
| **FR-1.1** | Create single notification (`recipient`, `channel`, `content`, `priority`) | `internal/api/handlers_notifications.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/a_single_notification_end_to_end` | **DONE** |
| **FR-1.2** | Batch creation (`<= 1000` notifications, `pgx.CopyFrom`, all-or-nothing validation) | `internal/api/handlers_notifications.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/c_batch_1000_and_batch_key_replay` | **DONE** |
| **FR-1.3** | Query notification status & `deliveryAttempts` by ID | `internal/api/handlers_notifications.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/a_single_notification_end_to_end` | **DONE** |
| **FR-1.4** | Query batch status summary (`countsByStatus`) & paginated items by batch ID | `internal/api/handlers_batches.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/c_batch_1000_and_batch_key_replay` | **DONE** |
| **FR-1.5** | Cancel pending notifications (`pending`/`retrying`/`scheduled` -> `cancelled`, `409 Conflict`) | `internal/api/handlers_notifications.go`, `internal/api/handlers_batches.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/d_cancel_race_no_cancelled_notification_reaches_provider` | **DONE** |
| **FR-1.6** | List notifications with filtering (`status`, `channel`, `priority`, `from`/`to`) & keyset pagination | `internal/api/handlers_notifications.go`, `internal/api/cursor.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/i_list_filters_and_three_page_keyset_pagination` | **DONE** |
| **FR-2.1** | Process notifications asynchronously via queue workers with atomic `Claim` & fencing | `internal/worker/worker.go`, `internal/queue/queue.go`, `cmd/worker/main.go` | `internal/worker/worker_test.go`, `TestCoreIntegration/a_single_notification_end_to_end` | **DONE** |
| **FR-2.2** | Distributed rate limiting: max `100` messages/sec per channel (`Burst=1`) | `internal/ratelimit/limiter.go` | `internal/ratelimit/limiter_test.go`, `TestCoreIntegration/g_distributed_rate_limit_500_sms_across_two_workers` | **DONE** |
| **FR-2.3** | Priority queue support (`high`, `normal`, `low`, 6:3:1 weighted anti-starvation) | `internal/queue/queue.go`, `internal/queue/lua/dequeue.lua` | `internal/queue/queue_test.go`, `TestCoreIntegration/f_priority_high_completes_ahead_of_low` | **DONE** |
| **FR-2.4** | Content & recipient validation (E.164, GSM-7/UCS-2 SMS segments, Email, Push) | `internal/domain/validation.go`, `internal/domain/sms.go` | `internal/domain/validation_test.go`, `internal/domain/sms_test.go`, `internal/api/handlers_test.go` | **DONE** |
| **FR-2.5** | Idempotency support (API `idempotency_keys` + SHA-256 canonical hash; provider `Idempotency-Key`) | `internal/domain/hash.go`, `internal/api/handlers_notifications.go`, `internal/provider/client.go` | `internal/domain/hash_test.go`, `internal/api/handlers_test.go`, `TestCoreIntegration/b_idempotent_create_replay_and_conflict` | **DONE** |
| **FR-3** | Delivery & retry logic (outcome classification, full-jitter exponential backoff, `Retry-After`) | `internal/worker/retry.go`, `internal/worker/worker.go`, `internal/provider/client.go` | `internal/worker/retry_test.go`, `internal/worker/worker_phase5_test.go`, `TestCoreIntegration/e_retry_500_500_202_and_permanent_400_to_dlq` | **DONE** |
| **FR-4.1** | Real-time metrics (`/metrics` Prometheus custom registry + `/api/v1/metrics` 2s cached JSON) | `internal/observability/metrics.go`, `internal/api/handlers_metrics.go`, `internal/api/middleware/metrics.go` | `internal/observability/metrics_test.go`, `internal/api/observability_test.go` | **DONE** |
| **FR-4.2** | Structured JSON logging (`log/slog`) with `correlation_id` propagation & PII masking | `internal/observability/logger.go`, `internal/api/middleware/correlation.go`, `internal/api/middleware/logger.go` | `internal/observability/logger_test.go`, `internal/api/middleware/middleware_test.go`, `internal/worker/observability_hooks_test.go` | **DONE** |
| **FR-4.3** | Health check endpoints (`/health/live`, `/health/ready`, `/health` with 1s DB/Redis timeout & `503`) | `internal/api/handlers_system.go`, `cmd/worker/main.go`, `cmd/healthcheck/main.go` | `internal/api/observability_test.go`, `cmd/healthcheck/main_test.go`, `scripts/smoke.sh` | **DONE** |
| **EXT-1** | External provider HTTP contract (`POST {to,channel,content}` -> `202 {messageId,status,timestamp}`) | `internal/provider/client.go`, `cmd/mockprovider/main.go` | `internal/provider/client_test.go`, `cmd/mockprovider/main_test.go`, `TestCoreIntegration/a_single_notification_end_to_end` | **DONE** |
| **B-1** | Failure handling (per-channel `gobreaker/v2`, DLQ, 5-step Reconciler, Inflight Sweeper, manual `/retry`) | `internal/worker/breaker.go`, `internal/worker/reconciler.go`, `internal/worker/worker.go` | `internal/worker/breaker_test.go`, `internal/worker/reconciler_test.go`, `internal/worker/stranded_repair_test.go`, `TestCoreIntegration/h_reconciler_repairs_untracked_pending_and_simulated_redis_loss` | **DONE** |
| **B-2** | Scheduled notifications (`scheduledAt` RFC3339 -> UTC, shared `{notif}:delayed` ZSET + `Claim` clock-skew guard, cancel-before-due) | `internal/api/handlers_notifications.go`, `internal/domain/validation.go`, `internal/queue/queue.go`, `internal/worker/reconciler.go` | `internal/api/handlers_test.go`, `internal/worker/worker_test.go`, `TestCoreIntegration/j_scheduled_due_in_2s_sent_after_due_and_cancelled_never_sent` | **DONE** |
| **B-3** | Message templates with safe `{{variable}}` substitution, preview endpoint, soft-delete, and render-at-create | `internal/template/template.go`, `internal/api/handlers_templates.go`, `migrations/000002_templates.up.sql` | `internal/template/template_test.go`, `internal/api/handlers_test.go`, `TestCoreIntegration/k_template_create_preview_and_batch_personalized_sms_delivery` | **DONE** |
| **D-1** | GitHub repository with clean Conventional Commits history | Git repository | `git log`, `.gitignore` | **DONE** |
| **D-2** | `README.md` with setup instructions, architecture overview, Mermaid diagrams, and API examples | `README.md` | `scripts/smoke.sh` | **DONE** |
| **D-3** | One-command zero-config Docker Compose (`docker compose up` & `docker-compose up`) | `docker-compose.yml`, `Dockerfile`, `.env.example` | Live Docker Compose build + `scripts/smoke.sh` | **DONE** |
| **D-4** | OpenAPI 3.0.3 specification (`api/openapi.yaml`) + Swagger UI (`:8081`) | `api/openapi.yaml`, `api/spec.go` | `internal/api/openapi_test.go` (`TestOpenAPISpecMatchesRouter`), Redocly lint | **DONE** |
| **D-5** | Versioned database migrations (`migrations/*.up.sql`, `.down.sql`) | `migrations/000001_init.up.sql`, `migrations/000001_init.down.sql`, `migrations/migrations.go` | `TestCoreIntegration` (`golang-migrate`) + Compose `migrate` service | **DONE** |
| **D-6** | Single-command test suite (`make test-all` / `go test ./...`) | `Makefile`, `internal/integration/integration_test.go` | `go test -race ./...` + `go test -race -tags=integration ./...` | **DONE** |


