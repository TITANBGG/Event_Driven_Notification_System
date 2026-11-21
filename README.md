# Event-Driven Notification System

[![CI](https://github.com/TITANBGG/Event_Driven_Notification_System/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/TITANBGG/Event_Driven_Notification_System/actions/workflows/ci.yml)
[![Release](https://github.com/TITANBGG/Event_Driven_Notification_System/actions/workflows/release.yml/badge.svg)](https://github.com/TITANBGG/Event_Driven_Notification_System/actions/workflows/release.yml)
![Go](https://img.shields.io/badge/Go-1.27.4-00ADD8?logo=go)

A notification service that accepts SMS, email and push requests over HTTP, stores them in PostgreSQL and delivers them asynchronously through an external provider. It caps sends at 100 messages per second per channel, delivers high-priority messages first without starving low-priority ones, retries failures, and reports delivery status through a REST API, a WebSocket stream, Prometheus metrics, structured logs and OpenTelemetry traces.

It was written in Go 1.27.4 for the Insider One Software Engineer Assessment 2026. Every functional requirement (`FR-1`…`FR-4`), the provider contract (`EXT-1`), all deliverables (`D-1`…`D-6`) and all six bonus features (`B-1`…`B-6`) are implemented and tested. Section 13, the [requirement checklist](#13-requirement-checklist), maps each one to its code and tests.

## Contents

1. [Quick start](#1-quick-start)
2. [Architecture](#2-architecture)
3. [Notification lifecycle and state machine](#3-notification-lifecycle-and-state-machine)
4. [Design decisions](#4-design-decisions)
5. [Failure scenarios](#5-failure-scenarios)
6. [Alternatives and scaling to 100M/day](#6-alternatives-and-scaling-to-100mday)
7. [API examples](#7-api-examples)
8. [Observability](#8-observability)
9. [Configuration](#9-configuration)
10. [Testing](#10-testing)
11. [Mock provider vs. webhook.site](#11-mock-provider-vs-webhooksite)
12. [Known limitations and next steps](#12-known-limitations-and-next-steps)
13. [Requirement checklist](#13-requirement-checklist)
14. [How this was built](#14-how-this-was-built)
15. [License](#license)

---

## 1. Quick start

Requirements: Docker with Compose. Nothing else is needed: there is no `.env` file to create, no image to pull by hand and no migration to run.

```bash
git clone https://github.com/TITANBGG/Event_Driven_Notification_System.git
cd Event_Driven_Notification_System
docker compose up --build        # Compose v2
# or
docker-compose up --build        # legacy Compose v1
```

Both commands were verified from a fresh clone with no `.env`: Docker Compose v5.7.0 (`docker compose`) and legacy Compose v1 1.29.2 (`docker-compose`); CI also parses the file with Compose v1 on every push. The first build takes a few minutes. The `migrate` service applies `migrations/*.up.sql` and exits; `api`, `worker-1` and `worker-2` start only after it has succeeded. When every container is healthy, run the smoke test (one request per endpoint, each with an expected status code):

```bash
./scripts/smoke.sh
```

To run the whole test suite with only Docker, see [section 10](#10-testing): `./scripts/verify.sh`.

| What | URL |
|---|---|
| REST API | http://localhost:8080 (`/api/v1/...`, `/health`, `/metrics`, `/openapi.yaml`) |
| Swagger UI | http://localhost:8081 ("Try it out" works; CORS allows this origin) |
| Live demo page (WebSocket) | http://localhost:8080/demo |
| Jaeger (traces) | http://localhost:16686 |
| Prometheus | http://localhost:9090 (scrapes the API and both workers every 5s) |
| Mock provider | http://localhost:8090 (`GET /stats`, `POST /reset`) |

**External provider.** The assignment's provider is `webhook.site`. Out of the box the workers send to the bundled mock provider instead (`cmd/mockprovider`, same request and `202` response contract), so a fresh `docker compose up` works offline and load tests do not exhaust webhook.site's free tier. To deliver to webhook.site, set its `202` response and restart the workers with your URL:

```bash
PROVIDER_URL="https://webhook.site/<uuid>" docker compose up -d worker-1 worker-2
```

Step-by-step setup and a verified run for all three channels are in [section 11](#11-mock-provider-vs-webhooksite).

On Windows, if `http://localhost:8080` times out while `http://127.0.0.1:8080` answers, Docker Desktop's IPv6 (`[::1]`) port forwarding has gone stale. This is a Docker Desktop issue, not this project's; use `127.0.0.1` or restart Docker Desktop.

PostgreSQL, Redis and the workers' `:9091` health/metrics port are reachable only inside the Compose network; they are not published on the host. Use `docker compose exec postgres psql -U notif notif` or `docker compose exec redis redis-cli` to inspect them.

---

## 2. Architecture

```mermaid
flowchart LR
    Client["Client / Swagger UI / loadgen"] -->|"REST + Idempotency-Key"| API["api :8080"]
    Browser["Browser /demo"] <-->|"WebSocket /api/v1/ws"| API
    API -->|"1. commit (source of truth)"| PG[("PostgreSQL 16")]
    API -->|"2. enqueue (hint)"| R[("Redis 7")]
    R -->|"atomic move into inflight list"| W["worker-1 / worker-2"]
    W -->|"GCRA token per channel"| R
    W -->|"claim + fenced updates"| PG
    W -->|"POST {to, channel, content}<br/>Idempotency-Key, X-Correlation-ID, traceparent"| P["Provider<br/>(mockprovider or webhook.site)"]
    W -->|"publish status event"| R
    R -->|"pub/sub notif:events"| API
    W -. "reconciler: repair Redis from PostgreSQL" .-> R
    API & W -->|"OTLP"| J["Jaeger"]
    Prom["Prometheus"] -->|"scrape /metrics"| API & W
```

| Binary | Responsibility |
|---|---|
| `cmd/api` | REST API, WebSocket hub, `/metrics`, queue-depth sampler (every 5s), `/openapi.yaml`, `/demo` |
| `cmd/worker` | Per-channel consumers, delayed-set mover, reconciler, inflight sweeper, heartbeat, `/metrics` and health on `:9091` |
| `cmd/mockprovider` | Provider that follows the `EXT-1` contract, with configurable latency, failure, 429 and permanent-error rates, plus `/stats` |
| `cmd/healthcheck` | Static HTTP probe used by Compose healthchecks (the distroless image has no curl) |
| `cmd/loadgen` | Load generator (section 10) |

All binaries are built by one multi-stage `Dockerfile` into a `distroless/static:nonroot` image.

**Path of one notification:**

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as API
    participant DB as PostgreSQL
    participant R as Redis
    participant W as Worker
    participant P as Provider
    C->>A: POST /api/v1/notifications (Idempotency-Key)
    A->>DB: BEGIN; INSERT idempotency_keys + notifications (status=pending); COMMIT
    A->>R: enqueue.lua (HSET {notif}:msg, LPUSH {notif}:q:sms:high)
    A-->>C: 202 Accepted {id, status: pending}
    W->>R: dequeue.lua (RPOP queue + LPUSH {notif}:inflight:worker-1)
    W->>R: GCRA rate limiter (wait for a token on {notif}:rl:sms)
    W->>DB: Claim: UPDATE ... SET status=processing, attempts=attempts+1 WHERE status IN (...)
    W->>P: POST {to, channel, content} + Idempotency-Key: <id>
    P-->>W: 202 {messageId, status: accepted}
    W->>DB: INSERT delivery_attempts; UPDATE status=sent WHERE status=processing AND attempts=$claimed
    W->>R: ack.lua (LREM inflight, HDEL msg) + PUBLISH notif:events
    R-->>A: notif:events
    A-->>C: WebSocket frame {type: event, status: sent}
```

---

## 3. Notification lifecycle and state machine

The allowed transitions are defined in one place, `internal/domain/statemachine.go`. Each SQL `UPDATE` repeats the same set as its `WHERE status = ANY(...)` guard.

```mermaid
stateDiagram-v2
    [*] --> pending : POST (no scheduledAt)
    [*] --> scheduled : POST (scheduledAt in the future)
    scheduled --> processing : worker claim (scheduled_at <= now + skew)
    scheduled --> cancelled : API cancel
    pending --> processing : worker claim
    pending --> cancelled : API cancel
    retrying --> processing : worker claim (next_attempt_at <= now + skew)
    retrying --> cancelled : API cancel
    processing --> sent : provider accepted
    processing --> retrying : transient failure / circuit open
    processing --> failed : permanent failure or attempts exhausted
    processing --> pending : reconciler, after VISIBILITY_TIMEOUT
    failed --> pending : manual retry (attempts = 0)
    sent --> [*]
    cancelled --> [*]
```

`scheduled`, `pending` and `retrying` can be cancelled. Cancelling a notification in any other status returns `409` and names the current status.

**Claim** is the only way a worker takes ownership, and the only place `attempts` is incremented:

```sql
UPDATE notifications
SET status='processing', attempts=attempts+1, processing_started_at=now(), updated_at=now()
WHERE id=$1 AND (
      status='pending'
   OR (status='retrying'  AND next_attempt_at <= now() + $skew)
   OR (status='scheduled' AND scheduled_at    <= now() + $skew))
RETURNING *;
```

**Fencing.** Every later write a worker makes (sent, retrying, failed, circuit deferral) adds `AND status='processing' AND attempts=$claimedAttempts`. Suppose a slow worker comes back after the reconciler has already reclaimed its row. Its update matches 0 rows, the worker logs `lease lost`, increments `lease_lost_total` and drops the item. The newer state is never overwritten.

---

## 4. Design decisions

### PostgreSQL is the source of truth; write it first

Every notification, batch, attempt, idempotency key and template lives in PostgreSQL. Every code path commits to PostgreSQL before it touches Redis. Redis is treated as a hint about which work is ready. If Redis loses an ID (a crash between commit and enqueue, AOF `everysec` losing the last second, a flushed instance), the reconciler rebuilds the Redis state from PostgreSQL. That repair is bounded by `PENDING_STALE_AFTER + RECONCILE_INTERVAL` (45s by default).

### What Redis is used for

All keys share the hash tag `{notif}`, so every multi-key Lua script touches a single Redis Cluster slot.

| Key | Type | Purpose |
|---|---|---|
| `{notif}:q:{channel}:{priority}` | LIST | Ready queue of notification IDs (`LPUSH` in, `RPOP` out = FIFO) |
| `{notif}:msg` | HASH | `id -> {channel, priority, correlationId, traceparent}`. An ID present here is "tracked". |
| `{notif}:inflight:{workerId}` | LIST | IDs a worker has dequeued and not yet acknowledged |
| `{notif}:hb:{workerId}` | STRING | Worker heartbeat, TTL 15s, refreshed every 5s |
| `{notif}:delayed` | ZSET | Retries and scheduled sends; score = due time in Unix ms |
| `{notif}:dlq` | LIST | IDs of failed notifications (capped at 100k; PostgreSQL has the full history) |
| `{notif}:reconciler:lock` | STRING | `SET NX PX 30000` lease so only one reconciler cycle runs at a time |
| `{notif}:rl:{channel}` | GCRA | Distributed rate limiter state (`go-redis/redis_rate`) |
| `notif:events` | PUB/SUB | Status events fanned out to every API replica's WebSocket hub |

### Reliable queue: atomic move to an inflight list + heartbeat

`dequeue.lua` moves an ID atomically from the ready queue into the worker's own inflight list (`RPOP` + `LPUSH` in one script, which is `LMOVE RIGHT LEFT`; see "Redis full" in the failure matrix for why it is split). If the worker dies, the ID is not lost: it stays in `{notif}:inflight:{workerId}`. Once the worker's heartbeat key expires, reconciler step (d) moves the orphaned list back to the ready queues.

A second safety net covers a live worker. If a PostgreSQL transition succeeds but the follow-up Redis write (`ack`, `schedule` or `deadletter`) fails, the ID is stuck in that worker's inflight list. The worker's inflight sweeper requeues any ID it has not been actively handling for two consecutive sweeps.

### Idempotent enqueue

`enqueue.lua` and `schedule.lua` do nothing for IDs that are already tracked in `{notif}:msg`. The reconciler can therefore re-enqueue anything it suspects is missing without creating duplicate queue entries.

### At-least-once delivery, made effectively-once where possible

Exactly-once delivery is not possible here. If the provider accepts a request but the response is lost (a timeout, or the worker is killed after the HTTP call), the worker has to send again. Three layers keep duplicates to that unavoidable case:

1. **API idempotency.** `Idempotency-Key` is stored in `idempotency_keys` in the same transaction as the notification, together with a SHA-256 hash of the canonical request body (sorted keys, normalized channel and priority, defaults applied). The primary key serializes concurrent duplicates: the integration test sends 20 simultaneous POSTs with one key and gets exactly one row. Keys expire after 24h; an expired key is atomically reclaimed on its next use.
2. **Atomic claim and fencing.** Two workers can never process the same row at the same time, and a stale worker can never overwrite a newer state.
3. **Provider idempotency.** Every attempt sends `Idempotency-Key: <notification id>`, which stays the same across retries, so a provider that deduplicates will drop the repeat. `webhook.site` does not deduplicate, so a timeout against it can produce a duplicate. Integration test `o` shows that duplicates are bounded by the number of sends in flight when a worker crashes.

### Atomic transitions resolve cancel races

Cancel is a conditional update: `UPDATE ... WHERE id=$1 AND status = ANY('{scheduled,pending,retrying}')`. If the cancel commits first, the worker's claim matches 0 rows and the worker acknowledges and drops the item. If the claim commits first, the cancel returns `409` with `current_status: processing`. Integration test `d` runs 100 concurrent cancels against live workers and checks that no cancelled ID reaches the provider.

### Weighted priority without starvation (6:3:1)

Each consumer goroutine cycles through a fixed 10-slot schedule `[h,h,h,h,h,h,n,n,n,l]`. For each slot, `dequeue.lua` tries that slot's priority first and then falls through the others in `high > normal > low` order. As a result, `low` gets at least 10% of the slots while higher-priority queues are busy, and all of the capacity when they are empty.

### Rate limit: distributed GCRA with `Burst = 1`

Workers acquire a token from `{notif}:rl:{channel}` after they claim a row, immediately before the provider call. The limiter spaces token grants, not sends, so anything between the two (a wait for a database connection, a GC pause) would let sends bunch up. That is what a stress test showed when the token was still taken before the claim: with 6 worker replicas exhausting PostgreSQL connections, the busiest second reached 107 sends per channel. The wait for a token is bounded (`VISIBILITY_TIMEOUT − 2 × PROVIDER_TIMEOUT − 5s`), so a claimed row is never held past its lease; if no token comes in time (Redis down), the claim is handed back with the same "not attempted" transition the circuit breaker uses, and no attempt is burned. The limiter is `Rate = 100/s, Burst = 1`, which spaces consecutive sends at least 10ms apart. Any one-second window therefore contains at most 100 sends, or 101 at an exact boundary. Integration test `g` measured a maximum of 97 sends in any one-second window over 1000 SMS across two workers. If Redis is unavailable, the limiter fails closed: no sends rather than unlimited sends. Concurrency is sized with Little's law: 100 msg/s × 0.3s p99 latency ≈ 30 concurrent sends per channel. Each replica runs 25 goroutines per channel × 2 replicas, which leaves headroom.

### Retry classification

| Provider result | Outcome |
|---|---|
| `202` (also `200`, `201`) | **Sent** (`messageId` stored; a non-202 2xx or a missing body is logged as a warning) |
| `408`, `425`, `429` | **Transient**; `Retry-After` is honored |
| `500`–`599` | **Transient**; `Retry-After` is honored on `503` |
| Timeout, connection refused or reset, DNS error, EOF | **Transient** (the provider may have accepted the request, see above) |
| Other `4xx`, any `3xx` (redirects are not followed) | **Permanent**: the same payload cannot succeed |
| Invalid URL, TLS error | **Permanent**: a configuration error |

### Exponential backoff with full jitter

`delay = max(RETRY_MIN, random[0, min(RETRY_CAP, RETRY_BASE × 2^(attempt-1))))`. If the provider sends `Retry-After`, the delay becomes `max(delay, retryAfter)`, capped at 15 minutes.

| Attempt that failed | Upper bound of the next delay |
|---|---|
| 1 | 2s |
| 2 | 4s |
| 3 | 8s |
| 4 | 16s |
| 5 | none: `MAX_ATTEMPTS=5` reached, the notification is marked `failed` |

Full jitter spreads out the retries of a burst that failed at the same moment, so they do not all return together and hit a recovering provider in lockstep.

### Circuit breaker, per channel and per worker replica

`sony/gobreaker/v2` opens after 10 consecutive transient failures, or when more than 50% of at least 20 requests fail. It stays open for 30s and then allows 3 half-open probe requests. Permanent `4xx` responses count as successes, because they mean bad input, not an unhealthy provider. Only the worker's own shutdown aborts are excluded; provider timeouts count as failures.

When the breaker is open, nothing is sent. The row goes back to `retrying` with `attempts - 1` and `deferrals + 1`, and the attempt is recorded as `outcome=circuit_open, counted=false`, so an outage does not use up the retry budget. After 120 deferrals (about an hour) the notification fails.

Breaker state is kept in memory in each replica rather than shared in Redis. Every replica calls the same provider, so they all see the same outage independently. This avoids a Redis round trip per send, and the breaker keeps working when Redis is the component in trouble.

### Dead-letter queue and manual retry

Failed notifications (permanent error, attempts exhausted, or the circuit open too long) go to `{notif}:dlq`; their full attempt history stays in `delivery_attempts`. `POST /api/v1/notifications/{id}/retry` moves a `failed` notification back to `pending` with `attempts=0`, clears `lastError` and enqueues it again.

### Reconciler instead of a transactional outbox

| | Transactional outbox | Reconciler (chosen) |
|---|---|---|
| How Redis learns about new work | A relay process tails an `outbox` table | The API enqueues directly after commit; a periodic scan repairs anything missed |
| Cost | An extra insert and delete per notification, plus a separate relay process | No extra writes on the happy path |
| Repair delay after a lost enqueue | Near zero | At most `PENDING_STALE_AFTER + RECONCILE_INTERVAL` (45s) |
| Crashed workers | Still need a visibility-timeout reclaimer | Handled by the same component |

The reconciler runs every 15s in the worker binary, guarded by a Redis lease. Each step handles at most 1000 rows per cycle, and its keyset cursor is kept between cycles. The steps, in execution order:

- **(d)** Requeue the inflight lists of workers whose heartbeat has expired.
- **(a)** Re-enqueue `pending` rows that are untracked and older than 30s.
- **(b)** Reschedule `retrying` and `scheduled` rows that are untracked and due within 5 minutes.
- **(c)** Reset `processing` rows older than `VISIBILITY_TIMEOUT` (60s) back to `pending`, or to `failed` if their attempts are exhausted, and record an `abandoned` attempt.
- **(e)** Delete expired idempotency keys.

### Other decisions

- **Templates are rendered at create time.** The stored notification contains the exact text that was sent, and later template edits never change notifications already in flight. Templates are soft-deleted. The renderer is a small regex-based `{{variable}}` substitution, not `text/template`: it has no logic and no injection surface, and a missing variable is a `422`.
- **Keyset pagination.** Lists are ordered by `(created_at DESC, id DESC)` and use an opaque base64url cursor. Pages stay stable under concurrent inserts and cost the same at any depth, unlike `OFFSET`.
- **Batches are all-or-nothing.** All items are validated first, then inserted with one `pgx.CopyFrom` in one transaction. One invalid item rejects the whole batch with per-index errors (`notifications[3].recipient`). The alternative, `207 Multi-Status` with partial success, forces every client to reconcile mixed results; here a client simply retries the whole batch with the same batch-level `Idempotency-Key`.
- **`text` + `CHECK` instead of PostgreSQL `ENUM`.** Adding a channel such as `whatsapp` is a single, ordinary migration, with none of the transaction restrictions of `ALTER TYPE ... ADD VALUE`.
- **`updated_at` is maintained by a trigger.** The invariant holds even for manual SQL, and ordering never relies on the application's clock.

---

## 5. Failure scenarios

| Scenario | System Behavior | Automatic Recovery |
|---|---|---|
| **Redis down during `POST /notifications`** | Row commits to PostgreSQL as `pending`; Redis enqueue logs `warn`; API still returns `202 Accepted` (durable). `/health/ready` returns `503`. | Reconciler step `(a)` enqueues untracked stale `pending` rows once Redis recovers. |
| **Redis down for workers** | Dequeue backs off with context-aware timer; GCRA rate limiter **fails closed** (no sends) to protect the `100/s` provider SLA. | Workers resume automatically as soon as Redis responds. |
| **Redis full (`maxmemory`, `noeviction`)** | API enqueue fails with `OOM`, the API still answers `202` (the row is committed). Dequeue and ack keep working: `dequeue.lua` starts with `RPOP`, which is not `denyoom`, and Redis lets a script that has already written finish. | Queues drain and free memory; reconciler `(a)` re-enqueues the rows whose enqueue failed. With `LMOVE` (a `denyoom` command) as the first write, a full Redis with nothing in flight could never drain again. |
| **Redis loses last ~1s (`AOF everysec`)** | Some IDs vanish from ready lists or `{notif}:delayed` ZSET. | Reconciler steps `(a)` (stale `pending`) and `(b)` (untracked `retrying`/`scheduled` due within 5m) restore them from PostgreSQL. |
| **PostgreSQL down** | API create returns `503 service_unavailable`; `/health/ready` returns `503`; worker `Claim` errors schedule the item `now+1s` out of inflight. | Resumes automatically when PostgreSQL recovers. |
| **PostgreSQL refuses connections right after the provider accepted a send** | `InsertAttempt` and `MarkSent` are retried for up to 5s with backoff, so a short outage or a full connection pool does not leave the row in `processing`. | If the database is still unavailable after 5s, the row stays `processing`; reconciler step `(c)` resets it after `VISIBILITY_TIMEOUT` and it is sent again (at-least-once, same `Idempotency-Key`). |
| **Worker crash (`SIGKILL`) mid-send** | Row stays `processing` in DB; ID stays in `{notif}:inflight:<deadWorkerId>`. | Step `(d)` requeues orphaned inflight list after `HEARTBEAT_TTL` (`15s`); step `(c)` resets `processing -> pending` (or `failed` if attempts exhausted) after `VISIBILITY_TIMEOUT` (`60s`) and records `outcome='abandoned'`. |
| **Redis write fails after DB transition (worker alive)** | Row transitioned in DB, but follow-up `Ack`/`Schedule`/`DeadLetter` failed; ID remains in live worker's inflight list. | Live worker's **Inflight Sweeper** detects IDs inactive across two consecutive sweeps and requeues them via `requeue_ids.lua`. |
| **API crash after DB commit, before Redis enqueue** | Row is `pending` in PostgreSQL and absent from `{notif}:msg`. | Reconciler step `(a)` (`force_enqueue.lua`) after `PENDING_STALE_AFTER` (`30s`). |
| **Provider slow / `5xx` outage** | Timeouts & `5xx` trigger full-jitter exponential backoff; after `10` consecutive failures (or `> 50%` of `20` requests), channel circuit breaker opens for `30s`. | Open-circuit items defer via `DeferCircuitOpen` (`attempts-1`, `deferrals+1`, `counted=false`); after `30s`, `3` half-open probes close the circuit on `2xx`. |
| **Provider `202` lost in network timeout** | Worker classifies timeout as `Transient` and retries with identical `Idempotency-Key: <notification.id>`. | Provider-side deduplication (or duplicate delivery if provider ignores `Idempotency-Key`). |
| **Client retries `POST` after network drop** | `idempotency_keys` lookup matches key + SHA-256 request hash. | Returns original `202` resource with `Idempotent-Replayed: true` header. |
| **Cancel vs. Worker race** | Both execute atomic conditional SQL `UPDATE ... WHERE id=$1 AND status IN (...)`. | Whichever transaction commits first wins; loser sees `0` rows (`409 Conflict` for API cancel, or `claimed=false -> Ack` for worker). |


### Verified failure drills

`scripts/drills.sh` (`make drills`) runs these against the live Compose stack: it breaks one dependency, checks the documented behavior, runs `scripts/invariants.sh` on the result and prints PASS/FAIL with measured times. The invariants use `delivery_attempts` in PostgreSQL as the complete history (no `sent` attempt on a cancelled row, no notification sent twice) and cross-check the mock provider's per-`Idempotency-Key` counts to find provider calls the system never recorded, which is how a worker crash shows up as an at-least-once duplicate. Results from 2026-11-21, on the hardware listed in section 10:

| Drill | What it does | Result |
|---|---|---|
| 1. Redis down during create | `docker compose stop redis`, then `POST` a notification | `202` in ~3s (all post-commit Redis work shares one 2s budget), `/health/ready` = `503`; sent 36s after Redis restarted (reconciler step a: `PENDING_STALE_AFTER` 30s + cycle) |
| 2. PostgreSQL down | `docker compose stop postgres`, then `POST` | `503 service_unavailable`, `/health/ready` = `503`; after restart a new notification was sent within 1s |
| 3. Worker SIGKILL mid-batch | 300 SMS with 200ms provider latency, `docker compose kill worker-1` after 1s | 300/300 sent 64s after the kill (orphaned inflight list requeued after the heartbeat expired, abandoned `processing` rows reclaimed after `VISIBILITY_TIMEOUT`); 4 to 11 duplicate provider calls across runs, one per send in flight at the moment of the kill, all with the same `Idempotency-Key`; the invariants check counts the same number of provider calls the system never recorded. `abandoned` attempts can be higher (24 vs 11 in one run): rows that were claimed and still waiting for a rate-limit token when the worker died were never sent, are reset by the reconciler, and lose one attempt |
| 4. Provider outage | Mock provider returns 500 for everything, 40 SMS | Breaker open (`circuit_breaker_state=2`) 6s into the outage, 0 notifications failed; 40/40 sent 25s after the provider recovered |
| 5. Redis data loss | `FLUSHALL` while a 500-SMS batch is being delivered (138 already sent) | 500/500 sent 34s later, rebuilt from PostgreSQL by the reconciler; 0 duplicates |

---

## 6. Alternatives and scaling to 100M/day

| Option | Strengths | Why not chosen for this scope |
|---|---|---|
| **A. PostgreSQL + Redis (chosen)** | Weighted priority, delayed set, distributed rate limiter and pub/sub in one fast dependency; simple Compose setup | Redis is not durable on its own (handled by the reconciler); single hash slot in Redis Cluster |
| **B. PostgreSQL + RabbitMQ** | Native acknowledgements, priority queues (`x-max-priority`), dead-letter exchanges, per-message TTL | Still needs Redis (or similar) for a distributed rate limiter; arbitrary per-message delays need the delayed-message plugin |
| **C. PostgreSQL only (`FOR UPDATE SKIP LOCKED`)** | No extra infrastructure, one transactional boundary | Polling 9 queues (3 channels × 3 priorities) at hundreds of messages per second makes the database the bottleneck (index churn, vacuum pressure) |
| **D. Kafka** | Very high throughput, durable replayable log, consumer groups | No native priority (one topic per priority), no per-message delay (retry topics instead), heavier operations |

**How I would scale to 100M notifications/day** (~1,200/s on average, 10–25k/s in bursts):

1. **Move dispatch to Kafka.** Use topics per `(channel, priority)` partitioned by tenant or recipient hash, tiered retry topics (`retry.5s`, `retry.1m`, `retry.10m`) and a `dlq` topic. Consumers read the high-priority topics more often to keep the weighted fairness. The API keeps writing PostgreSQL first, and a transactional outbox, possibly via Debezium CDC, replaces the reconciler's enqueue repair, because at this volume a 45s repair window is too coarse.
2. **Use provider-specific worker pools.** Each provider gets its own consumers, concurrency, rate limit and breaker, so one slow SMS vendor cannot hold capacity that email needs.
3. **Raise rate limits per provider account.** Real providers allow far more than 100/s. Keep GCRA but lease tokens in blocks (for example 10 per round trip) to cut Redis calls.
4. **Partition PostgreSQL.** Partition `notifications` and `delivery_attempts` by month on `created_at` (`pg_partman`) and drop or archive old partitions. Move `delivery_attempts` history to cheap storage (S3/Parquet, or ClickHouse for analytics) after N days.
5. **Use read replicas.** Serve list, batch status and the metrics snapshot from replicas so the primary only handles writes.
6. **Use Redis Cluster** with per-channel hash tags (`{notif:sms}`, `{notif:email}`, `{notif:push}`) instead of a single `{notif}` slot, so queue load spreads across shards.
7. **Add multi-tenancy.** Make `tenant_id` part of idempotency keys, rate-limit keys, partitions and authorization.

---

## 7. API examples

These examples were run exactly as written against a fresh `docker compose up --build`, and the outputs below are the real responses (JSON pretty-printed here; IDs and timestamps will differ on your machine). Run them in order in one shell: later blocks reuse `$SMS_ID`, `$BATCH_ID` and `$TPL_ID`. They need only `bash`, `curl`, `grep`, `cut` and GNU `date`.

Request bodies that contain non-ASCII characters (Turkish text) are passed on stdin with `--data-binary @- <<'JSON'`. On Windows, the native `curl.exe` re-encodes command-line arguments with the ANSI code page, so `ş`/`ğ` would arrive as invalid UTF-8; the API rejects such bodies with `400 malformed_json` instead of storing garbled text. Stdin bytes are sent unchanged on every platform.

The complete contract, with every status code and error example, is in [`api/openapi.yaml`](api/openapi.yaml) (served at `/openapi.yaml` and rendered by Swagger UI at http://localhost:8081). All errors use the same envelope:

```json
{"error": {"code": "validation_failed", "message": "...", "details": [{"field": "notifications[3].recipient", "code": "invalid_e164", "message": "..."}]}, "correlationId": "..."}
```

### Shell setup used by every example

```bash
BASE=http://localhost:8080
# Tiny helper so the examples need only curl, grep and cut (no jq).
json_get() { grep -o "\"$1\":\"[^\"]*\"" | head -n1 | cut -d'"' -f4; }
```

### Health probes (`GET /health/live`, `/health/ready`, `/health`) — `FR-4.3`

```bash
curl -s $BASE/health/live; echo
curl -s $BASE/health/ready; echo
curl -s -o /dev/null -w 'GET /health -> %{http_code}\n' $BASE/health
```

Output:

```text
{
  "status": "ok"
}

{
  "status": "ok",
  "checks": {
    "postgres": {
      "status": "ok",
      "latencyMs": 0
    },
    "redis": {
      "status": "ok",
      "latencyMs": 0
    }
  }
}

GET /health -> 200
```

### Create a notification (`POST /api/v1/notifications`) — `FR-1.1, FR-2.5`

```bash
RESP=$(curl -s -D - -X POST "$BASE/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-42-shipped" \
  -H "X-Correlation-ID: corr-order-42" \
  --data-binary @- <<'JSON'
{"recipient":"+905551234567","channel":"sms","priority":"high","content":"Siparişiniz kargoya verildi"}
JSON
)
echo "$RESP"
SMS_ID=$(echo "$RESP" | json_get id)
```

Output:

```text
HTTP/1.1 202 Accepted
Content-Type: application/json; charset=utf-8
Vary: Origin
X-Correlation-Id: corr-order-42
Date: Fri, 20 Nov 2026 22:05:45 GMT
Content-Length: 427

{
  "id": "01a1fafe-7f45-7603-8cc0-8fb7aa9b854a",
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
  "createdAt": "2026-11-20T22:05:45.157646Z",
  "updatedAt": "2026-11-20T22:05:45.157646Z",
  "sentAt": null,
  "failedAt": null,
  "cancelledAt": null
}
```

### Retry the same request: idempotent replay — `FR-2.5`

```bash
curl -s -i -X POST "$BASE/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-42-shipped" \
  --data-binary @- <<'JSON' | grep -iE '^(HTTP/|Idempotent-Replayed|\{)' | cut -c1-120
{"recipient":"+905551234567","channel":"sms","priority":"high","content":"Siparişiniz kargoya verildi"}
JSON
```

Output:

```text
HTTP/1.1 202 Accepted
Idempotent-Replayed: true
{"id":"01a1fafe-7f45-7603-8cc0-8fb7aa9b854a","batchId":null,"recipient":"+905551234567","channel":"sms","priority":"high
```

### Same key, different body: `409 idempotency_conflict` — `FR-2.5`

```bash
curl -s -w '\nHTTP %{http_code}\n' -X POST "$BASE/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-42-shipped" \
  -d '{"recipient":"+905551234567","channel":"sms","content":"A different message"}' 
```

Output:

```text
{
  "error": {
    "code": "idempotency_conflict",
    "message": "idempotency key was previously used with a different request payload"
  },
  "correlationId": "01a1fafe-8013-761f-96fa-4911ba2f3e62"
}

HTTP 409
```

### Validation errors are returned together (`422`) — `FR-2.4`

```bash
curl -s -w '\nHTTP %{http_code}\n' -X POST "$BASE/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -d '{"recipient":"05551234567","channel":"sms","subject":"not allowed on sms","content":"   "}' 
```

Output:

```text
{
  "error": {
    "code": "validation_failed",
    "message": "request has 3 invalid fields",
    "details": [
      {
        "field": "content",
        "code": "required",
        "message": "content is required"
      },
      {
        "field": "recipient",
        "code": "invalid_e164",
        "message": "must match ^\\+[1-9]\\d{7,14}$"
      },
      {
        "field": "subject",
        "code": "unsupported_field",
        "message": "subject is only supported for email channel"
      }
    ]
  },
  "correlationId": "01a1fafe-8035-77d0-9e9c-801b239a7581"
}

HTTP 422
```

### Email and push — `FR-1.1`

```bash
curl -s -X POST "$BASE/api/v1/notifications" -H "Content-Type: application/json" \
  -d '{"recipient":"customer@example.com","channel":"email","subject":"Order #42 shipped","content":"Your order has left our warehouse."}'; echo
curl -s -X POST "$BASE/api/v1/notifications" -H "Content-Type: application/json" \
  -d '{"recipient":"fcm-token-9b7a2c1e","channel":"push","title":"Package delivered","content":"Your delivery has arrived.","priority":"low"}'; echo
```

Output:

```text
{
  "id": "01a1fafe-8056-71d1-89e6-29d3dadb26d1",
  "batchId": null,
  "recipient": "customer@example.com",
  "channel": "email",
  "priority": "normal",
  "status": "pending",
  "subject": "Order #42 shipped",
  "content": "Your order has left our warehouse.",
  "attempts": 0,
  "maxAttempts": 5,
  "scheduledAt": null,
  "nextAttemptAt": null,
  "providerMessageId": null,
  "lastError": null,
  "createdAt": "2026-11-20T22:05:45.430236Z",
  "updatedAt": "2026-11-20T22:05:45.430236Z",
  "sentAt": null,
  "failedAt": null,
  "cancelledAt": null
}

{
  "id": "01a1fafe-8082-74d6-92c1-c03945d9f62d",
  "batchId": null,
  "recipient": "fcm-token-9b7a2c1e",
  "channel": "push",
  "priority": "low",
  "status": "pending",
  "title": "Package delivered",
  "content": "Your delivery has arrived.",
  "attempts": 0,
  "maxAttempts": 5,
  "scheduledAt": null,
  "nextAttemptAt": null,
  "providerMessageId": null,
  "lastError": null,
  "createdAt": "2026-11-20T22:05:45.474458Z",
  "updatedAt": "2026-11-20T22:05:45.474458Z",
  "sentAt": null,
  "failedAt": null,
  "cancelledAt": null
}

```

### Get status and delivery attempts (`GET /api/v1/notifications/{id}`) — `FR-1.3`

```bash
sleep 2   # give a worker time to deliver it
curl -s "$BASE/api/v1/notifications/$SMS_ID"
```

Output:

```text
{
  "id": "01a1fafe-7f45-7603-8cc0-8fb7aa9b854a",
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
  "providerMessageId": "f935673b-7b4e-470c-ad15-04b3627e6b0e",
  "lastError": null,
  "createdAt": "2026-11-20T22:05:45.157646Z",
  "updatedAt": "2026-11-20T22:05:45.332013Z",
  "sentAt": "2026-11-20T22:05:45.332013Z",
  "failedAt": null,
  "cancelledAt": null,
  "deliveryAttempts": [
    {
      "id": 1,
      "attemptNo": 1,
      "counted": true,
      "startedAt": "2026-11-20T22:05:45.326116Z",
      "durationMs": 1,
      "httpStatus": 202,
      "outcome": "sent",
      "error": null,
      "providerMessageId": "f935673b-7b4e-470c-ad15-04b3627e6b0e",
      "workerId": "worker-2"
    }
  ]
}
```

### Schedule for later, then cancel (`POST /api/v1/notifications/{id}/cancel`) — `B-2, FR-1.5`

```bash
SCHEDULED_AT=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)   # GNU date; on macOS: date -u -v+1H +%Y-%m-%dT%H:%M:%SZ
SCHED_ID=$(curl -s -X POST "$BASE/api/v1/notifications" -H "Content-Type: application/json" \
  -d "{\"recipient\":\"+905551234568\",\"channel\":\"sms\",\"content\":\"Flash sale starts now\",\"scheduledAt\":\"$SCHEDULED_AT\"}" | json_get id)
curl -s "$BASE/api/v1/notifications/$SCHED_ID" | grep -o '"status":"[a-z]*","content[^,]*\|"scheduledAt":"[^"]*"'
curl -s -X POST "$BASE/api/v1/notifications/$SCHED_ID/cancel" | grep -o '"status":"[a-z]*"\|"cancelledAt":"[^"]*"'
# Cancelling again (or cancelling a sent notification) is a 409 that reports the current status:
curl -s -w '\nHTTP %{http_code}\n' -X POST "$BASE/api/v1/notifications/$SCHED_ID/cancel"
```

Output:

```text
"status":"scheduled","content":"Flash sale starts now"
"scheduledAt":"2026-11-20T23:05:47Z"
"status":"cancelled"
"cancelledAt":"2026-11-20T22:05:47.791059Z"
{
  "error": {
    "code": "not_cancellable",
    "message": "notification cannot be cancelled in status \"cancelled\"",
    "details": [
      {
        "field": "status",
        "code": "current_status",
        "message": "cancelled"
      }
    ]
  },
  "correlationId": "01a1fafe-89bd-7c97-a80a-c1d36df17d11"
}

HTTP 409
```

### Batch create, query and cancel (`/api/v1/notifications/batch`, `/api/v1/batches/{id}`) — `FR-1.2, FR-1.4`

```bash
BATCH=$(curl -s -X POST "$BASE/api/v1/notifications/batch" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: flash-sale-2026-11-21" \
  --data-binary @- <<'JSON'
{"notifications":[
  {"recipient":"+905551234501","channel":"sms","priority":"high","content":"Flaş indirim başladı!"},
  {"recipient":"user@example.com","channel":"email","subject":"Flash sale","content":"50% off today."},
  {"recipient":"fcm-device-token-12345","channel":"push","priority":"low","title":"Sale","content":"Tap to view."}
]}
JSON
)
echo "$BATCH"
BATCH_ID=$(echo "$BATCH" | json_get batchId)
sleep 2
curl -s "$BASE/api/v1/batches/$BATCH_ID?limit=1"; echo
curl -s -X POST "$BASE/api/v1/batches/$BATCH_ID/cancel"; echo
```

Output:

```text
{
  "batchId": "01a1fafe-8a01-793f-a103-9e7b8987f2d2",
  "count": 3,
  "notificationIds": [
    "01a1fafe-8a01-7971-ac01-7d722ee694e6",
    "01a1fafe-8a01-797a-9e59-e0787fd3eb50",
    "01a1fafe-8a01-797d-95ca-5859c3363032"
  ]
}
{
  "batchId": "01a1fafe-8a01-793f-a103-9e7b8987f2d2",
  "total": 3,
  "countsByStatus": {
    "sent": 3
  },
  "createdAt": "2026-11-20T22:05:47.90581Z",
  "items": [
    {
      "id": "01a1fafe-8a01-797d-95ca-5859c3363032",
      "batchId": "01a1fafe-8a01-793f-a103-9e7b8987f2d2",
      "recipient": "fcm-device-token-12345",
      "channel": "push",
      "priority": "low",
      "status": "sent",
      "title": "Sale",
      "content": "Tap to view.",
      "attempts": 1,
      "maxAttempts": 5,
      "scheduledAt": null,
      "nextAttemptAt": null,
      "providerMessageId": "18966108-ead1-428e-bc91-d5833e77d5ba",
      "lastError": null,
      "createdAt": "2026-11-20T22:05:47.90581Z",
      "updatedAt": "2026-11-20T22:05:48.118976Z",
      "sentAt": "2026-11-20T22:05:48.118976Z",
      "failedAt": null,
      "cancelledAt": null
    }
  ],
  "nextCursor": "eyJ0IjoiMjAyNi0xMS0yMFQyMjowNTo0Ny45MDU4MVoiLCJpZCI6IjAxYTFmYWZlLThhMDEtNzk3ZC05NWNhLTU4NTljMzM2MzAzMiJ9"
}

{
  "batchId": "01a1fafe-8a01-793f-a103-9e7b8987f2d2",
  "cancelled": 0,
  "notCancellable": 3
}

```

### Templates: create, read, list, update, preview, use in a batch, delete (`/api/v1/templates`) — `B-3`

```bash
TPL=$(curl -s -X POST "$BASE/api/v1/templates" -H "Content-Type: application/json" --data-binary @- <<'JSON'
{"name":"otp_sms_tr","channel":"sms","body":"Merhaba {{name}}, doğrulama kodunuz: {{code}}","variables":["name","code"]}
JSON
)
echo "$TPL"
TPL_ID=$(echo "$TPL" | json_get id)
curl -s "$BASE/api/v1/templates/$TPL_ID" | grep -o '"name":"[^"]*"\|"variables":\[[^]]*\]'
curl -s "$BASE/api/v1/templates?channel=sms&limit=10" | grep -o '"name":"[^"]*"\|"nextCursor":[a-z]*'
curl -s -X PUT "$BASE/api/v1/templates/$TPL_ID" -H "Content-Type: application/json" --data-binary @- <<'JSON' | grep -o '"body":"[^"]*"'
{"name":"otp_sms_tr","channel":"sms","body":"Merhaba {{name}}, kodunuz: {{code}} (5 dk geçerli)","variables":["name","code"]}
JSON
curl -s -X POST "$BASE/api/v1/templates/$TPL_ID/preview" -H "Content-Type: application/json" --data-binary @- <<'JSON'; echo
{"variables":{"name":"Ayşe","code":"4821"}}
JSON
curl -s -X POST "$BASE/api/v1/notifications/batch" -H "Content-Type: application/json" --data-binary @- <<JSON; echo
{"notifications":[
  {"recipient":"+905551230001","channel":"sms","templateId":"$TPL_ID","templateVariables":{"name":"Ayşe","code":"4821"}},
  {"recipient":"+905551230002","channel":"sms","templateId":"$TPL_ID","templateVariables":{"name":"Mehmet","code":"9037"}}
]}
JSON
curl -s -o /dev/null -w 'DELETE -> %{http_code}\n' -X DELETE "$BASE/api/v1/templates/$TPL_ID"
curl -s -w '\nHTTP %{http_code}\n' "$BASE/api/v1/templates/$TPL_ID"
```

Output:

```text
{
  "id": "01a1fafe-9307-7394-ab35-8e6cb1d95abf",
  "name": "otp_sms_tr",
  "channel": "sms",
  "body": "Merhaba {{name}}, doğrulama kodunuz: {{code}}",
  "variables": [
    "name",
    "code"
  ],
  "createdAt": "2026-11-20T22:05:50.215831Z",
  "updatedAt": "2026-11-20T22:05:50.215831Z"
}
"name":"otp_sms_tr"
"variables":["name","code"]
"name":"otp_sms_tr"
"nextCursor":null
"body":"Merhaba {{name}}, kodunuz: {{code}} (5 dk geçerli)"
{
  "subject": null,
  "title": null,
  "content": "Merhaba Ayşe, kodunuz: 4821 (5 dk geçerli)",
  "sms": {
    "encoding": "UCS-2",
    "units": 42,
    "segments": 1
  },
  "unusedVariables": []
}

{
  "batchId": "01a1fafe-9444-747b-95d1-9da32571baca",
  "count": 2,
  "notificationIds": [
    "01a1fafe-9444-7495-a439-e78adbd94413",
    "01a1fafe-9444-7498-9f2d-04946c61c7d2"
  ]
}

DELETE -> 204
{
  "error": {
    "code": "not_found",
    "message": "requested resource was not found"
  },
  "correlationId": "01a1fafe-948f-7437-bb12-47a358edc02b"
}

HTTP 404
```

### List with filters and keyset pagination (`GET /api/v1/notifications`) — `FR-1.6`

```bash
sleep 1
PAGE1=$(curl -s "$BASE/api/v1/notifications?channel=sms&status=sent&from=2026-01-01T00:00:00Z&limit=2")
echo "$PAGE1" | grep -o '"id":"[^"]*","batchId[^,]*\|"nextCursor":[^}]*'
CURSOR=$(echo "$PAGE1" | json_get nextCursor)
curl -s "$BASE/api/v1/notifications?channel=sms&status=sent&from=2026-01-01T00:00:00Z&limit=2&cursor=$CURSOR" \
  | grep -o '"id":"[^"]*","batchId[^,]*\|"nextCursor":[^}]*'
curl -s -w '\nHTTP %{http_code}\n' "$BASE/api/v1/notifications?cursor=not-a-cursor"
```

Output:

```text
"id":"01a1fafe-9444-7498-9f2d-04946c61c7d2","batchId":"01a1fafe-9444-747b-95d1-9da32571baca"
"id":"01a1fafe-9444-7495-a439-e78adbd94413","batchId":"01a1fafe-9444-747b-95d1-9da32571baca"
"nextCursor":"eyJ0IjoiMjAyNi0xMS0yMFQyMjowNTo1MC41MzIzOVoiLCJpZCI6IjAxYTFmYWZlLTk0NDQtNzQ5NS1hNDM5LWU3OGFkYmQ5NDQxMyJ9"
"id":"01a1fafe-8a01-7971-ac01-7d722ee694e6","batchId":"01a1fafe-8a01-793f-a103-9e7b8987f2d2"
"id":"01a1fafe-7f45-7603-8cc0-8fb7aa9b854a","batchId":null
"nextCursor":null
{
  "error": {
    "code": "invalid_cursor",
    "message": "cursor parameter is malformed"
  },
  "correlationId": "01a1fafe-998a-7962-8034-b9c4cab6f2ca"
}

HTTP 400
```

### Dead-letter and manual retry (`POST /api/v1/notifications/{id}/retry`) — `B-1`

```bash
# Make the mock provider reject everything with a permanent 400 (run from the repository root).
MOCK_PERMANENT_RATE=1 docker compose up -d --wait mockprovider 2>/dev/null
FAIL_ID=$(curl -s -X POST "$BASE/api/v1/notifications" -H "Content-Type: application/json" \
  -d '{"recipient":"+905551234569","channel":"sms","content":"This one will be dead-lettered"}' | json_get id)
sleep 2
curl -s "$BASE/api/v1/notifications/$FAIL_ID" | grep -o '"status":"[a-z]*"\|"attempts":[0-9]*\|"lastError":"[^"]*"'
# Restore the provider and replay the notification from the DLQ.
docker compose up -d --wait mockprovider 2>/dev/null
curl -s -X POST "$BASE/api/v1/notifications/$FAIL_ID/retry" | grep -o '"status":"[a-z]*"\|"attempts":[0-9]*'
sleep 2
curl -s "$BASE/api/v1/notifications/$FAIL_ID" | grep -o '"status":"[a-z]*"\|"attempts":[0-9]*\|"outcome":"[a-z_]*"'
curl -s -w '\nHTTP %{http_code}\n' -X POST "$BASE/api/v1/notifications/$FAIL_ID/retry"
```

Output:

```text
"status":"failed"
"attempts":1
"lastError":"provider rejected request with HTTP 400"
"status":"pending"
"attempts":0
"status":"sent"
"attempts":1
"outcome":"permanent"
"outcome":"sent"
{
  "error": {
    "code": "not_retryable",
    "message": "notification cannot be retried in status \"sent\"",
    "details": [
      {
        "field": "status",
        "code": "current_status",
        "message": "sent"
      }
    ]
  },
  "correlationId": "01a1fafe-e0f2-77f9-a31f-b7f57490cee2"
}

HTTP 409
```

### Real-time updates over WebSocket (`GET /api/v1/ws`) — `B-4`

```bash
# Browser: open http://localhost:8080/demo. From a shell, curl can show the handshake and the first frame:
curl -s -i -N --max-time 2 \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  "$BASE/api/v1/ws?notificationId=$SMS_ID" | head -c 400 | tr -d '\r' | tr -c '[:print:]\n' '.'; echo
curl -s -w '\nHTTP %{http_code}\n' "$BASE/api/v1/ws"
```

Output:

```text
HTTP/1.1 101 Switching Protocols
Connection: Upgrade
Sec-Websocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
Upgrade: websocket
Vary: Origin
X-Correlation-Id: 01a1fafe-e119-7c5a-8fd1-1482ce8b849d
Date: Fri, 20 Nov 2026 22:06:10 GMT

.~..{"type":"snapshot","notification":{"id":"01a1fafe-7f45-7603-8cc0-8fb7aa9b854a","batchId":null,"recipient":"+905551234567","channel":"sms","priority":"high","sta
{
  "error": {
    "code": "missing_ws_filter",
    "message": "at least one filter query parameter (notificationId, batchId, or channel) is required"
  },
  "correlationId": "01a1fafe-e916-785e-82ec-3fdf5b02faa5"
}

HTTP 400
```

### Metrics (`GET /api/v1/metrics`, `GET /metrics`) — `FR-4.1`

```bash
curl -s "$BASE/api/v1/metrics"; echo
curl -s "$BASE/metrics" | grep -E '^notifications_created_total'
curl -s "$BASE/metrics" | grep -E '^http_requests_total\{code="202"' 
```

Output:

```text
{
  "generatedAt": "2026-11-20T22:06:12.280796769Z",
  "queues": {
    "sms": {
      "high": 0,
      "normal": 0,
      "low": 0
    },
    "email": {
      "high": 0,
      "normal": 0,
      "low": 0
    },
    "push": {
      "high": 0,
      "normal": 0,
      "low": 0
    },
    "delayed": 0,
    "dlq": 0,
    "inflight": 0
  },
  "windows": {
    "1m": {
      "sms": {
        "sent": 5,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      },
      "email": {
        "sent": 2,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      },
      "push": {
        "sent": 2,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      }
    },
    "5m": {
      "sms": {
        "sent": 5,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      },
      "email": {
        "sent": 2,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      },
      "push": {
        "sent": 2,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      }
    },
    "15m": {
      "sms": {
        "sent": 5,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      },
      "email": {
        "sent": 2,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      },
      "push": {
        "sent": 2,
        "failed": 0,
        "retrying": 0,
        "successRate": 1,
        "failureRate": 0
      }
    }
  },
  "latencyMs": {
    "endToEnd": {
      "p50": 204.88,
      "p95": 5792.1,
      "p99": 8519.56
    },
    "providerCall": {
      "p50": 0,
      "p95": 1.55,
      "p99": 1.91
    }
  }
}

notifications_created_total{channel="email",priority="normal"} 2
notifications_created_total{channel="push",priority="low"} 2
notifications_created_total{channel="sms",priority="high"} 2
notifications_created_total{channel="sms",priority="normal"} 4
http_requests_total{code="202",method="POST",route="/api/v1/notifications"} 6
http_requests_total{code="202",method="POST",route="/api/v1/notifications/batch"} 2
```

`successRate` and `failureRate` are `sent / (sent + failed)` and `failed / (sent + failed)`. They are `null` for a window in which a channel had neither, so an idle channel does not read as 0% success.

### OpenAPI spec and demo page (`GET /openapi.yaml`, `GET /demo`) — `D-4, B-4`

```bash
curl -s "$BASE/openapi.yaml" | head -n 4
curl -s -o /dev/null -w 'GET /demo -> %{http_code} %{content_type}\n' "$BASE/demo"
```

Output:

```text
openapi: 3.0.3
info:
  title: Event-Driven Notification System API
  version: 1.1.0
GET /demo -> 200 text/html; charset=utf-8
```

Swagger UI at http://localhost:8081 renders the same spec, and **Try it out** calls the API on port 8080 (CORS allows that origin):

![Swagger UI listing the System and Notifications endpoints of the 1.0.0 spec](docs/images/swagger.png)

The `/demo` page creates a batch and follows it over the WebSocket. Here 40 SMS went from `pending` through `processing` to `sent`, and the log shows the events as they arrived:

![Demo page after a 40-item SMS batch: every row sent, WebSocket event log below the table](docs/images/demo.png)

---

## 8. Observability

### Prometheus metrics

The API exposes them on `:8080/metrics` and each worker on `:9091/metrics`. Each process uses its own registry, and the Go and process collectors are included.

| Metric | Type | Labels | Emitted by |
|---|---|---|---|
| `notifications_created_total` | counter | `channel, priority` | api |
| `notifications_processed_total` | counter | `channel, status` (`sent`, `failed`, `retrying` from workers; `cancelled` from the API) | worker, api |
| `provider_requests_total` | counter | `channel, code` | worker |
| `delivery_duration_seconds` | histogram | `channel` | worker (provider HTTP call) |
| `end_to_end_latency_seconds` | histogram | `channel` | worker (`created_at` → `sent_at`) |
| `queue_depth` | gauge | `channel, priority` | api (sampled every 5s) |
| `delayed_depth`, `dlq_depth`, `inflight_depth` | gauge | — | api (sampled every 5s) |
| `rate_limited_total` | counter | `channel` | worker |
| `rate_limit_wait_seconds` | histogram | `channel` | worker |
| `circuit_breaker_state` | gauge | `channel` (`0` closed, `1` half-open, `2` open) | worker |
| `retries_scheduled_total` | counter | `channel` | worker |
| `lease_lost_total` | counter | — | worker |
| `reconciler_actions_total` | counter | `action` | worker |
| `worker_panics_total` | counter | — | worker |
| `http_requests_total` | counter | `route, method, code` (`route` is the chi pattern, never the raw path) | api |
| `http_request_duration_seconds` | histogram | `route, method` | api |
| `db_pool_acquired_conns`, `db_pool_idle_conns`, `db_pool_total_conns`, `db_pool_max_conns` | gauge | — | api, worker (read from `pgxpool.Stat()` at scrape time) |
| `db_pool_empty_acquire_total` | counter | — | api, worker (acquires that had to wait for a free connection: the pool-saturation signal) |

No metric is labeled with a notification ID, batch ID, recipient or correlation ID, which keeps the number of series bounded. Queue depths are sampled only by the API process, so two worker replicas do not report duplicate gauge series.

Useful queries (Prometheus UI at http://localhost:9090):

```promql
# Sends per second per channel; should stay <= 100
sum by (channel) (rate(notifications_processed_total{status="sent"}[1m]))

# p99 provider call latency per channel
histogram_quantile(0.99, sum by (le, channel) (rate(delivery_duration_seconds_bucket[5m])))

# Backlog per queue, and whether any breaker is open
sum by (channel, priority) (queue_depth)
max by (channel) (circuit_breaker_state)

# Share of provider calls that failed, per channel
sum by (channel) (rate(provider_requests_total{code!~"2.."}[5m])) / sum by (channel) (rate(provider_requests_total[5m]))
```

`GET /api/v1/metrics` returns a human-readable JSON snapshot, shown in the API examples above. It combines live Redis queue lengths with 1m/5m/15m counts and 15-minute latency percentiles computed in PostgreSQL (`percentile_cont` over partial indexes on `sent_at` and `failed_at`), behind a 2-second in-memory cache so dashboards cannot overload the database.

### Structured logs

Logs are JSON from `log/slog`, written to stdout. Every line carries `correlation_id`, plus `trace_id` and `span_id` when a span is active. Message content is never logged, and recipients are masked. This is a real line from `docker compose logs worker-1`:

```json
{"time":"2026-11-20T21:42:34.101265864Z","level":"INFO","msg":"notification sent","service":"notification-worker","worker_id":"worker-1","correlation_id":"corr-order-42","trace_id":"09d2ecbf25118793e9b2198108c26f54","span_id":"01b40b43cc72c10a","notification_id":"01a1fae9-4526-71b6-87c9-8179d430601b","channel":"sms","priority":"high","attempt":1,"recipient":"+90555***4567","outcome":"sent","duration_ms":1}
```

`correlation_id` is the business identifier: you can pass your own with `X-Correlation-ID`, it is stored on the row, carried through Redis and sent to the provider. `trace_id` is the technical identifier that links the spans in Jaeger. Both are kept because they answer different questions: "what happened to this order's message?" versus "where did this request spend its time?".

### Distributed tracing

OpenTelemetry spans cover the API request, every SQL query (`otelpgx`), the Redis enqueue, the worker's dequeue → rate-limit wait → claim → provider call → status update, and the provider side (mockprovider). The W3C `traceparent` is stored in the Redis metadata, so for a single notification the worker continues the API's trace. Batch items are different: each one starts its own trace with a span link back to the batch request. As children, one 1,000-item batch produced a single trace of 28,013 spans (~145 MB in Jaeger's memory); with links the batch request trace has 17 spans and each item ~28. Jaeger keeps at most 20,000 traces in memory (`JAEGER_MAX_TRACES`) under a 1.5 GB container limit (`JAEGER_MEM_LIMIT`): before that limit existed, a day of load tests grew it until Docker Desktop froze. Open http://localhost:16686, select service `notification-api` and open any `POST /api/v1/notifications` trace.

![Trace waterfall of one notification: API insert and enqueue, worker claim, provider call, retry](docs/images/trace.png)

*Jaeger UI screenshot of one notification (37 spans across `notification-api`, `notification-worker` and `notification-mockprovider`). The mock provider was set to fail half of the requests: the first attempt got a `500` (red markers), the worker recorded it and called `schedule.retry`, and the second attempt about 0.9s later ended in `MarkSent`.*

### Health checks

- `GET /health/live`: the process is up.
- `GET /health/ready` (and its alias `/health`): pings PostgreSQL and Redis with a 1s timeout each and returns `503` if either fails.

Workers expose the same probes on `:9091`. The Compose healthchecks use `cmd/healthcheck`.

---

## 9. Configuration

Every setting has a default inside `docker-compose.yml` (`${VAR:-default}`), so no `.env` file is required. `.env.example` lists them all. Invalid combinations stop the process at startup; for example, `VISIBILITY_TIMEOUT` must be at least `2 × PROVIDER_TIMEOUT + 15s`.

| Variable | Default | Description & Validation |
|---|---|---|
| `HTTP_ADDR` | `:8080` | API HTTP listen address |
| `WORKER_METRICS_ADDR` | `:9091` | Worker health & Prometheus `/metrics` listen address |
| `DATABASE_URL` | `postgres://notif:notif@postgres:5432/notif?sslmode=disable` | PostgreSQL connection string (password masked in logs) |
| `REDIS_ADDR` | `redis:6379` | Redis `host:port` |
| `REDIS_POOL_SIZE` | `0` (`auto`) | `0` auto-sizes to `3 * WORKER_CONCURRENCY_PER_CHANNEL + 20` |
| `DB_MAX_CONNS` | `0` (`auto`) | PostgreSQL pool size per process; `0` = API 20, worker 10. Every replica opens its own pool, so keep `replicas × pool + API ≤ max_connections` (100 by default) |
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
| `MAX_WS_CLIENTS` | `1000` | Maximum concurrent WebSocket clients per API replica (`>= 1`); more get `503 too_many_ws_clients` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4318` | OpenTelemetry OTLP HTTP endpoint |
| `OTEL_SAMPLER_RATIO` | `1.0` | Trace sampling ratio (`[0, 1]`) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |

---

## 10. Testing

**Fastest way to check everything, with only Docker installed** (no Go, `make` or GCC on the host):

```bash
./scripts/verify.sh              # lint, unit, integration, then the live stack: up, smoke test, invariants
./scripts/verify.sh --no-stack   # only the Go checks
./scripts/test-docker.sh         # unit + integration tests only (also: unit | integration | lint)
```

Both run the Go tools inside the pinned `golang:1.27.4` image, with module and build caches in named Docker volumes, so only the first run downloads anything. Integration tests start their PostgreSQL and Redis containers through the host's Docker socket. `verify.sh` runs every step even after a failure and ends with a PASS/FAIL table and per-step log files. On Windows, run these from Git Bash or WSL. From PowerShell without bash, the unit tests are one command:

```powershell
docker run --rm -v "${PWD}:/src" -w /src golang:1.27.4 go test -race ./...
```

### Test program summary

After v1.0.0 the system went through a test program aimed at breaking it, run against the live Compose stack. Every scenario also has to prove that its triggering condition actually happened. A cancel race in which no cancel won, or a shutdown with nothing in flight, counts as a failure, not a pass. The subsections below have the details.

| Area | What was done | Outcome |
|---|---|---|
| Unit, integration, fuzz | 134 test functions, 20 Testcontainers scenarios, 7 fuzz targets (~70M executions) | all pass; fuzzing found 3 bugs (below) |
| Throughput and rate limit | 100k flash sale; 250k into a 64 MB Redis; 60-minute soak at ~250/s | busiest 1 s window ≤ 98 per channel in every run |
| Concurrency | 6 race scenarios (cancel vs send, batch cancel, idempotency, manual retry, scheduled cancel, template delete) | 0 cancelled sends, 0 double creates |
| Failures | Worker/Redis/PostgreSQL kills, slow and failing provider, timeouts beyond `PROVIDER_TIMEOUT`, rolling restarts, graceful shutdown | 0 lost; duplicates only from `SIGKILL` mid-send (at-least-once, documented) |
| Isolation and priority | 30k SMS backlog vs email/push; 2x-capacity high traffic vs low | email/push p95 1.3 s; low kept 9–10% |
| Scale and endurance | 2M-row database; 60-minute soak with outages and 50 WebSocket readers | list queries 3.5–12 ms; flat goroutines, FDs and heap |

Bugs this found, all fixed (1–11 pinned by regression tests, 12 by the clean-room clone check):

| # | Bug | Found by | Fix |
|---|---|---|---|
| 1 | A fixed DB pool of 35 per worker exhausted PostgreSQL at 3 replicas | scaling test | `DB_MAX_CONNS`, smaller default (`bb60d93`) |
| 2 | Rate limit overrun to 107/s per channel when a DB stall bunched sends | 6-replica stress | rate-limit token taken after `Claim` (`bb60d93`) |
| 3 | A DB blip right after the provider accepted a send led to a re-send | failure drill | post-send writes retried for 5 s (`bb60d93`) |
| 4 | A 1,000-item batch made one 28,013-span trace (~145 MB) and filled Docker's memory | chaos test | batch items linked, not nested; Jaeger capped (`2582308`) |
| 5 | WebSocket hub dropped fast readers during batch bursts | WebSocket flood | per-client queues, progress-based drops (`52c1451`) |
| 6 | `/api/v1/metrics` took 1.7 s at 2M rows | data growth | one scan per source (`c250ce3`) |
| 7 | Log masking showed some phone numbers and tokens in full | fuzzing | at least 3 characters always hidden (`fcc91f4`) |
| 8 | A huge `Retry-After` overflowed to a negative delay | fuzzing | clamped (`fcc91f4`) |
| 9 | Non-upgrade `GET /api/v1/ws` answered plain-text `426` | fuzzing | standard error envelope (`fcc91f4`) |
| 10 | Workers could not dequeue while Redis was full | 64 MB Redis stress | `RPOP`+`LPUSH` instead of `LMOVE` (`e83c07e`) |
| 11 | Expired idempotency key cleanup scanned the whole table (378 ms at 2.25M keys) | `EXPLAIN` at 2M rows | primary-key probes, 29 ms (`f985544`) |
| 12 | A Windows clone (`autocrlf=true`) failed `make lint` on every Go file | clean-room clone | `.gitattributes` forces LF |

The test tools had bugs of their own. Several early scenarios passed without testing anything, and the invariants checker ran for 20 minutes at 500k keys. Each was fixed and is described in its subsection.

With Go 1.27 and `make` installed:

```bash
make test               # unit tests with -race (no Docker; miniredis + in-memory fakes)
make test-integration   # adds the Testcontainers suite (PostgreSQL 16 + Redis 7), -race
make test-all           # both
make test-docker        # the same with only Docker (scripts/test-docker.sh)
make verify             # one-command check: lint, unit, integration, stack up, smoke, invariants (scripts/verify.sh)
make cover              # unit coverage -> coverage.html + per-package table
make cover-all          # unit + integration coverage per internal package
make lint               # gofmt, go vet, golangci-lint
make load               # 2,000-notification load test against the running stack
make drills             # live failure drills against the running stack (~5 min, section 5)
make invariants         # check system invariants on the settled stack (no stuck rows, no cancelled or double sends, Redis matches PostgreSQL);
                        # covers the full history, so pass --since=<RFC3339> to ignore duplicates from an earlier crash drill
make prom-snapshot      # one-line resource snapshot: goroutines, heap, FDs, queue depths, DB pool, Redis memory
make ws-test            # 39 API boundary cases + WebSocket capacity, fast/slow consumers and batch watchers (~5 min)
make shutdown-test      # SIGTERM workers and API under load: drain, no abandoned attempts, no duplicates (~4 min)
make rolling-test       # restart worker-1/worker-2 alternately every ~8s for 2 min under load: 0 abandoned, 0 duplicates, 0 lease lost (~6 min)
make chaos              # sustained load + worker kills, Redis restart/FLUSHALL, PostgreSQL stop, slow/failing provider (~15 min)
make isolation          # 30,000 SMS backlog vs ~50/s email and push: email/push p95 < 2s, per-channel rate limit (~7 min)
make priority           # 1,000 low under 3 min of 2x-capacity high: low >= 8% per 10s slice, high overtakes older low (~9 min)
make provider-timeout   # provider slower than PROVIDER_TIMEOUT: breaker opens, deferrals not counted, all sent; permanent 4xx: 1 attempt, DLQ (~4 min)
make stress             # stress profile only: 50k then 200k into a 64 MB Redis; OOM on enqueue, nothing lost, no container killed (~20 min)
make soak               # 60 min: ~250/s mixed load, 50 WebSocket readers, a 30s provider outage every 10 min; leak, recovery and latency checks (~70 min)
make races              # concurrent cancel, batch cancel, idempotency, manual retry, scheduled-cancel and template-delete races (~4 min)
make ratecheck SINCE=2026-11-21T10:00:00Z   # busiest 1s window per channel since SINCE (FR-2.2)
make fuzz               # Go native fuzzing of 7 parsers of untrusted input, incl. PII masking, Retry-After and the HTTP API (FUZZTIME=5m each)
```

Every test package runs `go.uber.org/goleak` in `TestMain`, so a test that leaves a goroutine behind fails. For stress runs (`make stress`), `docker compose -f docker-compose.yml -f docker-compose.stress.yml up -d --wait` caps the API and workers at 0.5 CPU / 256 MB and Redis at 64 MB with `noeviction`, so resource exhaustion surfaces as errors instead of silent key eviction.

`make` and `-race` need Linux, macOS, WSL or CI. On native Windows without GCC, use `./scripts/test-docker.sh`, or run `go test ./...` and `go test -tags=integration ./internal/integration/...` without `-race`.

### Integration scenarios (`internal/integration`, one Testcontainers PostgreSQL + Redis per run)

| Subtest | What it proves |
|---|---|
| `a_single_notification_end_to_end` | API -> Postgres -> Redis -> worker -> provider (`EXT-1` body + `Idempotency-Key`) -> `sent` |
| `b_idempotent_create_replay_and_conflict` | Same key + same body -> original `202` + `Idempotent-Replayed`; different body -> `409` |
| `c_batch_1000_and_batch_key_replay` | 1000-item batch, batch-level replay returns the same `batchId` |
| `d_cancel_race_no_cancelled_notification_reaches_provider` | 100 concurrent cancels vs. running workers: no cancelled ID is ever sent |
| `e_retry_500_500_202_and_permanent_400_to_dlq` | Transient retries with backoff, permanent `400` straight to DLQ |
| `f_priority_high_completes_ahead_of_low` | Weighted 6:3:1 dequeue: high finishes ahead of an older low backlog |
| `g_distributed_rate_limit_1000_sms_across_two_workers` | 1000 SMS, 2 workers sharing one GCRA key: **max 97 sends in any 1s sliding window** (limit 101) |
| `h_reconciler_repairs_untracked_pending_and_simulated_redis_loss` | Redis keys wiped mid-run (pending + retrying + scheduled rows) -> reconciler (a)/(b) repairs all |
| `i_list_filters_and_three_page_keyset_pagination` | Status/channel/date filters and opaque keyset cursor over 3 pages |
| `j_scheduled_due_in_2s_sent_after_due_and_cancelled_never_sent` | `scheduledAt` never sent early; cancelled scheduled item never sent |
| `k_template_create_preview_and_batch_personalized_sms_delivery` | Template render-at-create; provider bodies contain the rendered text |
| `l_websocket_batch_subscription_receives_snapshot_and_live_sent_events` | WebSocket snapshot + live `sent` events for a batch |
| `m_tracing_traceparent_propagates_from_api_through_redis_to_provider` | Incoming W3C `traceparent` trace ID reaches the provider request |
| `n_circuit_breaker_opens_on_503_outage_without_burning_attempts_then_recovers` | Permanent `503`: breaker opens after 3 failures, **4 provider requests for 10 notifications**, `circuit_open` attempts are `counted=false`, nothing dead-lettered; provider recovers -> 10/10 sent |
| `o_worker_crash_mid_batch_recovered_by_reconciler_with_bounded_duplicates` | Exact state of a SIGKILLed worker (10 items in its inflight list + claimed, 5 already accepted by the provider, heartbeat gone): reconciler (d) drains the orphan list, (c) reclaims 10 `abandoned` rows -> 30/30 sent; **duplicates = 5 = sends in flight at crash**, all with the same `Idempotency-Key` |
| `p_idempotency_20_concurrent_posts_same_key_create_exactly_one_row` | 20 simultaneous POSTs with one key: all `202`, one id, exactly one non-replayed response, one DB row |
| `q_manual_retry_failed_to_sent` | `failed` (DLQ) -> `POST /retry` resets attempts -> `sent`; retrying a `sent` row -> `409` |
| `r_metrics_windows_and_percentiles_from_known_rows` | Rows with known `sent_at` / `failed_at` offsets and durations: exact 1m/5m/15m counts per channel and p50/p95/p99 (e.g. p95 = 8,600 ms from 1..9 s); non-counted and older attempts excluded |
| `s_dequeue_and_ack_drain_while_redis_is_out_of_memory` | Redis filled past a small `maxmemory` with `noeviction` (even a 1-byte `SET` and the former `LMOVE` script are refused): `Dequeue` and `Ack` still drain all 3 queued IDs in FIFO order |
| `t_expired_idempotency_keys_deleted_in_bounded_batches_live_keys_kept` | 25 expired + 5 live keys, batch limit 10: deletes 10, 10, 5, 0; the 5 live keys stay |

The full suite (20 scenarios) runs in ~30s locally and in the CI `integration` job.

### Fuzzing

`make fuzz` runs Go's native fuzzer on every function that parses untrusted input. Besides "never panics", each target checks the property below. Measured on 2026-11-21, 5 minutes per target:

| Target | Property | Executions |
|---|---|---:|
| `FuzzSMSInfo` | Deterministic; encoding is GSM-7 or UCS-2; segment count matches the 160/153 and 70/67 thresholds | 14.3M |
| `FuzzValidateNotification` | Never panics; an error always names a field; the idempotency request hash is the same before and after normalization | 7.5M |
| `FuzzDecodeCursor` | Either a cursor or an error, never both or a zero position; a decoded cursor round-trips through encode/decode | 4.5M |
| `FuzzTemplateEngine` | Template validation and rendering never panic; an error always names a field | 20.4M |
| `FuzzMaskRecipientNeverRevealsTheRawValue` | A masked recipient (phone, email, token) never contains the raw value and hides at least 3 characters | 10.5M |
| `FuzzParseRetryAfterIsNeverNegative` | Any `Retry-After` value gives a delay `>= 0` | 10.8M |
| `FuzzAPINeverReturns5xxForClientInput` | Any body, idempotency key or query string on 7 routes gets a 2xx or a 4xx with the standard error envelope, never a 5xx | 2.8M |

The new targets found three bugs, now fixed and kept as regression inputs under `testdata/fuzz/`:

- **PII leak in log masking.** The mask kept a 6-character prefix and a 4-character suffix, so a 10-character phone number came out whole (`+123456789` -> `+12345***6789`), and a 9-character push token kept 8 of 9 characters. Every mask now hides at least 3 characters, and values too short for that are replaced by `***`.
- **`Retry-After` overflow.** `Retry-After: 9227000000` (seconds) overflowed `time.Duration` to a negative delay, so the provider's back-off hint was silently ignored instead of being capped at `RETRY_AFTER_MAX`. The value is now clamped before conversion.
- **WebSocket `426` without the error envelope.** A plain `GET /api/v1/ws` with a valid filter reached the WebSocket library, which wrote a `text/plain` `426`. The handler now answers `426 upgrade_required` in the standard JSON envelope, with `Upgrade: websocket`.

### Coverage

Measured on 2026-11-21 (`make cover` for unit tests, `make cover-all` for unit + integration with `-coverpkg=./internal/...`):

| Package | Unit | Unit + integration |
|---|---:|---:|
| `internal/domain` | 85.7% | 89.0% |
| `internal/worker` | 78.4% | 80.9% |
| `internal/api` | 72.7% | 75.9% |
| `internal/api/middleware` | 79.8% | 89.9% |
| `internal/queue` | 91.8% | 92.7% |
| `internal/ratelimit` | 85.3% | 88.2% |
| `internal/provider` | 80.7% | 85.3% |
| `internal/realtime` | 76.8% | 83.2% |
| `internal/template` | 79.6% | 81.0% |
| `internal/config` | 80.0% | 80.0% |
| `internal/observability` | 69.9% | 71.7% |
| `internal/repository` (+ `sqlc`) | 7.1% | 62.7% (63.5%) |
| **`internal/...` total** | | **78.3%** |

`internal/repository` is intentionally exercised by the integration suite (real PostgreSQL), not by mocks.

### Load test results

`cmd/loadgen` creates batches concurrently, waits until every notification is terminal, then pages through the batches to compute per-channel send rates from the persisted `sentAt` timestamps:

```bash
MOCK_LATENCY_MS=50 docker compose up -d --build
go run ./cmd/loadgen -n 10000 -batch 500 -concurrency 8 -channel mixed -channel-mix 34:33:33 -priority-mix 20:60:20
```

Flags: `-n` total, `-batch` items per request (`<= 1000`), `-concurrency` parallel submitters, `-channel` (`mixed|sms|email|push`), `-channel-mix sms:email:push`, `-priority-mix high:normal:low`, `-poll-timeout`, `-channel-stats`. Exit code is `1` if anything is still non-terminal.

| Metric | Result (10,000 notifications, 34/33/33 channel mix, mock latency 50ms) |
|---|---|
| Batch create (20 x 500 items, 8 concurrent) | 287ms total, **34,787 notifications/s** accepted |
| Batch create latency | p50 89.7ms, p95 164.7ms, p99 165.1ms |
| Time to all terminal | **36.8s** (theoretical floor at 100/s/channel: ~34s) |
| End-to-end throughput | 271.9 sent/s across 3 channels |
| Effective send rate per channel | sms 94.9/s, email 94.9/s, push 95.0/s (limit 100/s) |
| Final statuses | 10,000 `sent`, 0 failed |
| Provider requests / unique `Idempotency-Key`s | 10,000 / 10,000 (no duplicate sends) |

Hardware: Intel Core i7-11800H (8C/16T), 32 GB RAM, Windows 11 + Docker Desktop 29.9.3 (Linux VM: 16 vCPU, 15.5 GB); API, 2 workers, PostgreSQL, Redis and the mock provider all on the same machine.

The effective rate is ~5% below the configured 100/s because a denied worker sleeps for the GCRA `RetryAfter` and then needs another Redis round trip before it can send, so slots are occasionally missed; the requirement (never more than 100/s per channel) is the hard bound and is asserted separately by integration test `g`.

Prometheus during a 24,000-notification run (`sum by (channel) (rate(notifications_processed_total{status="sent"}[15s]))`): all three channels hold a flat line at about 95/s and never cross 100/s.

![Prometheus graph: sms, email and push send rates plateau at about 95 per second under the 100 per second limit](docs/images/prometheus-rate-limit.png)

### Rate limit precision and horizontal scaling

`scripts/ratecheck.sh --since=<RFC3339>` measures the busiest sliding one-second window per channel from two clocks: `delivery_attempts.started_at` (taken right before the provider call) and the mock provider's own arrival timestamps. Measured on 2026-11-21, each run followed by `scripts/invariants.sh` (all passed: no loss, no stuck rows):

| Run | Setup | Busiest 1s window per channel | Effective rate |
|---|---|---|---|
| SMS only (before the fix) | 12,000 SMS, 2 workers | 99 (system) = 99 (provider) | 95.3/s |
| Mixed (after the fix) | 15,000, 2 workers | email 97, push 97, sms 97; provider total 290 of 300 | 95.2/s per channel |
| Scaled, before the fix | 30,000, **6 workers**, provider latency 100 ± 50ms | **105 / 106 / 107**, provider total **318** | 70–88/s |
| Scaled, after the fix | same | **94 / 94 / 97**, provider total 281; peak PostgreSQL connections 68 | 86.5/s per channel |

The scaled run found two real defects. The worker pool was fixed at 35 connections, so 6 replicas + API asked for 230 connections against PostgreSQL's 100, and requests failed with `too many clients`. And because the token was taken before the claim, sends that waited for a connection bunched up when connections freed, breaking the 100/s window. One send was also duplicated: the provider accepted it, the result could not be written, and the reconciler re-sent it. The fixes: `DB_MAX_CONNS` (worker default 10), the token taken right before the provider call, and a 5s retry for the writes that follow an accepted send. With 6 replicas and 100ms provider latency the effective rate is 86.5/s rather than 95/s: 150 senders per channel contend for each 10ms GCRA slot and some slots go unused. The limit, the hard requirement, holds.

### Flash sale: 100,000 notifications at once

100 batches of 1,000 (mixed channels, priorities 20/60/20) sent by 16 concurrent clients, with `scripts/prom-snapshot.sh watch 5` sampling every 5 seconds. Measured on 2026-11-21, 2 workers, default settings:

| Metric | Result |
|---|---|
| Accepted | 100,000 in 4.2s (23,775/s), batch p50 661ms, p99 828ms, no errors |
| Peak queue depth | 98,449 ready; it drained in a straight line at ~295/s (6m05s to all terminal; floor at 300/s is 5m33s) |
| Rate limit | busiest second 97 per channel, provider total 291 of 300; 93.3/s effective per channel |
| End-to-end latency (p50 / p95) | high 59s / 161s, normal 200s / 313s, low 303s / 351s: the 6:3:1 weighting gives `high` 60% of the slots under full load, and `low` is never starved |
| Resources (peak → after) | goroutines api 36 → 18, worker 117 → 91; heap api 18 MB, worker 33 MB; Redis 40 MB (~400 bytes per queued notification) → 3 MB; PostgreSQL 38 connections |
| Invariants | all passed: 0 stuck, 0 duplicates, Redis and PostgreSQL consistent |

Each worker's 10-connection pool was fully used during the burst, and 16% of connection acquires had to wait (`db_pool_empty_acquire_total`). That cost about 2% of throughput (93.3/s against 95.2/s in steady load). Goroutines and file descriptors returned to their baseline afterwards, so nothing leaked.

### Race conditions under real concurrency

`scripts/races.sh` (`make races`) fires many simultaneous requests at one resource from a single `curl --parallel` process (200 connections at once) and checks each outcome against the API responses, PostgreSQL and the provider's per-`Idempotency-Key` counts. Measured on 2026-11-21:

| Scenario | What runs at the same time | Result |
|---|---|---|
| Cancel vs worker | 1,000 cancels while the same 1,000 SMS are being delivered (300ms provider latency) | 689 cancels won: all ended `cancelled` with 0 provider calls. 311 lost to the worker (`409`): all ended `sent`, each delivered exactly once |
| Batch cancel storm | 20 cancels of the same 1,000-item batch | The 20 responses together report 768 cancelled = 768 `cancelled` rows in PostgreSQL (one atomic `UPDATE`, no double counting); 0 cancelled rows reached the provider |
| Idempotency, same body | 200 `POST`s with one `Idempotency-Key` | 1 created, 199 replayed (`Idempotent-Replayed: true`), one ID returned to all, 1 row |
| Idempotency, different bodies | 200 `POST`s, one key, 200 different bodies | One `202`, 199 × `409 idempotency_conflict`, 1 row |
| Manual retry storm | 50 retries for each of 20 failed notifications | Exactly one `200` per notification, the rest `409`; each sent exactly once afterwards and removed from the DLQ list |
| Scheduled send vs cancel | 5,000 SMS due at the same second; 2,500 of them cancelled concurrently, all cancels landing 3s before the due time | 2,500 × `200`, all `cancelled`, 0 provider calls and 0 left in the delayed set; the other 2,500 each sent exactly once after the due time |
| Template delete vs batch | A 1,000-item templated batch (300ms provider latency); the template is deleted while 908 items are still undelivered | All 1,000 sent with the text rendered at create time (0 raw `{{` placeholders); a new batch on the deleted template -> `422` |

The first version of the script spawned one `curl` per request with `xargs`. On Windows that spread 1,000 "concurrent" cancels over ~30 seconds, so the batch finished before any cancel arrived and the scenario passed without ever racing. The script now fails a scenario in which no cancel both won and lost. The scheduled-send race is likewise only counted when every cancel completed before the due time.

### Chaos under sustained load

`scripts/chaos.sh` (`make chaos`) submits a 1,000-item mixed batch every 5 seconds (~200/s) for 8 minutes while a fixed timeline breaks things, then waits until every accepted notification is terminal and runs `invariants.sh --allow-duplicates` and `ratecheck.sh`. The mock provider's fault profile is switched at runtime (`POST /config`) so its per-key counters survive. Measured on 2026-11-21, 2 workers:

| t | Event |
|---|---|
| 30s, 60s | `SIGKILL` worker-1, then worker-2 (each back after 15s) |
| 90s, 120s | Redis restart, then `FLUSHALL` |
| 150s–170s | PostgreSQL stopped |
| 200s, 240s, 280s | Provider 2s ± 0.5s latency, then 30% `500`, then 50% `429` |
| 300s | `SIGKILL` worker-1 again during the `429` storm |
| 360s | Provider healthy |

| Result | |
|---|---|
| Accepted | 84,000 notifications (3 of 87 batch requests rejected with `503` while PostgreSQL was down) |
| Final state | 83,994 `sent`, 6 `failed` (5 × `500`, in the DLQ), **0 lost, 0 stuck**; drained 45s after the load ended |
| Rate limit | busiest second 97 per channel, provider total 291 of 300, throughout the chaos |
| Duplicates | 147 provider calls the system never recorded, from the 3 worker kills (~49 sends in flight per kill), plus 2 notifications recorded as sent twice: one worker died right after recording its attempt, and one send was accepted at the moment PostgreSQL stopped, so the 5s post-send retry could not write it. All share the same `Idempotency-Key` (at-least-once, as documented) |
| Invariants | all passed; peak queue 17,009, Redis 14 MB, 25 PostgreSQL connections |

Two earlier runs of the same test found infrastructure problems rather than delivery problems. Jaeger's unbounded in-memory storage (see Distributed tracing) filled the Docker VM after a day of load tests and froze Docker Desktop in the middle of a run; after the restart the system recovered on its own, and the 2,357 notifications that were open at the freeze were all delivered within 40 seconds. The chaos script now retries its database queries and keeps its logs when a run fails.

### Graceful shutdown under load

`scripts/shutdown.sh` (`make shutdown-test`) streams a 1,000-item batch every 2 seconds (more than the 300/s the system may send, so work is always queued) with 300 ± 200ms provider latency, and sends `SIGTERM` to worker-1, then worker-2, then both, then the API. A graceful stop, unlike `SIGKILL`, must leave nothing for the reconciler. Measured on 2026-11-21:

| Stop | Result |
|---|---|
| worker-1, worker-2, both at once | each stopped in ~1.3s (limit `SHUTDOWN_TIMEOUT` = 15s); every stop finished **75 sends in flight** (25 goroutines × 3 channels, all mid-send) before exiting; heartbeat key and inflight list removed; nothing had to be requeued |
| API | stopped in 0.9s; the 3 batch requests that hit the stopped API got connection errors and created **no rows at all** (all-or-nothing), so the client can retry them with the same `Idempotency-Key` |
| Overall | 47,000 accepted, 47,000 sent, **0 `abandoned` attempts, 0 duplicates**, invariants passed without `--allow-duplicates` |

**Rolling restarts** (`./scripts/shutdown.sh --rolling`, `make rolling-test`): under the same load, worker-1 and worker-2 were restarted alternately with `compose restart`, one every ~8s (5s pause + ~1.7s restart) for 2 minutes, so one worker was always down or starting. All 15 restarts drained 75 in-flight sends and logged a clean stop; 65,000 accepted, 65,000 sent, **0 `abandoned` attempts, 0 duplicates, 0 "lease lost" log lines** (no fenced write was ever needed), invariants passed without `--allow-duplicates`.

The script fails a stop during which no send completed, because such a stop says nothing about draining. Its first version built request bodies in a bash loop that was too slow to keep the workers busy, and its first stop passed with an idle worker.

### WebSocket flood and boundary values

`scripts/ws-boundaries.sh` (`make ws-test`) runs `cmd/wsflood`: 39 HTTP boundary cases (SMS segment limits, E.164, email and push limits, batch size and body limits, idempotency keys, `scheduledAt` window, WebSocket filters) and three WebSocket scenarios against the live stack, followed by `invariants.sh` and `ratecheck.sh`. Measured on 2026-11-21:

| Scenario | Result |
|---|---|
| Capacity | 1,000 clients connect (`MAX_WS_CLIENTS`); 10 more get `503 too_many_ws_clients`; 200 closed slots are reused by 200 new clients |
| Fast and slow consumers | 400 clients reading on `channel=sms` and 50 that never read, while 4 × 1,000 SMS are delivered: all 400 readers received every one of the 4,000 events and none was dropped, in 3 of 3 runs; all 50 non-readers were disconnected; `/health/ready` stayed under 260ms |
| Batch watchers | 20 clients on `batchId`: 20/20 snapshots, 1,000/1,000 live events each |

The fast/slow scenario first failed: 324 of the 400 readers were dropped. Each client had a fixed 64-slot channel, and the Hub dropped any client whose channel was full at the moment an event arrived. But creating a 1,000-item batch publishes 1,000 `pending` events at once, so a reader that kept up was indistinguishable from one that had stopped. Raising the buffer to 1,024 only moved the threshold: four batches in a row still dropped 37 of 100 readers. The Hub now keeps a per-client queue that grows on demand and drops a client only when more than 1,024 frames are waiting **and** it has delivered none for 5 seconds, or when more than 8,192 are waiting. Each event is JSON-encoded once and the same bytes go to every client (before, every client re-encoded every event). A client that has stopped reading entirely cannot receive a close frame through its full TCP window, so it is cut by the 5-second write timeout and sees `1006`; `1008` is sent when the frame can still be delivered.

### Channel isolation and priority under saturation

Two scripts saturate one dimension and check that the others are unaffected. Measured on 2026-11-21, 2 workers:

**`scripts/isolation.sh` (`make isolation`).** 30,000 SMS are submitted at once (5 minutes of work at 100/s). While they drain, email and push each get a 100-item batch every 2 seconds (~50/s) for 2 minutes. The smallest SMS backlog seen during that time was 18,113.

| Channel | Sent | End-to-end p50 / p95 / p99 | Busiest 1s window |
|---|---|---|---|
| email | 6,000 / 6,000 | 696 / 1,293 / 1,919 ms | 97 |
| push | 6,000 / 6,000 | 682 / 1,263 / 1,795 ms | 97 |
| sms | 30,000 / 30,000 (drained in 5m15s) | backlog-bound | 97 |

Email and push were delivered as if SMS were idle: each channel has its own consumers and its own GCRA key. The ~0.7s median is the limiter spacing a 100-item batch that arrives in one go at 10ms intervals, not waiting behind SMS.

**`scripts/priority.sh` (`make priority`).** 1,000 `low` SMS are queued first, then `high` SMS arrive at ~200/s for 3 minutes. That is twice what the channel may send, so the high queue never emptied (smallest sample: 815).

| Check | Result |
|---|---|
| Low share in each 10s slice while both were queued | 9–10% in all 10 slices (6:3:1 schedule; the empty `normal` slots fall through to high) |
| First 1,000 high, created after all 1,000 low | p50 4.5s, all sent 14s after the low batch arrived; the low batch finished after 103s (p50 49s) |
| Everything | 37,000 / 37,000 sent, busiest 1s window 97, invariants passed |

The first version of this script compared the p50 of all high against all low and failed (high 110s, low 49s). That comparison measured the overload, not the priority: high arrived at twice its capacity, so its own FIFO backlog kept growing. The script now compares high and low notifications by arrival order.

### Provider slower than its timeout, and permanent errors

`scripts/provider-timeout.sh` (`make provider-timeout`) has two parts. Measured on 2026-11-21:

- **Slow provider.** The mock answers after 12s, which is beyond `PROVIDER_TIMEOUT` = 10s, for 90 seconds while 300 SMS are sent.
- **Permanent errors.** The mock answers every request with a permanent `4xx` while 20 SMS are sent.

| Check | Result |
|---|---|
| Timeouts | 77 provider timeouts, all recorded as `transient` |
| Breaker | `circuit_breaker_state{channel="sms"}` reached 2 (open) and was back at 0 after recovery |
| Deferrals while open | 1,191 `circuit_open` rows, **0 counted as attempts**; every notification's `attempts` equals its counted rows; highest `attempts` was 3 of 5 |
| Outcome | 300 / 300 sent, 0 failed, about 1 minute after the provider recovered |
| Permanent `4xx` | 20 / 20 `failed` with exactly 1 attempt each, no retry and no deferral (a `4xx` counts as a breaker success); all 20 in the DLQ list |
| Invariants | passed, 0 recorded double sends |

Without the breaker, 5 attempts (10s timeout each plus 2–16s backoff) take about 80s, less than the 90s outage, so most of the batch would likely have been dead-lettered. The mock counts a request when it arrives and abandons it when the client disconnects, so the timed-out calls were never accepted by the provider. Against a real provider, a timeout can hide an accepted send, which is why the same `Idempotency-Key` goes on every attempt.

### Constrained resources: Redis full

`scripts/stress.sh` (`make stress`) runs against the stress profile (`docker-compose.stress.yml`). There, the API and each worker get 0.5 CPU / 256 MB, PostgreSQL 1 CPU / 512 MB, and Redis is capped at 64 MB with `noeviction`. Redis is only a hint of work to do, so a full Redis must slow the system down without losing anything. Measured on 2026-11-21, 2 workers:

| Phase | Load | Result |
|---|---|---|
| A | 50 × 1,000 mixed batches, 4 in parallel | all `202`, request p50 387 ms / p99 1.1 s; drained in 174 s |
| B | 200 × 1,000, 4 in parallel: more queue metadata than 64 MB holds (~400 B per tracked ID) | all `202`, p50 303 ms / p99 580 ms; Redis hit 64 MB with 165,580 tracked IDs; 31 batches got `OOM` on enqueue and stayed `pending` in PostgreSQL; reconciler step (a) re-enqueued ~30,000 rows as the queues drained; drained in 702 s (285/s) |
| Overall | 250,000 | 250,000 sent; no container OOM-killed or restarted; busiest 1 s window 98 per channel; invariants passed |

The first run found a real bug. While Redis was full, workers could not dequeue: 1,668 `OOM` warnings in 2 minutes. `dequeue.lua` started with `LMOVE`, and Redis refuses a script whose first write is a `denyoom` command. The run recovered only because acks of in-flight sends freed memory. A full Redis with nothing in flight, for example a backlog parked in the delayed set during a provider outage, would never have drained again. `dequeue.lua` now does `RPOP` + `LPUSH` (see the failure matrix), integration scenario `s` pins it against a real full Redis, and the rerun above logged 0 dequeue failures.

The same run exposed a slow test tool. `invariants.sh` parsed the mock's 24 MB single-line `/stats` with an awk loop that copied the rest of the line after every match, which is quadratic. With 500,000 keys it ran for over 20 minutes; it now uses `grep -o` and finishes in 33 s.

### Data growth: 2 million notifications

A separate database was seeded with 2,000,000 notifications (1,000 batches of 1,000 plus 1,000,000 singles over 30 days; 95% sent, 3% failed, 1.9% cancelled, a few hundred pending and retrying) and 1.96 million delivery attempts (1.46 GB), with ~280,000 sends in the last 15 minutes, which is what a steady 300/s looks like. A second API instance pointed at it; 7 calls per endpoint, measured on 2026-11-21:

| Endpoint | Median |
|---|---|
| List, first page / `status=failed` (3%) / `status=pending` (0.05%) | 4.3 / 6.0 / 5.1 ms |
| List, `channel` + `priority` / one-day `from`–`to` range | 3.7 / 3.6 ms |
| Notification by ID with attempts / batch of 1,000 with counts | 3.5 / 5.2 ms |
| Keyset pagination, 100 pages × 200 rows | page 1: 10 ms, page 100: 12 ms (depth does not matter) |
| `/api/v1/metrics`, uncached | **1.7 s before**, 0.33–0.53 s after the fix below |

Every list query used its composite index, and 30 repeated calls showed no slowdown when PostgreSQL switches prepared statements to a generic plan. The metrics snapshot was the outlier: its window counts ran 27 correlated subqueries and its latency query 6 separate percentile scans, each over the last 15 minutes. With the 2s cache, every API replica would spend most of a CPU core on it. The rewrite reads each outcome once (the 1m and 5m windows are `FILTER`s of the 15m scan) and computes p50/p95/p99 with one `percentile_cont(ARRAY[...])` per source. Run side by side in one transaction on the same data, the old and new queries returned identical results, and the time dropped from 0.8–1.6 s to 127–144 ms (window counts) and from 1.5–1.7 s to 0.53–0.59 s (percentiles). The rest is sorting ~280k rows; at higher volume these numbers should come from Prometheus histograms instead (section 12). Integration scenario `r` pins both queries to hand-computed answers.

**Reconciler scans on the same 2M rows.** To give each scan real work, the growth database got 2,000 stale `pending`, 2,000 abandoned `processing` and 2,000 due `retrying` rows, plus 2.25M idempotency keys (50,000 expired). Each query was run as a prepared statement with `EXPLAIN ANALYZE`, once with a custom plan and once with a forced generic plan:

| Reconciler query (page of 1,000) | Plan | Time |
|---|---|---|
| (a) `FindStalePending` | index scan on the partial `idx_notifications_reconciler` | 3.3–6.6 ms |
| (b) `FindDelayedDueSoon` | same partial index | 2.0–5.2 ms |
| (c) `FindAbandonedProcessing` | same partial index | 1.2 ms |
| (e) `DeleteExpiredIdempotencyKeys` (10,000) | **was:** hash semi join over a sequential scan of the whole key table | **378 ms** at 2.25M keys, growing with the table |
| | **now:** `key = ANY(ARRAY(subquery))`, primary-key probes | **29–37 ms**, independent of table size |

The partial index holds only non-terminal rows, so the scans cost the same whatever the size of the `sent` history. The key cleanup did not. With `IN (SELECT ... LIMIT)` the planner hash-joined the 10,000 expired keys against every key in the table. With a 24-hour TTL and an `Idempotency-Key` on most requests, the table holds a day of traffic (tens of millions of rows at the stated volume), and the reconciler would have scanned all of it every 15 seconds. Integration scenario `t` covers the rewritten query (bounded batches, live keys kept).

### Endurance soak: 60 minutes

`scripts/soak.sh` (`make soak`) keeps the system busy for an hour. Measured on 2026-11-21 with `OTEL_SAMPLER_RATIO=0.1`, so Jaeger stays within its trace window. The load:

- **Traffic:** a 1,000-item batch every 4 seconds, mixed channels and priorities, about 250/s. The mock provider fails 0.5% of calls with a transient error.
- **Probes:** every minute, one scheduled notification, one idempotent create plus replay, and reads of `/health/ready`, `/api/v1/metrics` and the list endpoint.
- **WebSocket:** 50 readers held open the whole hour (`wsflood -hold`, run in a container on the Compose network).
- **Provider outages:** every 10 minutes, starting at minute 5, every provider call answers `500` for 30 seconds.

```bash
OTEL_SAMPLER_RATIO=0.1 docker compose up -d --force-recreate --wait
API_URL=http://127.0.0.1:8080 MOCK_URL=http://127.0.0.1:8090 PROM_URL=http://127.0.0.1:9090 ./scripts/soak.sh
```

| Check | Result |
|---|---|
| Throughput | 898/898 batches `202` (request p50 117 ms, p99 322 ms); 898,120/898,120 sent, 0 failed; 60 probe cycles, 0 errors |
| Rate limit | busiest 1 s window 97 per channel for the whole hour |
| WebSocket | all 50 readers stayed connected and received 46.4 million events; none dropped, none starved |
| Outages | 5 outages, 39,106 `circuit_open` deferrals (no attempt burned). Each built a backlog of 6,500–8,700 that drained 4.4–5.7 minutes after the outage ended. The queue was back at its pre-outage level before every next outage, so nothing accumulated |
| End-to-end latency | p50 1.9–2.9 s in every 10-minute slice. p99 74–88 s in the five slices with an outage (the 30 s outage plus the breaker's 30 s open state), 4.1 s in the last slice without one; no upward trend |
| Restarts | 0 container restarts or OOM kills |
| Invariants | passed (0 stuck, 0 duplicates, Redis consistent with PostgreSQL) |

| Resource (`api` / `worker-1` / `worker-2`) | Before load | First quarter | Last quarter | After drain |
|---|---|---|---|---|
| Goroutines | 18 / 91 / 91 | 114 / 99 / 99 | 118 / 99 / 99 | 18 / 99 / 99 |
| Open FDs | 15 / 87 / 87 | — | 65 / 108 / 108 | 15 / 104 / 104 |
| Go heap | — | 5.2 / 13.9 / 14.9 MB | 5.3 / 14.4 / 14.6 MB | 6.5 / 16.7 / 17.3 MB |
| Redis `used_memory` | 3.5 MB | — | — | 2.2 MB |

How to read the resource table:

- The API's ~100 extra goroutines and ~50 extra FDs are the 50 WebSocket clients (a reader and a writer each). They return to baseline when the clients disconnect.
- The workers' small rise is connection-pool warm-up: go-redis keeps idle connections for 30 minutes.
- Jaeger plateaued at 1,021 MiB of its 1.5 GiB limit.

**Spare capacity.** 250/s against ~290/s of capacity leaves only ~40/s to drain a backlog, which is why a 30-second outage takes ~5 minutes to absorb. If outages like this are expected at peak load, plan for more headroom: a third worker does not help, because the per-channel rate limit caps throughput. Faster recovery needs either more headroom (sending below the cap) or a higher provider limit.

### CI/CD (GitHub Actions, `B-6`)

`.github/workflows/ci.yml` runs on every branch push and pull request (`permissions: contents: read`, superseded runs on the same ref are cancelled):

| Job | What it proves |
|---|---|
| `lint` | `gofmt`, `go vet` (with and without the `integration` tag), `go mod tidy -diff`, `golangci-lint` v2 (built with Go 1.27.4 via `install-mode: goinstall`) |
| `test` | `make test` with `-race` and `-coverprofile`; total coverage in the job summary, `coverage.out` uploaded as an artifact |
| `integration` | `make test-integration` (testcontainers PostgreSQL 16 + Redis 7 on the runner's Docker daemon) |
| `sqlc-check` | Pinned `sqlc` 1.28.0 regenerates `internal/repository/sqlc`; `git diff --exit-code` fails on drift |
| `migrations-check` | Postgres service container; `migrate up` -> `down -all` -> `up` proves every `.down.sql` reverses its `.up.sql` |
| `openapi-lint` | `@redocly/cli` (pinned) lints `api/openapi.yaml` using `redocly.yaml` |
| `docker` | Builds the image with `docker/build-push-action` (no push, GHA layer cache) and validates `docker compose config` without any `.env` |

`.github/workflows/release.yml` is the CD part: pushing a tag such as `v1.0.0` builds `linux/amd64` + `linux/arm64` images and pushes them to `ghcr.io/titanbgg/notification-system` (`{{version}}`, `{{major}}.{{minor}}`, `sha-<short>` tags) using `GITHUB_TOKEN`, with `packages: write` granted only to that job. A second job then creates the GitHub Release, using the matching [`CHANGELOG.md`](CHANGELOG.md) section as the release notes (`scripts/release-notes.sh`). A tag without a changelog section fails that job. That job is the only one granted `contents: write`.

To run the released image instead of building locally, retag it to the name Compose expects:

```bash
docker pull ghcr.io/titanbgg/notification-system:1.1.0
docker tag ghcr.io/titanbgg/notification-system:1.1.0 notification-system:local
docker compose up --no-build
```

Actions are pinned to **major versions** (`actions/checkout@v7`): they receive security/bug fixes automatically while breaking changes require a deliberate bump. Pinning to full commit SHAs is the stricter supply-chain alternative (immutable, typically paired with Dependabot/Renovate to keep them current). Tool versions (`golangci-lint`, `sqlc`, `migrate`, Redocly) are pinned exactly in the workflow `env:` block so CI results are reproducible.

---

## 11. Mock provider vs. webhook.site

By default the workers send to `cmd/mockprovider` (`PROVIDER_URL=http://mockprovider:8090/`). It follows the same contract as the assignment's provider: `POST {"to","channel","content"}` → `202 {"messageId","status":"accepted","timestamp"}`. It is deterministic enough for tests and adjustable without code changes:

| Variable | Default | Effect |
|---|---|---|
| `MOCK_LATENCY_MS` / `MOCK_LATENCY_JITTER_MS` | `0` / `0` | Added response latency |
| `MOCK_FAILURE_RATE` | `0` | Probability of a `500` (transient) |
| `MOCK_429_RATE` | `0` | Probability of a `429` with `Retry-After: 1` |
| `MOCK_PERMANENT_RATE` | `0` | Probability of a `400` (permanent) |
| `MOCK_SEED` | `0` | Fixed random seed (`0` = random) |

```bash
MOCK_LATENCY_MS=50 MOCK_FAILURE_RATE=0.2 docker compose up -d mockprovider   # slow, flaky provider
curl -s http://localhost:8090/stats | head -c 300; echo                       # requests received per Idempotency-Key
docker compose up -d mockprovider                                             # back to defaults
```

The fault profile can also be changed at runtime without restarting the container, which keeps the `/stats` counters that the duplicate checks read. Omitted fields keep their value; rates must be between 0 and 1, latencies between 0 and 60,000 ms:

```bash
curl -s -X POST http://localhost:8090/config -H 'Content-Type: application/json' -d '{"latencyMs":2000,"latencyJitterMs":500}'
curl -s -X POST http://localhost:8090/config -H 'Content-Type: application/json' -d '{"latencyMs":0,"latencyJitterMs":0,"failureRate":0,"rate429":0,"permanentRate":0}'
```

To send to the real external provider used in the assignment, `webhook.site`:

1. Open https://webhook.site and copy your unique URL (`https://webhook.site/<uuid>`).
2. Under **Edit**, set the default response status to `202`, content type to `application/json`, and the body to `{"messageId":"webhook-site-1","status":"accepted","timestamp":"2026-11-21T10:00:00Z"}`. Any 2xx response counts as sent. A body missing `messageId` is logged as a warning and the message is still marked sent.
3. Restart the workers with that URL. The API does not call the provider, so it does not need restarting:
   ```bash
   PROVIDER_URL="https://webhook.site/<uuid>" docker compose up -d worker-1 worker-2
   ```
4. Create a notification (section 7). The request appears on webhook.site with the JSON body and the `Idempotency-Key`, `X-Correlation-ID` and `traceparent` headers.

**Verified on 2026-11-21.** With the response configured as in step 2, the workers were pointed at a webhook.site URL and one notification per channel was created (fake recipients). All three reached `sent` on the first attempt with `providerMessageId: "webhook-site-1"` taken from the `202` body. webhook.site recorded exactly three `POST` requests with these bodies:

```json
{"to":"+905551234567","channel":"sms","content":"EXT-1 proof: your order has shipped"}
{"to":"test@example.com","channel":"email","content":"EXT-1 proof: your order has shipped"}
{"to":"fake-device-token-ext1-proof","channel":"push","content":"EXT-1 proof: your order has shipped"}
```

Each request carried `Content-Type: application/json`, `Idempotency-Key: <notification id>`, the client's `X-Correlation-ID` and a W3C `traceparent`. The optional `subject` and `title` were not sent because `PROVIDER_EXTENDED_FIELDS` defaults to `false`, which keeps the body exactly as in the assignment.

webhook.site's free tier limits the number of requests, so use the mock provider for load tests. webhook.site does not deduplicate on `Idempotency-Key`, so a provider timeout followed by a retry can produce two requests there (see section 4).

---

## 12. Known limitations and next steps

What is deliberately missing or simplified, and what I would do with more time:

- **No authentication, authorization or multi-tenancy.** Idempotency keys are global. Next steps: API keys or OAuth2 client credentials, a `tenant_id` on every table and key, and per-tenant rate limits.
- **`429` burns attempts.** A `429` is a transient error (it honors `Retry-After`) and counts against `MAX_ATTEMPTS` like a `5xx`. In the chaos test, 37 of 76,000 notifications sent while the provider returned `429` for half of all requests used all 5 attempts and went to the DLQ (recoverable with a manual retry). A `429` means "slow down", not "this message is broken", so treating it like an open circuit (deferral without burning an attempt, capped by `MAX_CIRCUIT_DEFERRALS`) would be the better policy for a long throttling period.
- **Duplicates on provider timeouts.** At-least-once is the honest guarantee. Exactly-once would need a provider that deduplicates on `Idempotency-Key`.
- **Each replica opens its own PostgreSQL pool.** `DB_MAX_CONNS` (worker default 10) fits 6 worker replicas + the API into PostgreSQL's default 100 connections; beyond that I would put PgBouncer in transaction mode in front of PostgreSQL.
- **Limiter contention at many replicas.** With 6 replicas and 100ms provider latency, 150 senders per channel contend for each 10ms GCRA slot and the effective rate drops to ~86/s (95/s with 2 replicas). The 100/s ceiling holds; a per-channel token dispatcher (one process hands out slots) would recover the unused slots.
- **The circuit breaker is per replica.** With N replicas, a provider outage costs up to N × 10 failed calls before every breaker is open. A shared breaker in Redis is possible but adds a round trip per send and fails when Redis does.
- **No Turkish national language shift table for SMS.** `ç ğ ı İ ş Ş` force UCS-2 (70/67 characters per segment) instead of the 3GPP TS 23.038 Turkish single-shift table that some carriers support.
- **The Redis DLQ list is capped at 100,000 IDs.** PostgreSQL (`status='failed'` plus `delivery_attempts`) keeps the full history; the list only drives `dlq_depth`. A manual retry removes the ID from the list.
- **The reconciler repair delay of up to 45s** is fine for this scale; at 100M/day I would switch to an outbox or CDC (section 6).
- **One Redis instance.** Redis Sentinel or Cluster with per-channel hash tags would be next.
- **Metrics windows are computed from PostgreSQL** (cached for 2s). At higher volume they should come from Prometheus recording rules instead.
- **Templates have no versioning or localization.** Rendering at create time keeps that safe for in-flight messages, but a template history table would help auditing.

---

## 13. Requirement checklist

Every item from the assignment, where it is implemented, and how it is tested. "Integration" refers to subtests of `TestCoreIntegration` in `internal/integration`.

| ID | Requirement | Implementation File(s) | Automated Test(s) | Status |
|---|---|---|---|---|
| **FR-1.1** | Create single notification (`recipient`, `channel`, `content`, `priority`) | `internal/api/handlers_notifications.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/a_single_notification_end_to_end` | **DONE** |
| **FR-1.2** | Batch creation (`<= 1000` notifications, `pgx.CopyFrom`, all-or-nothing validation) | `internal/api/handlers_notifications.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/c_batch_1000_and_batch_key_replay` | **DONE** |
| **FR-1.3** | Query notification status & `deliveryAttempts` by ID | `internal/api/handlers_notifications.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/a_single_notification_end_to_end` | **DONE** |
| **FR-1.4** | Query batch status summary (`countsByStatus`) & paginated items by batch ID | `internal/api/handlers_batches.go`, `internal/repository/repository.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/c_batch_1000_and_batch_key_replay` | **DONE** |
| **FR-1.5** | Cancel pending notifications (`pending`/`retrying`/`scheduled` -> `cancelled`, `409 Conflict`) | `internal/api/handlers_notifications.go`, `internal/api/handlers_batches.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/d_cancel_race_no_cancelled_notification_reaches_provider` | **DONE** |
| **FR-1.6** | List notifications with filtering (`status`, `channel`, `priority`, `from`/`to`) & keyset pagination | `internal/api/handlers_notifications.go`, `internal/api/cursor.go` | `internal/api/handlers_test.go`, `TestCoreIntegration/i_list_filters_and_three_page_keyset_pagination` | **DONE** |
| **FR-2.1** | Process notifications asynchronously via queue workers with atomic `Claim` & fencing | `internal/worker/worker.go`, `internal/queue/queue.go`, `cmd/worker/main.go` | `internal/worker/worker_test.go`, `TestCoreIntegration/a_single_notification_end_to_end` | **DONE** |
| **FR-2.2** | Distributed rate limiting: max `100` messages/sec per channel (`Burst=1`) | `internal/ratelimit/limiter.go` | `internal/ratelimit/limiter_test.go`, `TestCoreIntegration/g_distributed_rate_limit_1000_sms_across_two_workers` | **DONE** |
| **FR-2.3** | Priority queue support (`high`, `normal`, `low`, 6:3:1 weighted anti-starvation) | `internal/queue/queue.go`, `internal/queue/lua/dequeue.lua` | `internal/queue/queue_test.go`, `TestCoreIntegration/f_priority_high_completes_ahead_of_low` | **DONE** |
| **FR-2.4** | Content & recipient validation (E.164, GSM-7/UCS-2 SMS segments, Email, Push) | `internal/domain/validation.go`, `internal/domain/sms.go` | `internal/domain/validation_test.go`, `internal/domain/sms_test.go`, `internal/api/handlers_test.go` | **DONE** |
| **FR-2.5** | Idempotency support (API `idempotency_keys` + SHA-256 canonical hash; provider `Idempotency-Key`) | `internal/domain/hash.go`, `internal/api/handlers_notifications.go`, `internal/provider/client.go` | `internal/domain/hash_test.go`, `internal/api/handlers_test.go`, `TestCoreIntegration/b_idempotent_create_replay_and_conflict`, `p_idempotency_20_concurrent_posts_same_key_create_exactly_one_row` | **DONE** |
| **FR-3** | Delivery & retry logic (outcome classification, full-jitter exponential backoff, `Retry-After`) | `internal/worker/retry.go`, `internal/worker/worker.go`, `internal/provider/client.go` | `internal/worker/retry_test.go`, `internal/worker/worker_phase5_test.go`, `TestCoreIntegration/e_retry_500_500_202_and_permanent_400_to_dlq` | **DONE** |
| **FR-4.1** | Real-time metrics (`/metrics` Prometheus custom registry + `/api/v1/metrics` 2s cached JSON) | `internal/observability/metrics.go`, `internal/api/handlers_metrics.go`, `internal/api/middleware/metrics.go` | `internal/observability/metrics_test.go`, `internal/api/observability_test.go` | **DONE** |
| **FR-4.2** | Structured JSON logging (`log/slog`) with `correlation_id` propagation & PII masking | `internal/observability/logger.go`, `internal/api/middleware/correlation.go`, `internal/api/middleware/logger.go` | `internal/observability/logger_test.go`, `internal/api/middleware/middleware_test.go`, `internal/worker/observability_hooks_test.go` | **DONE** |
| **FR-4.3** | Health check endpoints (`/health/live`, `/health/ready`, `/health` with 1s DB/Redis timeout & `503`) | `internal/api/handlers_system.go`, `cmd/worker/main.go`, `cmd/healthcheck/main.go` | `internal/api/observability_test.go`, `cmd/healthcheck/main_test.go`, `scripts/smoke.sh` | **DONE** |
| **EXT-1** | External provider HTTP contract (`POST {to,channel,content}` -> `202 {messageId,status,timestamp}`) | `internal/provider/client.go`, `cmd/mockprovider/main.go` | `internal/provider/client_test.go`, `cmd/mockprovider/main_test.go`, `TestCoreIntegration/a_single_notification_end_to_end`; live webhook.site run (section 11) | **DONE** |
| **B-1** | Failure handling (per-channel `gobreaker/v2`, DLQ, 5-step Reconciler, Inflight Sweeper, manual `/retry`) | `internal/worker/breaker.go`, `internal/worker/reconciler.go`, `internal/worker/worker.go` | `internal/worker/breaker_test.go`, `internal/worker/reconciler_test.go`, `internal/worker/stranded_repair_test.go`, `TestCoreIntegration/h_reconciler_repairs_untracked_pending_and_simulated_redis_loss`, `n_circuit_breaker_opens_on_503_outage_without_burning_attempts_then_recovers`, `o_worker_crash_mid_batch_recovered_by_reconciler_with_bounded_duplicates`, `q_manual_retry_failed_to_sent` | **DONE** |
| **B-2** | Scheduled notifications (`scheduledAt` RFC3339 -> UTC, shared `{notif}:delayed` ZSET + `Claim` clock-skew guard, cancel-before-due) | `internal/api/handlers_notifications.go`, `internal/domain/validation.go`, `internal/queue/queue.go`, `internal/worker/reconciler.go` | `internal/api/handlers_test.go`, `internal/worker/worker_test.go`, `TestCoreIntegration/j_scheduled_due_in_2s_sent_after_due_and_cancelled_never_sent` | **DONE** |
| **B-3** | Message templates with safe `{{variable}}` substitution, preview endpoint, soft-delete, and render-at-create | `internal/template/template.go`, `internal/api/handlers_templates.go`, `migrations/000002_templates.up.sql` | `internal/template/template_test.go`, `internal/api/handlers_test.go`, `TestCoreIntegration/k_template_create_preview_and_batch_personalized_sms_delivery` | **DONE** |
| **B-4** | Real-time WebSocket status updates (`GET /api/v1/ws`, Redis Pub/Sub `notif:events`, initial DB snapshot, `/demo` UI) | `internal/realtime/hub.go`, `internal/api/handlers_ws.go`, `internal/api/static/demo.html` | `internal/realtime/hub_test.go`, `internal/api/handlers_test.go`, `TestCoreIntegration/l_websocket_batch_subscription_receives_snapshot_and_live_sent_events` | **DONE** |
| **B-5** | Distributed tracing with OpenTelemetry (`otlptracehttp` to Jaeger `:4318`, `otelhttp`, `otelpgx`, Redis `traceparent` propagation, `trace_id`/`span_id` in `slog`) | `internal/observability/tracing.go`, `internal/api/middleware/tracing.go`, `internal/worker/worker.go`, `internal/provider/client.go` | `internal/observability/tracing_test.go`, `TestCoreIntegration/m_tracing_traceparent_propagates_from_api_through_redis_to_provider` | **DONE** |
| **B-6** | GitHub Actions CI/CD (lint, race + coverage, integration, sqlc drift, migrations up/down/up, OpenAPI lint, image build; GHCR release on `v*` tags) | `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `.golangci.yml`, `redocly.yaml` | `actionlint`; CI run on every push / PR | **DONE** |
| **D-1** | GitHub repository with clean Conventional Commits history | Git repository | `git log`, `.gitignore` | **DONE** |
| **D-2** | `README.md` with setup instructions, architecture overview, Mermaid diagrams, and API examples | `README.md` | `scripts/smoke.sh` | **DONE** |
| **D-3** | One-command zero-config Docker Compose (`docker compose up` & `docker-compose up`) | `docker-compose.yml`, `Dockerfile`, `.env.example` | Live Docker Compose build + `scripts/smoke.sh` | **DONE** |
| **D-4** | OpenAPI 3.0.3 specification (`api/openapi.yaml`) + Swagger UI (`:8081`) | `api/openapi.yaml`, `api/spec.go` | `internal/api/openapi_test.go` (`TestOpenAPISpecMatchesRouter`), Redocly lint | **DONE** |
| **D-5** | Versioned database migrations (`migrations/*.up.sql`, `.down.sql`) | `migrations/000001_init.up.sql`, `migrations/000001_init.down.sql`, `migrations/migrations.go` | `TestCoreIntegration` (`golang-migrate`) + Compose `migrate` service + CI `migrations-check` (up -> down -> up) | **DONE** |
| **D-6** | Single-command test suite (`make test-all` / `go test ./...`) | `Makefile`, `internal/integration/integration_test.go`, `internal/integration/extended_test.go`, `cmd/loadgen` | `go test -race ./...` + `go test -race -tags=integration ./...` (20 scenarios), `make cover`, 10k load test | **DONE** |
| **Context: burst traffic** | Flash sales / breaking news: absorb bursts without dropping or overrunning the provider | `POST /batch` (1000 items, one `CopyFrom`), Redis queues as buffer, per-channel GCRA limiter | Load test: 10,000 notifications accepted in 287ms (34,787/s), drained at ~95/s/channel with 0 failures; integration `c`, `g` | **DONE** |
| **Context: intelligent retry** | Retry only what can succeed, back off, protect a failing provider | `internal/provider/client.go` (classification), `internal/worker/retry.go` (full jitter + `Retry-After`), `internal/worker/breaker.go` | `internal/worker/retry_test.go`, `internal/provider/client_test.go`, integration `e`, `n`, `q` | **DONE** |
| **Context: visibility for internal teams** | Metrics, logs and traces for operators | Prometheus `/metrics` (API + workers), `slog` JSON with `correlation_id`/`trace_id`, OpenTelemetry → Jaeger | `internal/observability/*_test.go`, integration `m`, PromQL queries in section 8 run against Prometheus | **DONE** |
| **Context: visibility for API consumers** | Delivery status for the clients that send notifications | `GET /notifications/{id}` (+ `deliveryAttempts`), `GET /batches/{id}` (`countsByStatus`), WebSocket `/api/v1/ws`, `/demo` | `internal/api/handlers_test.go`, `internal/realtime/hub_test.go`, integration `l` | **DONE** |

---

## 14. How this was built

I built this project with an AI pair programmer (Claude Code). The shared specification was [`CLAUDE.md`](CLAUDE.md): the architecture, domain rules, state machine, Redis data model, failure handling and coding conventions were written down there first, and every phase was implemented, reviewed and verified against it, with `gofmt`, `go vet`, `golangci-lint`, `-race` unit tests and the integration suite run after each phase.

The design decisions in this README are mine, and I can explain and defend each of them, including the trade-offs I chose not to take.

---

## License

[MIT](LICENSE)
