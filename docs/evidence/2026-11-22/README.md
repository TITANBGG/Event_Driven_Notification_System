# Test evidence, 2026-11-22

Raw output of live runs against the Docker Compose stack (API, two workers, PostgreSQL 16, Redis 7, mock
provider). Every log starts with the command, the start time and the git commit it ran on. The summary is
in [`docs/reports/test-evidence-report.pdf`](../../reports/test-evidence-report.pdf).

| Log | Scenario | Result |
|---|---|---|
| [`01-verify.log`](01-verify.log) | `./scripts/verify.sh`: lint, race-enabled unit tests, integration tests, live stack, smoke test of every endpoint, invariants | PASS, 7/7 steps |
| [`02-idempotency-1000-concurrent.log`](02-idempotency-1000-concurrent.log) | 1,000 concurrent `POST /api/v1/notifications` with one `Idempotency-Key` | 1 created, 999 replayed, 1 id, 1 row |
| [`03-ratecheck-two-workers.log`](03-ratecheck-two-workers.log) | 3,000 SMS through two workers, busiest one-second window on two clocks | 96 per second (limit 100) |
| [`04-races.log`](04-races.log) | `./scripts/races.sh`: six concurrency races | PASS, 6/6 |
| [`05-drills.log`](05-drills.log) | `./scripts/drills.sh`: Redis down, PostgreSQL down, worker SIGKILL, provider outage, Redis `FLUSHALL` | PASS, 5/5 |
| [`06-isolation.log`](06-isolation.log) | `./scripts/isolation.sh`: saturated SMS does not slow email or push | PASS, 8/8 checks |
| [`12-chaos.log`](12-chaos.log) | `./scripts/chaos.sh`: 80,000 notifications while workers, Redis, PostgreSQL and the provider fail | 0 lost, 0 stuck, 97 per second |
| [`00-reset-stack.log`](00-reset-stack.log) | Fresh stack (`docker compose down -v`, `up --build --wait`) before the runs | all services healthy |

Rerun any of them with the command at the top of its log (`API_URL`, `MOCK_URL` and `PROM_URL` point at
`127.0.0.1` on Windows hosts).
