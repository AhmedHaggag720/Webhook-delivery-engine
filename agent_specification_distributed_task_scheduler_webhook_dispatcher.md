# Project Specification: Distributed Rate-Limited Task Scheduler & Webhook Dispatcher

## 1. Objective
Build a standalone, fault-tolerant service that schedules delayed and recurring jobs, enforces per-tenant rate limits, protects failing downstream endpoints with circuit breakers, and guarantees at-least-once webhook delivery under worker crashes and slow/unreliable targets.

---

## 2. Tech Stack & Infrastructure
* **Language:** Go 1.22+ (preferred for concurrency, `context` timeouts, and low overhead) — *alternatively TypeScript (Node.js) or Python (FastAPI + asyncio) if the repository already uses one of those.*
* **Primary Datastore:** PostgreSQL 16 (source of truth for jobs, leasing, and delivery history).
* **Coordination / Traffic Control:** Redis 7 (atomic Lua scripts for rate limiting and circuit breaking).
* **Observability:** OpenTelemetry (OTLP -> Jaeger) and Prometheus (`/metrics`).
* **Local Orchestration:** `docker-compose.yml` spinning up PostgreSQL, Redis, Prometheus, Jaeger, the Scheduler/API service, Worker instances, and the Mock Webhook Receiver.

---

## 3. Functional & Technical Requirements

### 3.1. Data Model & Durable State (PostgreSQL)
Create versioned SQL migrations for the following tables:

1. **`jobs` table**
   * `id` (`UUID`, primary key)
   * `tenant_id` (`TEXT`, not null)
   * `target_url` (`TEXT`, not null)
   * `payload` (`JSONB`, not null)
   * `status` (`TEXT`, not null: `'pending' | 'processing' | 'completed' | 'dlq'`)
   * `run_at` (`TIMESTAMPTZ`, not null, default `NOW()`)
   * `locked_until` (`TIMESTAMPTZ`, nullable)
   * `cron_expr` (`TEXT`, nullable — optional support for recurring jobs; on completion, enqueue next occurrence)
   * `attempts` (`INT`, not null, default `0`)
   * `max_attempts` (`INT`, not null, default `5`)
   * `idempotency_key` (`TEXT`, not null)
   * `trace_context` (`JSONB`, nullable — stores serialized OpenTelemetry traceparent/tracestate)
   * `created_at` (`TIMESTAMPTZ`, default `NOW()`)
   * `updated_at` (`TIMESTAMPTZ`, default `NOW()`)
   * **Constraint:** `UNIQUE (tenant_id, idempotency_key)` to prevent duplicate ingestion.

2. **`delivery_attempts` table**
   * `id` (`BIGSERIAL`, primary key)
   * `job_id` (`UUID`, foreign key -> `jobs(id)` ON DELETE CASCADE)
   * `attempt_number` (`INT`, not null)
   * `status_code` (`INT`, nullable)
   * `response_body` (`TEXT`, nullable — truncate to 2 KB)
   * `latency_ms` (`INT`, not null)
   * `error_message` (`TEXT`, nullable)
   * `created_at` (`TIMESTAMPTZ`, default `NOW()`)

3. **Indexing**
   * Add a partial index to keep polling fast regardless of historical job volume:
     ```sql
     CREATE INDEX idx_jobs_poll
     ON jobs (run_at)
     WHERE status IN ('pending', 'processing');
     ```

4. **Safe Concurrent Leasing**
   * Workers MUST lease batches atomically using `FOR UPDATE SKIP LOCKED` with a visibility timeout (e.g., 30 seconds) so crashed workers never stall jobs:
     ```sql
     WITH next_jobs AS (
       SELECT id
       FROM jobs
       WHERE (status = 'pending' AND run_at <= NOW())
          OR (status = 'processing' AND locked_until <= NOW())
       ORDER BY run_at ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE jobs
     SET status = 'processing',
         locked_until = NOW() + ($2 || ' seconds')::INTERVAL,
         attempts = attempts + 1,
         updated_at = NOW()
     FROM next_jobs
     WHERE jobs.id = next_jobs.id
     RETURNING jobs.*;
     ```

---

### 3.2. Per-Tenant Rate Limiting & Circuit Breaking (Redis)

1. **Per-Tenant Rate Limiter (Atomic Lua Script)**
   * Implement a **Token Bucket** or **Sliding Window** rate limiter keyed by `ratelimit:{tenant_id}` via a single atomic Redis Lua script (`EVALSHA`).
   * Configurable defaults (e.g., capacity = 20 tokens, refill rate = 10 tokens/sec) with optional per-tenant overrides.
   * **On Throttle (Noisy Neighbor Protection):**
     * Do **not** fail the job or consume a retry attempt.
     * Release the job back to `status = 'pending'`, set `locked_until = NULL`, decrement `attempts = attempts - 1`, and push `run_at = NOW() + retry_after_ms` (plus small jitter).

2. **Per-Destination Circuit Breaker (Redis)**
   * Keyed by destination host (`cb:{host}`), maintaining state: `closed`, `open`, or `half-open`.
   * **Closed $\rightarrow$ Open:** Trip to `open` after $N$ consecutive failures (5xx or network timeout; e.g., $N = 5$) within a sliding window.
   * **Open:** Short-circuit immediately without making an outbound HTTP request. Reschedule the job to `pending` with `run_at = NOW() + cooldown_remaining` and decrement `attempts` so jobs aren't burned while the downstream is known-down.
   * **Open $\rightarrow$ Half-Open:** After cooldown (e.g., 30s), allow a single probe request.
     * Probe succeeds (2xx) $\rightarrow$ transition to `closed` and reset failure counter.
     * Probe fails $\rightarrow$ transition back to `open` and reset cooldown timer.

---

### 3.3. Delivery Worker Pool, Signing, Backoff & DLQ

1. **Worker Pool**
   * Configurable concurrency (e.g., `WORKER_CONCURRENCY=20`, `BATCH_SIZE=10`, `POLL_INTERVAL_MS=100`).
   * Graceful shutdown on `SIGINT` / `SIGTERM`: stop polling new batches and wait for in-flight HTTP deliveries to complete (up to a shutdown timeout) before exiting.

2. **Outbound Webhook Signing & Timeouts**
   * Sign every outbound request using **HMAC-SHA256** over `{timestamp}.{raw_json_body}` using a per-tenant secret.
   * Attach headers:
     * `Content-Type: application/json`
     * `X-Webhook-ID: {job.id}`
     * `X-Webhook-Timestamp: {unix_timestamp}`
     * `X-Webhook-Signature: v1={hex_hmac}`
   * Enforce strict HTTP client timeouts:
     * Dial/Connect timeout: `2s`
     * Total request timeout: `5s`

3. **Retry Policy: Exponential Backoff + Full Jitter**
   * **2xx:** Mark job `completed` (and if `cron_expr` is set, insert the next scheduled run).
   * **429, 5xx, or network timeout:** Retry using exponential backoff with full jitter:
     $$\text{delay} = \text{random}\left(0,\ \min\left(\text{cap},\ \text{base} \times 2^{\text{attempt}}\right)\right)$$
     *(Suggested defaults: $\text{base} = 1\text{s}$, $\text{cap} = 60\text{s}$. Respect `Retry-After` header on `429` if present.)*
   * **Non-retryable 4xx (e.g., 400, 401, 403, 404):** Do not retry; route directly to `dlq`.
   * **Exhausted Retries:** If `attempts >= max_attempts`, set `status = 'dlq'` and `locked_until = NULL`.
   * Record every delivery attempt in `delivery_attempts`.

---

### 3.4. HTTP API Contract

| Method | Path | Description |
| :--- | :--- | :--- |
| `POST` | `/jobs` | Enqueue a job (`tenant_id`, `target_url`, `payload`, `run_at?`, `cron_expr?`, `max_attempts?`, `idempotency_key`). Returns `201 Created` (or `200 OK` with existing job on idempotency conflict). |
| `GET` | `/jobs/{id}` | Fetch job status and its `delivery_attempts` history. |
| `POST` | `/admin/dlq/replay` | Replay DLQ jobs by `job_id` or `tenant_id` (resets `status = 'pending'`, `attempts = 0`, `run_at = NOW()`). |
| `GET` | `/health` | Readiness check verifying PostgreSQL and Redis connectivity. |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint. |

---

### 3.5. Observability & Chaos Benchmarks

1. **OpenTelemetry Tracing**
   * Instrument HTTP ingestion (`POST /jobs`), job leasing, Redis rate-limit/circuit-breaker checks, and outbound webhook delivery.
   * Propagate trace context via `jobs.trace_context` so ingestion and asynchronous worker delivery appear as linked spans in Jaeger.

2. **Prometheus Metrics**
   * `queue_lag_seconds` (Histogram/Gauge): $t_{\text{start}} - t_{\text{run\_at}}$ when a worker begins processing a job.
   * `delivery_latency_seconds` (Histogram): outbound HTTP request duration (with buckets suitable for computing `p99`).
   * `dlq_size` (Gauge): current count of jobs in `status = 'dlq'`.
   * `rate_limit_throttles_total` (Counter, labeled by `tenant_id`).
   * `circuit_breaker_state` (Gauge, labeled by `host`).

3. **Mock Webhook Receiver & Chaos Load Test**
   * Build a standalone mock receiver service that:
     * Verifies `X-Webhook-Signature` using the tenant secret.
     * Tracks unique delivered `job_id`s.
     * Injects configurable chaos: **30% random `500`/`429` failures** and occasional slow responses (`> 5s`) to trigger client timeouts.
   * Build an automated chaos/load test script (`scripts/chaos_test.*`) that:
     1. Blasts **5,000 jobs across 20 tenants** (with 2 noisy tenants sending 40% of traffic to verify rate-limit isolation).
     2. Mid-flight, issues `SIGKILL` (`kill -9`) to one active worker process and verifies remaining workers reclaim its in-flight jobs after `locked_until` expires.
     3. Waits for queue drain and asserts:
        * **Zero lost jobs:** `completed + dlq == 5,000`.
        * No noisy tenant starved healthy tenants.
        * Replaying DLQ via `/admin/dlq/replay` drains remaining jobs when chaos is disabled.
     4. Queries Prometheus and outputs exact `p99` delivery latency, max `queue_lag_seconds`, and throughput.

---

## 4. Suggested Repository Structure
```text
.
├── cmd/
│   ├── api/            # Ingestion & Admin API server
│   ├── worker/         # Concurrent leasing & delivery worker process
│   └── mock-receiver/  # Chaos-enabled webhook receiver for testing
├── internal/
│   ├── db/             # Postgres pool, migrations, atomic lease queries
│   ├── redis/          # Lua scripts (rate limiter, circuit breaker)
│   ├── dispatcher/     # Worker pool, HMAC signing, HTTP client, backoff
│   ├── api/            # HTTP handlers & routing
│   └── telemetry/      # OpenTelemetry & Prometheus setup
├── scripts/
│   └── chaos_test.sh   # 5,000-job load + SIGKILL + 30% failure benchmark
├── docker-compose.yml
├── prometheus.yml
└── README.md
```

---

## 5. Definition of Done (Acceptance Checklist)
Before completing the task, the agent must verify and document all of the following:
- [ ] `docker compose up` brings up the entire stack cleanly with migrations applied automatically.
- [ ] Unit tests pass for HMAC signing, exponential backoff + jitter, Redis Lua rate limiter, and circuit breaker state transitions.
- [ ] Integration tests verify idempotent job creation, `SKIP LOCKED` concurrency, visibility timeout recovery after worker crash, and `/admin/dlq/replay`.
- [ ] Chaos test script runs end-to-end (5,000 jobs, 20 tenants, worker `SIGKILL`, 30% downstream failures) and asserts **0 lost jobs**.
- [ ] `README.md` includes architecture diagram, setup/run instructions, and the **actual recorded benchmark results** (total jobs, lost jobs = 0, `p99` delivery latency, queue lag, and throughput).