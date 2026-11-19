-- Migration 001: Initial Schema for Distributed Task Scheduler & Webhook Dispatcher

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    target_url TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'dlq')),
    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until TIMESTAMPTZ NULL,
    cron_expr TEXT NULL,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    idempotency_key TEXT NOT NULL,
    trace_context JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_idempotency UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    status_code INT NULL,
    response_body TEXT NULL,
    latency_ms INT NOT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partial index for high-throughput poll query regardless of table volume
CREATE INDEX IF NOT EXISTS idx_jobs_poll
ON jobs (run_at)
WHERE status IN ('pending', 'processing');

-- Index for retrieving delivery attempts by job
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_job_id
ON delivery_attempts (job_id, attempt_number DESC);

-- Index for DLQ replay by tenant
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_status
ON jobs (tenant_id, status);

