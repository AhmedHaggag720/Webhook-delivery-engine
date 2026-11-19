import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid
import asyncpg
from croniter import croniter

from src.config import settings

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            logger.info("Initializing asyncpg connection pool...")
            self.pool = await asyncpg.create_pool(
                host=settings.pg_host,
                port=settings.pg_port,
                user=settings.pg_user,
                password=settings.pg_password,
                database=settings.pg_db,
                min_size=settings.pg_min_pool,
                max_size=settings.pg_max_pool,
            )
            logger.info("Asyncpg connection pool established.")

    async def disconnect(self):
        if self.pool:
            logger.info("Closing asyncpg connection pool...")
            await self.pool.close()
            self.pool = None

    async def apply_migrations(self, migration_file_path: str):
        if not self.pool:
            await self.connect()
        with open(migration_file_path, "r", encoding="utf-8") as f:
            sql = f.read()
        async with self.pool.acquire() as conn:
            await conn.execute(sql)
        logger.info(f"Applied migrations from {migration_file_path}")

    async def check_health(self) -> bool:
        if not self.pool:
            return False
        try:
            async with self.pool.acquire() as conn:
                res = await conn.fetchval("SELECT 1")
                return res == 1
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False

    async def enqueue_job(
        self,
        tenant_id: str,
        target_url: str,
        payload: Dict[str, Any],
        idempotency_key: str,
        run_at: Optional[datetime] = None,
        cron_expr: Optional[str] = None,
        max_attempts: int = 5,
        trace_context: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], bool]:
        """
        Enqueues a job. If (tenant_id, idempotency_key) already exists, returns existing job and created=False.
        Otherwise creates new job and returns it with created=True.
        """
        if not self.pool:
            await self.connect()

        payload_json = json.dumps(payload)
        trace_context_json = json.dumps(trace_context) if trace_context else None
        target_run_at = run_at or datetime.now(timezone.utc)

        query = """
        INSERT INTO jobs (
            tenant_id, target_url, payload, status, run_at, cron_expr, max_attempts, idempotency_key, trace_context
        )
        VALUES ($1, $2, $3::jsonb, 'pending', $4, $5, $6, $7, $8::jsonb)
        ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
        RETURNING *;
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                query,
                tenant_id,
                target_url,
                payload_json,
                target_run_at,
                cron_expr,
                max_attempts,
                idempotency_key,
                trace_context_json,
            )
            if row:
                return dict(row), True

            # Existing job fetch
            existing = await conn.fetchrow(
                "SELECT * FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2",
                tenant_id,
                idempotency_key,
            )
            return dict(existing), False

    async def get_job_with_history(self, job_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            job_row = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
            if not job_row:
                return None

            job_dict = dict(job_row)
            attempts_rows = await conn.fetch(
                """
                SELECT id, job_id, attempt_number, status_code, response_body, latency_ms, error_message, created_at
                FROM delivery_attempts
                WHERE job_id = $1
                ORDER BY attempt_number ASC
                """,
                job_id,
            )
            job_dict["delivery_attempts"] = [dict(r) for r in attempts_rows]
            return job_dict

    async def lease_jobs_batch(self, limit: int, visibility_timeout_sec: int) -> List[Dict[str, Any]]:
        """
        Safely lease batches atomically using FOR UPDATE SKIP LOCKED.
        Matches Section 3.1 of specification.
        """
        if not self.pool:
            await self.connect()

        query = """
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
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit, str(visibility_timeout_sec))
            return [dict(r) for r in rows]

    async def record_delivery_attempt(
        self,
        job_id: uuid.UUID,
        attempt_number: int,
        status_code: Optional[int],
        response_body: Optional[str],
        latency_ms: int,
        error_message: Optional[str] = None,
    ):
        if not self.pool:
            await self.connect()

        truncated_body = response_body[:2048] if response_body else None

        query = """
        INSERT INTO delivery_attempts (job_id, attempt_number, status_code, response_body, latency_ms, error_message)
        VALUES ($1, $2, $3, $4, $5, $6);
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                job_id,
                attempt_number,
                status_code,
                truncated_body,
                latency_ms,
                error_message,
            )

    async def mark_job_completed(self, job: Dict[str, Any]):
        """
        Mark job completed. If recurring (cron_expr is set), calculate next run_at and schedule next job occurrence.
        """
        if not self.pool:
            await self.connect()

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'completed',
                        locked_until = NULL,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    job["id"],
                )

                cron_expr = job.get("cron_expr")
                if cron_expr:
                    try:
                        base_time = datetime.now(timezone.utc)
                        iter_cron = croniter(cron_expr, base_time)
                        next_run_at = iter_cron.get_next(datetime)
                        new_idempotency_key = f"{job['idempotency_key']}_{int(next_run_at.timestamp())}"
                        
                        await conn.execute(
                            """
                            INSERT INTO jobs (
                                tenant_id, target_url, payload, status, run_at, cron_expr, max_attempts, idempotency_key, trace_context
                            )
                            VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8)
                            ON CONFLICT (tenant_id, idempotency_key) DO NOTHING;
                            """,
                            job["tenant_id"],
                            job["target_url"],
                            job["payload"] if isinstance(job["payload"], str) else json.dumps(job["payload"]),
                            next_run_at,
                            cron_expr,
                            job.get("max_attempts", 5),
                            new_idempotency_key,
                            job.get("trace_context"),
                        )
                        logger.info(f"Scheduled next recurring job occurrence at {next_run_at} for tenant {job['tenant_id']}")
                    except Exception as e:
                        logger.error(f"Failed to schedule recurring job {job['id']} with cron {cron_expr}: {e}")

    async def mark_job_dlq(self, job_id: uuid.UUID):
        if not self.pool:
            await self.connect()

        query = """
        UPDATE jobs
        SET status = 'dlq',
            locked_until = NULL,
            updated_at = NOW()
        WHERE id = $1;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, job_id)

    async def reschedule_retry(self, job_id: uuid.UUID, delay_seconds: float):
        """
        Retry job using backoff delay.
        """
        if not self.pool:
            await self.connect()

        query = """
        UPDATE jobs
        SET status = 'pending',
            locked_until = NULL,
            run_at = NOW() + ($2 || ' seconds')::INTERVAL,
            updated_at = NOW()
        WHERE id = $1;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, job_id, str(delay_seconds))

    async def release_throttled_job(self, job_id: uuid.UUID, delay_seconds: float):
        """
        Noisy neighbor protection:
        Do not consume retry attempt. Release back to pending, attempts = attempts - 1,
        locked_until = NULL, run_at = NOW() + retry_after + jitter.
        """
        if not self.pool:
            await self.connect()

        query = """
        UPDATE jobs
        SET status = 'pending',
            locked_until = NULL,
            attempts = GREATEST(attempts - 1, 0),
            run_at = NOW() + ($2 || ' seconds')::INTERVAL,
            updated_at = NOW()
        WHERE id = $1;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, job_id, str(delay_seconds))

    async def release_circuit_broken_job(self, job_id: uuid.UUID, delay_seconds: float):
        """
        Downstream destination open:
        Short-circuit without consuming retry attempt. Release back to pending, attempts = attempts - 1,
        locked_until = NULL, run_at = NOW() + cooldown_remaining.
        """
        if not self.pool:
            await self.connect()

        query = """
        UPDATE jobs
        SET status = 'pending',
            locked_until = NULL,
            attempts = GREATEST(attempts - 1, 0),
            run_at = NOW() + ($2 || ' seconds')::INTERVAL,
            updated_at = NOW()
        WHERE id = $1;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, job_id, str(delay_seconds))

    async def replay_dlq(self, job_id: Optional[uuid.UUID] = None, tenant_id: Optional[str] = None) -> int:
        """
        Replay DLQ jobs by job_id or tenant_id. Resets status='pending', attempts=0, run_at=NOW().
        Returns count of affected jobs.
        """
        if not self.pool:
            await self.connect()

        if job_id:
            query = """
            UPDATE jobs
            SET status = 'pending',
                attempts = 0,
                locked_until = NULL,
                run_at = NOW(),
                updated_at = NOW()
            WHERE id = $1 AND status = 'dlq';
            """
            async with self.pool.acquire() as conn:
                res = await conn.execute(query, job_id)
                count = int(res.split(" ")[-1]) if res else 0
                return count
        elif tenant_id:
            query = """
            UPDATE jobs
            SET status = 'pending',
                attempts = 0,
                locked_until = NULL,
                run_at = NOW(),
                updated_at = NOW()
            WHERE tenant_id = $1 AND status = 'dlq';
            """
            async with self.pool.acquire() as conn:
                res = await conn.execute(query, tenant_id)
                count = int(res.split(" ")[-1]) if res else 0
                return count
        else:
            # Replay all DLQ jobs
            query = """
            UPDATE jobs
            SET status = 'pending',
                attempts = 0,
                locked_until = NULL,
                run_at = NOW(),
                updated_at = NOW()
            WHERE status = 'dlq';
            """
            async with self.pool.acquire() as conn:
                res = await conn.execute(query)
                count = int(res.split(" ")[-1]) if res else 0
                return count

    async def get_dlq_count(self) -> int:
        if not self.pool:
            await self.connect()
        async with self.pool.acquire() as conn:
            val = await conn.fetchval("SELECT COUNT(*) FROM jobs WHERE status = 'dlq'")
            return val or 0

db = Database()

