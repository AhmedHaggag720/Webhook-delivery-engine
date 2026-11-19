import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse
import httpx
from opentelemetry import trace

from src.config import settings
from src.db import db
from src.redis_ops import redis_manager
from src.telemetry import (
    DELIVERY_LATENCY_SECONDS,
    DELIVERIES_TOTAL,
    QUEUE_LAG_SECONDS,
    RATE_LIMIT_THROTTLES_TOTAL,
    extract_trace_context,
    tracer,
    update_cb_metric,
)

logger = logging.getLogger(__name__)

def get_tenant_secret(tenant_id: str) -> str:
    """
    Derives deterministic HMAC signing secret for a tenant using salt.
    """
    return hmac.new(
        settings.tenant_secret_salt.encode("utf-8"),
        tenant_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

def sign_webhook_payload(tenant_secret: str, timestamp: int, raw_body: str) -> str:
    """
    HMAC-SHA256 over {timestamp}.{raw_json_body}
    Returns 'v1={hex_hmac}'
    """
    signature_payload = f"{timestamp}.{raw_body}".encode("utf-8")
    mac = hmac.new(
        tenant_secret.encode("utf-8"),
        signature_payload,
        hashlib.sha256,
    ).hexdigest()
    return f"v1={mac}"

def compute_exponential_backoff_jitter(
    attempt: int,
    base_seconds: Optional[float] = None,
    cap_seconds: Optional[float] = None,
) -> float:
    """
    Full jitter exponential backoff:
    delay = random(0, min(cap, base * 2^attempt))
    """
    base = base_seconds if base_seconds is not None else settings.backoff_base_seconds
    cap = cap_seconds if cap_seconds is not None else settings.backoff_cap_seconds
    temp = min(cap, base * (2 ** attempt))
    return random.uniform(0.0, temp)

def parse_retry_after(header_val: Optional[str]) -> Optional[float]:
    """
    Parses Retry-After header (seconds or http-date format).
    """
    if not header_val:
        return None
    try:
        # Standard integer seconds
        return float(header_val.strip())
    except ValueError:
        pass
    # Try HTTP date
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(header_val)
        now = datetime.now(timezone.utc)
        diff = (dt - now).total_seconds()
        return max(0.0, diff)
    except Exception:
        return None

class WebhookDispatcher:
    def __init__(self):
        self.running: bool = False
        self.active_tasks: set[asyncio.Task] = set()
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.http_client: Optional[httpx.AsyncClient] = None

    async def start(self):
        self.running = True
        self.semaphore = asyncio.Semaphore(settings.worker_concurrency)
        limits = httpx.Limits(
            max_connections=settings.worker_concurrency * 2,
            max_keepalive_connections=settings.worker_concurrency,
        )
        transport = httpx.AsyncHTTPTransport(retries=0)
        self.http_client = httpx.AsyncClient(
            limits=limits,
            transport=transport,
            timeout=httpx.Timeout(
                timeout=settings.http_total_timeout,
                connect=settings.http_connect_timeout,
            ),
        )
        logger.info(f"Dispatcher started with concurrency limit={settings.worker_concurrency}")

    async def stop(self):
        self.running = False
        logger.info("Stopping dispatcher and waiting for active deliveries to drain...")
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks, return_exceptions=True)
        if self.http_client:
            await self.http_client.aclose()
        logger.info("Dispatcher stopped cleanly.")

    async def run_loop(self):
        """
        Continuous polling loop leasing batches via SKIP LOCKED.
        """
        while self.running:
            try:
                # Only poll if we have capacity in semaphore
                if self.semaphore and self.semaphore.locked():
                    await asyncio.sleep(settings.poll_interval_ms / 1000.0)
                    continue

                jobs = await db.lease_jobs_batch(
                    limit=settings.batch_size,
                    visibility_timeout_sec=settings.visibility_timeout_seconds,
                )

                if not jobs:
                    await asyncio.sleep(settings.poll_interval_ms / 1000.0)
                    continue

                for job in jobs:
                    task = asyncio.create_task(self._process_job_wrapper(job))
                    self.active_tasks.add(task)
                    task.add_done_callback(self.active_tasks.discard)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in dispatcher polling loop: {e}", exc_info=True)
                await asyncio.sleep(settings.poll_interval_ms / 1000.0)

    async def _process_job_wrapper(self, job: Dict[str, Any]):
        try:
            if not self.semaphore:
                await self._process_job(job)
                return

            async with self.semaphore:
                await self._process_job(job)
        except Exception as e:
            logger.error(f"Unexpected error processing job {job.get('id')}: {e}", exc_info=True)
            try:
                await db.reschedule_retry(job["id"], delay_seconds=2.0)
            except Exception:
                pass

    async def _process_job(self, job: Dict[str, Any]):
        job_id = job["id"]
        tenant_id = job["tenant_id"]
        target_url = job["target_url"]
        attempts = job["attempts"]
        max_attempts = job["max_attempts"]
        run_at = job["run_at"]

        # Parse target host for circuit breaker
        effective_url = target_url
        if settings.pg_host == "postgres" or os.getenv("RUNNING_IN_DOCKER"):
            if "localhost:8080" in effective_url:
                effective_url = effective_url.replace("localhost:8080", "mock-receiver:8080")
            elif "127.0.0.1:8080" in effective_url:
                effective_url = effective_url.replace("127.0.0.1:8080", "mock-receiver:8080")

        parsed_url = urlparse(effective_url)
        host = parsed_url.netloc or parsed_url.path

        # 1. Check Circuit Breaker
        cb_allowed, cb_state, cooldown_rem = await redis_manager.check_circuit_breaker(host)
        update_cb_metric(host, cb_state)

        if not cb_allowed:
            # Short-circuit immediately without sending HTTP request
            logger.warning(
                f"Circuit breaker OPEN for host {host}. Short-circuiting job {job_id}."
            )
            await db.release_circuit_broken_job(job_id, max(1.0, cooldown_rem))
            return

        # 2. Check Rate Limiter
        rl_allowed, remaining_tokens, wait_time = await redis_manager.check_rate_limit(tenant_id)
        if not rl_allowed:
            # Throttled by tenant rate limit: noisy neighbor protection
            RATE_LIMIT_THROTTLES_TOTAL.labels(tenant_id=tenant_id).inc()
            jitter = random.uniform(0.05, 0.2)
            retry_delay = max(0.1, wait_time) + jitter
            logger.info(
                f"Tenant {tenant_id} rate-limited. Releasing job {job_id} back to pending with delay={retry_delay:.2f}s."
            )
            await db.release_throttled_job(job_id, retry_delay)
            return

        # 3. Calculate and record queue lag
        now_utc = datetime.now(timezone.utc)
        lag_seconds = max(0.0, (now_utc - run_at).total_seconds())
        QUEUE_LAG_SECONDS.observe(lag_seconds)

        # 4. OpenTelemetry span setup & tracing linkage
        trace_carrier = job.get("trace_context")
        extracted_context = extract_trace_context(trace_carrier) if trace_carrier else None

        span_links = []
        if extracted_context:
            span_ctx = trace.get_current_span(extracted_context).get_span_context()
            if span_ctx.is_valid:
                span_links.append(trace.Link(span_ctx))

        with tracer.start_as_current_span("worker.deliver_webhook", links=span_links) as span:
            span.set_attribute("job.id", str(job_id))
            span.set_attribute("tenant.id", tenant_id)
            span.set_attribute("target.url", target_url)
            span.set_attribute("attempt.number", attempts)

            await self._execute_delivery(job, host)

    async def _execute_delivery(self, job: Dict[str, Any], host: str):
        job_id = job["id"]
        tenant_id = job["tenant_id"]
        target_url = job["target_url"]

        if settings.pg_host == "postgres" or os.getenv("RUNNING_IN_DOCKER"):
            if "localhost:8080" in target_url:
                target_url = target_url.replace("localhost:8080", "mock-receiver:8080")
            elif "127.0.0.1:8080" in target_url:
                target_url = target_url.replace("127.0.0.1:8080", "mock-receiver:8080")

        attempts = job["attempts"]
        max_attempts = job["max_attempts"]
        raw_payload = job["payload"]

        raw_body = raw_payload if isinstance(raw_payload, str) else json.dumps(raw_payload)
        now_ts = int(time.time())
        secret = get_tenant_secret(tenant_id)
        signature = sign_webhook_payload(secret, now_ts, raw_body)

        headers = {
            "Content-Type": "application/json",
            "X-Webhook-ID": str(job_id),
            "X-Webhook-Timestamp": str(now_ts),
            "X-Webhook-Signature": signature,
        }

        start_time = time.perf_counter()
        status_code: Optional[int] = None
        response_body: Optional[str] = None
        error_message: Optional[str] = None
        is_success = False
        is_network_failure = False
        retry_after_delay: Optional[float] = None

        try:
            if not self.http_client:
                raise RuntimeError("HTTP client not initialized")

            resp = await self.http_client.post(
                target_url,
                content=raw_body.encode("utf-8"),
                headers=headers,
            )
            status_code = resp.status_code
            response_body = resp.text
            retry_after_delay = parse_retry_after(resp.headers.get("Retry-After"))

            if 200 <= status_code < 300:
                is_success = True
                DELIVERIES_TOTAL.labels(status="2xx").inc()
            elif status_code == 429:
                DELIVERIES_TOTAL.labels(status="429").inc()
            elif 400 <= status_code < 500:
                DELIVERIES_TOTAL.labels(status="4xx").inc()
            else:
                DELIVERIES_TOTAL.labels(status="5xx").inc()

        except httpx.TimeoutException as exc:
            is_network_failure = True
            error_message = f"HTTP timeout: {exc}"
            DELIVERIES_TOTAL.labels(status="timeout").inc()
        except httpx.RequestError as exc:
            is_network_failure = True
            error_message = f"Network connection error: {exc}"
            DELIVERIES_TOTAL.labels(status="error").inc()
        except Exception as exc:
            error_message = f"Unexpected error during delivery: {exc}"
            DELIVERIES_TOTAL.labels(status="error").inc()

        elapsed_ms = int((time.perf_counter() - start_time) * 1000)
        DELIVERY_LATENCY_SECONDS.observe(elapsed_ms / 1000.0)

        # Record attempt in database
        await db.record_delivery_attempt(
            job_id=job_id,
            attempt_number=attempts,
            status_code=status_code,
            response_body=response_body,
            latency_ms=elapsed_ms,
            error_message=error_message,
        )

        # Handle Circuit Breaker update
        if is_success:
            new_cb_state = await redis_manager.record_circuit_breaker_success(host)
            update_cb_metric(host, new_cb_state)
        elif is_network_failure or (status_code and status_code >= 500):
            new_cb_state, _, _ = await redis_manager.record_circuit_breaker_failure(host)
            update_cb_metric(host, new_cb_state)

        # Handle Job State Transition & Retries
        if is_success:
            await db.mark_job_completed(job)
            logger.info(f"Job {job_id} delivered successfully (2xx, attempt={attempts}).")
            return

        # Check for non-retryable 4xx (400, 401, 403, 404, etc.)
        if status_code and 400 <= status_code < 500 and status_code != 429:
            logger.warning(
                f"Job {job_id} received non-retryable status {status_code}. Routing directly to DLQ."
            )
            await db.mark_job_dlq(job_id)
            return

        # Retryable failure (429, 5xx, or network timeout/error)
        if attempts >= max_attempts:
            logger.error(
                f"Job {job_id} exhausted max retries ({attempts}/{max_attempts}). Moving to DLQ."
            )
            await db.mark_job_dlq(job_id)
            return

        # Compute backoff delay
        if status_code == 429 and retry_after_delay is not None:
            delay = retry_after_delay
        else:
            delay = compute_exponential_backoff_jitter(attempts)

        logger.info(
            f"Rescheduling job {job_id} for retry #{attempts + 1} with delay={delay:.2f}s (status={status_code}, error={error_message})."
        )
        await db.reschedule_retry(job_id, delay)

dispatcher = WebhookDispatcher()

