from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional
import uuid
from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from src.config import settings
from src.db import db
from src.redis_ops import redis_manager
from src.telemetry import (
    DLQ_SIZE,
    inject_trace_context,
    setup_telemetry,
    tracer,
)

# Request / Response Schemas
class EnqueueJobRequest(BaseModel):
    tenant_id: str = Field(..., min_length=1, description="Tenant identifier")
    target_url: str = Field(..., min_length=1, description="Downstream webhook destination URL")
    payload: Dict[str, Any] = Field(..., description="JSON payload to dispatch")
    idempotency_key: str = Field(..., min_length=1, description="Unique key for deduplication")
    run_at: Optional[datetime] = Field(default=None, description="Optional scheduled execution time")
    cron_expr: Optional[str] = Field(default=None, description="Optional cron expression for recurring jobs")
    max_attempts: Optional[int] = Field(default=5, ge=1, le=20, description="Max delivery retry attempts")

class ReplayDLQRequest(BaseModel):
    job_id: Optional[uuid.UUID] = Field(default=None, description="Specific job ID to replay")
    tenant_id: Optional[str] = Field(default=None, description="Specific tenant ID whose DLQ jobs to replay")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    setup_telemetry("webhook-api")
    await db.connect()
    await redis_manager.connect()
    try:
        await db.apply_migrations("migrations/001_initial_schema.sql")
    except Exception as e:
        print(f"Warning applying migrations: {e}")
    yield
    # Shutdown
    await redis_manager.disconnect()
    await db.disconnect()

app = FastAPI(
    title="Distributed Task Scheduler & Webhook Dispatcher API",
    version="1.0.0",
    lifespan=lifespan,
)

if settings.enable_tracing:
    FastAPIInstrumentor.instrument_app(app)

@app.get("/")
async def root():
    return {
        "service": "Distributed Task Scheduler & Webhook Dispatcher API",
        "status": "running",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics",
        "endpoints": {
            "enqueue_job": "POST /jobs",
            "get_job": "GET /jobs/{id}",
            "replay_dlq": "POST /admin/dlq/replay",
        },
    }

@app.post("/jobs")
async def enqueue_job(req: EnqueueJobRequest, response: Response):
    with tracer.start_as_current_span("api.enqueue_job") as span:
        span.set_attribute("tenant.id", req.tenant_id)
        span.set_attribute("target.url", req.target_url)
        span.set_attribute("idempotency_key", req.idempotency_key)

        trace_ctx = inject_trace_context()

        job, created = await db.enqueue_job(
            tenant_id=req.tenant_id,
            target_url=req.target_url,
            payload=req.payload,
            idempotency_key=req.idempotency_key,
            run_at=req.run_at,
            cron_expr=req.cron_expr,
            max_attempts=req.max_attempts or 5,
            trace_context=trace_ctx,
        )

        if created:
            response.status_code = status.HTTP_201_CREATED
        else:
            response.status_code = status.HTTP_200_OK

        return {
            "status": "success",
            "created": created,
            "job": job,
        }

@app.get("/jobs/{job_id}")
async def get_job(job_id: uuid.UUID):
    job = await db.get_job_with_history(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job with ID {job_id} not found")
    return job

@app.post("/admin/dlq/replay")
async def replay_dlq(req: ReplayDLQRequest):
    with tracer.start_as_current_span("api.admin_dlq_replay"):
        count = await db.replay_dlq(job_id=req.job_id, tenant_id=req.tenant_id)
        current_dlq_count = await db.get_dlq_count()
        DLQ_SIZE.set(current_dlq_count)
        return {
            "status": "success",
            "replayed_jobs": count,
            "remaining_dlq_jobs": current_dlq_count,
        }

@app.get("/health")
async def health_check():
    pg_ok = await db.check_health()
    redis_ok = await redis_manager.check_health()

    status_code = status.HTTP_200_OK if (pg_ok and redis_ok) else status.HTTP_503_SERVICE_UNAVAILABLE
    return Response(
        content=f'{{"status":"{"healthy" if (pg_ok and redis_ok) else "unhealthy"}", "postgres": {str(pg_ok).lower()}, "redis": {str(redis_ok).lower()}}}',
        media_type="application/json",
        status_code=status_code,
    )

@app.get("/metrics")
async def metrics():
    # Update current DLQ count metric
    try:
        count = await db.get_dlq_count()
        DLQ_SIZE.set(count)
    except Exception:
        pass

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )

