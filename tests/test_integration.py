import asyncio
from datetime import datetime, timezone
import uuid
import pytest
from httpx import AsyncClient, ASGITransport

from src.api import app
from src.config import settings
from src.db import db
from src.redis_ops import redis_manager

@pytest.fixture(scope="module")
async def pg_available():
    try:
        await db.connect()
        ok = await db.check_health()
        return ok
    except Exception:
        return False

@pytest.mark.asyncio
async def test_idempotent_job_creation(pg_available):
    if not pg_available:
        pytest.skip("PostgreSQL is not running locally")

    tenant_id = f"test_tenant_{uuid.uuid4().hex[:6]}"
    idempotency_key = f"idemp_{uuid.uuid4().hex}"

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        payload = {
            "tenant_id": tenant_id,
            "target_url": "http://localhost:8080/webhook",
            "payload": {"hello": "world"},
            "idempotency_key": idempotency_key,
        }

        # 1st creation -> 201 Created
        r1 = await ac.post("/jobs", json=payload)
        assert r1.status_code == 201
        data1 = r1.json()
        assert data1["created"] is True
        job_id = data1["job"]["id"]

        # 2nd creation with same (tenant_id, idempotency_key) -> 200 OK
        r2 = await ac.post("/jobs", json=payload)
        assert r2.status_code == 200
        data2 = r2.json()
        assert data2["created"] is False
        assert data2["job"]["id"] == job_id

@pytest.mark.asyncio
async def test_skip_locked_concurrency(pg_available):
    if not pg_available:
        pytest.skip("PostgreSQL is not running locally")

    tenant_id = f"test_concur_{uuid.uuid4().hex[:6]}"
    # Insert 5 test jobs
    for i in range(5):
        await db.enqueue_job(
            tenant_id=tenant_id,
            target_url="http://localhost:8080/webhook",
            payload={"i": i},
            idempotency_key=f"conc_{i}_{uuid.uuid4().hex}",
        )

    # Worker A leases 3 jobs
    batch_a = await db.lease_jobs_batch(limit=3, visibility_timeout_sec=30)
    assert len(batch_a) <= 3
    ids_a = {j["id"] for j in batch_a}

    # Worker B leases 3 jobs concurrently -> should NOT receive any jobs leased by Worker A
    batch_b = await db.lease_jobs_batch(limit=3, visibility_timeout_sec=30)
    ids_b = {j["id"] for j in batch_b}

    assert ids_a.isdisjoint(ids_b), "SKIP LOCKED violated: Worker B leased jobs already locked by Worker A!"

@pytest.mark.asyncio
async def test_dlq_replay(pg_available):
    if not pg_available:
        pytest.skip("PostgreSQL is not running locally")

    tenant_id = f"test_dlq_{uuid.uuid4().hex[:6]}"
    job, _ = await db.enqueue_job(
        tenant_id=tenant_id,
        target_url="http://localhost:8080/webhook",
        payload={"dlq": True},
        idempotency_key=f"dlq_key_{uuid.uuid4().hex}",
    )
    job_id = uuid.UUID(job["id"])

    # Force job into DLQ
    await db.mark_job_dlq(job_id)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        r = await ac.post("/admin/dlq/replay", json={"job_id": str(job_id)})
        assert r.status_code == 200
        assert r.json()["replayed_jobs"] == 1

        # Check job status is pending with attempts = 0
        job_state = await db.get_job_with_history(job_id)
        assert job_state["status"] == "pending"
        assert job_state["attempts"] == 0

