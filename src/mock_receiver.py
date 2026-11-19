import asyncio
import hashlib
import hmac
import logging
import random
import time
from typing import Dict, Set
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from pydantic import BaseModel

from src.config import settings
from src.dispatcher import get_tenant_secret

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [mock-receiver]: %(message)s",
)
logger = logging.getLogger("mock_receiver")

app = FastAPI(title="Mock Chaos-Enabled Webhook Receiver")

# In-memory store for tracking deliveries
received_job_ids: Set[str] = set()
delivery_counts_by_tenant: Dict[str, int] = {}
chaos_enabled: bool = True
failure_rate: float = 0.30  # 30% failure rate
slow_response_rate: float = 0.05  # 5% chance of > 5s timeout

class ChaosConfig(BaseModel):
    enabled: bool
    failure_rate: float = 0.30
    slow_response_rate: float = 0.05

@app.post("/webhook")
async def receive_webhook(
    request: Request,
    x_webhook_id: str = Header(..., alias="X-Webhook-ID"),
    x_webhook_timestamp: str = Header(..., alias="X-Webhook-Timestamp"),
    x_webhook_signature: str = Header(..., alias="X-Webhook-Signature"),
):
    raw_body_bytes = await request.body()
    raw_body_str = raw_body_bytes.decode("utf-8")

    # Determine tenant from URL query or infer / default
    # If client passed tenant_id query param: /webhook?tenant_id=tenant_1
    tenant_id = request.query_params.get("tenant_id")

    # If tenant_id not in query, try parsing from body json
    if not tenant_id:
        try:
            import json
            data = json.loads(raw_body_str)
            tenant_id = data.get("tenant_id") or "tenant_default"
        except Exception:
            tenant_id = "tenant_default"

    # Verify signature
    secret = get_tenant_secret(tenant_id)
    expected_signed_payload = f"{x_webhook_timestamp}.{raw_body_str}".encode("utf-8")
    expected_mac = hmac.new(secret.encode("utf-8"), expected_signed_payload, hashlib.sha256).hexdigest()
    expected_signature = f"v1={expected_mac}"

    if not hmac.compare_digest(x_webhook_signature, expected_signature):
        logger.warning(f"Signature mismatch for job {x_webhook_id}. Expected {expected_signature}, got {x_webhook_signature}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid HMAC signature",
        )

    # Chaos Injection
    if chaos_enabled:
        rnd = random.random()
        # Occasional slow response (> 5s) to trigger client timeout
        if rnd < slow_response_rate:
            logger.info(f"Injecting slow response (5.5s) for job {x_webhook_id}")
            await asyncio.sleep(5.5)
            # return 504 Gateway Timeout or success after timeout
            return Response(content='{"status":"slow_timeout"}', status_code=504, media_type="application/json")

        # 30% random 500 / 429 failures
        if rnd < failure_rate:
            failure_code = random.choice([500, 502, 503, 429])
            headers = {}
            if failure_code == 429:
                headers["Retry-After"] = str(random.choice([1, 2]))
            logger.info(f"Injecting chaos status {failure_code} for job {x_webhook_id}")
            return Response(
                content=f'{{"status":"chaos_injected_error", "code":{failure_code}}}',
                status_code=failure_code,
                headers=headers,
                media_type="application/json",
            )

    # Success delivery
    received_job_ids.add(x_webhook_id)
    delivery_counts_by_tenant[tenant_id] = delivery_counts_by_tenant.get(tenant_id, 0) + 1

    return {
        "status": "delivered",
        "job_id": x_webhook_id,
        "tenant_id": tenant_id,
        "received_at": time.time(),
    }

@app.get("/stats")
async def get_stats():
    return {
        "unique_delivered_jobs": len(received_job_ids),
        "total_deliveries_by_tenant": delivery_counts_by_tenant,
        "chaos_enabled": chaos_enabled,
        "failure_rate": failure_rate,
        "slow_response_rate": slow_response_rate,
    }

@app.post("/stats/reset")
async def reset_stats():
    received_job_ids.clear()
    delivery_counts_by_tenant.clear()
    return {"status": "reset", "unique_delivered_jobs": 0}

@app.post("/chaos/config")
async def configure_chaos(config: ChaosConfig):
    global chaos_enabled, failure_rate, slow_response_rate
    chaos_enabled = config.enabled
    failure_rate = config.failure_rate
    slow_response_rate = config.slow_response_rate
    return {
        "status": "updated",
        "chaos_enabled": chaos_enabled,
        "failure_rate": failure_rate,
        "slow_response_rate": slow_response_rate,
    }

@app.get("/health")
async def health():
    return {"status": "ok"}

