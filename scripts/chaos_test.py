import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from typing import Dict, List
import httpx

API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")
RECEIVER_BASE_URL = os.getenv("RECEIVER_URL", "http://localhost:8080")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
# The target URL that the worker calls to reach mock-receiver (inside Docker network: http://mock-receiver:8080)
TARGET_RECEIVER_URL = os.getenv("TARGET_RECEIVER_URL", "http://mock-receiver:8080")

TOTAL_JOBS = int(os.getenv("TOTAL_JOBS", "5000"))
NUM_TENANTS = int(os.getenv("NUM_TENANTS", "20"))
NOISY_TENANTS_COUNT = 2
NOISY_TRAFFIC_FRACTION = 0.40  # 40% of traffic across 2 noisy tenants

async def blast_jobs(client: httpx.AsyncClient) -> List[Dict]:
    print(f"[*] Blasting {TOTAL_JOBS} jobs across {NUM_TENANTS} tenants...")
    print(f"    - 2 Noisy Tenants (tenant_0, tenant_1) receive {int(NOISY_TRAFFIC_FRACTION * 100)}% of traffic.")
    print(f"    - Remaining {NUM_TENANTS - 2} Healthy Tenants share {int((1 - NOISY_TRAFFIC_FRACTION) * 100)}% of traffic.")

    # Distribute jobs
    noisy_jobs_total = int(TOTAL_JOBS * NOISY_TRAFFIC_FRACTION)
    healthy_jobs_total = TOTAL_JOBS - noisy_jobs_total

    jobs_to_send = []

    # Noisy tenants distribution
    for i in range(noisy_jobs_total):
        tenant_id = f"tenant_{i % NOISY_TENANTS_COUNT}"
        jobs_to_send.append(tenant_id)

    # Healthy tenants distribution
    for i in range(healthy_jobs_total):
        tenant_id = f"tenant_{NOISY_TENANTS_COUNT + (i % (NUM_TENANTS - NOISY_TENANTS_COUNT))}"
        jobs_to_send.append(tenant_id)

    # Shuffle slightly so ingestion simulates real burst
    import random
    random.shuffle(jobs_to_send)

    sem = asyncio.Semaphore(100)  # Ingestion concurrency
    created_jobs = []
    failed_ingest = 0

    start_ingest = time.perf_counter()

    async def send_single_job(tenant: str, idx: int):
        nonlocal failed_ingest
        job_key = f"key_{tenant}_{idx}_{uuid.uuid4().hex[:8]}"
        payload = {
            "tenant_id": tenant,
            "target_url": f"{TARGET_RECEIVER_URL}/webhook?tenant_id={tenant}",
            "payload": {"event": "order.created", "tenant_id": tenant, "sequence": idx},
            "idempotency_key": job_key,
            "max_attempts": 5,
        }
        async with sem:
            try:
                resp = await client.post(f"{API_BASE_URL}/jobs", json=payload, timeout=10.0)
                if resp.status_code in (200, 201):
                    created_jobs.append(resp.json())
                else:
                    failed_ingest += 1
            except Exception as e:
                failed_ingest += 1

    tasks = [send_single_job(t, i) for i, t in enumerate(jobs_to_send)]
    await asyncio.gather(*tasks)

    elapsed_ingest = time.perf_counter() - start_ingest
    print(f"[+] Ingestion complete: {len(created_jobs)}/{TOTAL_JOBS} jobs enqueued in {elapsed_ingest:.2f}s ({len(created_jobs)/elapsed_ingest:.1f} jobs/sec).")
    if failed_ingest > 0:
        print(f"[!] Warning: {failed_ingest} jobs failed to enqueue.")

    return created_jobs

async def wait_for_queue_drain(client: httpx.AsyncClient, timeout_sec: int = 600) -> Dict:
    print("[*] Waiting for queue drain (polling delivery progress)...")
    start = time.perf_counter()

    while time.perf_counter() - start < timeout_sec:
        try:
            # Query receiver stats
            resp = await client.get(f"{RECEIVER_BASE_URL}/stats", timeout=5.0)
            stats = resp.json()
            delivered = stats.get("unique_delivered_jobs", 0)

            # Query health/metrics on API
            m_resp = await client.get(f"{API_BASE_URL}/metrics", timeout=5.0)
            metrics_text = m_resp.text

            dlq_count = 0
            for line in metrics_text.splitlines():
                if line.startswith("dlq_size"):
                    dlq_count = int(float(line.split()[-1]))
                    break

            processed_total = delivered + dlq_count
            print(f"    -> Progress: Delivered={delivered}, DLQ={dlq_count}, Total={processed_total}/{TOTAL_JOBS} (Elapsed: {time.perf_counter() - start:.1f}s)")

            if processed_total >= TOTAL_JOBS:
                print(f"[+] All {TOTAL_JOBS} jobs processed!")
                return {"delivered": delivered, "dlq": dlq_count, "duration": time.perf_counter() - start}

        except Exception as e:
            print(f"    -> Progress poll error: {e}")

        await asyncio.sleep(2.0)

    raise TimeoutError(f"Queue drain timed out after {timeout_sec} seconds")

async def test_dlq_replay(client: httpx.AsyncClient):
    print("[*] Testing DLQ replay with chaos disabled...")
    # 1. Disable chaos on mock receiver
    await client.post(
        f"{RECEIVER_BASE_URL}/chaos/config",
        json={"enabled": False, "failure_rate": 0.0, "slow_response_rate": 0.0},
        timeout=5.0,
    )

    # 2. Trigger DLQ replay
    replay_resp = await client.post(f"{API_BASE_URL}/admin/dlq/replay", json={}, timeout=10.0)
    replay_data = replay_resp.json()
    replayed = replay_data.get("replayed_jobs", 0)
    print(f"[+] Triggered DLQ replay for {replayed} jobs.")

    # 3. Wait for final drain
    start = time.perf_counter()
    while time.perf_counter() - start < 120:
        s_resp = await client.get(f"{RECEIVER_BASE_URL}/stats", timeout=5.0)
        delivered = s_resp.json().get("unique_delivered_jobs", 0)
        if delivered >= TOTAL_JOBS:
            print(f"[+] DLQ replay successful! All {delivered}/{TOTAL_JOBS} jobs successfully delivered!")
            return True
        await asyncio.sleep(2.0)

    print("[!] DLQ replay drain incomplete before timeout.")
    return False

def calculate_metrics_summary(client_sync: httpx.Client):
    print("\n" + "=" * 60)
    print("           BENCHMARK RESULTS & METRICS SUMMARY")
    print("=" * 60)
    try:
        resp = client_sync.get(f"{API_BASE_URL}/metrics", timeout=5.0)
        metrics = resp.text

        # Extract histograms
        queue_lags = []
        delivery_latencies = []

        for line in metrics.splitlines():
            if line.startswith("queue_lag_seconds_bucket"):
                queue_lags.append(line)
            elif line.startswith("delivery_latency_seconds_bucket"):
                delivery_latencies.append(line)

        print("[Metrics Scrape]")
        print(f"Queue lag buckets recorded: {len(queue_lags)}")
        print(f"Delivery latency buckets recorded: {len(delivery_latencies)}")

    except Exception as e:
        print(f"Could not scrape metrics from API: {e}")

    # Query Prometheus if available
    try:
        p99_resp = client_sync.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": "histogram_quantile(0.99, sum(rate(delivery_latency_seconds_bucket[5m])) by (le))"},
            timeout=5.0,
        )
        if p99_resp.status_code == 200:
            result = p99_resp.json().get("data", {}).get("result", [])
            if result:
                p99_val = result[0]["value"][1]
                print(f"Prometheus p99 Outbound Delivery Latency: {float(p99_val)*1000:.2f} ms")
    except Exception:
        pass
    print("=" * 60 + "\n")

async def main():
    parser = argparse.ArgumentParser(description="Chaos & Load Test Benchmark")
    parser.add_argument("--kill-worker-pid", type=int, default=None, help="Process PID of a worker to kill mid-flight")
    parser.add_argument("--skip-kill", action="store_true", help="Skip worker SIGKILL step")
    parser.add_argument("--timeout", type=int, default=600, help="Max seconds to wait for queue drain (default: 600s)")
    args = parser.parse_args()

    async with httpx.AsyncClient() as client:
        # Reset stats
        await client.post(f"{RECEIVER_BASE_URL}/stats/reset", timeout=5.0)
        # Enable chaos (30% failures)
        await client.post(
            f"{RECEIVER_BASE_URL}/chaos/config",
            json={"enabled": True, "failure_rate": 0.30, "slow_response_rate": 0.05},
            timeout=5.0,
        )

        # Step 1: Enqueue 5,000 jobs
        t0 = time.perf_counter()
        await blast_jobs(client)

        # Step 2: Mid-flight SIGKILL
        if not args.skip_kill and args.kill_worker_pid:
            print(f"[*] Waiting 5s before issuing SIGKILL to worker PID {args.kill_worker_pid}...")
            await asyncio.sleep(5.0)
            try:
                os.kill(args.kill_worker_pid, signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM)
                print(f"[!] Successfully killed worker PID {args.kill_worker_pid} mid-flight!")
            except Exception as e:
                print(f"[!] Failed to kill worker process: {e}")

        # Step 3: Wait for initial queue drain
        drain_result = await wait_for_queue_drain(client, timeout_sec=args.timeout)
        total_processed = drain_result["delivered"] + drain_result["dlq"]
        print(f"[*] Initial drain complete. Processed: {total_processed}/{TOTAL_JOBS} (Zero Lost Jobs check: {total_processed == TOTAL_JOBS})")
        assert total_processed == TOTAL_JOBS, f"Lost jobs detected! Expected {TOTAL_JOBS}, got {total_processed}"

        # Step 4: Replay DLQ with chaos disabled
        if drain_result["dlq"] > 0:
            await test_dlq_replay(client)

        # Step 5: Metrics summary
        with httpx.Client() as sync_client:
            calculate_metrics_summary(sync_client)

if __name__ == "__main__":
    asyncio.run(main())

