import hashlib
import hmac
import time
import pytest
from src.dispatcher import (
    get_tenant_secret,
    sign_webhook_payload,
    compute_exponential_backoff_jitter,
    parse_retry_after,
)

def test_hmac_signing_validity():
    secret = "test-secret-key-12345"
    timestamp = 1700000000
    raw_body = '{"event":"order.completed","amount":99.99}'

    signature = sign_webhook_payload(secret, timestamp, raw_body)
    assert signature.startswith("v1=")
    expected_mac = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.{raw_body}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    assert signature == f"v1={expected_mac}"

def test_hmac_signature_tampering():
    secret = "test-secret-key-12345"
    timestamp = 1700000000
    raw_body = '{"event":"order.completed","amount":99.99}'

    sig_original = sign_webhook_payload(secret, timestamp, raw_body)
    sig_tampered_body = sign_webhook_payload(secret, timestamp, '{"event":"order.completed","amount":100.00}')
    sig_tampered_time = sign_webhook_payload(secret, timestamp + 1, raw_body)
    sig_tampered_key = sign_webhook_payload("wrong-secret", timestamp, raw_body)

    assert sig_original != sig_tampered_body
    assert sig_original != sig_tampered_time
    assert sig_original != sig_tampered_key

def test_backoff_and_full_jitter_bounds():
    base = 1.0
    cap = 60.0

    # For attempt 0: temp = min(60, 1.0 * 2^0) = 1.0 -> 0 <= delay <= 1.0
    for _ in range(50):
        delay = compute_exponential_backoff_jitter(0, base_seconds=base, cap_seconds=cap)
        assert 0.0 <= delay <= 1.0

    # For attempt 3: temp = min(60, 1.0 * 2^3) = 8.0 -> 0 <= delay <= 8.0
    for _ in range(50):
        delay = compute_exponential_backoff_jitter(3, base_seconds=base, cap_seconds=cap)
        assert 0.0 <= delay <= 8.0

    # For attempt 10: temp = min(60, 1.0 * 2^10 = 1024) = 60.0 -> 0 <= delay <= 60.0
    for _ in range(50):
        delay = compute_exponential_backoff_jitter(10, base_seconds=base, cap_seconds=cap)
        assert 0.0 <= delay <= 60.0

def test_parse_retry_after():
    assert parse_retry_after("10") == 10.0
    assert parse_retry_after(" 5 ") == 5.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("invalid") is None

