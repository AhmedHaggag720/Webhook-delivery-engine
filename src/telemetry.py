import logging
from typing import Any, Dict, Optional
from prometheus_client import Counter, Gauge, Histogram, REGISTRY
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from src.config import settings

logger = logging.getLogger(__name__)

# Prometheus Metrics Definition
QUEUE_LAG_SECONDS = Histogram(
    "queue_lag_seconds",
    "Time between job run_at and actual worker processing start time",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

DELIVERY_LATENCY_SECONDS = Histogram(
    "delivery_latency_seconds",
    "Outbound HTTP delivery request latency in seconds",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0),
)

DLQ_SIZE = Gauge(
    "dlq_size",
    "Current count of jobs in status = 'dlq'",
)

RATE_LIMIT_THROTTLES_TOTAL = Counter(
    "rate_limit_throttles_total",
    "Total count of rate limit throttles",
    ["tenant_id"],
)

CIRCUIT_BREAKER_STATE = Gauge(
    "circuit_breaker_state",
    "Circuit breaker state: 0=closed, 1=half-open, 2=open",
    ["host"],
)

DELIVERIES_TOTAL = Counter(
    "deliveries_total",
    "Total outbound webhook delivery attempts",
    ["status"],  # e.g., '2xx', '4xx', '5xx', 'timeout', 'error'
)

tracer: trace.Tracer = trace.get_tracer("webhook-delivery-engine")
propagator = TraceContextTextMapPropagator()

def setup_telemetry(service_name: str = "webhook-delivery-engine"):
    if not settings.enable_tracing:
        logger.info("OpenTelemetry tracing is disabled.")
        return

    try:
        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True))
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        logger.info(f"OpenTelemetry tracing configured for {service_name} at {settings.otlp_endpoint}")
    except Exception as e:
        logger.warning(f"Could not initialize OpenTelemetry exporter: {e}")

def inject_trace_context() -> Dict[str, str]:
    carrier: Dict[str, str] = {}
    propagator.inject(carrier)
    return carrier

def extract_trace_context(carrier: Optional[Any]) -> Optional[trace.Context]:
    if not carrier:
        return None
    if isinstance(carrier, str):
        try:
            import json
            carrier = json.loads(carrier)
        except Exception:
            return None
    if not isinstance(carrier, dict):
        return None
    return propagator.extract(carrier)

def update_cb_metric(host: str, state_str: str):
    state_map = {"closed": 0.0, "half-open": 1.0, "open": 2.0}
    val = state_map.get(state_str.lower(), 0.0)
    CIRCUIT_BREAKER_STATE.labels(host=host).set(val)

