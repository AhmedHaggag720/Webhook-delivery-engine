import os
from pydantic import BaseModel, Field

class Settings(BaseModel):
    # PostgreSQL settings
    pg_host: str = Field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    pg_port: int = Field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    pg_db: str = Field(default_factory=lambda: os.getenv("POSTGRES_DB", "webhook_engine"))
    pg_user: str = Field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    pg_password: str = Field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres"))
    pg_min_pool: int = Field(default_factory=lambda: int(os.getenv("POSTGRES_MIN_POOL", "5")))
    pg_max_pool: int = Field(default_factory=lambda: int(os.getenv("POSTGRES_MAX_POOL", "25")))

    # Redis settings
    redis_host: str = Field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    redis_port: int = Field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    redis_password: str | None = Field(default_factory=lambda: os.getenv("REDIS_PASSWORD", None))
    redis_db: int = Field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))

    # Worker settings
    worker_concurrency: int = Field(default_factory=lambda: int(os.getenv("WORKER_CONCURRENCY", "20")))
    batch_size: int = Field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "10")))
    poll_interval_ms: int = Field(default_factory=lambda: int(os.getenv("POLL_INTERVAL_MS", "100")))
    visibility_timeout_seconds: int = Field(default_factory=lambda: int(os.getenv("VISIBILITY_TIMEOUT_SECONDS", "30")))

    # Rate limiting defaults
    rate_limit_capacity: float = Field(default_factory=lambda: float(os.getenv("RATE_LIMIT_CAPACITY", "20.0")))
    rate_limit_refill_rate: float = Field(default_factory=lambda: float(os.getenv("RATE_LIMIT_REFILL_RATE", "10.0")))

    # Circuit breaker defaults
    cb_failure_threshold: int = Field(default_factory=lambda: int(os.getenv("CB_FAILURE_THRESHOLD", "5")))
    cb_cooldown_seconds: float = Field(default_factory=lambda: float(os.getenv("CB_COOLDOWN_SECONDS", "30.0")))

    # HTTP & Delivery settings
    http_connect_timeout: float = Field(default_factory=lambda: float(os.getenv("HTTP_CONNECT_TIMEOUT", "2.0")))
    http_total_timeout: float = Field(default_factory=lambda: float(os.getenv("HTTP_TOTAL_TIMEOUT", "5.0")))
    backoff_base_seconds: float = Field(default_factory=lambda: float(os.getenv("BACKOFF_BASE_SECONDS", "1.0")))
    backoff_cap_seconds: float = Field(default_factory=lambda: float(os.getenv("BACKOFF_CAP_SECONDS", "60.0")))
    tenant_secret_salt: str = Field(default_factory=lambda: os.getenv("TENANT_SECRET_SALT", "whsec_default_signing_key"))

    # Observability
    otlp_endpoint: str = Field(default_factory=lambda: os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"))
    enable_tracing: bool = Field(default_factory=lambda: os.getenv("ENABLE_TRACING", "false").lower() in ("true", "1", "yes"))

    @property
    def postgres_dsn(self) -> str:
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"

settings = Settings()

