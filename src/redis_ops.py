import logging
import time
from typing import Any, Optional, Tuple
import redis.asyncio as aioredis

from src.config import settings

logger = logging.getLogger(__name__)

# Lua script for Token Bucket Rate Limiting
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
else
    local elapsed = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + elapsed * refill_rate)
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    local ttl = math.ceil((capacity / refill_rate) * 2) + 60
    redis.call('EXPIRE', key, ttl)
    return {1, tokens, 0}
else
    local needed = requested - tokens
    local wait_time_sec = needed / refill_rate
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    local ttl = math.ceil((capacity / refill_rate) * 2) + 60
    redis.call('EXPIRE', key, ttl)
    return {0, tokens, wait_time_sec}
end
"""

# Lua script for Circuit Breaker Check / Acquire Permission
CB_CHECK_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])

local data = redis.call('HMGET', key, 'state', 'open_until', 'probe_in_flight')
local state = data[1] or 'closed'
local open_until = tonumber(data[2]) or 0
local probe_in_flight = tonumber(data[3]) or 0

if state == 'closed' then
    return {1, 'closed', 0}
elseif state == 'open' then
    if now >= open_until then
        -- Cooldown passed, transition to half-open and permit single probe
        redis.call('HMSET', key, 'state', 'half-open', 'probe_in_flight', 1)
        return {1, 'half-open', 0}
    else
        return {0, 'open', open_until - now}
    end
elseif state == 'half-open' then
    if probe_in_flight == 0 then
        redis.call('HSET', key, 'probe_in_flight', 1)
        return {1, 'half-open', 0}
    else
        return {0, 'half-open', math.max(0, open_until - now)}
    end
else
    return {1, 'closed', 0}
end
"""

# Lua script for Circuit Breaker Success
CB_RECORD_SUCCESS_LUA = """
local key = KEYS[1]

local state = redis.call('HGET', key, 'state') or 'closed'

if state == 'half-open' or state == 'closed' then
    redis.call('HMSET', key, 'state', 'closed', 'failure_count', 0, 'probe_in_flight', 0, 'open_until', 0)
    redis.call('EXPIRE', key, 86400)
    return 'closed'
end
return state
"""

# Lua script for Circuit Breaker Failure
CB_RECORD_FAILURE_LUA = """
local key = KEYS[1]
local threshold = tonumber(ARGV[1])
local cooldown = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'state', 'failure_count')
local state = data[1] or 'closed'
local failure_count = tonumber(data[2]) or 0

if state == 'half-open' then
    -- Probe failed, re-open immediately
    local open_until = now + cooldown
    redis.call('HMSET', key, 'state', 'open', 'open_until', open_until, 'probe_in_flight', 0)
    redis.call('EXPIRE', key, math.ceil(cooldown * 2) + 60)
    return {'open', 0, cooldown}
elseif state == 'closed' then
    failure_count = failure_count + 1
    if failure_count >= threshold then
        local open_until = now + cooldown
        redis.call('HMSET', key, 'state', 'open', 'open_until', open_until, 'failure_count', failure_count, 'probe_in_flight', 0)
        redis.call('EXPIRE', key, math.ceil(cooldown * 2) + 60)
        return {'open', failure_count, cooldown}
    else
        redis.call('HSET', key, 'failure_count', failure_count)
        redis.call('EXPIRE', key, 3600)
        return {'closed', failure_count, 0}
    end
else
    -- Already open, maintain state
    return {'open', failure_count, 0}
end
"""

class RedisManager:
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None
        self._tb_script: Optional[Any] = None
        self._cb_check_script: Optional[Any] = None
        self._cb_success_script: Optional[Any] = None
        self._cb_failure_script: Optional[Any] = None

    async def connect(self):
        if not self.client:
            logger.info(f"Connecting to Redis at {settings.redis_host}:{settings.redis_port}...")
            self.client = aioredis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                password=settings.redis_password,
                db=settings.redis_db,
                decode_responses=True,
            )
            # Register Lua scripts
            self._tb_script = self.client.register_script(TOKEN_BUCKET_LUA)
            self._cb_check_script = self.client.register_script(CB_CHECK_LUA)
            self._cb_success_script = self.client.register_script(CB_RECORD_SUCCESS_LUA)
            self._cb_failure_script = self.client.register_script(CB_RECORD_FAILURE_LUA)
            logger.info("Connected to Redis and registered Lua scripts.")

    async def disconnect(self):
        if self.client:
            logger.info("Closing Redis connection...")
            await self.client.close()
            self.client = None

    async def check_health(self) -> bool:
        if not self.client:
            return False
        try:
            return await self.client.ping()
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False

    async def check_rate_limit(
        self,
        tenant_id: str,
        capacity: Optional[float] = None,
        refill_rate: Optional[float] = None,
        requested: float = 1.0,
    ) -> Tuple[bool, float, float]:
        """
        Evaluates rate limit for tenant using atomic Lua script.
        Returns: (allowed: bool, remaining_tokens: float, wait_time_seconds: float)
        """
        if not self.client or not self._tb_script:
            await self.connect()

        cap = capacity if capacity is not None else settings.rate_limit_capacity
        rate = refill_rate if refill_rate is not None else settings.rate_limit_refill_rate
        now = time.time()
        key = f"ratelimit:{tenant_id}"

        res = await self._tb_script(
            keys=[key],
            args=[cap, rate, now, requested],
        )
        allowed = bool(res[0])
        remaining = float(res[1])
        wait_time = float(res[2])
        return allowed, remaining, wait_time

    async def check_circuit_breaker(self, host: str) -> Tuple[bool, str, float]:
        """
        Checks if request to destination host is permitted.
        Returns: (allowed: bool, state: str, remaining_cooldown_seconds: float)
        """
        if not self.client or not self._cb_check_script:
            await self.connect()

        now = time.time()
        key = f"cb:{host}"

        res = await self._cb_check_script(keys=[key], args=[now])
        allowed = bool(res[0])
        state = str(res[1])
        cooldown_remaining = float(res[2])
        return allowed, state, cooldown_remaining

    async def record_circuit_breaker_success(self, host: str) -> str:
        if not self.client or not self._cb_success_script:
            await self.connect()

        key = f"cb:{host}"
        new_state = await self._cb_success_script(keys=[key])
        return str(new_state)

    async def record_circuit_breaker_failure(
        self,
        host: str,
        threshold: Optional[int] = None,
        cooldown: Optional[float] = None,
    ) -> Tuple[str, int, float]:
        if not self.client or not self._cb_failure_script:
            await self.connect()

        thresh = threshold if threshold is not None else settings.cb_failure_threshold
        cd = cooldown if cooldown is not None else settings.cb_cooldown_seconds
        now = time.time()
        key = f"cb:{host}"

        res = await self._cb_failure_script(keys=[key], args=[thresh, cd, now])
        state = str(res[0])
        failure_count = int(res[1])
        cooldown_remaining = float(res[2])
        return state, failure_count, cooldown_remaining

    async def get_circuit_breaker_state(self, host: str) -> str:
        if not self.client:
            await self.connect()
        state = await self.client.hget(f"cb:{host}", "state")
        return state or "closed"

redis_manager = RedisManager()
