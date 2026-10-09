import asyncio
from weakref import WeakValueDictionary

from redis.asyncio import Redis

from .config import settings

redis_client: Redis | None = None


async def init_redis() -> Redis:
    global redis_client
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    return redis_client


async def close_redis() -> None:
    global redis_client
    if redis_client is not None:
        await redis_client.aclose()
        redis_client = None


def get_redis() -> Redis:
    if redis_client is None:
        raise RuntimeError("Redis is not initialized")
    return redis_client


# Only active callers retain locks; completed pair lookups do not grow memory.
_cache_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()


def cache_lock(key: str) -> asyncio.Lock:
    lock = _cache_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _cache_locks[key] = lock
    return lock
