import redis.asyncio as redis
from redis.exceptions import WatchError

from core.config import settings
from utils.bucket_refil import refill_tokens
from database import get_redis_client
from services.logger import setup_logger

logger = setup_logger()


async def create_tokens_bucket(key: str, mappings: dict, is_global: bool) -> bool:
    try:
        redis_client: redis.Redis = get_redis_client()

        # Use a pipeline to ensure atomicity
        pipe = redis_client.pipeline()

        await pipe.watch(key)

        if await pipe.exists(key):
            await pipe.unwatch()
            return False

        pipe.multi()

        await pipe.hset(name=key, mapping=mappings)

        if not is_global:
            await pipe.expire(name=key, time=settings.IP_BUCKET_TTL)
        await pipe.execute()

        logger.info(f"Created tokens bucket for key: {key} with mappings: {mappings}")

        return True
    except WatchError as e:
        logger.error(f"Watched key modified during transaction: {e}")
        raise e
    finally:
        await pipe.reset()


async def update_tokens_bucket(
    key: str, tokens: int, last_refill: int, is_global: bool
) -> bool:
    redis_client: redis.Redis = get_redis_client()

    now_s, _ = await redis_client.time()

    capacity = (
        settings.GLOBAL_BUCKET_CAPACITY if is_global else settings.IP_BUCKET_CAPACITY
    )
    refill_rate = (
        settings.GLOBAL_BUCKET_REFILL_RATE
        if is_global
        else settings.IP_BUCKET_REFILL_RATE
    )

    refill_interval = (
        settings.GLOBAL_BUCKET_REFILL_INTERVAL
        if is_global
        else settings.IP_BUCKET_REFILL_INTERVAL
    )
    try:
        # Use a pipeline to ensure atomicity
        pipe = redis_client.pipeline()

        await pipe.watch(key)

        new_tokens = refill_tokens(
            tokens=tokens,
            capacity=capacity,
            last_refill_s=last_refill,
            now_s=now_s,
            refill_rate_per_sec=refill_rate / refill_interval,
        )

        pipe.multi()

        if new_tokens < 1:
            await pipe.unwatch()
            return False

        new_tokens -= 1
        await pipe.hset(
            name=key,
            mapping={
                "tokens": int(new_tokens),
                "last_refill": str(now_s),
            },
        )

        if not is_global:
            await pipe.expire(name=key, time=settings.IP_BUCKET_TTL)

        logger.info(
            f"Updated tokens bucket for key: {key}, new tokens: {new_tokens}, last refill: {now_s}"
        )

        await pipe.execute()

        return True

    except WatchError as e:
        logger.error(f"Watched key modified during transaction: {e}")
        raise e
    finally:
        await pipe.reset()
