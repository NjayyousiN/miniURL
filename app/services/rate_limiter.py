from fastapi import HTTPException, Request, status
import redis.asyncio as redis

from core.config import settings
from database import get_redis_client
from services.tokens_bucket import create_tokens_bucket, update_tokens_bucket


async def ip_rate_limit_checker(request: Request):
    """Check and enforce a rate limit for each client IP address.

    This function creates a token bucket for each IP address if one does not already
    exist. It then checks if the bucket has enough tokens to allow the request. If
    not, it raises a 429 Too Many Requests error.

    Args:
        request (Request): The incoming request object.
    """
    redis_client: redis.Redis = get_redis_client()
    now_s, _ = await redis_client.time()
    client_ip = request.client.host

    if not await create_tokens_bucket(
        key=f"ip:{client_ip}",
        mappings={
            "tokens": settings.IP_BUCKET_CAPACITY,
            "last_refill": str(now_s),
        },
        is_global=False,
    ):
        client_bucket = await redis_client.hgetall(f"ip:{client_ip}")
        tokens = int(client_bucket.get("tokens"))
        last_refill = int(client_bucket.get("last_refill"))

        if not await update_tokens_bucket(
            key=f"ip:{client_ip}",
            tokens=tokens,
            last_refill=last_refill,
            is_global=False,
        ):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    "Rate limit reached. Please wait for a few seconds before sending"
                    " other requests. Your killing my server (:"
                ),
            )
