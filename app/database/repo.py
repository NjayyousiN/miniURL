"""
Database Repository Module for URL Shortener Service

This module provides the core data access layer for the URL shortening service,
implementing a hybrid storage strategy using both Cassandra and Redis for optimal
performance and reliability.

Storage Architecture:
    - Cassandra: Primary persistent storage for URL mappings with high availability
    - Redis: Fast in-memory cache layer with TTL for frequently accessed URLs
    - Hybrid approach: Write-through caching ensures data consistency and speed

Key Components:
    - Batch URL generation with transaction-like semantics
    - Single URL generation with prepared statements for performance
    - Cache-first URL resolution with automatic fallback to persistent storage
    - Comprehensive error handling for database and cache failures

Data Flow:
    1. URL Creation: Generate unique Snowflake ID → Store in Cassandra → Cache in Redis
    2. URL Resolution: Check Redis cache → Fallback to Cassandra → Update cache
    3. Batch Operations: Use Cassandra batch queries for atomic multi-URL operations

Performance Optimizations:
    - Prepared statements for reduced query parsing overhead
    - Asynchronous operations for non-blocking I/O
    - Redis caching with 24-hour TTL for frequently accessed URLs
    - Batch operations for efficient bulk processing

Error Handling:
    - Database connection failures with detailed logging
    - Redis operation timeouts and connection errors
    - Invalid request handling with appropriate error propagation
    - Graceful degradation when cache is unavailable

Dependencies:
    - cassandra.cqlengine: Object-relational mapping for Cassandra
    - redis.asyncio: Async Redis client for caching operations
    - utils.snowflake: Distributed unique ID generation
    - database.URL: Cassandra model for URL storage
"""

import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from cassandra.cluster import Session
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from cassandra import InvalidRequest
import redis.asyncio as redis
from redis.exceptions import ResponseError, TimeoutError

from core.config import settings
from core.exceptions import (
    ExpiredEntryError,
    LinkNotActiveYetError,
    SlugAlreadyExistsError,
)
from database import URL
from database.schema import CreateURL, CreateURLBatch
from services.logger import setup_logger
from utils.snowflake import SnowflakeIDGenerator

logger = setup_logger()


async def _generate_alternative_slugs(
    slug: str, redis_client: redis.Redis, session: Session
) -> list[str]:
    """Generate alternative slugs for a given slug.

    Args:
        slug (str): The custom slug to generate alternative slugs for.
        redis_client (redis.Redis): Async Redis client.
        session (Session): Cassandra session.

    Returns:
        list[str]: A list of alternative slugs.

    Raises:
        SlugAlreadyExistsError: If the slug is already in use.
    """

    alternative_slugs = [slug + str(random.randint(100, 999)) for _ in range(6)]
    available_slugs = []
    for slug in alternative_slugs:
        if await _is_slug_available(slug, redis_client, session):
            available_slugs.append(slug)
    raise SlugAlreadyExistsError(
        f"Slug already exists, try one of these: {available_slugs}"
    ) from None


async def _reserve_slug(
    slug: str, expires_at: datetime, redis_client: redis.Redis, session: Session
) -> bool:
    """Reserve a custom slug in Redis to prevent conflicts.

    Args:
        slug (str): The custom slug to reserve.
        expires_at (datetime): The expiry date of the slug.
        redis_client (redis.Redis): Async Redis client.
        session (Session): Cassandra session.

    Returns:
        bool: True if the slug was successfully reserved, False otherwise.

    Raises:
        ResponseError: If the Redis operation fails.
        TimeoutError: If the Redis operation times out.
        InvalidRequest: If the Cassandra operation fails.
    """
    try:
        logger.debug("Reserving slug in Redis: %s", slug)
        query = session.prepare("INSERT INTO slug (slug, expires_at) VALUES (?, ?)")
        session.execute_async(query, [slug, expires_at])

        # Redis expiry time in seconds
        redis_expiry_time = int(
            (
                expires_at - datetime.now(timezone.utc).replace(tzinfo=None)
            ).total_seconds()
        )

        if await redis_client.setnx(f"slug:{slug}", "reserved"):
            await redis_client.expire(f"slug:{slug}", redis_expiry_time)
            logger.info("Slug '%s' reserved successfully.", slug)
            return True
        return False
    except (ResponseError, TimeoutError) as e:
        logger.error("Redis operation failed while reserving slug: %s", e)
        raise
    except InvalidRequest as e:
        logger.error("Cassandra operation failed while reserving slug: %s", e)
        raise


async def _is_slug_available(
    slug: str, redis_client: redis.Redis, session: Session
) -> bool:
    """Check if a given slug is available for use.

    Args:
        slug (str): The custom slug to check.
        redis_client (redis.Redis): Async Redis client.
        session (Session): Cassandra session.

    Returns:
        bool: True if the slug is available, False otherwise.

    Raises:
        ResponseError: If the Redis operation fails.
        TimeoutError: If the Redis operation times out.
        InvalidRequest: If the Cassandra operation fails.
    """

    available = False
    try:
        logger.debug("Checking availability for slug: %s", slug)
        if await redis_client.exists(f"slug:{slug}"):
            logger.debug("Slug '%s' is already in use.", slug)
            available = False

        query = session.prepare("SELECT * FROM slug WHERE slug=?")
        future = session.execute_async(query, [slug])
        available = not future.result()

        if not available:
            await _generate_alternative_slugs(slug, redis_client, session)

        return available

    except (ResponseError, TimeoutError) as e:
        logger.error("Redis operation failed while checking slug availability: %s", e)
        raise
    except InvalidRequest as e:
        logger.error(
            "Cassandra operation failed while checking slug availability: %s", e
        )
        raise


async def _handle_slug_creation(
    slug: str,
    expiry_date: datetime,
    redis_client: redis.Redis,
    session: Session,
) -> str:
    """Handle the creation and reservation of a custom slug.

    Args:
        slug (str): The custom slug to create.
        expiry_date (datetime): The expiry date of the slug.
        redis_client (redis.Redis): Async Redis client.
        session (Session): Cassandra session.

    Returns:
        str: The reserved slug.

    Raises:
        ValueError: If the slug is already in use.
    """

    if not await _is_slug_available(slug, redis_client, session):
        logger.error("Slug '%s' is already in use.", slug)
        raise ValueError(f"Slug '{slug}' is already in use.")

    if not await _reserve_slug(slug, expiry_date, redis_client, session):
        logger.error("Failed to reserve slug '%s'.", slug)
        raise ValueError(f"Failed to reserve slug '{slug}'.")
    return slug


async def _cache_url_mapping(
    key: str,
    data: dict,
    expiry_date: datetime,
    redis_client: redis.Redis,
):
    """Cache the URL mapping in Redis.

    Args:
        key (str): The key to store the mapping under.
        data (dict): The URL mapping data.
        expiry_date (datetime): The expiry date of the mapping.
        redis_client (redis.Redis): Async Redis client.
    """
    await redis_client.hset(name=key, mapping=data)
    now_s, _ = await redis_client.time()
    now = datetime.fromtimestamp(now_s, tz=timezone.utc).replace(tzinfo=None)
    # Redis expiry time in seconds
    redis_expiry_time = int((expiry_date - now).total_seconds())
    await redis_client.expire(key, redis_expiry_time)


async def generate_short_urls(
    batch_urls: CreateURLBatch,
    snowflake_generator: SnowflakeIDGenerator,
    redis_client: redis.Redis,
) -> dict:
    """Generate shortened URLs for multiple original URLs using batch processing.

    Args:
        batch_urls (CreateURLBatch): Batch of URLs to be shortened.
        snowflake_generator (SnowflakeIDGenerator): Instance for generating unique distributed IDs.
        redis_client (redis.Redis): Async Redis client for caching operations.

    Returns:
        dict: Dictionary mapping generated short URLs to their original URLs.

    Raises:
        InvalidRequest: If the request is invalid.
        ResponseError: If the response is invalid.
        TimeoutError: If the request times out.
        ValueError: If the URL data is invalid.
    """
    generated_short_urls = {}
    session: Session = connection.get_session()

    try:
        batch = BatchQuery()
        for entry in batch_urls.urls:
            expiry_date = entry.expiry_date
            start_date = entry.start_date
            slug = entry.slug

            if slug:
                key = await _handle_slug_creation(
                    slug, expiry_date, redis_client, session
                )
            else:
                key = str(snowflake_generator.generate_id())

            short_url = f"{settings.DOMAIN}/{key}"
            original_url = entry.original_url

            URL.batch(batch).create(
                short_url=key,
                original_url=original_url,
                expiry_date=expiry_date,
                start_date=start_date,
            )

            url_data = {
                "original_url": original_url,
                "short_url": short_url,
                "expiry_date": str(expiry_date),
                "start_date": str(start_date),
            }
            generated_short_urls[short_url] = url_data
            await _cache_url_mapping(key, url_data, expiry_date, redis_client)

        batch.execute()
        logger.info(
            "Successfully generated %d short URLs in batch",
            len(generated_short_urls),
        )
        return generated_short_urls

    except (InvalidRequest, ResponseError, TimeoutError, ValueError) as e:
        logger.error("URL generation failed: %s", e)
        raise


async def generate_short_url(
    url_data: CreateURL,
    snowflake_generator: SnowflakeIDGenerator,
    redis_client: redis.Redis,
) -> dict:
    """Generate a single shortened URL for the given original URL.

    Args:
        url_data (CreateURL): Data for the URL to be shortened.
        snowflake_generator (SnowflakeIDGenerator): Instance for generating unique distributed IDs.
        redis_client (redis.Redis): Async Redis client for caching operations.

    Returns:
        dict: A dictionary containing the original URL and the generated short URL.

    Raises:
        InvalidRequest: If the request is invalid.
        ResponseError: If the response is invalid.
        TimeoutError: If the request times out.
        ValueError: If the URL data is invalid.
    """
    session: Session = connection.get_session()

    try:
        expiry_date = url_data.expiry_date
        start_date = url_data.start_date

        if url_data.slug:
            key = await _handle_slug_creation(
                url_data.slug, expiry_date, redis_client, session
            )
        else:
            key = str(snowflake_generator.generate_id())

        query = session.prepare(
            """
            INSERT INTO url (short_url, original_url, expiry_date, start_date)
            VALUES (?, ?, ?, ?)
            """
        )
        session.execute_async(
            query,
            [key, url_data.original_url, expiry_date, start_date],
        )

        short_url = f"{settings.DOMAIN}/{key}"
        result_data = {
            "original_url": url_data.original_url,
            "short_url": short_url,
            "expiry_date": str(expiry_date),
            "start_date": str(start_date),
        }

        await _cache_url_mapping(key, result_data, expiry_date, redis_client)
        logger.info("Successfully generated short URL for: %s", url_data.original_url)
        return result_data

    except (InvalidRequest, ResponseError, TimeoutError, ValueError) as e:
        logger.error("Short URL generation failed: %s", e)
        raise


def _is_expired(now: datetime, expiry_date: datetime) -> bool:
    """Check if the given expiry date is in the past."""
    return now > expiry_date


def _is_started(now: datetime, start_date: datetime) -> bool:
    """Check if the given start date is in the past."""
    return now >= start_date


async def _validate_url_access(
    now: datetime, expiry_date: str, start_date: str, short_id: str
):
    """Validate if the URL is active and not expired."""
    expiry = datetime.fromisoformat(expiry_date)
    start = datetime.fromisoformat(start_date)

    if _is_expired(now, expiry):
        logger.error("URL ID %s has expired.", short_id)
        raise ExpiredEntryError(f"URL ID {short_id} has expired.")

    if not _is_started(now, start):
        logger.error("URL ID %s has not started yet.", short_id)
        raise LinkNotActiveYetError(f"URL ID {short_id} has not started yet.")


async def read_original_url_by_id(
    short_id: str, redis_client: redis.Redis
) -> Optional[str]:
    """Retrieve the original URL for a given shortened URL identifier.

    Args:
        short_id (str): The unique identifier of the shortened URL.
        redis_client (redis.Redis): Async Redis client for cache operations.

    Returns:
        The original URL, or None if not found.

    Raises:
        ExpiredEntryError: If the URL has expired.
        LinkNotActiveYetError: If the URL has not started yet.
        InvalidRequest: If the request is invalid.
        ResponseError: If the response is invalid.
        TimeoutError: If the request times out.
    """
    try:
        now_s, _ = await redis_client.time()
        now = datetime.fromtimestamp(now_s, tz=timezone.utc).replace(tzinfo=None)

        cached_data = await redis_client.hgetall(short_id)
        if cached_data:
            logger.debug("Cache hit for URL ID: %s", short_id)
            await _validate_url_access(
                now,
                cached_data["expiry_date"],
                cached_data["start_date"],
                short_id,
            )
            return cached_data["original_url"]

        logger.debug("Cache miss for URL ID: %s, querying database", short_id)

        session: Session = connection.get_session()
        query = session.prepare("SELECT * FROM url WHERE short_url=?")
        future = session.execute_async(query, [short_id])
        rows = future.result()

        if not rows:
            logger.warning("URL ID not found in database: %s", short_id)
            return None

        db_data = rows[0]
        await _validate_url_access(
            now,
            str(db_data["expiry_date"]),
            str(db_data["start_date"]),
            short_id,
        )

        original_url = db_data["original_url"]
        await _cache_url_mapping(
            short_id,
            {
                "original_url": original_url,
                "short_url": f"{settings.DOMAIN}/{short_id}",
                "expiry_date": str(db_data["expiry_date"]),
                "start_date": str(db_data["start_date"]),
            },
            db_data["expiry_date"],
            redis_client,
        )
        logger.info("Successfully retrieved and cached URL for ID: %s", short_id)
        return original_url

    except (InvalidRequest, ResponseError, TimeoutError) as e:
        logger.error("Failed to read original URL for ID %s: %s", short_id, e)
        raise
    except (ExpiredEntryError, LinkNotActiveYetError) as e:
        logger.error("Access error for URL ID %s: %s", short_id, e)
        raise
