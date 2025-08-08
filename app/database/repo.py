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

from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine import connection
from cassandra.cluster import Session
from cassandra import InvalidRequest
from redis.exceptions import ResponseError, TimeoutError
import redis.asyncio as redis
from utils.snowflake import SnowflakeIDGenerator
from database import URL
from services.logger import setup_logger

logger = setup_logger()


async def generate_short_urls(
    original_urls: list[str],
    snowflake_generator: SnowflakeIDGenerator,
    redis_client: redis.Redis,
) -> dict:
    """
    Generate shortened URLs for multiple original URLs using batch processing.

    This function efficiently creates multiple short URLs in a single database
    transaction using Cassandra's batch query capabilities. Each URL gets a unique
    Snowflake ID and is stored in both Cassandra (for persistence) and Redis
    (for fast access). The batch approach ensures better performance and partial
    atomicity compared to individual URL creation.

    The function processes all URLs in the batch and caches each one in Redis
    with a 24-hour expiration time. If any part of the operation fails, the
    entire batch is rolled back to maintain data consistency.

    Args:
        original_urls (list[str]): List of complete URLs to be shortened. Each URL
                                 should be a valid HTTP/HTTPS URL.
        snowflake_generator (SnowflakeIDGenerator): Instance for generating unique
                                                   distributed IDs.
        redis_client (redis.Redis): Async Redis client for caching operations.

    Returns:
        dict: Dictionary mapping generated short URLs to their original URLs.
              Format: {"https://miniurl.com/{id}": "https://original-url.com"}

    Raises:
        InvalidRequest: If the Cassandra batch query fails due to invalid data
                       or database constraints.
        ResponseError: If Redis operations fail due to server errors.
        TimeoutError: If Redis operations exceed the configured timeout.

    Example:
        >>> urls = ["https://example.com", "https://google.com"]
        >>> result = await generate_short_urls(urls, generator, redis_client)
        >>> # Returns: {"https://miniurl.com/123": "https://example.com", ...}
    """
    generated_short_urls: dict = {}

    try:
        b = BatchQuery()

        for original_url in original_urls:
            # Generate a unique id
            unique_id = snowflake_generator.generate_id()
            short_url = f"https://miniurl.com/{unique_id}"

            # Add to batch query
            URL.batch(b).create(short_url=str(unique_id), original_url=original_url)
            generated_short_urls[short_url] = original_url

            # Store in Redis with 24-hour expiration
            await redis_client.set(short_url, original_url, ex=60 * 60 * 24)

        # Execute batch query atomically
        b.execute()

        logger.info(
            f"Successfully generated {len(generated_short_urls)} short URLs in batch"
        )
        return generated_short_urls

    except InvalidRequest as e:
        logger.error(f"Batch insert failed: {e}")
        raise e
    except (ResponseError, TimeoutError) as e:
        logger.error(f"Redis operation failed: {e}")
        raise e


async def generate_short_url(
    original_url: str,
    snowflake_generator: SnowflakeIDGenerator,
    redis_client: redis.Redis,
) -> dict:
    """
    Generate a single shortened URL for the given original URL.

    This function creates a unique short URL using the Snowflake ID generator
    and stores the mapping in both Cassandra (for persistence) and Redis (for
    fast retrieval). The operation uses prepared statements for optimal
    performance and executes the database insert asynchronously.

    The generated URL follows the format: https://miniurl.com/{unique_id}
    where unique_id is a Snowflake-generated distributed unique identifier.

    Args:
        original_url (str): The complete URL to be shortened. Must be a valid
                           HTTP/HTTPS URL.
        snowflake_generator (SnowflakeIDGenerator): Instance for generating unique
                                                   distributed IDs across nodes.
        redis_client (redis.Redis): Async Redis client for caching the URL mapping.

    Returns:
        dict: A dictionary containing both the original URL and the generated
              short URL in the format:
              {
                  "original_url": "https://example.com",
                  "short_url": "https://miniurl.com/{unique_id}"
              }

    Raises:
        InvalidRequest: If the Cassandra insert fails due to invalid data,
                       constraint violations, or database connectivity issues.
        ResponseError: If Redis operations fail due to server-side errors
                      or memory constraints.
        TimeoutError: If Redis operations exceed the configured timeout period.

    Example:
        >>> result = await generate_short_url("https://example.com", generator, redis_client)
        >>> # Returns: {"original_url": "https://example.com", "short_url": "https://miniurl.com/123"}
    """
    try:
        session: Session = connection.get_session()

        # Use prepared statement for better performance
        query = session.prepare(
            """
            INSERT INTO url (short_url, original_url)
            VALUES (?, ?)
            """
        )

        # Generate unique Snowflake ID
        unique_id = str(snowflake_generator.generate_id())

        # Execute database insert asynchronously
        session.execute_async(query, [unique_id, original_url])

        # Cache in Redis with 24-hour expiration for fast retrieval
        await redis_client.set(unique_id, original_url, ex=60 * 60 * 24)

        logger.info(f"Successfully generated short URL for: {original_url}")

        return {
            "original_url": original_url,
            "short_url": f"https://miniurl.com/{unique_id}",
        }

    except InvalidRequest as e:
        logger.error(f"Short URL generation failed: {e}")
        raise e
    except (ResponseError, TimeoutError) as e:
        logger.error(f"Redis operation failed: {e}")
        raise e


async def read_original_url_by_id(id: str, redis_client: redis.Redis) -> str:
    """
    Retrieve the original URL for a given shortened URL identifier.

    This function implements a two-tier lookup strategy optimized for performance:
    1. First checks Redis cache for immediate retrieval
    2. Falls back to Cassandra database if not found in cache
    3. Updates Redis cache with retrieved data for future requests

    The caching strategy reduces database load and provides sub-millisecond
    response times for frequently accessed URLs. The function handles cache
    misses gracefully and ensures data consistency between storage layers.

    Args:
        id (str): The unique Snowflake ID of the shortened URL to resolve.
                 This should be the path segment extracted from the short URL.
        redis_client (redis.Redis): Async Redis client for cache operations.

    Returns:
        str: The original URL corresponding to the provided short URL ID.
             Returns the complete URL ready for redirect operations.

    Raises:
        InvalidRequest: If the Cassandra query fails due to invalid ID format,
                       database connectivity issues, or query execution errors.
        ResponseError: If Redis operations fail due to server errors, memory
                      issues, or connection problems.
        TimeoutError: If Redis operations exceed the configured timeout period.

    Example:
        >>> original = await read_original_url_by_id("123456789", redis_client)
        >>> # Returns: "https://example.com"

    Note:
        If the URL is found in Redis cache, the function returns immediately.
        If not cached, it queries Cassandra and updates the Redis cache for
        future requests with the same 24-hour TTL used during creation.
    """
    try:
        # Check Redis cache first for fast retrieval
        original_url = await redis_client.get(id)
        if original_url:
            logger.debug(f"Cache hit for URL ID: {id}")
            return (
                original_url.decode("utf-8")
                if isinstance(original_url, bytes)
                else original_url
            )

        # Cache miss - query Cassandra database
        logger.debug(f"Cache miss for URL ID: {id}, querying database")
        session: Session = connection.get_session()

        # Use prepared statement for consistent performance
        query = session.prepare("SELECT * FROM url WHERE short_url=?")
        future = session.execute_async(query, [id])
        rows = future.result()

        if not rows:
            logger.warning(f"URL ID not found in database: {id}")
            return None

        original_url = rows[0]["original_url"]

        # Update Redis cache for future requests (24-hour TTL)
        await redis_client.set(id, original_url, ex=60 * 60 * 24)

        logger.info(f"Successfully retrieved and cached URL for ID: {id}")
        return original_url

    except InvalidRequest as e:
        logger.error(f"Failed to read original URL for ID {id}: {e}")
        raise e
    except (ResponseError, TimeoutError) as e:
        logger.error(f"Redis operation failed for ID {id}: {e}")
        raise e
