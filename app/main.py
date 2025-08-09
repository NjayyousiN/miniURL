"""
FastAPI URL Shortener Service

A high-performance URL shortening service built with FastAPI, Cassandra, and Redis.
This service provides endpoints to create shortened URLs from long URLs and redirect
users from short URLs back to their original destinations.

Key Features:
    - Single URL shortening with unique Snowflake ID generation
    - Batch URL shortening for multiple URLs at once
    - Fast URL resolution with Redis caching
    - Persistent storage using Cassandra database
    - CORS middleware support for cross-origin requests
    - Comprehensive error handling and logging

Architecture:
    - FastAPI for the web framework and automatic API documentation
    - Cassandra for persistent URL storage and scalability
    - Redis for high-performance caching and session management
    - Snowflake ID generator for unique, distributed ID creation
    - Structured logging for monitoring and debugging

Dependencies:
    - FastAPI: Modern web framework for building APIs
    - Cassandra: Distributed NoSQL database for URL storage
    - Redis: In-memory data store for caching
    - Custom utilities: Snowflake ID generator, database repositories
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status, Depends, Path, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from cassandra import InvalidRequest
from cassandra.cqlengine import connection
from redis.exceptions import ConnectionError, ResponseError, TimeoutError
import redis.asyncio as redis

from services.logger import setup_logger
from core.config import settings
from utils.snowflake import SnowflakeIDGenerator
from database import connect_to_db, connect_to_redis, get_redis_client
from database.repo import (
    generate_short_url,
    generate_short_urls,
    read_original_url_by_id,
)

# Setup logger
logger = setup_logger()

# Initialize Snowflake ID generator
node_id = int(settings.NODE_ID)
epoch = int(settings.EPOCH)
snowflake_generator = SnowflakeIDGenerator(node_id, epoch)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan event handler to initialize and cleanup database connections.

    This context manager ensures proper initialization of both Cassandra and Redis
    connections when the application starts, and graceful shutdown when the application
    terminates. It handles connection errors and raises appropriate HTTP exceptions
    if initialization fails.

    Args:
        app (FastAPI): The FastAPI application instance

    Raises:
        HTTPException: 503 Service Unavailable if database or Redis initialization fails

    Yields:
        None: Control to the application during its lifetime
    """
    logger.info("Starting application and initializing database...")

    try:
        connect_to_db()
        connect_to_redis()

        yield

        logger.info("Application is shutting down.")

        connection.get_session().shutdown()
        connection.get_session().cluster.shutdown()
        await get_redis_client().aclose()

    except RuntimeError as e:
        logger.error(f"Database initialization failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database initialization failed",
        )

    except ConnectionError as e:
        logger.error(f"Redis connection failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis connection failed",
        )

    except FileNotFoundError as e:
        logger.error(f"Secure connect bundle not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Secure connect bundle not found",
        )


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for CORS (For development purposes only)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)


@app.post(
    "/shorten",
    status_code=status.HTTP_201_CREATED,
    response_model=dict,
    summary="Create a shortened URL",
    description="""
    Generate a unique shortened URL for a given original URL.
    
    This endpoint creates a new short URL using a Snowflake ID generator to ensure
    uniqueness across distributed systems. The mapping between the original URL and
    the shortened version is stored in both Cassandra (for persistence) and Redis
    (for fast retrieval). The generated short URL can be used to redirect users
    back to the original destination.
    
    The service automatically handles URL validation, duplicate detection, and
    error recovery. If the same original URL is shortened multiple times, the
    system may return the existing short URL or create a new one based on the
    implementation strategy.
    """,
    responses={
        201: {
            "description": "Shortened URL created successfully",
            "content": {
                "application/json": {
                    "example": {
                        "original_url": "https://example.com",
                        "short_url": "https://miniurl.com/1234567890",
                    }
                }
            },
        },
        500: {
            "description": "Internal server error during URL generation",
            "content": {
                "application/json": {
                    "example": {"detail": "Short URL generation failed"}
                }
            },
        },
    },
)
async def create_url(
    original_url: str = Query(
        ..., description="Original URL to be shortened", example="https://example.com"
    ),
    redis_client: redis.Redis = Depends(get_redis_client),
):
    """
    Create a shortened URL for a single original URL.

    This endpoint processes a single URL and returns a shortened version that can be
    used for sharing, tracking, or space-saving purposes. The original URL must be
    a valid HTTP/HTTPS URL.

    Args:
        original_url (str): The complete URL to be shortened. Must be a valid HTTP/HTTPS URL.
        redis_client (redis.Redis): Redis client instance for caching (injected dependency).

    Returns:
        dict: A dictionary containing both the original URL and the generated short URL.

    Raises:
        HTTPException: 500 for database/Redis errors.
    """
    try:
        generated_short_url = await generate_short_url(
            original_url, snowflake_generator, redis_client
        )

        return generated_short_url

    except (InvalidRequest, ResponseError, TimeoutError) as e:
        logger.error(f"Short URL generation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Short URL generation failed",
        )


@app.post(
    "/shorten-batch",
    status_code=status.HTTP_201_CREATED,
    response_model=dict,
    summary="Create shortened URLs in batch",
    description="""
    Generate shortened URLs for multiple original URLs in a single request.
    
    This endpoint is optimized for bulk URL shortening operations, making it ideal
    for applications that need to process multiple URLs efficiently. The batch
    processing reduces network overhead and improves performance compared to
    multiple individual requests.
    
    Each URL in the batch is processed independently, so if some URLs fail to
    generate short versions, the successful ones will still be returned. The
    response maps each generated short URL to its corresponding original URL.
    
    The batch size may be limited by server configuration to prevent resource
    exhaustion and ensure reasonable response times.
    """,
    responses={
        201: {
            "description": "Batch of shortened URLs created successfully",
            "content": {
                "application/json": {
                    "example": {
                        "https://miniurl.com/1234567890": "https://example.com",
                        "https://miniurl.com/0987654321": "https://another-example.com",
                    }
                }
            },
        },
        500: {
            "description": "Internal server error during batch processing",
            "content": {
                "application/json": {
                    "example": {"detail": "Batch URL generation failed"}
                }
            },
        },
    },
)
async def create_urls(
    original_urls: list[str] = Body(
        ...,
        description="List of original URLs to be shortened",
        example=["https://example.com", "https://example2.com", "https://example3.com"],
    ),
    redis_client: redis.Redis = Depends(get_redis_client),
):
    """
    Create shortened URLs for multiple original URLs in a single batch operation.

    This endpoint efficiently processes multiple URLs simultaneously, reducing the
    number of API calls required for bulk operations. Each URL is validated and
    processed independently, ensuring that failures in individual URLs don't
    affect the processing of others in the batch.

    Args:
        original_urls (list[str]): A list of complete URLs to be shortened. Each URL
                                 must be a valid HTTP/HTTPS URL.
        redis_client (redis.Redis): Redis client instance for caching (injected dependency).

    Returns:
        dict: A dictionary mapping each generated short URL to its original URL.
              Only successfully processed URLs are included in the response.

    Raises:
        HTTPException: 500 for database/Redis errors.
    """
    try:
        generated_short_urls = await generate_short_urls(
            original_urls, snowflake_generator, redis_client
        )

        return generated_short_urls

    except (InvalidRequest, ResponseError, TimeoutError) as e:
        logger.error(f"Batch URL generation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch URL generation failed",
        )


@app.get(
    "/{id}",
    summary="Redirect to original URL",
    description="""
    Resolve a shortened URL ID and redirect the user to the original destination.
    
    This endpoint serves as the core redirect functionality of the URL shortener.
    When a user visits a short URL, this endpoint looks up the corresponding
    original URL and immediately redirects the browser to that destination.
    
    The lookup process first checks Redis cache for fast retrieval, and falls
    back to the Cassandra database if the entry is not cached. This two-tier
    approach ensures both speed and reliability.
    
    The redirect uses HTTP 307 (Temporary Redirect) status code, which preserves
    the original HTTP method and is appropriate for URL shortening services.
    """,
    responses={
        307: {
            "description": "Temporary redirect to the original URL",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Client will be redirected to the original URL: https://example.com"
                    }
                }
            },
        },
        404: {
            "description": "Short URL not found",
            "content": {
                "application/json": {"example": {"detail": "Short URL not found"}}
            },
        },
        500: {
            "description": "Internal server error during URL retrieval",
            "content": {
                "application/json": {
                    "example": {"detail": "Error retrieving original URL"}
                }
            },
        },
    },
)
async def get_url(
    id: str = Path(
        ...,
        description="The unique identifier of the shortened URL (Snowflake ID)",
        example="123456789012345678",
    ),
    redis_client: redis.Redis = Depends(get_redis_client),
):
    """
    Retrieve and redirect to the original URL for a given shortened URL identifier.

    This endpoint handles the resolution of short URLs back to their original
    destinations. It implements a fast lookup strategy using Redis cache with
    Cassandra fallback, ensuring both performance and data persistence.

    The function performs an HTTP 307 temporary redirect, which maintains the
    original request method and indicates to browsers and crawlers that the
    redirect is not permanent.

    Args:
        id (str): The unique Snowflake ID representing the shortened URL.
                 This should be the path segment from the short URL.
        redis_client (redis.Redis): Redis client instance for caching (injected dependency).

    Returns:
        RedirectResponse: HTTP 307 redirect to the original URL.

    Raises:
        HTTPException: 404 if the short URL is not found, 500 for database/Redis errors.
    """
    try:
        original_url = await read_original_url_by_id(id, redis_client)

        if original_url:
            return RedirectResponse(url=original_url)
        raise HTTPException(status_code=404, detail="Short URL not found")

    except (InvalidRequest, ResponseError, TimeoutError) as e:
        logger.error(f"Error retrieving original URL: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving original URL",
        )
