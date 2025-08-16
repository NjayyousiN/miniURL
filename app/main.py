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

from cassandra import InvalidRequest
from cassandra.cqlengine import connection
from fastapi import Body, Depends, FastAPI, HTTPException, Path, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import redis.asyncio as redis
from redis.exceptions import ConnectionError, ResponseError, TimeoutError, WatchError

from core.config import settings
from core.exceptions import ExpiredEntryError, LinkNotActiveYetError, SlugAlreadyExistsError
from database import connect_to_db, connect_to_redis, get_redis_client
from database.repo import (
    generate_short_url,
    generate_short_urls,
    read_original_url_by_id,
)
from database.schema import CreateURL
from services.logger import setup_logger
from services.rate_limiter import ip_rate_limit_checker
from services.tokens_bucket import create_tokens_bucket, update_tokens_bucket
from utils.snowflake import SnowflakeIDGenerator

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

        app.state.redis = get_redis_client()

        # PLEASE REMOVE IT WHEN U DEPLOYYYYYYYYYYYYYYYYYYYYYY
        await app.state.redis.flushall()
        # Use a single time source
        now_s, _ = await app.state.redis.time()

        # Initialize global API service request limiter
        await create_tokens_bucket(
            key=settings.GLOBAL_BUCKET_NAME,
            mappings={
                "tokens": settings.GLOBAL_BUCKET_CAPACITY,
                "last_refill": str(now_s),
            },
            is_global=True,
        )

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
    except WatchError as e:
        logger.warning(
            "Global bucket initialization failed, watched key is changed: %s", e
        )


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def global_rate_limiter(request: Request, call_next):
    """Middleware to enforce a global rate limit for all incoming requests.

    This middleware checks and updates a global token bucket stored in Redis to prevent
    the server from being overwhelmed by too many requests. If the rate limit is

    exceeded, it returns a 429 Too Many Requests error.

    Args:
        request (Request): The incoming request object.
        call_next (Callable): The next middleware or endpoint in the request chain.

    Returns:
        Response: The response from the next middleware or endpoint, or a 429 error.
    """
    redis_client: redis.Redis = app.state.redis
    global_bucket = await redis_client.hgetall(settings.GLOBAL_BUCKET_NAME)

    if not global_bucket:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Rate Limiter not configured.",
        )

    now_s, _ = await redis_client.time()
    last_refill = int(global_bucket.get("last_refill"))

    if (now_s - last_refill) >= 1:
        is_updated = await update_tokens_bucket(
            key=settings.GLOBAL_BUCKET_NAME,
            tokens=int(global_bucket["tokens"]),
            last_refill=int(global_bucket["last_refill"]),
            is_global=True,
        )

        if not is_updated:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    "Rate limit reached. Please wait for a few seconds before sending"
                    " other requests. Your killing my server (:"
                ),
            )

    response = await call_next(request)
    return response


@app.post(
    "/shorten",
    status_code=status.HTTP_201_CREATED,
    response_model=dict,
    summary="Create a shortened URL",
    dependencies=[Depends(ip_rate_limit_checker)],
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
    request: Request,
    url_data: CreateURL,
    redis_client: redis.Redis = Depends(get_redis_client),
):
    """Create a shortened URL for a single original URL.

    This endpoint processes a single URL and returns a shortened version.

    Args:
        url_data (CreateURL): The request body containing the original URL.
        redis_client (redis.Redis): Redis client instance for caching.

    Returns:
        dict: A dictionary containing the original URL and the generated short URL.
    """
    try:
        return await generate_short_url(
            url_data,
            snowflake_generator,
            redis_client,
        )
    except (InvalidRequest, ResponseError, TimeoutError) as e:
        logger.error("Short URL generation failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Short URL generation failed",
        )
    except SlugAlreadyExistsError as e:
        logger.error("Slug already exists: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except ValueError as e:
        logger.error("Invalid expiry date provided: %s", e)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Expiry date must be in the future.",
        )


@app.post(
    "/shorten-batch",
    status_code=status.HTTP_201_CREATED,
    response_model=dict,
    summary="Create shortened URLs in batch",
    dependencies=[Depends(ip_rate_limit_checker)],
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
    original_urls: list[dict] = Body(
        ...,
        description=(
            "List of original URLs to be shortened along with optional expiry dates"
            " and one-time use flags"
        ),
        example=[
            {
                "original_url": "https://example.com",
                "expiry_date": "2023-12-31T23:59:59",
                "start_date": "2023-12-31T23:59:59",
                "slug": "example",
            },
            {
                "original_url": "https://another-example.com",
                "expiry_date": "2024-01-15T12:00:00",
                "start_date": "2023-12-31T23:59:59",
                "slug": "another-example",
            },
        ],
    ),
    redis_client: redis.Redis = Depends(get_redis_client),
):
    """Create shortened URLs for multiple original URLs in a single batch operation.

    Args:
        original_urls (list[dict]): A list of dicts, each containing the URL data.
        redis_client (redis.Redis): Redis client instance for caching.

    Returns:
        dict: A dictionary mapping each generated short URL to its original URL.
    """
    try:
        return await generate_short_urls(
            original_urls, snowflake_generator, redis_client
        )
    except (InvalidRequest, ResponseError, TimeoutError) as e:
        logger.error("Batch URL generation failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch URL generation failed",
        )
    except ValueError as e:
        logger.error("Invalid expiry date provided: %s", e)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Expiry date must be in the future.",
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
    short_id: str = Path(
        ...,
        description="The unique identifier of the shortened URL (Snowflake ID)",
        example="123456789012345678",
    ),
    redis_client: redis.Redis = Depends(get_redis_client),
):
    """Retrieve and redirect to the original URL for a given shortened URL identifier.

    Args:
        short_id (str): The unique ID representing the shortened URL.
        redis_client (redis.Redis): Redis client instance for caching.

    Returns:
        RedirectResponse: HTTP 307 redirect to the original URL.
    """
    try:
        original_url = await read_original_url_by_id(short_id, redis_client)
        if original_url:
            return RedirectResponse(url=original_url)
        raise HTTPException(status_code=404, detail="Short URL not found")
    except (InvalidRequest, ResponseError, TimeoutError) as e:
        logger.error("Error retrieving original URL: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving original URL",
        )
    except LinkNotActiveYetError as e:
        logger.warning("Link not active: %s", e)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e),
        )
    except ExpiredEntryError as e:
        logger.warning("Link expired: %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
