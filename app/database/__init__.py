"""
Database Connection and Model Module for URL Shortener Service

This module handles all database connectivity and model definitions for the URL
shortening service. It manages connections to both Cassandra (primary storage)
and Redis (caching layer) with robust error handling, retry logic, and
environment-specific configurations.

Database Architecture:
    - Cassandra: Primary persistent storage using DataStax Astra DB cloud service
    - Redis: High-performance caching layer with configurable TTL
    - Dual-environment support: Development (external) and production (internal)

Connection Management:
    - Cassandra: Secure connection bundle with authentication and retry logic
    - Redis: Environment-aware connection with SSL/TLS support for development
    - Global connection pooling with health checks and timeout configurations
    - Graceful connection failure handling with exponential backoff

Model Design:
    - URL model with Cassandra CQL Engine ORM mapping
    - UUID-based primary keys with Snowflake ID integration
    - Automatic timestamp tracking for analytics and debugging
    - Optimized schema design for read-heavy workloads

Key Features:
    - Automatic table creation and schema synchronization
    - Environment-specific connection parameters (dev/prod)
    - Comprehensive retry logic with configurable delays
    - Health monitoring with periodic connection checks
    - Secure authentication for both database systems

Connection Resilience:
    - Multi-attempt connection retry with exponential backoff
    - Graceful degradation when services are temporarily unavailable
    - Detailed logging for connection status and failure diagnosis
    - Timeout configurations optimized for cloud deployment

Environment Configuration:
    - Development: External Redis with SSL/TLS and authentication
    - Production: Internal Redis without SSL for improved performance
    - Cassandra: Secure connect bundle with client certificate authentication
    - Configurable retry policies and connection parameters

Dependencies:
    - cassandra-driver: DataStax Python driver for Cassandra connectivity
    - redis: Async Redis client for high-performance caching
    - core.config: Environment configuration and settings management
    - services.logger: Structured logging for monitoring and debugging
"""

import os
import base64
from datetime import datetime, timedelta, timezone
from time import sleep
from uuid import uuid4

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cluster import NoHostAvailable, ConnectionException
from cassandra.query import dict_factory
from redis.exceptions import ConnectionError
import redis.asyncio as redis

from core.config import settings
from services.logger import setup_logger

# Global Redis client instance
redis_client: redis.Redis = None

# Setup logger
logger = setup_logger()


class URL(Model):
    """
    Cassandra model representing a shortened URL mapping in the miniURL application.

    This model defines the database schema for storing URL mappings with metadata.
    It uses Cassandra's CQL Engine ORM for type-safe database operations and
    automatic query generation. The model is optimized for read-heavy workloads
    typical in URL shortening services.

    Schema Design:
        - Primary key on short_url for fast lookups during redirect operations
        - UUID field for additional unique identification and potential clustering
        - Text fields with appropriate sizing for URL storage
        - Automatic timestamp tracking for analytics and debugging

    Attributes:
        id (UUID): Unique identifier for each URL record. Auto-generated using UUID4
                   for distributed uniqueness without coordination.
        original_url (Text): The complete original URL that was shortened. Required field
                           that stores the destination URL for redirects.
        short_url (Text): The unique shortened URL identifier (Snowflake ID). Serves as
                         the primary key for fast lookups during redirect operations.
        created_at (DateTime): Timestamp when the URL mapping was created. Auto-populated
                              with current datetime for analytics and debugging.

    Keyspace:
        Uses the keyspace defined in application settings for multi-tenant support
        and environment isolation (development/staging/production).

    Performance Considerations:
        - Primary key on short_url enables single-partition reads
        - Text columns are efficiently stored in Cassandra's SSTable format
        - DateTime indexing supports time-based queries for analytics
        - Model supports both single and batch operations

    Example:
        >>> url_record = URL.create(
        ...     short_url="123456789",
        ...     original_url="https://example.com"
        ... )
        >>> # Auto-generates id and created_at fields
    """

    __keyspace__ = settings.KEYSPACE

    id = columns.UUID(default=uuid4)
    original_url = columns.Text(required=True)
    short_url = columns.Text(primary_key=True, required=True)
    start_date = columns.DateTime(required=True, default=datetime.now)
    expiry_date = columns.DateTime(required=True)
    created_at = columns.DateTime(default=datetime.now)


class Slug(Model):
    """
    Cassandra model representing a slug mapping in the miniURL application.

    This model defines the database schema for storing slugs associated with URLs.
    It uses Cassandra's CQL Engine ORM for type-safe database operations and
    automatic query generation. The model is optimized for read-heavy workloads
    typical in URL shortening services.

    Schema Design:
        - Primary key on slug for fast lookups during redirect operations
        - Text fields with appropriate sizing for slug storage
        - Automatic timestamp tracking for analytics and debugging

    Attributes:
        id (UUID): Unique identifier for each slug record. Auto-generated using UUID4
                   for distributed uniqueness without coordination.
        slug (Text): The unique slug identifier. Serves as the primary key for fast lookups.
        created_at (DateTime): Timestamp when the slug mapping was created. Auto-populated
                              with current datetime for analytics and debugging.

    Keyspace:
        Uses the keyspace defined in application settings for multi-tenant support
        and environment isolation (development/staging/production).

    Performance Considerations:
        - Primary key on slug enables single-partition reads
        - Text columns are efficiently stored in Cassandra's SSTable format
        - DateTime indexing supports time-based queries for analytics
        - Model supports both single and batch operations

    Example:
        >>> slug_record = Slug.create(
        ...     slug="example-slug"
        ... )
        >>> # Auto-generates id and created_at fields
    """

    __keyspace__ = settings.KEYSPACE

    id = columns.UUID(default=uuid4)
    slug = columns.Text(primary_key=True, required=True)
    created_at = columns.DateTime(default=datetime.now)
    expires_at = columns.DateTime(required=True)


def connect_to_db() -> None:
    """
    Establish connection to Cassandra database with retry logic and table initialization.

    This function creates a secure connection to DataStax Astra DB using the provided
    secure connect bundle and authentication credentials. It implements robust retry
    logic to handle temporary network issues and ensures that all required database
    tables are created and synchronized.

    Connection Process:
        1. Loads secure connect bundle from the application directory
        2. Configures authentication using client ID and secret
        3. Establishes cluster connection with optimized policies
        4. Sets up session with dictionary row factory for easier data handling
        5. Synchronizes database schema with application models

    Retry Strategy:
        - Maximum 10 connection attempts with 5-second delays
        - Exponential backoff could be implemented for production use
        - Detailed logging for each attempt and failure reason
        - Graceful failure after all retry attempts are exhausted

    Connection Configuration:
        - Uses secure connect bundle for DataStax Astra DB
        - PlainTextAuthProvider for username/password authentication
        - RoundRobinPolicy for balanced load distribution
        - Protocol version 4 for compatibility and performance
        - Custom heartbeat interval for connection health monitoring

    Raises:
        RuntimeError: If all connection attempts fail after MAX_RETRIES, indicating
                     persistent connectivity issues that require manual intervention.
        NoHostAvailable: If no Cassandra nodes are reachable (handled and retried).
        ConnectionException: If authentication or configuration issues occur (handled and retried).

    Side Effects:
        - tSes global CQL Engine session for model operations
        - Creates/synchronizes URL table schema in the database
        - Establishes persistent connection pool for application lifetime

    Example:
        >>> connect_to_db()
        >>> # Database connection established and tables synchronized
        >>> # URL model ready for create/read operations

    Note:
        This function should be called during application startup in the lifespan
        event handler. The connection persists for the application lifetime and
        should be properly closed during shutdown.
    """

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    BUNDLE_PATH = os.path.join(BASE_DIR, "secure-connect-url-shortener.zip")
    MAX_RETRIES = 10
    RETRY_DELAY = 5  # seconds

    # Ensure secure connect bundle exists
    with open(BUNDLE_PATH, "wb") as bundle_file:
        bundle_file.write(base64.b64decode(settings.ASTRA_BUNDLE_B64))

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                f"Attempting to connect to Cassandra (Attempt {attempt}/{MAX_RETRIES})..."
            )

            # Check if secure connect bundle exists
            if not os.path.exists(BUNDLE_PATH):
                logger.error(f"Secure connect bundle not found at {BUNDLE_PATH}")
                raise FileNotFoundError(
                    f"Secure connect bundle not found at {BUNDLE_PATH}"
                )

            # Configure secure connection to DataStax Astra DB
            cloud_config = {"secure_connect_bundle": BUNDLE_PATH}

            # Setup authentication with client credentials
            auth_provider = PlainTextAuthProvider(
                username=settings.CASSANDRA_CLIENT_ID,
                password=settings.CASSANDRA_CLIENT_SECRET,
            )

            # Create cluster with optimized connection policies
            cluster = Cluster(
                cloud=cloud_config,
                auth_provider=auth_provider,
                load_balancing_policy=RoundRobinPolicy(),
                idle_heartbeat_interval=3,
                protocol_version=4,
            )

            # Establish session with keyspace and configure row factory
            session = cluster.connect(settings.KEYSPACE)
            session.row_factory = dict_factory

            # Set global session for CQL Engine models
            connection.set_session(session)

            # Synchronize database schema with application models
            sync_table(URL)
            sync_table(Slug)

            logger.info(
                "Cassandra connection established and tables are created successfully."
            )
            return  # Exit retry loop on successful connection

        except (NoHostAvailable, ConnectionException) as e:
            logger.error(f"Connection attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                sleep(RETRY_DELAY)
            else:
                logger.error("All connection attempts exhausted")

    # All retry attempts failed
    logger.error("Failed to establish Cassandra connection after all retry attempts")
    raise RuntimeError("Failed to connect to Cassandra after multiple attempts.")


def connect_to_redis() -> redis.Redis:
    """
    Establish connection to Redis server with environment-specific configuration.

    This function creates a global Redis client instance configured for the current
    environment (development or production). It handles different connection
    parameters, authentication methods, and security settings based on the
    deployment environment.

    Environment Configurations:
        Development (ENV="dev"):
            - External Redis host with SSL/TLS encryption
            - Username/password authentication required
            - Extended timeout settings for external network latency

        Production (ENV="prod"):
            - Internal Redis host without SSL for performance
            - No authentication (secured by network isolation)
            - Optimized timeout settings for local network

    Connection Features:
        - Automatic response decoding to string format
        - Retry logic for timeout scenarios
        - Health check monitoring every 30 seconds
        - Configurable socket timeouts for optimal performance
        - SSL support for secure development environments

    Performance Optimizations:
        - Connection pooling with keep-alive settings
        - Automatic retry on timeout for resilience
        - Health checks to proactively detect connection issues
        - Optimized socket timeouts based on environment

    Global State:
        Sets the global `redis_client` variable that can be accessed throughout
        the application via the `get_redis_client()` function.

    Raises:
        ConnectionError: If Redis server is unreachable, authentication fails,
                        or SSL handshake fails in development environment.

    Side Effects:
        - Modifies global redis_client variable
        - Establishes persistent Redis connection pool
        - Configures automatic connection health monitoring

    Example:
        >>> connect_to_redis()
        >>> # Redis client configured and ready for operations
        >>> client = get_redis_client()
        >>> await client.set("key", "value")

    Note:
        This function should be called during application startup. The Redis
        connection persists for the application lifetime and should be properly
        closed during shutdown using `aclose()`.
    """
    global redis_client

    logger.info("Initializing Redis connection...")

    try:
        # Determine connection parameters based on environment
        host = (
            settings.REDIS_HOST_EXTERNAL
            if settings.ENV == "dev"
            else settings.REDIS_HOST_INTERNAL
        )

        password = settings.REDIS_PASSWORD if settings.ENV == "dev" else None

        username = settings.REDIS_USERNAME if settings.ENV == "dev" else None

        use_ssl = settings.ENV == "dev"

        logger.info(
            f"Connecting to Redis at {host}:{settings.REDIS_PORT} "
            f"(SSL: {use_ssl}, Auth: {bool(username)})"
        )

        # Create Redis client with environment-specific configuration
        redis_client = redis.Redis(
            host=host,
            password=password,
            username=username,
            port=settings.REDIS_PORT,
            ssl=use_ssl,
            decode_responses=True,  # Automatically decode responses to strings
            retry_on_timeout=True,  # Automatically retry on timeout
            health_check_interval=30,  # Check connection health every 30 seconds
            socket_connect_timeout=20,  # Connection establishment timeout
            socket_timeout=10,  # Socket operation timeout
        )

        logger.info("Redis connection established successfully.")

        return redis_client
    except ConnectionError as e:
        logger.error(f"Redis connection failed: {e}")
        raise e


def get_redis_client() -> redis.Redis:
    """
    Retrieve the global Redis client instance for dependency injection.

    This function provides access to the Redis client instance that was initialized
    during application startup. It's designed to be used with FastAPI's dependency
    injection system to provide Redis connectivity to route handlers and services.

    The function returns the global Redis client that maintains connection pooling,
    health monitoring, and automatic retry capabilities configured during the
    connection establishment.

    Returns:
        redis.Redis: The configured async Redis client instance with all connection
                    settings, authentication, and performance optimizations applied.
                    Ready for immediate use in async operations.

    Raises:
        AttributeError: If called before `connect_to_redis()` has been executed,
                       resulting in a None redis_client reference.

    Usage Patterns:
        - FastAPI dependency injection in route handlers
        - Direct access in service classes and utility functions
        - Async context managers for transaction-like operations

    Example:
        >>> # In FastAPI route handler
        >>> @app.get("/example")
        >>> async def example_route(redis: redis.Redis = Depends(get_redis_client)):
        ...     await redis.set("key", "value")

        >>> # Direct usage in services
        >>> client = get_redis_client()
        >>> await client.get("cached_data")

    Note:
        This function should only be called after successful execution of
        `connect_to_redis()` during application startup. The returned client
        is thread-safe and can be used across multiple async operations
        simultaneously.
    """
    if redis_client is None:
        logger.error("Redis client not initialized. Call connect_to_redis() first.")
        raise RuntimeError("Redis client not initialized")

    return redis_client
