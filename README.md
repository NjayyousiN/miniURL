# miniURL 🔗

A high-performance URL shortening service built with FastAPI, Cassandra, and Redis. Transform long URLs into short, shareable links with advanced features like expiration dates, custom slugs, and comprehensive rate limiting.

**Live Demo:** [https://miniurl-wmmw.onrender.com/](https://miniurl-wmmw.onrender.com/)

## Features

- **Single & Batch URL Shortening**: Create individual short URLs or process multiple URLs in bulk
- **Custom Slugs**: Create memorable short URLs with custom slugs
- **Expiration Control**: Set start and expiry dates for time-sensitive links
- **High Performance**: Redis caching with Cassandra persistence for optimal speed and reliability
- **Advanced Rate Limiting**: Multi-tier token bucket rate limiting (global + per-IP)
- **Snowflake ID Generation**: Unique, distributed ID generation for scalability
- **Comprehensive Logging**: Structured logging for monitoring and debugging
- **CORS Support**: Cross-origin resource sharing for web applications
- **Auto-generated API Documentation**: Interactive Swagger/OpenAPI docs

## Architecture

- **FastAPI**: Modern web framework with automatic API documentation
- **Cassandra**: Distributed NoSQL database for persistent URL storage
- **Redis**: High-performance in-memory caching layer
- **Token Bucket Rate Limiting**: Prevents server overload with configurable limits
- **Snowflake ID Generator**: Ensures unique IDs across distributed systems

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.13+ (for local development)

### Running with Docker (Recommended)

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd miniURL
   ```

2. **Start the services**

   ```bash
   docker-compose up -d
   ```

3. **Access the application**
   - API: http://localhost:8000
   - Interactive API docs: http://localhost:8000/docs
   - Redis: localhost:6379
   - Cassandra: localhost:9042

### Local Development Setup

1. **Install dependencies**

   ```bash
   cd app
   pip install -r requirements.txt
   ```

2. **Set up environment variables**

   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start external services**

   ```bash
   docker-compose up redis cassandra -d
   ```

4. **Run the application**
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

## 📡 API Endpoints

### Create Short URL

```http
POST /shorten
Content-Type: application/json

{
  "original_url": "https://example.com",
  "slug": "custom-slug",
  "expiry_date": "2024-12-31T23:59:59",
  "start_date": "2024-01-01T00:00:00"
}
```

### Batch URL Shortening

```http
POST /shorten-batch
Content-Type: application/json

[
  {
    "original_url": "https://example.com",
    "slug": "example"
  },
  {
    "original_url": "https://another-site.com",
    "expiry_date": "2024-06-30T23:59:59"
  }
]
```

### Redirect to Original URL

```http
GET /{short_id}
```

Returns a 307 redirect to the original URL.

## ⚡ Rate Limiting

The service implements a sophisticated two-tier rate limiting system:

### Global Rate Limiting

- **Purpose**: Protects the entire service from overload
- **Implementation**: Token bucket algorithm with Redis
- **Scope**: All incoming requests across all clients

### Per-IP Rate Limiting

- **Purpose**: Prevents individual clients from overwhelming the service
- **Implementation**: Individual token buckets per IP address
- **Features**:
  - Automatic bucket creation and cleanup
  - Configurable capacity and refill rates
  - TTL-based bucket expiration

### Configuration

Rate limiting parameters are configurable via environment variables:

- `GLOBAL_BUCKET_CAPACITY`: Maximum tokens for global bucket
- `GLOBAL_BUCKET_REFILL_RATE`: Tokens added per refill interval
- `IP_BUCKET_CAPACITY`: Maximum tokens per IP
- `IP_BUCKET_REFILL_RATE`: Tokens added per IP per interval
- `IP_BUCKET_TTL`: Time-to-live for IP buckets

## Project Structure

```
miniURL/
├── app/
│   ├── core/                    # Core configuration and exceptions
│   │   ├── config.py           # Application settings and environment config
│   │   └── exceptions.py       # Custom exception definitions
│   ├── database/               # Database layer
│   │   ├── __init__.py        # Database connections (Cassandra + Redis)
│   │   ├── repo.py            # Repository pattern for data operations
│   │   └── schema.py          # Pydantic models for request/response
│   ├── services/               # Business logic services
│   │   ├── logger.py          # Structured logging configuration
│   │   ├── rate_limiter.py    # IP-based rate limiting
│   │   └── tokens_bucket.py   # Token bucket implementation
│   ├── utils/                  # Utility functions
│   │   ├── bucket_refil.py    # Token bucket refill logic
│   │   └── snowflake.py       # Snowflake ID generator
│   ├── tests/                  # Test suites
│   │   ├── api/               # API endpoint tests
│   │   └── stress/            # Load testing scripts
│   ├── main.py                # FastAPI application and routes
│   ├── requirements.txt       # Python dependencies
│   └── Dockerfile            # Container configuration
├── docker-compose.yml         # Multi-service orchestration
└── README.md                 # This file
```

## Configuration

Key environment variables:

```bash
# Database Configuration
CASSANDRA_CLIENT_ID=your_client_id
CASSANDRA_CLIENT_SECRET=your_client_secret
KEYSPACE=miniurl

# Redis Configuration
REDIS_HOST_DEV=localhost
REDIS_HOST_PROD=redis
REDIS_PORT=6379

# Rate Limiting
GLOBAL_BUCKET_CAPACITY=1000
IP_BUCKET_CAPACITY=100
GLOBAL_BUCKET_REFILL_RATE=100
IP_BUCKET_REFILL_RATE=10

# Application
ENV=dev
```

## Testing

```bash
# Run API tests
cd app
pytest tests/api/

# Load testing with Redis
redis-cli --eval tests/stress/sync_test.lua
```

## Deployment

The application is containerized and can be deployed to any Docker-compatible platform:

- **Development**: Docker Compose for local development
- **Production**: onrender to host the web-app and redis instance
- **Cloud**: Astra DB to host cassandra
