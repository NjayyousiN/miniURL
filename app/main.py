import os
import asyncio
import time

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from cassandra.cqlengine import connection
from fastapi.responses import RedirectResponse

from core.config import settings
from utils.snowflake import SnowflakeIDGenerator
from database import init_db
from database.repo import (
    generate_short_url,
    generate_short_urls,
    read_original_url_by_id,
)
from services.logger import setup_logger


logger = setup_logger()

# Initialize Snowflake ID generator
node_id = int(os.getenv("NODE_ID", 1))
epoch = int(os.getenv("EPOCH", 1577836800))
snowflake_generator = SnowflakeIDGenerator(node_id, epoch)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan event handler to initialize the database.
    This function is called when the application starts and ensures that the database is initialized.
    """
    logger.info("Starting application and initializing database...")

    try:
        init_db()
        logger.info("Database initialized successfully.")

        yield

        logger.info("Application is shutting down.")

        connection.get_session().shutdown()
        connection.get_session().cluster.shutdown()

    except RuntimeError as e:
        logger.error(f"Database initialization failed: {e}")
        raise e


app = FastAPI(lifespan=lifespan)


@app.post(
    "/shorten",
    status_code=status.HTTP_200_OK,
    response_model=dict,
)
async def create_url(original_url: str):
    generated_short_url = await asyncio.to_thread(
        generate_short_url, original_url, snowflake_generator
    )

    if not generated_short_url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Short url generation failed - please check logs.",
        )

    return generated_short_url


@app.post(
    "/shorten-batch",
    status_code=status.HTTP_200_OK,
    response_model=dict,
)
async def create_urls(original_urls: list[str]):
    """
    Create a shortened URL for the given original URL.
    This endpoint generates a unique short URL using the Snowflake ID generator and stores it in the database.

    Args:
        original_urls (list[str]): The original url/s the user wants to shorten.

    Returns:
        generated_short_urls(list[dict]): List of shortened url/s mapped to their respective original url.
    """

    start = time.perf_counter()

    generated_short_urls = await asyncio.to_thread(
        generate_short_urls, original_urls, snowflake_generator
    )

    duration = time.perf_counter() - start
    logger.info(f"Async route completed in {duration:.4f} seconds")

    if not generated_short_urls:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Short url generation failed - please check logs.",
        )

    return generated_short_urls


@app.post("/shorten-sync", response_model=dict)
def create_urls_sync(original_urls: list[str]):
    start = time.perf_counter()

    generated_short_urls = generate_short_urls(original_urls, snowflake_generator)

    duration = time.perf_counter() - start
    logger.info(f"Sync route completed in {duration:.4f} seconds")

    if not generated_short_urls:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Short url generation failed - please check logs.",
        )

    return generated_short_urls


@app.get("/{id}", status_code=status.HTTP_307_TEMPORARY_REDIRECT)
async def read_url(id: str):
    """
    Retrieve the original URL for a given shortened URL.
    This endpoint looks up the original URL in the database using the provided short URL.

    Args:
        id (str): Shortened URL ID
    """

    original_url = await asyncio.to_thread(read_original_url_by_id, id)

    if original_url:
        return RedirectResponse(url=original_url)
    raise HTTPException(status_code=404, detail="Short URL not found")
