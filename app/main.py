import os

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from cassandra.cqlengine import connection


from utils.snowflake import SnowflakeIDGenerator
from database import URL, init_db
from services.logger import setup_logger


logger = setup_logger()

# Initialize Snowflake ID generator
node_id = int(os.getenv("NODE_ID", 1))
epoch = int(os.getenv("EPOCH", 1609459200000))
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


@app.get("/")
async def read_root():
    return {"message": "Welcome to the miniurl API!"}


@app.post("/shorten")
async def create_short_url(original_url: str):
    """
    Create a shortened URL for the given original URL.
    This endpoint generates a unique short URL using the Snowflake ID generator and stores it in the database.
    """

    unique_id = snowflake_generator.generate_id()
    short_url = f"https://miniurl.com/{unique_id}"
    url = URL(original_url=original_url, short_url=str(unique_id))
    url.save()

    if not url:
        raise HTTPException(status_code=500, detail="Failed to create short URL")

    return {"short_url": short_url, "original_url": original_url}


@app.get("/{id}")
async def read_url(id: str):
    """
    Retrieve the original URL for a given shortened URL.
    This endpoint looks up the original URL in the database using the provided short URL.
    """

    url: URL = URL.get(short_url=id)
    if url:
        if not url:
            raise HTTPException(status_code=404, detail="Short URL not found")

    return {"original_url": url.original_url}
