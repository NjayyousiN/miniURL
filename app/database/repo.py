from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine import connection
from cassandra.cluster import Session
from cassandra import InvalidRequest

from utils.snowflake import SnowflakeIDGenerator
from database import URL
from services.logger import setup_logger

logger = setup_logger()


def generate_short_urls(
    original_urls: list[str], snowflake_generator: SnowflakeIDGenerator
) -> dict:

    generated_short_urls: dict = {}

    try:

        b = BatchQuery()
        for original_url in original_urls:
            # Generate a unique id
            unique_id = snowflake_generator.generate_id()
            short_url = f"https://miniurl.com/{unique_id}"

            URL.batch(b).create(short_url=str(unique_id), original_url=original_url)

            generated_short_urls[short_url] = original_url
        # Execute batch query
        b.execute()

        return generated_short_urls
    except InvalidRequest as e:
        logger.error(f"Batch insert failed: {e}")
        return {}


def generate_short_url(original_url: str, snowflake_generator: SnowflakeIDGenerator):
    try:
        session: Session = connection.get_session()
        query = session.prepare(
            (
                """
            INSERT INTO url (short_url, original_url)
            VALUES (?, ?)
            """
            )
        )

        unique_id = str(snowflake_generator.generate_id())

        session.execute_async(query, [unique_id, original_url])

        return {
            "original_url": original_url,
            "short_url": f"https://miniurl.com/{unique_id}",
        }
    except InvalidRequest as e:
        logger.error(f"Short url generation failed: {e}")
        return ""


def read_original_url_by_id(id: str):
    try:
        session: Session = connection.get_session()
        query = session.prepare("SELECT * FROM url WHERE short_url=?")

        future = session.execute_async(query, [id])

        rows = future.result()
        print(rows[0])
        original_url = rows[0]["original_url"]

        return original_url
    except InvalidRequest as e:
        logger.error(f"Original url retrieval failed: {e}")
        return ""
