import os
from datetime import datetime
from time import sleep
from uuid import uuid4

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cluster import NoHostAvailable, ConnectionException
from cassandra.query import dict_factory

from services.logger import setup_logger

KEYSPACE = os.getenv("KEYSPACE", "miniurl")

# Setup logger
logger = setup_logger()


class URL(Model):
    """
    Model representing a shortened URL in the miniURL application.
    This model is used to store the original URL, the shortened URL, and the creation timestamp.
    It uses Cassandra's CQL Engine for ORM-like functionality.
    """

    __keyspace__ = KEYSPACE
    id = columns.UUID(default=uuid4)
    original_url = columns.Text(required=True)
    short_url = columns.Text(primary_key=True, required=True)
    created_at = columns.DateTime(default=datetime.now)


def init_db():
    """
    Create the necessary tables in the Cassandra database.
    This function is called to ensure that the URL model is created in the database.
    """

    MAX_RETRIES = 10
    RETRY_DELAY = 5  # seconds

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                f"Attempting to connect to Cassandra (Attempt {attempt}/{MAX_RETRIES})..."
            )

            # Establish a connection to the Cassandra database
            cluster = Cluster(
                contact_points=["cassandra"], load_balancing_policy=RoundRobinPolicy()
            )
            session = cluster.connect(KEYSPACE)
            session.row_factory = dict_factory

            # Set the session for the Cassandra CQL Engine
            connection.set_session(session)

            sync_table(URL)
            logger.info("Cassandra connection established...")
            return  # Exit the loop if successful

        except (NoHostAvailable, ConnectionException) as e:
            logger.error(f"Connection attempt {attempt} failed: {e}")
            sleep(RETRY_DELAY)

    logger.info("Cassandra connection closed.")
    raise RuntimeError("Failed to connect to Cassandra after multiple attempts.")
