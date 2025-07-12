from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy


def get_cassandra_session():
    """
    Establish a connection to the Cassandra database and return the session.
    """
    cluster = Cluster(
        contact_points=["cassandra"], load_balancing_policy=RoundRobinPolicy()
    )
    session = cluster.connect("miniurl")

    try:
        yield session
    finally:
        session.shutdown()
        cluster.shutdown()
