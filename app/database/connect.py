from cassandra.cqlengine import connection


def get_cassandra_session():
    """
    Returns the online session.
    """
    session = connection.get_session()
    yield session
