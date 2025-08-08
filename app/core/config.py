from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    NODE_ID: int
    EPOCH: int
    KEYSPACE: str
    CASSANDRA_HOST: str
    CASSANDRA_CLIENT_ID: str
    CASSANDRA_CLIENT_SECRET: str
    CASSANDRA_SECURE_CONNECT_BUNDLE: str

    class Config:
        env_file = ".env"


settings = Settings()
