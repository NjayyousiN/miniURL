from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str
    NODE_ID: int
    EPOCH: int
    KEYSPACE: str
    REDIS_PASSWORD: str
    REDIS_USERNAME: str
    REDIS_HOST_EXTERNAL: str
    REDIS_HOST_INTERNAL: str
    REDIS_PORT: int
    CASSANDRA_HOST: str
    CASSANDRA_CLIENT_ID: str
    CASSANDRA_CLIENT_SECRET: str
    ASTRA_BUNDLE_B64: str

    class Config:
        env_file = ".env"


settings = Settings()
