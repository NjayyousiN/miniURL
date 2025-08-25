from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ENV: str
    NODE_ID: int
    EPOCH: int
    DEFAULT_EXPIRY_DURATION: int
    DOMAIN: str
    GLOBAL_BUCKET_NAME: str
    GLOBAL_BUCKET_CAPACITY: int
    GLOBAL_BUCKET_REFILL_RATE: int
    GLOBAL_BUCKET_REFILL_INTERVAL: int
    IP_BUCKET_TTL: int
    IP_BUCKET_CAPACITY: int
    IP_BUCKET_REFILL_RATE: int
    IP_BUCKET_REFILL_INTERVAL: int
    KEYSPACE: str
    REDIS_HOST_DEV: str
    REDIS_HOST_PROD: str
    REDIS_PORT: int
    CASSANDRA_HOST: str
    CASSANDRA_CLIENT_ID: str
    CASSANDRA_CLIENT_SECRET: str
    ASTRA_BUNDLE_B64: str

    class Config:
        env_file = ".env"


settings = Settings()
