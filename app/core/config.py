from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    NODE_ID: int
    EPOCH: int
    KEYSPACE: str
    CASSANDRA_HOST: str

    class Config:
        env_file = ".env"


settings = Settings()
