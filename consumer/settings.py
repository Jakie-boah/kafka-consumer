from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    CLICKHOUSE_HOST: str = Field("clickhouse", env="CLICKHOUSE_HOST")
    CLICKHOUSE_DB: str = Field("metrics", env="CLICKHOUSE_DB")
    CLICKHOUSE_TABLE: str = Field("events", env="CLICKHOUSE_TABLE")

    KAFKA_BOOTSTRAP_SERVERS: str = Field("localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_GROUP_ID: str = Field("metrics-group", env="KAFKA_GROUP_ID")
    KAFKA_SECURITY_PROTOCOL: str | None = Field(None, env="KAFKA_SECURITY_PROTOCOL")
    KAFKA_SASL_MECHANISMS: str | None = Field(None, env="KAFKA_SASL_MECHANISMS")
    KAFKA_SASL_USERNAME: str | None = Field(None, env="KAFKA_SASL_USERNAME")
    KAFKA_SASL_PASSWORD: str | None = Field(None, env="KAFKA_SASL_PASSWORD")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
