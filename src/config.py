from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "dev"
    log_level: str = "INFO"

    postgres_dsn: str = "postgresql://dsearch:dsearch@localhost:5432/dsearch"
    redis_url: str = "redis://localhost:6379/0"
    opensearch_url: str = "http://localhost:9200"

    index_name: str = "documents"
    index_stream: str = "docs.index.v1"
    indexer_group: str = "indexers"
    indexer_consumer: str = "indexer-1"

    search_cache_ttl_seconds: int = 30
    default_rate_limit_rps: int = 50
    rate_limit_burst_multiplier: int = 2

    # Client timeouts — prevent unbounded waits on a single slow dependency.
    opensearch_timeout_seconds: float = 0.7
    postgres_command_timeout_seconds: float = 0.5
    redis_socket_timeout_seconds: float = 0.1

    # Indexer DLQ
    indexer_max_deliveries: int = 5
    index_dlq_stream: str = "docs.index.dlq"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


settings = Settings()
