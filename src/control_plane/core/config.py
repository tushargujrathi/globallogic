"""Runtime configuration, read from environment variables (12-factor)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings shared by every control-plane process (api, relay, consumer)."""

    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    database_url: str
    rabbitmq_url: str = "amqp://localhost:5672/%2F"
    log_level: str = "INFO"

    # Binds all interfaces *inside* the container; exposure is decided by compose.
    api_host: str = "0.0.0.0"  # noqa: S104  # nosec B104
    api_port: int = 8000

    outbox_batch_size: int = 100
    outbox_poll_interval_seconds: float = 0.5

    consumer_prefetch_count: int = 10


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings, loaded once."""
    return Settings()  # values come from the environment
