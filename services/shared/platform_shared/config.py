from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

ALLOWED_ENVIRONMENTS = {"local", "test", "staging", "production"}


def _as_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _as_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    environment: str
    log_level: str
    service_name: str
    database_url: str
    redis_url: str
    kafka_bootstrap_servers: str
    kafka_client_id: str
    kafka_consumer_group: str
    kafka_enable_consumer: bool
    default_cache_ttl_seconds: int
    rate_limit_requests_per_minute: int

    @classmethod
    def from_env(cls, service_name: str) -> Settings:
        return cls(
            environment=os.getenv("ENVIRONMENT", "local"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            service_name=service_name,
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql://platform:platform@localhost:5432/data_platform",
            ),
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_client_id=os.getenv("KAFKA_CLIENT_ID", f"{service_name}-client"),
            kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", service_name),
            kafka_enable_consumer=_as_bool("KAFKA_ENABLE_CONSUMER", True),
            default_cache_ttl_seconds=_as_int("DEFAULT_CACHE_TTL_SECONDS", 120),
            rate_limit_requests_per_minute=_as_int("RATE_LIMIT_REQUESTS_PER_MINUTE", 120),
        )


def service_settings(service_name: str) -> Settings:
    return Settings.from_env(service_name)


def validate_settings(settings: Settings) -> list[str]:
    errors: list[str] = []
    if settings.environment not in ALLOWED_ENVIRONMENTS:
        errors.append(
            f"ENVIRONMENT must be one of {sorted(ALLOWED_ENVIRONMENTS)}, got {settings.environment!r}"
        )
    if not settings.service_name:
        errors.append("service_name must not be empty")
    if not settings.kafka_bootstrap_servers:
        errors.append("KAFKA_BOOTSTRAP_SERVERS must not be empty")
    if settings.default_cache_ttl_seconds <= 0:
        errors.append("DEFAULT_CACHE_TTL_SECONDS must be positive")
    if settings.rate_limit_requests_per_minute <= 0:
        errors.append("RATE_LIMIT_REQUESTS_PER_MINUTE must be positive")

    database = urlparse(settings.database_url)
    if database.scheme not in {"postgresql", "postgres"} or not database.hostname:
        errors.append("DATABASE_URL must be a PostgreSQL URL")

    redis = urlparse(settings.redis_url)
    if redis.scheme != "redis" or not redis.hostname:
        errors.append("REDIS_URL must be a Redis URL")

    if settings.environment in {"staging", "production"}:
        if "localhost" in settings.database_url:
            errors.append("staging/production DATABASE_URL should not point at localhost")
        if "localhost" in settings.redis_url:
            errors.append("staging/production REDIS_URL should not point at localhost")
    return errors
