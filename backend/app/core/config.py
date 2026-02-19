from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # OWS V3
    ows_token: str
    ows_base_url: str = "https://ows.goszakup.gov.kz"

    # Database
    database_url: str
    postgres_user: str = "postgres"
    postgres_password: str = "tender_radar_secret"
    postgres_db: str = "tender_radar"
    postgres_host: str = "postgres"
    postgres_port: int = 5432

    # Redis & Celery
    redis_url: str = "redis://redis:6379/0"
    celery_broker_url: str = "redis://redis:6379/0"
    celery_result_backend: str = "redis://redis:6379/1"

    # JWT
    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 60
    jwt_refresh_token_expire_days: int = 7

    # App
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    cors_origins: str = "http://localhost:3000"

    # ETL
    etl_backfill_date_from: str = "2024-01-01"
    etl_backfill_date_to: str = "2025-12-31"
    etl_rate_limit_delay: float = 0.5
    etl_max_retries: int = 3
    etl_page_size: int = 50

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",")]

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
