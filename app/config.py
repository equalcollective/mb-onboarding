"""Application configuration using pydantic-settings."""

from typing import Optional
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Metabase
    metabase_url: str = "https://metabase.equalcollective.com"
    metabase_api_key: str = ""

    # Card IDs
    card_id_ads_report: int = 665        # onboarding_ads_ap
    card_id_asin_mapping: int = 666      # onboarding_asins
    card_id_business_report: int = 681   # onboarding_biz

    # Cache
    cache_ttl_seconds: int = 300

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
