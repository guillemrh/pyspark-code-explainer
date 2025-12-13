# backend/app/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    gemini_api_key: str
    backend_port: int = 8050  # default port for Docker network
    timeout_seconds: int = 15
    redis_url: str = "redis://redis:6379/0"
    cache_ttl: int = 3600  # seconds

    class Config:
        env_file = ".env"

settings = Settings()