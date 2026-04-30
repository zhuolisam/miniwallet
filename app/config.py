from uuid import UUID
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_env: str = "development"
    database_url: str = "postgresql+asyncpg://minibank:minibank@localhost:5432/minibank"
    redis_url: str = "redis://localhost:6379/0"

    jwt_secret: str = "dev-secret-change-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    system_account_id: UUID = UUID("00000000-0000-0000-0000-000000000000")

settings = Settings()
SYSTEM_ACCOUNT_ID: UUID = settings.system_account_id
