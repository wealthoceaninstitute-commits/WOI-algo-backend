from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./local.db"
    SECRET_KEY: str = "dev-secret-change-in-production-please-use-openssl"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 30
    ENCRYPTION_KEY: str = "0" * 64
    ALLOWED_ORIGINS: str = "*"
    DHAN_BASE_URL: str = "https://api.dhan.co"
    MASTER_EMAIL: str = ""
    MASTER_PASSWORD: str = ""
    MASTER_NAME: str = "Master Admin"

    class Config:
        env_file = ".env"
        extra = "ignore"

    @property
    def origins_list(self) -> list[str]:
        if self.ALLOWED_ORIGINS.strip() == "*":
            return ["*"]
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]


@lru_cache()
def get_settings() -> Settings:
    return Settings()
