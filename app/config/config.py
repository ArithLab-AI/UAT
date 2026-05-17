from typing import Optional
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    POSTGRES_USER: Optional[str] = None
    POSTGRES_PASSWORD: Optional[str] = None
    POSTGRES_DB: Optional[str] = None
    DATABASE_HOST: Optional[str] = None
    DATABASE_PORT: Optional[int] = None

    SECRET_KEY: Optional[str] = None
    ALGORITHM: Optional[str] = None
    ACCESS_TOKEN_EXPIRE_MINUTES: Optional[int] = None
    REFRESH_TOKEN_EXPIRE_DAYS: Optional[int] = None
    OTP_EXPIRE_MINUTES: Optional[int] = None
    
    SMTP_EMAIL: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: Optional[int] = None

     # Storage settings
    S3_ENDPOINT_URL: Optional[str] = None
    S3_ACCESS_KEY_ID: Optional[str] = None
    S3_SECRET_ACCESS_KEY: Optional[str] = None
    S3_BUCKET_NAME: Optional[str] = None
    S3_REGION: str = "us-east-1"
    CHUNK_SIZE: int = 1000

    class Config:
        env_file = ".env"

    @property
    def database_url(self):
        return (
            f"postgresql://{self.POSTGRES_USER}:"
            f"{self.POSTGRES_PASSWORD}@"
            f"{self.DATABASE_HOST}:"
            f"{self.DATABASE_PORT}/"
            f"{self.POSTGRES_DB}"
        )

settings = Settings()
