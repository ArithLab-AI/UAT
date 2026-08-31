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
    # OAuth 2.0 web-client ID used to validate Google ID tokens submitted by the
    # browser. Keep this server-side configuration separate from the client UI.
    GOOGLE_CLIENT_ID: Optional[str] = None
    # Retained for OAuth authorization-code flows and to allow standard Google
    # OAuth environment files. The current ID-token login endpoint does not
    # send this value to clients or use it for token verification.
    GOOGLE_CLIENT_SECRET: Optional[str] = None
    # The production page that hosts the Google sign-in UI.  The API verifies
    # the credential submitted from this page at POST /auth/google.
    FRONTEND_LOGIN_URL: str = "https://www.arithlab.ai/"
    
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

    OPENAI_API_KEY: Optional[str] = None
    OPENAI_BASE_URL: Optional[str] = None

    UAT_ANALYSIS_LLM_PROVIDER: str = "openai"
    UAT_ANALYSIS_LLM_MODEL: Optional[str] = "gpt-4o-mini"
    UAT_ANALYSIS_LLM_TEMPERATURE: float = 0
    UAT_ANALYSIS_LLM_MAX_TOKENS: int = 1200
    UAT_AI_CLEANING_MODEL: Optional[str] = None
    UAT_AI_CLEANING_TEMPERATURE: float = 0
    UAT_AI_CLEANING_MAX_TOKENS: int = 3000
    # Per-request LLM timeout (seconds) and retry cap. Without these the OpenAI SDK can
    # block a cleaning batch for its long default timeout, leaving the background job
    # stuck in "processing". A bounded timeout makes a hung call fail fast so the job is
    # marked "failed" instead of hanging forever.
    UAT_AI_LLM_TIMEOUT_SECONDS: float = 60.0
    UAT_AI_LLM_MAX_RETRIES: int = 2
    UAT_AI_BATCH_SIZE: int = 50
    # Caps how deep the batch bisection fallback recurses before a stubborn
    # sub-batch is returned as-is (original rows). Bounds worst-case token use
    # when the model keeps returning malformed/short responses for a chunk.
    UAT_AI_CLEANING_MAX_FALLBACK_DEPTH: int = 3
    UAT_AI_VALUE_BATCH_SIZE: int = 500
    UAT_AI_VALUE_BATCH_PARALLELISM: int = 8
    UAT_AI_VALUE_CLEAN_MAX_UNIQUE_VALUES: int = 5000
    UAT_AI_VALUE_CLEAN_MAX_COLUMNS: int = 5
    UAT_AI_ROW_LEVEL_LARGE_DATASET_THRESHOLD: int = 50000
    UAT_AI_PLANNER_SAMPLE_ROWS: int = 100
    UAT_AI_CLEAN_PREVIEW_ROWS: int = 25

    # Data-chat (natural-language query) DuckDB execution guards. These cap a single
    # LLM-generated query so one heavy/runaway query cannot exhaust the server: RAM cap,
    # CPU-thread cap, and a wall-clock timeout after which the query is interrupted.
    UAT_DATA_CHAT_MEMORY_LIMIT: str = "512MB"
    UAT_DATA_CHAT_MAX_THREADS: int = 2
    UAT_DATA_CHAT_TIMEOUT_SECONDS: int = 20
    # Ceiling for the insight narrative specifically. Kept as its own knob because when the
    # JSON is cut off mid-object the section silently falls back to the rule-based text
    # rather than erroring, so the limit needs to be tunable without touching every call.
    UAT_DATA_CHAT_INSIGHT_MAX_TOKENS: int = 1200

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
