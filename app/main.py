import time
from fastapi import FastAPI, Request
from app.core.logging_config import configure_logging, get_logger
from app.routes.auth_route import router as auth_router
from app.routes.subscription_route import router as subscription_router
from app.routes.health_route import router as health_router
from app.db.database import engine, Base, SessionLocal
from app.config.settings import settings
from app.utils.seed import seed_subscription_plans

configure_logging()
logger = get_logger(__name__)

app = FastAPI(title="Authentication API")

app.include_router(auth_router)
app.include_router(subscription_router)
app.include_router(health_router)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.exception(
            "Unhandled error for %s %s after %.2fms",
            request.method,
            request.url.path,
            elapsed_ms,
        )
        raise

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        "HTTP %s %s -> %s (%.2fms)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


@app.on_event("startup")
def startup_event():
    logger.info("Application startup complete")
    required = [
        settings.POSTGRES_USER,
        settings.POSTGRES_PASSWORD,
        settings.POSTGRES_DB,
        settings.DATABASE_HOST,
        settings.DATABASE_PORT,
    ]
    if any(v in (None, "") for v in required):
        logger.warning("Database env not fully configured; skipping DB init on startup.")
        return

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        seed_subscription_plans(db)
    finally:
        db.close()
