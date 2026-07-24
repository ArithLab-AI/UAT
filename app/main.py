from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from app.routes.auth_route import router as auth_router
from app.routes.csv_dataset_route import router as csv_dataset_router
from app.routes.subscription_route import router as subscription_router
from app.routes.health_route import router as health_router
from app.routes.cleaning import router as cleaning_router
from app.routes.ai_cleaning_route import router as ai_cleaning_router
from app.routes.analysis_route import router as analysis_router
from app.routes.analysis_suggestion_route import router as analysis_suggestion_router
from app.routes.test_llm_route import router as test_llm_router
from app.routes.token_usage_route import router as token_usage_router
from app.routes.data_chat_route import router as data_chat_router
from app.db.database import engine, Base, SessionLocal
from app.config.config import settings
from app.utils.auth_schema_setup import ensure_auth_schema
from app.utils.csv_dataset_setup import ensure_csv_dataset_schema
from app.utils.file_upload_schema_setup import ensure_file_upload_schema
from app.utils.object_storage import get_object_storage_service
from app.utils.responses import http_exception_response, validation_error_response
from app.utils.subs_plan_seed import seed_subscription_plans
from app.utils.ai_cleaning_schema_setup import ensure_ai_cleaning_schema
from app.utils.ai_cleaning_job_reaper import reap_stale_ai_cleaning_jobs
from app.utils.subscription_schema_setup import ensure_subscription_schema
from app.services.file_retention_service import (
    start_file_retention_scheduler,
    stop_file_retention_scheduler,
)

app = FastAPI(title="Authentication API")

app.include_router(health_router)
app.include_router(auth_router)
app.include_router(subscription_router)
app.include_router(csv_dataset_router)
app.include_router(cleaning_router)
app.include_router(ai_cleaning_router)
app.include_router(analysis_router)
app.include_router(analysis_suggestion_router)
app.include_router(test_llm_router)
app.include_router(token_usage_router)
app.include_router(data_chat_router)

@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
    return validation_error_response(exc)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return http_exception_response(exc)


@app.on_event("startup")
def startup_event():
    required = [
        settings.POSTGRES_USER,
        settings.POSTGRES_PASSWORD,
        settings.POSTGRES_DB,
        settings.DATABASE_HOST,
        settings.DATABASE_PORT,
    ]
    if any(v in (None, "") for v in required):
        return

    Base.metadata.create_all(bind=engine)
    ensure_auth_schema(engine)
    ensure_subscription_schema(engine)
    ensure_csv_dataset_schema(engine)
    ensure_file_upload_schema(engine)
    ensure_ai_cleaning_schema(engine)
    get_object_storage_service().ensure_bucket()
    db = SessionLocal()
    try:
        seed_subscription_plans(db)
        reap_stale_ai_cleaning_jobs(db)
    finally:
        db.close()
    start_file_retention_scheduler()


@app.on_event("shutdown")
def shutdown_event():
    stop_file_retention_scheduler()
