from fastapi import FastAPI, Request
from app.routes.auth_route import router as auth_router
from app.routes.csv_dataset_route import router as csv_dataset_router
from app.routes.subscription_route import router as subscription_router
from app.routes.health_route import router as health_router
from app.db.database import engine, Base, SessionLocal
from app.config.config import settings
from app.utils.auth_schema_setup import ensure_auth_schema
from app.utils.csv_dataset_setup import ensure_csv_dataset_schema
from app.utils.subs_plan_seed import seed_subscription_plans
from app.utils.subscription_schema_setup import ensure_subscription_schema

app = FastAPI(title="Authentication API")

app.include_router(health_router)
app.include_router(auth_router)
app.include_router(subscription_router)
app.include_router(csv_dataset_router)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    try:
        response = await call_next(request)
    except Exception:
        raise

    return response


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
    db = SessionLocal()
    try:
        seed_subscription_plans(db)
    finally:
        db.close()
