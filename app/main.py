from fastapi import FastAPI
from app.db.database import engine, Base, SessionLocal
from app.utils.subs_plan_seed import seed_subscription_plans
from app.routes.auth_route import router as auth_router
from app.routes.subscription_route import router as subscription_router

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Authentication API")

app.include_router(auth_router)
app.include_router(subscription_router)

@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    try:
        seed_subscription_plans(db)
    finally:
        db.close()