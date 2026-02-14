from fastapi import FastAPI
from app.database import engine, Base
from app.routes import router

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Authentication API")

app.include_router(router)
