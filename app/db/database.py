import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config.config import settings
logger = logging.getLogger(__name__)

DATABASE_URL = (
    f"postgresql://{settings.POSTGRES_USER}:"
    f"{settings.POSTGRES_PASSWORD}@"
    f"{settings.DATABASE_HOST}:"
    f"{settings.DATABASE_PORT}/"
    f"{settings.POSTGRES_DB}"
)

try:
    engine = create_engine(DATABASE_URL)
    logger.info("Database engine initialized")
except Exception:
    logger.exception("Failed to initialize database engine")
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        logger.exception("Database session error; rolled back transaction")
        raise
    finally:
        db.close()
