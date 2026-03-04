import logging
from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["Healthy"])
logger = logging.getLogger(__name__)

@router.get("")
def health_check():
    logger.debug("Health check endpoint hit")
    return {"status": "ok"}
