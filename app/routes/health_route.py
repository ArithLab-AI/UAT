import logging
from fastapi import APIRouter
from app.schemas.common_schema import MessageSuccessResponse
from app.utils.responses import success_response

router = APIRouter(prefix="/health", tags=["Healthy"])
logger = logging.getLogger(__name__)

@router.get("", response_model=MessageSuccessResponse)
def health_check():
    logger.debug("Health check endpoint hit")
    return success_response("Health check successful", data={"status": "ok"})
