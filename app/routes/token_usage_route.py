import logging

from fastapi import APIRouter

from app.utils.responses import success_response
from app.utils.token_usage import get_token_usage_totals, reset_token_usage_totals

router = APIRouter(prefix="/token-usage", tags=["Token Usage"])
logger = logging.getLogger(__name__)


@router.get("")
def read_token_usage():
    """Cumulative LLM token usage since the process started (or last reset)."""
    return success_response("Token usage fetched", data=get_token_usage_totals())


@router.post("/reset")
def reset_token_usage():
    """Zero the counters; returns the totals recorded just before the reset."""
    previous = reset_token_usage_totals()
    logger.info("Token usage counters reset; previous totals=%s", previous)
    return success_response("Token usage reset", data={"previous_totals": previous})
