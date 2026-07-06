from datetime import datetime, timedelta
import logging
from jose import jwt
from app.config.config import settings

logger = logging.getLogger(__name__)

def create_token(data: dict, expires_delta: timedelta, token_type: str):
    to_encode = data.copy()
    now = datetime.utcnow()
    to_encode.update({
        "iat": now,
        "exp": now + expires_delta,
        "type": token_type
    })
    token = jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM
    )
    logger.debug(
        "Created %s token for subject=%s exp=%s",
        token_type,
        to_encode.get("sub"),
        to_encode.get("exp"),
    )
    return token

def create_access_token(data: dict):
    return create_token(
        data,
        timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
        "access"
    )

def create_refresh_token(data: dict):
    return create_token(
        data,
        timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS),
        "refresh"
    )
