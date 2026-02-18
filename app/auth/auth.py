from datetime import datetime, timedelta
from jose import jwt
from app.config.config import settings

def create_token(data: dict, expires_delta: timedelta, token_type: str):
    to_encode = data.copy()
    now = datetime.utcnow()
    to_encode.update({
        "iat": now,
        "exp": now + expires_delta,
        "type": token_type
    })
    return jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM
    )

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
