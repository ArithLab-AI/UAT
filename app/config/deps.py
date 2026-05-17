import logging
from fastapi import Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from sqlalchemy.orm import Session
from app.config.config import settings
from app.db.database import get_db
from app.models.auth_models import TokenBlacklist, User
from app.utils.email_utils import missing_smtp_settings, send_plain_email
from app.utils.mail_body import mail_body
from app.utils.responses import error_response

security = HTTPBearer()
logger = logging.getLogger(__name__)


def _validate_smtp_settings() -> None:
    missing = missing_smtp_settings()
    if missing:
        logger.error("SMTP configuration missing required settings: %s", ", ".join(missing))
        raise error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="SMTP is not configured correctly"
        )

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)) -> User:
    if not credentials:
        logger.warning("Authorization credentials not provided")
        raise error_response(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization credentials not provided"
        )
    token = credentials.credentials
    blacklisted = db.query(TokenBlacklist).filter(
        TokenBlacklist.token == token
    ).first()

    if blacklisted:
        logger.warning("Blocked request with blacklisted token")
        raise error_response(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been revoked"
        )
    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        email = payload.get("sub")
        if not email:
            logger.warning("Token payload missing subject")
            raise error_response(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    except JWTError:
        logger.warning("JWT decode failed: invalid or expired token")
        raise error_response(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
    
    user = db.query(User).filter(User.email == email).first()

    if user is None:
        logger.warning("User in token payload not found: email=%s", email)
        raise error_response(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    logger.debug("Authenticated user_id=%s email=%s", user.id, user.email)
    return user

def send_otp_email(email: str, otp: str):
    _validate_smtp_settings()
    subject = "Airthlab OTP Code"
    body = mail_body(otp)
    logger.info("Sending OTP email to recipient=%s", email)

    try:
        send_plain_email(recipient=email, subject=subject, body=body)
    except Exception:
        logger.exception("Failed to send OTP email to recipient=%s", email)
        raise error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP email"
        )

    logger.info("OTP email sent to recipient=%s", email)
