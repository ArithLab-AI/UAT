import logging
import smtplib
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from email.mime.text import MIMEText
from sqlalchemy.orm import Session

from app.config.config import settings
from app.db.database import get_db
from app.models.auth_models import TokenBlacklist, User
from app.utils.mail_body import mail_body

security = HTTPBearer()
logger = logging.getLogger(__name__)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)) -> User:
    if not credentials:
        logger.warning("Authorization credentials not provided")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization credentials not provided"
        )
    token = credentials.credentials
    blacklisted = db.query(TokenBlacklist).filter(
        TokenBlacklist.token == token
    ).first()

    if blacklisted:
        logger.warning("Blocked request with blacklisted token")
        raise HTTPException(
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
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    except JWTError:
        logger.warning("JWT decode failed: invalid or expired token")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
    
    user = db.query(User).filter(User.email == email).first()

    if user is None:
        logger.warning("User in token payload not found: email=%s", email)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    logger.debug("Authenticated user_id=%s email=%s", user.id, user.email)
    return user

def send_otp_email(email: str, otp: str):
    subject = "Airthlab OTP Code"
    body = mail_body(otp)

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = settings.SMTP_EMAIL
    msg["To"] = email
    logger.info("Sending OTP email to recipient=%s", email)

    try:
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
            server.sendmail(settings.SMTP_EMAIL, email, msg.as_string())
    except Exception:
        logger.exception("Failed to send OTP email to recipient=%s", email)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send OTP email"
        )

    logger.info("OTP email sent to recipient=%s", email)
