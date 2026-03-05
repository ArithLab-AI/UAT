import secrets
import logging
from app.models import auth_models
from app.schemas import auth_schema
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.auth import auth
from app.db.database import get_db
from app.config.deps import get_current_user, send_otp_email
from app.config.config import settings
from app.auth.security import hash_password, verify_password
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()
router = APIRouter(prefix="/auth", tags=["Authentication"])
logger = logging.getLogger(__name__)

@router.post("/register", response_model=auth_schema.UserResponse, status_code=201)
def register(payload: auth_schema.Register, db: Session = Depends(get_db)):
    logger.info("Register requested for email=%s username=%s", payload.email, payload.username)

    if len(payload.password) < 8:
        logger.warning("Register rejected for email=%s: weak password", payload.email)
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 8 characters"
        )

    existing_email = db.query(auth_models.User).filter(
        auth_models.User.email == payload.email
    ).first()

    if existing_email:
        logger.warning("Register rejected for email=%s: email already registered", payload.email)
        raise HTTPException(status_code=400, detail="Email already registered")

    existing_username = db.query(auth_models.User).filter(
        auth_models.User.username == payload.username
    ).first()

    if existing_username:
        logger.warning("Register rejected for username=%s: username already taken", payload.username)
        raise HTTPException(status_code=400, detail="Username already taken")

    user = auth_models.User(
        email=payload.email,
        username=payload.username,
        first_name=payload.first_name,
        last_name=payload.last_name,
        password=hash_password(payload.password),
        is_verified=True
    )

    db.add(user)
    db.commit()
    db.refresh(user)
    logger.info("User registered successfully user_id=%s email=%s", user.id, user.email)

    return user

@router.post("/login", response_model=auth_schema.Token)
def login(payload: auth_schema.Login, db: Session = Depends(get_db)):
    logger.info("Login requested for email=%s", payload.email)

    user = db.query(auth_models.User).filter(
        auth_models.User.email == payload.email
    ).first()

    if not user or not user.password:
        logger.warning("Login failed for email=%s: user not found or password missing", payload.email)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

    if not verify_password(payload.password, user.password):
        logger.warning("Login failed for email=%s: invalid credentials", payload.email)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

    if not user.is_verified:
        logger.warning("Login blocked for email=%s: account not verified", payload.email)
        raise HTTPException(
            status_code=403,
            detail="Account not verified"
        )

    user.last_login = datetime.utcnow()
    db.commit()

    access_token = auth.create_access_token({"sub": user.email})
    refresh_token = auth.create_refresh_token({"sub": user.email})
    logger.info("Login successful user_id=%s email=%s", user.id, user.email)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }

@router.post("/request-otp")
def request_otp(payload: auth_schema.RequestOTP, db: Session = Depends(get_db)):
    logger.info("OTP request initiated for email=%s", payload.email)

    user = db.query(auth_models.User).filter(
        auth_models.User.email == payload.email
    ).first()

    if not user:
        logger.warning("OTP request failed: user not found for email=%s", payload.email)
        raise HTTPException(status_code=404, detail="User not found")

    db.query(auth_models.OTP).filter(
        auth_models.OTP.email == payload.email,
        auth_models.OTP.is_used == False
    ).delete()

    otp_code = str(secrets.randbelow(900000) + 100000)

    db_otp = auth_models.OTP(
        email=payload.email,
        otp_code=otp_code,
        expires_at=datetime.utcnow() + timedelta(
            minutes=settings.OTP_EXPIRE_MINUTES
        )
    )

    db.add(db_otp)
    db.commit()

    send_otp_email(payload.email, otp_code)
    logger.info("OTP generated and sent for email=%s", payload.email)

    return {"message": "OTP sent successfully"}

@router.post("/verify-otp", response_model=auth_schema.Token)
def verify_otp(payload: auth_schema.VerifyOTP, db: Session = Depends(get_db)):
    logger.info("OTP verification requested for email=%s", payload.email)

    db_otp = db.query(auth_models.OTP).filter(
        auth_models.OTP.email == payload.email,
        auth_models.OTP.is_used == False
    ).order_by(auth_models.OTP.id.desc()).first()

    if not db_otp:
        logger.warning("OTP verification failed for email=%s: OTP not found", payload.email)
        raise HTTPException(status_code=400, detail="OTP not found")

    if db_otp.expires_at < datetime.utcnow():
        logger.warning("OTP verification failed for email=%s: OTP expired", payload.email)
        raise HTTPException(status_code=400, detail="OTP expired")

    if db_otp.otp_code != payload.otp:
        logger.warning("OTP verification failed for email=%s: invalid OTP", payload.email)
        raise HTTPException(status_code=400, detail="Invalid OTP")

    user = db.query(auth_models.User).filter(
        auth_models.User.email == payload.email
    ).first()

    if not user:
        logger.warning("OTP verification failed for email=%s: user not found", payload.email)
        raise HTTPException(status_code=404, detail="User not found")

    db_otp.is_used = True
    user.last_login = datetime.utcnow()
    db.commit()

    access_token = auth.create_access_token({"sub": user.email})
    refresh_token = auth.create_refresh_token({"sub": user.email})
    logger.info("OTP verification successful for user_id=%s email=%s", user.id, user.email)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }

@router.post("/forgot-password")
def forgot_password(payload: auth_schema.ForgotPassword, db: Session = Depends(get_db)):
    logger.info("Forgot-password requested for email=%s", payload.email)

    user = db.query(auth_models.User).filter(
        auth_models.User.email == payload.email
    ).first()

    if not user:
        logger.warning("Forgot-password failed: user not found for email=%s", payload.email)
        raise HTTPException(status_code=404, detail="User not found")

    # Delete previous unused OTPs
    db.query(auth_models.OTP).filter(
        auth_models.OTP.email == payload.email,
        auth_models.OTP.is_used == False
    ).delete()

    otp_code = str(secrets.randbelow(900000) + 100000)

    db_otp = auth_models.OTP(
        email=user.email,
        otp_code=otp_code,
        expires_at=datetime.utcnow() + timedelta(
            minutes=settings.OTP_EXPIRE_MINUTES
        )
    )

    db.add(db_otp)
    db.commit()

    send_otp_email(user.email, otp_code)
    logger.info("Forgot-password OTP sent for user_id=%s email=%s", user.id, user.email)

    return {"message": "Password reset OTP sent"}

@router.post("/reset-password")
def reset_password(payload: auth_schema.ResetPassword, db: Session = Depends(get_db)):
    logger.info("Reset-password requested for email=%s", payload.email)

    if len(payload.new_password) < 8:
        logger.warning("Reset-password rejected for email=%s: weak password", payload.email)
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 8 characters"
        )

    db_otp = db.query(auth_models.OTP).filter(
        auth_models.OTP.email == payload.email,
        auth_models.OTP.is_used == False
    ).order_by(auth_models.OTP.id.desc()).first()

    if not db_otp:
        logger.warning("Reset-password failed for email=%s: OTP not found", payload.email)
        raise HTTPException(status_code=400, detail="OTP not found")

    if db_otp.expires_at < datetime.utcnow():
        logger.warning("Reset-password failed for email=%s: OTP expired", payload.email)
        raise HTTPException(status_code=400, detail="OTP expired")

    if db_otp.otp_code != payload.otp:
        logger.warning("Reset-password failed for email=%s: invalid OTP", payload.email)
        raise HTTPException(status_code=400, detail="Invalid OTP")

    user = db.query(auth_models.User).filter(
        auth_models.User.email == payload.email
    ).first()

    if not user:
        logger.warning("Reset-password failed: user not found for email=%s", payload.email)
        raise HTTPException(status_code=404, detail="User not found")

    user.password = hash_password(payload.new_password)
    db_otp.is_used = True

    db.commit()
    logger.info("Password reset successful for user_id=%s email=%s", user.id, user.email)

    return {"message": "Password reset successful"}

@router.get("/protected", response_model=auth_schema.ProtectedResponse)
def protected_route(current_user: auth_models.User = Depends(get_current_user)):
    logger.info("Protected route accessed by user_id=%s", current_user.id)
    return {
        "message": f"Welcome back, {current_user.username}",
        "user": {
            "username": current_user.username,
            "email": current_user.email,
            "last_login": current_user.last_login
        }
    }


@router.post("/logout")
def logout(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    token = credentials.credentials
    blacklisted_token = auth_models.TokenBlacklist(token=token)

    db.add(blacklisted_token)
    db.commit()
    logger.info("User logged out and token blacklisted")

    return {"message": "Successfully logged out"}
