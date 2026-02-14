from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app import models, schemas, auth
from app.database import get_db
from app.deps import get_current_user, send_otp_email
from datetime import datetime, timedelta
from app.config import settings
import random

router = APIRouter()

@router.post("/request-otp")
def request_otp(email: str, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.email == email).first()

    if not user:
        user = models.User(email=email)
        db.add(user)
        db.commit()

    otp_code = str(random.randint(100000, 999999))
    expires_at = datetime.utcnow() + timedelta(minutes=settings.OTP_EXPIRE_MINUTES)

    db_otp = models.OTP(email=email, otp_code=otp_code, expires_at=expires_at)
    db.add(db_otp)
    db.commit()

    send_otp_email(email, otp_code)

    return {"message": "OTP sent to your email"}

@router.post("/verify-otp", response_model=schemas.Token)
def verify_otp(email: str, otp: str, db: Session = Depends(get_db)):

    db_otp = (
        db.query(models.OTP)
        .filter(models.OTP.email == email)
        .order_by(models.OTP.id.desc())
        .first()
    )

    if not db_otp:
        raise HTTPException(status_code=400, detail="OTP not found")

    if db_otp.otp_code != otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    if db_otp.expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail="OTP expired")

    access_token = auth.create_access_token({"sub": email})

    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/protected")
def protected_route(current_user: str = Depends(get_current_user)):
    return {"message": f"Hello {current_user}, you are authenticated"}

@router.post("/logout")
def logout():
    return {"message": "Logout successful (client should discard token)"}
