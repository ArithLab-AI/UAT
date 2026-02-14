from fastapi import Depends, HTTPException, status
from jose import jwt, JWTError
from app.config import settings
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import smtplib
from email.mime.text import MIMEText
from app.mail_body import mail_body

security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials

    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        username = payload.get("username") if payload.get("username") else payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        return username

    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

def send_otp_email(email: str, otp: str):
    subject = "Airthlab OTP Code"
    body = mail_body(otp)

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = settings.SMTP_EMAIL
    msg["To"] = email
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        server.sendmail(settings.SMTP_EMAIL, email, msg.as_string())
        server.quit()
