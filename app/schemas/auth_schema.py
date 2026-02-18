from pydantic import BaseModel, EmailStr
from datetime import datetime
from typing import Optional

class Register(BaseModel):
    email: EmailStr
    username: str
    password: str

class Login(BaseModel):
    email: EmailStr
    password: str

class RequestOTP(BaseModel):
    email: EmailStr

class VerifyOTP(BaseModel):
    email: EmailStr
    otp: str

class ForgotPassword(BaseModel):
    email: EmailStr

class ResetPassword(BaseModel):
    email: EmailStr
    otp: str
    new_password: str

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str


class UserResponse(BaseModel):
    email: EmailStr
    username: str
    created_at: datetime
    last_login: datetime | None

    class Config:
        from_attributes = True

class ProtectedUser(BaseModel):
    username: str
    email: EmailStr
    last_login: Optional[datetime]

    class Config:
        from_attributes = True

class ProtectedResponse(BaseModel):
    message: str
    user: ProtectedUser