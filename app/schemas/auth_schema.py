from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, validator

from app.enum.user_role_enum import normalize_user_role


class Register(BaseModel):
    email: EmailStr
    username: str
    password: str
    first_name: str
    last_name: str
    user_role: int

    @validator("user_role", pre=True)
    def validate_user_role(cls, value):
        return normalize_user_role(value)


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
    user_role: int
    created_at: datetime
    last_login: datetime | None

    class Config:
        from_attributes = True


class ProtectedUser(BaseModel):
    username: str
    email: EmailStr
    user_role: int
    last_login: Optional[datetime]

    class Config:
        from_attributes = True


class ProtectedResponse(BaseModel):
    message: str
    user: ProtectedUser
