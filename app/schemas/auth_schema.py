from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, validator

from app.enum.user_role_enum import DEFAULT_USER_ROLE
from app.enum.user_role_enum import normalize_user_role
from app.schemas.common_schema import SuccessResponse


class Register(BaseModel):
    email: EmailStr
    username: str
    password: str
    first_name: str
    last_name: str
    user_role: int = DEFAULT_USER_ROLE

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


class ProtectedUserData(BaseModel):
    username: str
    email: EmailStr
    user_role: int
    last_login: Optional[datetime]


UserSuccessResponse = SuccessResponse[UserResponse]
TokenSuccessResponse = SuccessResponse[Token]
ProtectedSuccessResponse = SuccessResponse[ProtectedUserData]
