from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, Field, model_validator, validator

from app.enum.user_role_enum import DEFAULT_USER_ROLE, FREE_USER
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
        normalized_role = normalize_user_role(value)
        if normalized_role != FREE_USER:
            raise ValueError("New users must register with the Free plan")
        return normalized_role


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


class RefreshToken(BaseModel):
    refresh_token: str


class MyAccountUpdate(BaseModel):
    email: EmailStr | None = None
    username: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def require_update_field(self):
        if self.email is None and self.username is None:
            raise ValueError("Email or username is required")
        if self.username is not None and not self.username.strip():
            raise ValueError("Username cannot be empty")
        return self


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
    retention_plan: Optional[str] = None
    retention_pending_days: Optional[int] = None
    retention_pending_hours: Optional[int] = None
    next_file_expiry_at: Optional[datetime] = None
    next_expiring_file_name: Optional[str] = None
    retention_file_count: int = 0


UserSuccessResponse = SuccessResponse[UserResponse]
TokenSuccessResponse = SuccessResponse[Token]
ProtectedSuccessResponse = SuccessResponse[ProtectedUserData]
