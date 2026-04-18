from pydantic import BaseModel
from datetime import datetime
from app.schemas.common_schema import SuccessResponse

class PlanResponse(BaseModel):
    id: int
    name: str
    user_role: int
    price: float
    duration_days: int

    class Config:
        from_attributes = True


class SubscribeRequest(BaseModel):
    plan_id: int


class SubscriptionResponse(BaseModel):
    id: int
    name: str
    user_role: int
    price: float
    duration_days: int
    start_date: datetime
    end_date: datetime
    status: str
    total_file_size_bytes: int | None = None
    used_file_size_bytes: int | None = None
    remaining_file_size_bytes: int | None = None

    class Config:
        from_attributes = True


PlanListSuccessResponse = SuccessResponse[list[PlanResponse]]
SubscriptionSuccessResponse = SuccessResponse[SubscriptionResponse]
