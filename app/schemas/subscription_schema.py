from pydantic import BaseModel
from datetime import datetime

class PlanResponse(BaseModel):
    id: int
    name: str
    price: float
    duration_days: int

    class Config:
        from_attributes = True

class SubscribeRequest(BaseModel):
    plan_id: int

class SubscriptionResponse(BaseModel):
    id: int
    plan: PlanResponse
    start_date: datetime
    end_date: datetime
    status: str
    remaining_days: int

    class Config:
        from_attributes = True
