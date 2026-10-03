from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")

class SuccessResponse(BaseModel, Generic[T]):
    status_code: int
    message: str
    data: T

MessageSuccessResponse = SuccessResponse[dict | None]
