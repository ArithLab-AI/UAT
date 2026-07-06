from typing import Generic, TypeVar
from pydantic.generics import GenericModel

T = TypeVar("T")

class SuccessResponse(GenericModel, Generic[T]):
    status_code: int
    message: str
    data: T

MessageSuccessResponse = SuccessResponse[dict | None]
