from datetime import datetime

from pydantic import BaseModel

from app.schemas.common_schema import SuccessResponse


class UploadedFileResponse(BaseModel):
    file_id: str
    filename: str
    file_url: str
    status: str
    created_at: datetime

    class Config:
        from_attributes = True


FileUploadSuccessResponse = SuccessResponse[UploadedFileResponse]
FileUploadListSuccessResponse = SuccessResponse[list[UploadedFileResponse]]
