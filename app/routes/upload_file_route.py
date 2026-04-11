import logging

from fastapi import APIRouter, Depends, File, status, UploadFile
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.models.file_upload_models import UploadedFile
from app.schemas.file_upload_schema import (
    FileUploadListSuccessResponse,
    FileUploadSuccessResponse,
    UploadedFileResponse,
)
from app.services.file_upload_service import (
    SUPPORTED_UPLOAD_EXTENSIONS,
    is_supported_upload_file,
    save_upload_file,
)
from app.utils.responses import error_response, success_response

router = APIRouter(prefix="/files", tags=["files"])
logger = logging.getLogger(__name__)
ANONYMOUS_UPLOAD_EMAIL = "guest-upload@local.invalid"
ANONYMOUS_UPLOAD_USERNAME = "guest_upload"


def _get_anonymous_upload_owner(db: Session) -> User:
    upload_owner = db.query(User).filter(User.email == ANONYMOUS_UPLOAD_EMAIL).first()
    if upload_owner:
        return upload_owner

    upload_owner = User(
        email=ANONYMOUS_UPLOAD_EMAIL,
        username=ANONYMOUS_UPLOAD_USERNAME,
        is_verified=True,
    )
    db.add(upload_owner)
    db.commit()
    db.refresh(upload_owner)
    logger.warning("Created fallback owner for unauthenticated uploads user_id=%s", upload_owner.id)
    return upload_owner


@router.get("", response_model=FileUploadListSuccessResponse)
def list_uploaded_files(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    uploaded_files = (
        db.query(UploadedFile)
        .filter(UploadedFile.created_by_user_id == current_user.id)
        .order_by(UploadedFile.created_at.desc())
        .all()
    )
    logger.info("Fetched %s uploaded files for user_id=%s", len(uploaded_files), current_user.id)
    return success_response(
        "Uploaded files fetched successfully",
        data=[UploadedFileResponse.model_validate(uploaded_file) for uploaded_file in uploaded_files],
    )


@router.post("/upload", response_model=FileUploadSuccessResponse, status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """Upload a CSV/Excel file to S3 and persist the upload metadata."""
    if not is_supported_upload_file(file):
        supported_extensions = ", ".join(sorted(SUPPORTED_UPLOAD_EXTENSIONS))
        raise error_response(
            status_code=400,
            detail=f"Only CSV or Excel files are allowed ({supported_extensions}).",
        )

    try:
        file_id, storage_key, file_url = save_upload_file(file)
    except ValueError as exc:
        raise error_response(status_code=400, detail=str(exc)) from exc

    upload_owner = _get_anonymous_upload_owner(db)
    uploaded_file = UploadedFile(
        file_id=file_id,
        filename=file.filename,
        storage_key=storage_key,
        file_url=file_url,
        status="uploaded",
        created_by_user_id=upload_owner.id,
    )
    db.add(uploaded_file)
    db.commit()
    db.refresh(uploaded_file)

    logger.info("Uploaded file_id=%s to S3 for user_id=%s", file_id, upload_owner.id)
    return success_response(
        "File uploaded successfully",
        status_code=status.HTTP_201_CREATED,
        data=UploadedFileResponse.model_validate(uploaded_file),
    )
