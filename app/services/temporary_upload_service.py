import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from secrets import token_urlsafe

from app.utils.responses import error_response

TEMP_UPLOAD_TTL_MINUTES = 30
TEMP_UPLOAD_DIR = Path(tempfile.gettempdir()) / "analys_pro_pending_uploads"


@dataclass(frozen=True)
class TemporaryUpload:
    token: str
    original_file_name: str
    temp_file_path: str
    available_sheets: list[str]
    created_at: datetime
    expires_at: datetime
    user_id: int
    file_size: int


def _metadata_path(token: str) -> Path:
    return TEMP_UPLOAD_DIR / f"{token}.json"


def _file_path(token: str, suffix: str) -> Path:
    return TEMP_UPLOAD_DIR / f"{token}{suffix}"


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _read_metadata(token: str) -> TemporaryUpload:
    metadata_path = _metadata_path(token)
    if not metadata_path.exists():
        raise error_response(status_code=404, detail="Invalid or expired upload token")

    try:
        data = json.loads(metadata_path.read_text(encoding="utf-8"))
        return TemporaryUpload(
            token=data["token"],
            original_file_name=data["original_file_name"],
            temp_file_path=data["temp_file_path"],
            available_sheets=list(data["available_sheets"]),
            created_at=_parse_datetime(data["created_at"]),
            expires_at=_parse_datetime(data["expires_at"]),
            user_id=int(data["user_id"]),
            file_size=int(data["file_size"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        delete_temporary_upload(token)
        raise error_response(status_code=404, detail="Invalid or expired upload token") from exc


def cleanup_expired_temporary_uploads() -> None:
    TEMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.utcnow()
    for metadata_path in TEMP_UPLOAD_DIR.glob("*.json"):
        try:
            data = json.loads(metadata_path.read_text(encoding="utf-8"))
            token = data["token"]
            expires_at = _parse_datetime(data["expires_at"])
        except (KeyError, ValueError, json.JSONDecodeError):
            metadata_path.unlink(missing_ok=True)
            continue

        if expires_at <= now:
            delete_temporary_upload(token)


def store_temporary_upload(
    *,
    content: bytes,
    original_file_name: str,
    available_sheets: list[str],
    user_id: int,
) -> TemporaryUpload:
    cleanup_expired_temporary_uploads()
    TEMP_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    suffix = Path(original_file_name).suffix.lower()
    token = token_urlsafe(32)
    temp_file_path = _file_path(token, suffix)
    temp_file_path.write_bytes(content)

    created_at = datetime.utcnow()
    upload = TemporaryUpload(
        token=token,
        original_file_name=original_file_name,
        temp_file_path=str(temp_file_path),
        available_sheets=available_sheets,
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=TEMP_UPLOAD_TTL_MINUTES),
        user_id=user_id,
        file_size=len(content),
    )
    _metadata_path(token).write_text(
        json.dumps(
            {
                "token": upload.token,
                "original_file_name": upload.original_file_name,
                "temp_file_path": upload.temp_file_path,
                "available_sheets": upload.available_sheets,
                "created_at": upload.created_at.isoformat(),
                "expires_at": upload.expires_at.isoformat(),
                "user_id": upload.user_id,
                "file_size": upload.file_size,
            }
        ),
        encoding="utf-8",
    )
    return upload


def validate_temporary_upload(*, token: str, user_id: int) -> TemporaryUpload:
    cleanup_expired_temporary_uploads()
    upload = _read_metadata(token)
    if upload.expires_at <= datetime.utcnow():
        delete_temporary_upload(token)
        raise error_response(status_code=404, detail="Invalid or expired upload token")
    if upload.user_id != user_id:
        raise error_response(status_code=404, detail="Invalid or expired upload token")
    if not Path(upload.temp_file_path).exists():
        delete_temporary_upload(token)
        raise error_response(status_code=404, detail="Invalid or expired upload token")
    return upload


def delete_temporary_upload(token: str) -> None:
    metadata_path = _metadata_path(token)
    if metadata_path.exists():
        try:
            data = json.loads(metadata_path.read_text(encoding="utf-8"))
            temp_file_path = data.get("temp_file_path")
        except json.JSONDecodeError:
            temp_file_path = None
        if temp_file_path:
            Path(temp_file_path).unlink(missing_ok=True)
    metadata_path.unlink(missing_ok=True)
