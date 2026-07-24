from typing import Literal

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.data_chat_schema import DataChatQueryRequest
from app.services.data_chat_service import (
    get_session_messages,
    list_sessions,
    run_data_chat_query,
)
from app.utils.responses import success_response

router = APIRouter(prefix="/data-chat", tags=["Data Chat"])

DatasetType = Literal["uploaded", "merged"]


@router.post("/{dataset_type}/{dataset_id}/query")
def query_dataset(
    dataset_type: DatasetType,
    dataset_id: int,
    payload: DataChatQueryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Ask a natural-language question about a dataset and get data + a chart spec."""
    data = run_data_chat_query(
        db,
        current_user,
        dataset_type=dataset_type,
        dataset_id=dataset_id,
        question=payload.question,
        is_clean=payload.is_clean,
        session_id=payload.session_id,
    )
    return success_response("Query processed", data=data)


@router.get("/{dataset_type}/{dataset_id}/sessions")
def get_sessions(
    dataset_type: DatasetType,
    dataset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = list_sessions(db, current_user, dataset_type=dataset_type, dataset_id=dataset_id)
    return success_response("Sessions fetched", data=data)


@router.get("/sessions/{session_id}/messages")
def get_messages(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = get_session_messages(db, current_user, session_id)
    return success_response("Messages fetched", data=data)
