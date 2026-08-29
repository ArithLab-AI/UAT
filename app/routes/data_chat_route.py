from typing import Literal

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.data_chat_schema import DataChatQueryRequest
from app.services.data_chat_service import (
    DEFAULT_SUGGESTED_QUESTIONS,
    delete_session,
    get_session_messages,
    get_suggested_questions,
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
        include_insight=payload.include_insight,
    )
    # Shape wahi rehta hai; sirf message ab result ko reflect karta hai taaki UI toast me
    # technical text ke bajaye plain-English wajah dikhe.
    status = data.get("status")
    if status == "success":
        message = "Query processed"
    elif status == "clarify":
        message = str(data.get("answer") or "Please clarify your question")
    else:
        message = str(data.get("error") or data.get("answer") or "Query could not be processed")
    return success_response(message, data=data)


@router.get("/{dataset_type}/{dataset_id}/suggested-questions")
def get_suggested_questions_route(
    dataset_type: DatasetType,
    dataset_id: int,
    is_clean: bool = False,
    count: int = DEFAULT_SUGGESTED_QUESTIONS,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Generate a few dummy questions for a dataset and return each with its answer and chart."""
    data = get_suggested_questions(
        db,
        current_user,
        dataset_type=dataset_type,
        dataset_id=dataset_id,
        is_clean=is_clean,
        count=count,
    )
    return success_response("Suggested questions generated", data=data)


@router.get("/{dataset_type}/{dataset_id}/sessions")
def get_sessions(
    dataset_type: DatasetType,
    dataset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = list_sessions(db, current_user, dataset_type=dataset_type, dataset_id=dataset_id)
    return success_response("Sessions fetched", data=data)


@router.delete("/sessions/{session_id}")
def delete_session_route(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a chat session along with all of its messages."""
    data = delete_session(db, current_user, session_id)
    return success_response("Session deleted", data=data)


@router.get("/sessions/{session_id}/messages")
def get_messages(
    session_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = get_session_messages(db, current_user, session_id)
    return success_response("Messages fetched", data=data)
