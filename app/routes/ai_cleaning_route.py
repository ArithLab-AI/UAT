import os
from typing import Literal

from fastapi import APIRouter, Depends, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.ai_cleaning_schema import (
    AICleaningAnalysisRequest,
    AICleaningAnalysisSuccessResponse,
    AICleaningBatchRunRequest,
    AICleaningBatchRunSuccessResponse,
    AICleaningDetailSuccessResponse,
    AICleaningRunRequest,
    AICleaningRunSuccessResponse,
)
from app.services.ai_cleaning_service import (
    analyze_ai_cleaning_output,
    get_ai_cleaned_download_payload,
    get_ai_cleaning_detail,
    get_ai_cleaning_detail_by_job_id,
    run_ai_cleaning,
    run_ai_cleaning_batch,
)
from app.utils.responses import success_response

router = APIRouter(prefix="/ai-cleaning", tags=["AI Cleaning"])


@router.post(
    "",
    response_model=AICleaningRunSuccessResponse,
    response_model_exclude_none=True,
    status_code=201,
)
def create_ai_cleaning_job(
    payload: AICleaningRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = run_ai_cleaning(
        db,
        current_user=current_user,
        dataset_id=payload.dataset_id,
        dataset_type=payload.dataset_type,
        suggestion_id=payload.suggestion_id,
        source_ai_job_id=payload.source_ai_job_id,
        custom_prompt=payload.custom_prompt,
    )
    return success_response(
        "AI cleaning completed successfully",
        status_code=201,
        data=data,
    )


@router.post(
    "/batch",
    response_model=AICleaningBatchRunSuccessResponse,
    response_model_exclude_none=True,
    status_code=201,
)
def create_ai_cleaning_batch_job(
    payload: AICleaningBatchRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = run_ai_cleaning_batch(
        db,
        current_user=current_user,
        dataset_id=payload.dataset_id,
        dataset_type=payload.dataset_type,
        suggestion_ids=payload.suggestion_ids,
        source_ai_job_id=payload.source_ai_job_id,
    )
    return success_response(
        "AI cleaning completed successfully for all selected suggestions",
        status_code=201,
        data=data,
    )


@router.get(
    "/{dataset_id:int}",
    response_model=AICleaningDetailSuccessResponse,
    response_model_exclude_none=True,
)
def get_ai_cleaning_job(
    dataset_id: int,
    dataset_type: Literal["uploaded", "merged"] | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return success_response(
        "AI cleaning data fetched successfully",
        data=get_ai_cleaning_detail(
            db,
            current_user=current_user,
            dataset_id=dataset_id,
            dataset_type=dataset_type,
        ),
    )


@router.get(
    "/job/{job_id}",
    response_model=AICleaningDetailSuccessResponse,
    response_model_exclude_none=True,
)
def get_ai_cleaning_job_by_job_id(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return success_response(
        "AI cleaning job fetched successfully",
        data=get_ai_cleaning_detail_by_job_id(
            db,
            current_user=current_user,
            job_id=job_id,
        ),
    )


@router.get("/{job_id}/download")
def download_ai_cleaned_file(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    path, filename = get_ai_cleaned_download_payload(
        db,
        current_user=current_user,
        job_id=job_id,
    )
    return FileResponse(
        path,
        media_type="text/csv",
        filename=filename,
        background=BackgroundTask(lambda: os.path.exists(path) and os.remove(path)),
    )


# @router.post(
#     "/{job_id}/analysis",
#     response_model=AICleaningAnalysisSuccessResponse,
#     response_model_exclude_none=True,
# )
# def analyze_ai_cleaning_job(
#     job_id: str,
#     payload: AICleaningAnalysisRequest,
#     db: Session = Depends(get_db),
#     current_user: User = Depends(get_current_user),
# ):
#     data = analyze_ai_cleaning_output(
#         db,
#         current_user=current_user,
#         job_id=job_id,
#         use_llm=payload.use_llm,
#         llm_provider=payload.llm_provider,
#         llm_model=payload.llm_model,
#     )
#     if not payload.include_profile:
#         data["dataset_profile"] = None
#     return success_response(
#         "AI-cleaned dataset analysis completed successfully",
#         data=data,
#     )
