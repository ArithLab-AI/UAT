from fastapi import APIRouter

from app.schemas.analysis_schema import LLMTestRequest, LLMTestSuccessResponse
from app.services.analysis_llm_service import get_analysis_llm_config
from app.utils.openai_utils import get_openai_inference_status
from app.utils.responses import error_response, success_response

router = APIRouter(prefix="/test-llm", tags=["LLM Test"])


@router.post(
    "",
    response_model=LLMTestSuccessResponse,
    response_model_exclude_none=True,
)
def test_llm(
    payload: LLMTestRequest,
):
    llm_config = get_analysis_llm_config()
    status = get_openai_inference_status(
        model_id=llm_config.model,
        prompt=payload.prompt,
    )

    if status.get("error"):
        raise error_response(status_code=503, detail=f"LLM test failed: {status['error']}")

    return success_response(
        "LLM test completed successfully",
        data=status,
    )
