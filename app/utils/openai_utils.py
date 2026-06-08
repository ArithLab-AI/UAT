from functools import lru_cache
from typing import Any

from app.config.config import settings

DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"


class OpenAIConnectionError(RuntimeError):
    pass


def format_openai_exception(exc: Exception) -> str:
    parts = [f"{type(exc).__name__}: {exc}"]

    cause = getattr(exc, "__cause__", None)
    if cause:
        parts.append(f"cause={type(cause).__name__}: {cause}")

    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if status_code is not None:
            parts.append(f"status_code={status_code}")

    body = getattr(exc, "body", None)
    if body:
        parts.append(f"body={body}")

    request = getattr(exc, "request", None)
    request_url = getattr(request, "url", None)
    if request_url:
        parts.append(f"url={request_url}")

    return " | ".join(str(part) for part in parts if part)


def _import_openai():
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as exc:
        raise OpenAIConnectionError("langchain-openai is required to use the OpenAI LLM integration.") from exc
    return ChatOpenAI


def get_openai_api_key() -> str:
    api_key = (settings.OPENAI_API_KEY or "").strip()
    if not api_key:
        raise OpenAIConnectionError("Set OPENAI_API_KEY to use the OpenAI LLM integration.")
    return api_key


@lru_cache(maxsize=4)
def get_openai_client():
    ChatOpenAI = _import_openai()
    base_url = (settings.OPENAI_BASE_URL or "").strip() or DEFAULT_OPENAI_BASE_URL
    model = (settings.UAT_ANALYSIS_LLM_MODEL or "gpt-4.1-mini").strip()
    return ChatOpenAI(
        api_key=get_openai_api_key(),
        base_url=base_url,
        model=model,
        temperature=float(settings.UAT_ANALYSIS_LLM_TEMPERATURE),
        max_tokens=int(settings.UAT_ANALYSIS_LLM_MAX_TOKENS),
    )


def get_openai_inference_status(
    *,
    model_id: str,
    prompt: str | None = None,
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "provider": "openai",
        "model_id": model_id,
        "api_key_configured": False,
        "client_ready": False,
        "inference_ok": False,
        "response_text": None,
        "error": None,
    }

    try:
        client = get_openai_client()
        status["api_key_configured"] = True
        status["client_ready"] = True
    except Exception as exc:
        status["error"] = str(exc)
        return status

    prompt_text = (prompt or "Reply with OK").strip()
    if not prompt_text:
        prompt_text = "Reply with OK"

    try:
        response = client.bind(model=model_id).invoke(prompt_text)
    except Exception as exc:
        status["error"] = f"OpenAI inference failed: {format_openai_exception(exc)}"
        return status

    raw_text = str(getattr(response, "content", "") or "").strip()
    if not raw_text:
        status["error"] = "OpenAI inference returned an empty response."
        return status

    status["inference_ok"] = True
    status["response_text"] = raw_text
    return status
