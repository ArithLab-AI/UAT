from typing import Any

from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

def success_response(
    message: str,
    *,
    status_code: int = 200,
    data: Any = None,
) -> dict[str, Any]:
    return {
        "status_code": status_code,
        "message": message,
        "data": data,
    }

def error_response(*, status_code: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status_code, detail=detail)


def http_exception_response(exc: HTTPException) -> JSONResponse:
    detail = exc.detail

    if isinstance(detail, dict):
        fields = detail.get("fields", [])
        message = detail.get("message", "Request failed")
    else:
        fields = []
        message = str(detail)

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status_code": exc.status_code,
            "fields": fields,
            "message": message,
        },
        headers=exc.headers,
    )


def validation_error_response(
    exc: RequestValidationError,
    *,
    status_code: int = 422,
) -> JSONResponse:
    fields: list[str] = []
    messages: list[str] = []

    for error in exc.errors():
        location = error.get("loc", ())
        field_parts = [str(part) for part in location if part != "body"]
        field_name = ".".join(field_parts) if field_parts else "request"
        fields.append(field_name)
        messages.append(error.get("msg", "Invalid request"))

    return JSONResponse(
        status_code=status_code,
        content={
            "status_code": status_code,
            "fields": list(dict.fromkeys(fields)),
            "message": "; ".join(dict.fromkeys(messages)),
        },
    )
