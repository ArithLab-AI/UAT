from typing import Any, Optional

from pydantic import BaseModel, Field


class DataChatQueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    session_id: Optional[str] = None
    is_clean: bool = False


class ChartSpec(BaseModel):
    type: str = "table"  # table | bar | line | pie | scatter | kpi
    x: Optional[str] = None
    y: Optional[list[str]] = None
    series: Optional[str] = None
    title: Optional[str] = None


class DataChatQueryResponse(BaseModel):
    session_id: str
    message_id: str
    status: str  # success | error | clarify
    answer: str
    sql: Optional[str] = None
    columns: list[str] = []
    rows: list[dict[str, Any]] = []
    row_count: int = 0
    chart_spec: Optional[ChartSpec] = None
    attempts: int = 1
    error: Optional[str] = None
