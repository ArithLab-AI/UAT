from typing import Any, Optional

from pydantic import BaseModel, Field


class DataChatQueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    session_id: Optional[str] = None
    is_clean: bool = False
    # Costs one extra LLM call per query; send false to skip the detailed insight.
    include_insight: bool = True


class ChartMapping(BaseModel):
    category: Optional[str] = None
    value: list[str] = Field(default_factory=list)
    series: Optional[str] = None
    x: Optional[str] = None
    y: list[str] = Field(default_factory=list)
    size: Optional[str] = None


class ChartOptions(BaseModel):
    orientation: Optional[str] = None
    smooth: bool = False
    stack: Optional[str] = None
    dual_axis: bool = False
    radius: Optional[list[str]] = None
    item_border_radius: Optional[int] = None
    helper_series_name: Optional[str] = None
    delta_series_name: Optional[str] = None
    visual_map: Optional[dict[str, Any]] = None


class ChartSpec(BaseModel):
    format_version: int = 1
    type: str = "table"
    title: Optional[str] = None
    # AI-written headline for the question and its result, e.g. "Sales Peaked in March".
    summary_title: Optional[str] = None
    mapping: ChartMapping = Field(default_factory=ChartMapping)
    options: ChartOptions = Field(default_factory=ChartOptions)
    payload: dict[str, Any] = Field(default_factory=dict)


class DataChatQueryResponse(BaseModel):
    session_id: str
    message_id: str
    status: str  # success | error | clarify
    answer: str
    sql: Optional[str] = None
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    chart_spec: Optional[ChartSpec] = None
    insight: Optional[dict[str, Any]] = None
    attempts: int = 1
    error: Optional[str] = None
