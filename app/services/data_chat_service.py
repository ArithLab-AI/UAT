"""Natural-language data chat orchestration.

Flow (LangGraph state machine):
    generate_sql -> execute -> (on SQL error, feed error back and retry, max N) -> summarize

Everything is scoped to a single dataset. Every query + generated SQL + result is
persisted to ``data_chat_messages`` so the full history is queryable.
"""

from __future__ import annotations

import logging
from datetime import datetime
from functools import lru_cache
from typing import Any, Optional, TypedDict

from sqlalchemy.orm import Session

from app.db.database import Base, engine
from app.models.auth_models import User
from app.models.data_chat_models import DataChatMessage, DataChatSession, DataChatSuggestionCache
from app.services.data_chat_chart_service import normalize_chart_spec
from app.services.analysis_service import DatasetSource, _resolve_analysis_source
from app.services.data_chat_llm_service import (
    build_schema_context,
    generate_sample_questions,
    generate_sql,
    summarize_result,
)
from app.services.data_chat_query_engine import (
    MAX_PREVIEW_ROWS,
    SqlValidationError,
    load_dataset_dataframe,
    run_sql,
)
from app.utils.responses import error_response

logger = logging.getLogger(__name__)

MAX_SQL_ATTEMPTS = 3
DEFAULT_SUGGESTED_QUESTIONS = 5
SUGGESTIONS_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours


@lru_cache(maxsize=1)
def ensure_data_chat_tables() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[
            DataChatSession.__table__,
            DataChatMessage.__table__,
            DataChatSuggestionCache.__table__,
        ],
    )


class _ChatState(TypedDict, total=False):
    question: str
    schema_context: str
    df: Any
    history: list[dict[str, str]]
    sql: str
    columns: list[str]
    rows: list[dict[str, Any]]
    error: Optional[str]
    attempts: int
    status: str  # success | error | clarify
    answer: str
    chart: Optional[dict[str, Any]]
    tokens: int


def _node_generate_sql(state: _ChatState) -> _ChatState:
    payload, tokens = generate_sql(
        state["question"],
        state["schema_context"],
        history=state.get("history"),
        error_feedback=state.get("error"),
    )
    state["tokens"] = state.get("tokens", 0) + tokens
    state["attempts"] = state.get("attempts", 0) + 1

    if payload.get("needs_clarification"):
        state["status"] = "clarify"
        state["answer"] = str(payload.get("clarification") or "Could you clarify your question?")
        state["sql"] = ""
        return state

    state["sql"] = str(payload.get("sql") or "").strip()
    state["error"] = None
    return state


def _node_execute(state: _ChatState) -> _ChatState:
    try:
        columns, rows = run_sql(state["df"], state["sql"])
        state["columns"] = columns
        state["rows"] = rows
        state["error"] = None
        state["status"] = "success"
    except (SqlValidationError, Exception) as exc:  # noqa: BLE001 - feed error back to the LLM
        state["error"] = f"{type(exc).__name__}: {exc}"
        state["status"] = "error"
    return state


def _node_summarize(state: _ChatState) -> _ChatState:
    payload, tokens = summarize_result(state["question"], state["columns"], state["rows"])
    state["tokens"] = state.get("tokens", 0) + tokens
    state["answer"] = str(payload.get("answer") or "Here are the results.")
    state["chart"] = normalize_chart_spec(
        state["question"],
        state.get("columns", []) or [],
        state.get("rows", []) or [],
        payload.get("chart") if isinstance(payload, dict) else None,
    )
    return state


def _route_after_sql(state: _ChatState) -> str:
    return "clarify" if state.get("status") == "clarify" else "execute"


def _route_after_execute(state: _ChatState) -> str:
    if state.get("status") == "success":
        return "summarize"
    if state.get("attempts", 0) < MAX_SQL_ATTEMPTS:
        return "retry"
    return "fail"


@lru_cache(maxsize=1)
def _build_graph():
    from langgraph.graph import END, StateGraph

    graph = StateGraph(_ChatState)
    graph.add_node("generate_sql", _node_generate_sql)
    graph.add_node("execute", _node_execute)
    graph.add_node("summarize", _node_summarize)

    graph.set_entry_point("generate_sql")
    graph.add_conditional_edges(
        "generate_sql", _route_after_sql, {"execute": "execute", "clarify": END}
    )
    graph.add_conditional_edges(
        "execute",
        _route_after_execute,
        {"summarize": "summarize", "retry": "generate_sql", "fail": END},
    )
    graph.add_edge("summarize", END)
    return graph.compile()


def _get_or_create_session(
    db: Session,
    current_user: User,
    source: DatasetSource,
    session_id: Optional[str],
    question: str,
) -> DataChatSession:
    if session_id:
        session = (
            db.query(DataChatSession)
            .filter(
                DataChatSession.id == session_id,
                DataChatSession.created_by_user_id == current_user.id,
            )
            .first()
        )
        if session is None:
            raise error_response(status_code=404, detail="Chat session not found")
        return session

    session = DataChatSession(
        source_dataset_id=source.dataset_id,
        source_type=source.dataset_type,
        is_clean=source.is_clean,
        created_by_user_id=current_user.id,
        dataset_name=source.dataset_name,
        title=question[:120],
    )
    db.add(session)
    db.flush()
    return session


def _session_history(db: Session, session_id: str) -> list[dict[str, str]]:
    rows = (
        db.query(DataChatMessage)
        .filter(
            DataChatMessage.session_id == session_id,
            DataChatMessage.status == "success",
        )
        .order_by(DataChatMessage.created_at.desc())
        .limit(3)
        .all()
    )
    return [{"q": r.nl_query, "sql": r.generated_sql or ""} for r in reversed(rows)]


def run_data_chat_query(
    db: Session,
    current_user: User,
    *,
    dataset_type: str,
    dataset_id: int,
    question: str,
    is_clean: bool,
    session_id: Optional[str],
) -> dict[str, Any]:
    ensure_data_chat_tables()

    source = _resolve_analysis_source(
        db, current_user, dataset_type=dataset_type, dataset_id=dataset_id, is_clean=is_clean
    )

    session = _get_or_create_session(db, current_user, source, session_id, question)
    history = _session_history(db, session.id)

    df = load_dataset_dataframe(source)
    if df.empty:
        raise error_response(status_code=400, detail="Dataset has no data to query.")

    schema_context = build_schema_context(df)

    initial: _ChatState = {
        "question": question,
        "schema_context": schema_context,
        "df": df,
        "history": history,
        "attempts": 0,
        "tokens": 0,
    }

    try:
        final: _ChatState = _build_graph().invoke(initial)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Data chat graph failed")
        final = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "answer": "Sorry, I could not process that question.",
            "attempts": initial.get("attempts", 1) or 1,
            "tokens": initial.get("tokens", 0),
        }

    status = final.get("status", "error")
    columns = final.get("columns", []) or []
    rows = final.get("rows", []) or []

    message = DataChatMessage(
        session_id=session.id,
        source_dataset_id=source.dataset_id,
        source_type=source.dataset_type,
        created_by_user_id=current_user.id,
        nl_query=question,
        generated_sql=final.get("sql") or None,
        assistant_text=final.get("answer"),
        chart_spec=final.get("chart"),
        result_preview=rows[:MAX_PREVIEW_ROWS] if rows else None,
        row_count=len(rows),
        status=status,
        error_message=final.get("error"),
        attempts=int(final.get("attempts", 1) or 1),
        tokens_used=int(final.get("tokens", 0) or 0),
    )
    db.add(message)
    session.updated_at = datetime.utcnow()
    db.add(session)
    db.commit()
    db.refresh(message)

    return {
        "session_id": session.id,
        "message_id": message.id,
        "status": status,
        "answer": final.get("answer") or "",
        "sql": final.get("sql") or None,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "chart_spec": final.get("chart"),
        "attempts": message.attempts,
        "error": final.get("error"),
    }


def get_suggested_questions(
    db: Session,
    current_user: User,
    *,
    dataset_type: str,
    dataset_id: int,
    is_clean: bool,
    count: int = DEFAULT_SUGGESTED_QUESTIONS,
) -> list[dict[str, Any]]:
    """Generate a handful of dummy questions for a dataset and answer each with its chart,
    so the frontend can show a preview of what data chat can do without the user typing anything.
    Results are cached per dataset so repeated hits skip the LLM entirely."""
    ensure_data_chat_tables()

    cache_row = (
        db.query(DataChatSuggestionCache)
        .filter(
            DataChatSuggestionCache.source_dataset_id == dataset_id,
            DataChatSuggestionCache.source_type == dataset_type,
            DataChatSuggestionCache.is_clean == is_clean,
        )
        .first()
    )
    cache_age = (
        (datetime.utcnow() - cache_row.updated_at).total_seconds() if cache_row else None
    )
    cached_suggestions = cache_row.suggestions if cache_row else None
    if cached_suggestions and cache_age is not None and cache_age < SUGGESTIONS_CACHE_TTL_SECONDS:
        if len(cached_suggestions) >= count:
            return cached_suggestions[:count]

    source = _resolve_analysis_source(
        db, current_user, dataset_type=dataset_type, dataset_id=dataset_id, is_clean=is_clean
    )

    df = load_dataset_dataframe(source)
    if df.empty:
        raise error_response(status_code=400, detail="Dataset has no data to query.")

    schema_context = build_schema_context(df)
    questions, _ = generate_sample_questions(schema_context, count)

    results: list[dict[str, Any]] = []
    for question in questions:
        initial: _ChatState = {
            "question": question,
            "schema_context": schema_context,
            "df": df,
            "history": [],
            "attempts": 0,
            "tokens": 0,
        }
        try:
            final: _ChatState = _build_graph().invoke(initial)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Data chat suggestion graph failed")
            final = {"status": "error", "error": f"{type(exc).__name__}: {exc}", "answer": ""}

        status = final.get("status", "error")
        if status != "success":
            continue

        chart = final.get("chart") or {}
        results.append(
            {
                "question": question,
                "chart_type": chart.get("type") or "table",
            }
        )

    if results:
        if cache_row is None:
            cache_row = DataChatSuggestionCache(
                source_dataset_id=dataset_id,
                source_type=dataset_type,
                is_clean=is_clean,
                suggestions=results,
            )
            db.add(cache_row)
        else:
            cache_row.suggestions = results
            cache_row.updated_at = datetime.utcnow()
        db.commit()

    return results

    return results


def get_session_messages(
    db: Session, current_user: User, session_id: str
) -> list[dict[str, Any]]:
    session = (
        db.query(DataChatSession)
        .filter(
            DataChatSession.id == session_id,
            DataChatSession.created_by_user_id == current_user.id,
        )
        .first()
    )
    if session is None:
        raise error_response(status_code=404, detail="Chat session not found")

    messages = (
        db.query(DataChatMessage)
        .filter(DataChatMessage.session_id == session_id)
        .order_by(DataChatMessage.created_at.asc())
        .all()
    )
    return [
        {
            "message_id": m.id,
            "question": m.nl_query,
            "answer": m.assistant_text,
            "sql": m.generated_sql,
            "chart_spec": m.chart_spec,
            "rows": m.result_preview or [],
            "row_count": m.row_count,
            "status": m.status,
            "error": m.error_message,
            "created_at": m.created_at.isoformat() if m.created_at else None,
        }
        for m in messages
    ]


def list_sessions(
    db: Session, current_user: User, *, dataset_type: str, dataset_id: int
) -> list[dict[str, Any]]:
    sessions = (
        db.query(DataChatSession)
        .filter(
            DataChatSession.created_by_user_id == current_user.id,
            DataChatSession.source_dataset_id == dataset_id,
            DataChatSession.source_type == dataset_type,
        )
        .order_by(DataChatSession.updated_at.desc())
        .all()
    )
    return [
        {
            "session_id": s.id,
            "title": s.title,
            "dataset_name": s.dataset_name,
            "is_clean": s.is_clean,
            "created_at": s.created_at.isoformat() if s.created_at else None,
            "updated_at": s.updated_at.isoformat() if s.updated_at else None,
        }
        for s in sessions
    ]
