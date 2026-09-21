"""Natural-language data chat orchestration.

Flow (LangGraph state machine):
    generate_sql -> execute -> (on SQL error, feed error back and retry, max N) -> summarize -> insight

Everything is scoped to a single dataset. Every query + generated SQL + result is
persisted to ``data_chat_messages`` so the full history is queryable.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from functools import lru_cache
from typing import Any, Optional, TypedDict

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.config.config import settings
from app.db.database import Base, engine
from app.models.auth_models import User
from app.models.data_chat_models import DataChatMessage, DataChatSession, DataChatSuggestionCache
from app.services.data_chat_chart_service import detect_explicit_chart_type, normalize_chart_spec
from app.services.analysis_service import DatasetSource, _resolve_analysis_source
from app.services.data_chat_insight_service import (
    build_fallback_insight,
    compute_result_statistics,
)
from app.services.data_chat_llm_service import (
    build_schema_context,
    generate_insight,
    generate_sample_questions,
    generate_sql,
    summarize_result,
)
from app.services.data_chat_query_engine import (
    MAX_PREVIEW_ROWS,
    SqlValidationError,
    load_dataset_dataframe,
    run_sql,
    to_user_message,
)
from app.utils.responses import error_response

logger = logging.getLogger(__name__)

MAX_SQL_ATTEMPTS = 3


def _should_expose_sql(debug_sql: bool) -> bool:
    """SQL response me tabhi jaata hai jab env flag ON ho AUR caller ne maanga ho."""
    return bool(debug_sql) and bool(settings.UAT_DATA_CHAT_EXPOSE_SQL)
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
    # create_all only creates missing tables, never adds a column to one that already
    # exists, so a database created before `insight` needs the column added by hand.
    inspector = inspect(engine)
    if not inspector.has_table("data_chat_messages"):
        return
    existing = {column["name"] for column in inspector.get_columns("data_chat_messages")}
    if "insight" not in existing:
        with engine.begin() as connection:
            connection.execute(text("ALTER TABLE data_chat_messages ADD COLUMN insight JSON"))
        logger.info("Added data_chat_messages.insight column")


class _ChatState(TypedDict, total=False):
    question: str
    schema_context: str
    df: Any
    history: list[dict[str, str]]
    sql: str
    columns: list[str]
    rows: list[dict[str, Any]]
    total_rows: int
    error: Optional[str]
    attempts: int
    status: str  # success | error | clarify
    answer: str
    chart: Optional[dict[str, Any]]
    insight: Optional[dict[str, Any]]
    want_insight: bool
    tokens: int


def _previous_sql_for_chart_change(state: _ChatState) -> str:
    """SQL of the last successful turn, but only when this turn is just a chart-type change.

    "Ise pie chart bana do" jaise follow-ups me naya SQL banane ki zaroorat nahi hai. Agar LLM
    phir bhi clarification maange ya khaali SQL de, to pichla SQL dobara chala dete hain warna
    user ko chart ke bajaye clarification milta hai.
    """
    if not detect_explicit_chart_type(state.get("question", "")):
        return ""
    for entry in reversed(state.get("history") or []):
        previous_sql = str(entry.get("sql") or "").strip()
        if previous_sql:
            return previous_sql
    return ""



# The model's own clarification text is kept only when it actually asks the user something
# useful. Two kinds get replaced: text that leaks internals ("regarding the previous SQL
# query"), and empty filler ("what information are you looking for?") that tells the user
# nothing they did not already know.
_CLARIFICATION_JARGON = re.compile(
    r"\bsql\b|\bschema\b|\bdataset table\b|previous query|generated query", re.I
)
_CLARIFICATION_FILLER = re.compile(
    r"what (specific |kind of |sort of )?(information|analysis|data|insight|detail)"
    r"|what (would|do) you (like|want)"
    r"|could you (please )?(clarify|specify|elaborate|provide)"
    r"|what are you looking for"
    r"|please (clarify|specify)",
    re.I,
)
MAX_CLARIFICATION_COLUMNS = 8


def _fallback_clarification(state: _ChatState) -> str:
    """Generic ask-again message, naming this dataset's own columns.

    Listing the real columns turns a dead end into something actionable: the user can
    see what there is to ask about instead of guessing.
    """
    frame = state.get("df")
    columns = [str(column) for column in getattr(frame, "columns", [])]
    if not columns:
        return (
            "I couldn't tell what you're asking about. Please clarify a bit more - rephrase "
            "your question and name what you would like to see from this dataset."
        )

    shown = columns[:MAX_CLARIFICATION_COLUMNS]
    listed = ", ".join(shown)
    if len(columns) > len(shown):
        listed += f", and {len(columns) - len(shown)} more"
    return (
        "I couldn't tell what you're asking about. Please clarify a bit more - rephrase your "
        "question and name what you want from the data, for example a count, a total, or a "
        f"comparison across one of these columns: {listed}."
    )


def _clarification_text(payload: dict[str, Any], state: _ChatState) -> str:
    text = str(payload.get("clarification") or "").strip()
    if not text or _CLARIFICATION_JARGON.search(text) or _CLARIFICATION_FILLER.search(text):
        return _fallback_clarification(state)
    return text


def _node_generate_sql(state: _ChatState) -> _ChatState:
    payload, tokens = generate_sql(
        state["question"],
        state["schema_context"],
        history=state.get("history"),
        error_feedback=state.get("error"),
    )
    state["tokens"] = state.get("tokens", 0) + tokens
    state["attempts"] = state.get("attempts", 0) + 1

    generated_sql = str(payload.get("sql") or "").strip()
    if payload.get("needs_clarification") or not generated_sql:
        fallback_sql = _previous_sql_for_chart_change(state)
        if fallback_sql:
            state["sql"] = fallback_sql
            state["error"] = None
            return state

    if payload.get("needs_clarification"):
        state["status"] = "clarify"
        state["answer"] = _clarification_text(payload, state)
        state["sql"] = ""
        return state

    state["sql"] = generated_sql
    state["error"] = None
    return state


def _node_execute(state: _ChatState) -> _ChatState:
    try:
        columns, rows, total_rows = run_sql(state["df"], state["sql"])
        state["columns"] = columns
        state["rows"] = rows
        state["total_rows"] = total_rows
        state["error"] = None
        state["status"] = "success"
    except (SqlValidationError, Exception) as exc:  # noqa: BLE001 - feed error back to the LLM
        state["error"] = f"{type(exc).__name__}: {exc}"
        state["status"] = "error"
    return state


def _fallback_answer(state: _ChatState) -> str:
    """Plain answer built from the result itself, for when the summariser is unavailable."""
    rows = state.get("rows") or []
    columns = state.get("columns") or []
    total_rows = state.get("total_rows")
    total_rows = len(rows) if total_rows is None else int(total_rows)
    if not rows:
        return "No rows matched that question."
    if total_rows == 1 and len(columns) == 1:
        return f"{columns[0]}: {rows[0].get(columns[0])}"
    return f"Found {total_rows} matching row(s)."


def _node_summarize(state: _ChatState) -> _ChatState:
    try:
        payload, tokens = summarize_result(
            state["question"],
            state["columns"],
            state["rows"],
            total_rows=state.get("total_rows"),
        )
    except Exception:  # noqa: BLE001 - SQL already ran; show the data instead of failing
        logger.exception("Data chat summary failed; falling back to the raw result")
        payload, tokens = {}, 0
    state["tokens"] = state.get("tokens", 0) + tokens
    state["answer"] = str(payload.get("answer") or _fallback_answer(state))
    state["chart"] = normalize_chart_spec(
        state["question"],
        state.get("columns", []) or [],
        state.get("rows", []) or [],
        payload.get("chart") if isinstance(payload, dict) else None,
    )
    return state



# The sections the insight is reported in. executive_summary is prose; the rest are
# lists of one-line points so the frontend can render each section on its own.
# question_type and confidence_in_analysis describe the reading rather than being part of
# it, so they are carried through but left out of the emptiness check below.
_INSIGHT_METADATA_KEYS = ("question_type", "confidence_in_analysis")
_INSIGHT_TEXT_KEYS = _INSIGHT_METADATA_KEYS + ("executive_summary",)
_INSIGHT_LIST_KEYS = (
    "data_observations",
    "important_patterns",
    "comparative_analysis",
    "correlation_insights",
    "what_this_data_cannot_tell_you",
    "actionable_recommendations",
)
# Decisions stay objects -- what/who/why/measure/confidence only mean something together.
_INSIGHT_OBJECT_KEYS = ("decisions",)
_INSIGHT_NARRATIVE_KEYS = ("executive_summary",) + _INSIGHT_LIST_KEYS + _INSIGHT_OBJECT_KEYS
_DECISION_FIELDS = ("what", "who", "why", "measure", "confidence")


_INSIGHT_TEXT_FIELDS = ("explanation", "text", "point", "insight", "description", "summary")


def _narrative_line(item: Any) -> str:
    """Coerce one list entry to a sentence.

    The model sometimes echoes a whole statistics object instead of writing prose. Rather
    than stringifying the dict into the response, pull the sentence out of it.
    """
    if isinstance(item, dict):
        for field in _INSIGHT_TEXT_FIELDS:
            value = item.get(field)
            if isinstance(value, str) and value.strip():
                return value.strip()
        # No known field: fall back to the longest string in the object, which is the
        # narrative whenever there is one at all.
        strings = [value.strip() for value in item.values() if isinstance(value, str) and value.strip()]
        return max(strings, key=len) if strings else ""
    return str(item).strip()


def _decision_entry(item: Any) -> dict[str, str] | None:
    """Coerce one decision to an object, dropping any entry with no action in it.

    The model still returns a bare sentence sometimes; that becomes the action with the
    other fields blank, so the frontend renders one shape either way.
    """
    if isinstance(item, str):
        text = item.strip()
        return {field: text if field == "what" else "" for field in _DECISION_FIELDS} if text else None
    if not isinstance(item, dict):
        return None
    entry = {field: str(item.get(field) or "").strip() for field in _DECISION_FIELDS}
    return entry if entry["what"] else None


def _coerce_insight_narrative(payload: Any) -> dict[str, Any] | None:
    """Keep only the expected narrative keys, with list fields forced to lists of text.

    The model occasionally returns a bare string where a list belongs; wrapping it
    keeps the response shape stable for the frontend. Sections the model left out for
    this question type come back empty rather than missing, for the same reason.
    """
    if not isinstance(payload, dict):
        return None

    narrative: dict[str, Any] = {}
    for key in _INSIGHT_TEXT_KEYS:
        narrative[key] = str(payload.get(key) or "").strip()
    for key in _INSIGHT_OBJECT_KEYS:
        value = payload.get(key)
        items = value if isinstance(value, list) else []
        narrative[key] = [entry for entry in (_decision_entry(item) for item in items) if entry]
    for key in _INSIGHT_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, str):
            entries = [value.strip()] if value.strip() else []
        elif isinstance(value, list):
            entries = [line for line in (_narrative_line(item) for item in value) if line]
        else:
            entries = []
        narrative[key] = entries

    if not any(narrative[key] for key in _INSIGHT_NARRATIVE_KEYS):
        return None
    return narrative



def _node_insight(state: _ChatState) -> _ChatState:
    """Attach a detailed, plain-language reading of the result, section by section.

    Statistics are computed from the rows first and passed to the model to quote, so
    the numbers hold even when the LLM is unavailable -- in that case a rule-based
    narrative is built from the same figures instead of dropping the field.
    """
    columns = state.get("columns", []) or []
    rows = state.get("rows", []) or []
    if not state.get("want_insight", True) or not rows:
        state["insight"] = None
        return state

    statistics = compute_result_statistics(columns, rows, total_rows=state.get("total_rows"))

    narrative: dict[str, Any] | None = None
    try:
        payload, tokens = generate_insight(
            state["question"],
            columns,
            rows,
            statistics,
            total_rows=state.get("total_rows"),
        )
        state["tokens"] = state.get("tokens", 0) + tokens
        narrative = _coerce_insight_narrative(payload)
    except Exception:  # noqa: BLE001 - the answer already exists; insight must not fail the turn
        logger.exception("Data chat insight generation failed; falling back to computed statistics")

    generated_by = "llm"
    if narrative is None:
        narrative = build_fallback_insight(statistics)
        generated_by = "rules"

    # Statistics are what the narrative is written from, but the client only needs the
    # narrative, so they stay server-side.
    state["insight"] = {**narrative, "generated_by": generated_by}
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
    graph.add_node("insight", _node_insight)

    graph.set_entry_point("generate_sql")
    graph.add_conditional_edges(
        "generate_sql", _route_after_sql, {"execute": "execute", "clarify": END}
    )
    graph.add_conditional_edges(
        "execute",
        _route_after_execute,
        {"summarize": "summarize", "retry": "generate_sql", "fail": END},
    )
    graph.add_edge("summarize", "insight")
    graph.add_edge("insight", END)
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
    include_insight: bool = True,
    debug_sql: bool = False,
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
        "want_insight": include_insight,
    }

    try:
        final: _ChatState = _build_graph().invoke(initial)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Data chat graph failed")
        final = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            # answer set nahi karte: neeche to_user_message() se plain-English wajah milti hai,
            # generic "could not process" ke bajaye.
            "attempts": initial.get("attempts", 1) or 1,
            "tokens": initial.get("tokens", 0),
        }

    status = final.get("status", "error")
    columns = final.get("columns", []) or []
    rows = final.get("rows", []) or []
    total_rows = final.get("total_rows")
    total_rows = len(rows) if total_rows is None else int(total_rows)

    message = DataChatMessage(
        session_id=session.id,
        source_dataset_id=source.dataset_id,
        source_type=source.dataset_type,
        created_by_user_id=current_user.id,
        nl_query=question,
        generated_sql=final.get("sql") or None,
        assistant_text=final.get("answer"),
        chart_spec=final.get("chart"),
        insight=final.get("insight"),
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

    payload: dict[str, Any] = {
        "session_id": session.id,
        "message_id": message.id,
        "status": status,
        "answer": final.get("answer")
        or (to_user_message(final.get("error")) if status == "error" else ""),
        "columns": columns,
        "rows": rows,
        # row_count pehle jaisa hi hai: kitni rows response me bheji gayi (MAX_RESULT_ROWS par
        # capped). Query se match hui asli rows alag field me jaati hain, taaki frontend ka
        # existing behaviour na badle.
        "row_count": len(rows),
        "total_row_count": total_rows,
        "chart_spec": final.get("chart"),
        # Plain-language reading of the result. Which sections carry content depends on
        # the question type. None when the turn returned no rows or insight was skipped.
        "insight": final.get("insight"),
        "attempts": message.attempts,
        # Technical error DB/logs me hi rehta hai; client ko plain-English message jaata hai.
        "error": to_user_message(final.get("error")) if status == "error" else None,
    }
    # Raw SQL normally response me nahi jaati (DB ke generated_sql me hi rehti hai);
    # sirf debugging ke liye, dono switch ON hone par wapas add hoti hai.
    if _should_expose_sql(debug_sql):
        payload["sql"] = final.get("sql") or None
    return payload


def get_suggested_questions(
    db: Session,
    current_user: User,
    *,
    dataset_type: str,
    dataset_id: int,
    is_clean: bool,
    count: int = DEFAULT_SUGGESTED_QUESTIONS,
    regenerate: bool = False,
) -> list[dict[str, Any]]:
    """Generate a handful of dummy questions for a dataset and answer each with its chart,
    so the frontend can show a preview of what data chat can do without the user typing anything.
    Results are cached per dataset so repeated hits skip the LLM entirely, unless ``regenerate``
    is set -- that always calls the LLM again and steers it away from the cached batch so the
    user gets a different set of questions instead of the same one back."""
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
    if (
        not regenerate
        and cached_suggestions
        and cache_age is not None
        and cache_age < SUGGESTIONS_CACHE_TTL_SECONDS
    ):
        if len(cached_suggestions) >= count:
            return cached_suggestions[:count]

    source = _resolve_analysis_source(
        db, current_user, dataset_type=dataset_type, dataset_id=dataset_id, is_clean=is_clean
    )

    df = load_dataset_dataframe(source)
    if df.empty:
        raise error_response(status_code=400, detail="Dataset has no data to query.")

    schema_context = build_schema_context(df)

    # regenerate=true ke liye pichla cached batch hi "avoid" list hai -- iske alawa kuch
    # store nahi karna padta aur "not previous one" ki ask exactly yehi cover karti hai.
    avoid_questions: list[str] | None = None
    if regenerate and cached_suggestions:
        avoid_questions = [
            str(item.get("question") or "").strip()
            for item in cached_suggestions
            if str(item.get("question") or "").strip()
        ] or None

    questions, _ = generate_sample_questions(schema_context, count, avoid_questions=avoid_questions)

    if avoid_questions:
        # Model kabhi kabhi avoid list ke bawajood ek purana sawaal repeat kar deta hai;
        # yeh safety net unhe drop karta hai aur zaroorat pade to bacha hua count ek aur
        # call se bhar deta hai, taaki regenerate hamesha genuinely different lage.
        avoid_normalised = {question.lower() for question in avoid_questions}
        questions = [q for q in questions if q.lower() not in avoid_normalised]
        if len(questions) < count:
            extra, _ = generate_sample_questions(
                schema_context,
                count - len(questions),
                avoid_questions=avoid_questions + questions,
            )
            questions.extend(q for q in extra if q.lower() not in avoid_normalised)

    results: list[dict[str, Any]] = []
    for question in questions:
        initial: _ChatState = {
            "question": question,
            "schema_context": schema_context,
            "df": df,
            "history": [],
            "attempts": 0,
            "tokens": 0,
            # Suggestions only need the chart type, so skip the insight LLM call that
            # would otherwise run once per suggested question.
            "want_insight": False,
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


def get_session_messages(
    db: Session, current_user: User, session_id: str, debug_sql: bool = False
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
    expose_sql = _should_expose_sql(debug_sql)
    history: list[dict[str, Any]] = []
    for m in messages:
        entry: dict[str, Any] = {
            "message_id": m.id,
            "question": m.nl_query,
            "answer": m.assistant_text
            or (to_user_message(m.error_message) if m.status == "error" else None),
            "chart_spec": m.chart_spec,
            "insight": m.insight,
            "rows": m.result_preview or [],
            "row_count": m.row_count,
            "status": m.status,
            "error": to_user_message(m.error_message) if m.status == "error" else None,
            "created_at": m.created_at.isoformat() if m.created_at else None,
        }
        # Query API jaisa hi rule: normally SQL client tak nahi jaati, sirf debugging ke
        # liye dono switch ON hone par history me wapas aati hai.
        if expose_sql:
            entry["sql"] = m.generated_sql
        history.append(entry)
    return history



def get_session_chart_specs(
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
            "chart_spec": m.chart_spec,
        }
        for m in messages
        if m.chart_spec
    ]


def delete_session(db: Session, current_user: User, session_id: str) -> dict[str, Any]:
    """Delete one chat session and every message in it.

    Messages are removed explicitly rather than relying on the FK cascade, so the delete
    behaves the same on a database whose data_chat_messages table pre-dates that
    constraint. Ownership is checked first: another user's session reads as not found.
    """
    ensure_data_chat_tables()

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

    deleted_messages = (
        db.query(DataChatMessage)
        .filter(DataChatMessage.session_id == session_id)
        .delete(synchronize_session=False)
    )
    db.delete(session)
    db.commit()

    logger.info(
        "Deleted data chat session_id=%s with %s message(s) for user_id=%s",
        session_id,
        deleted_messages,
        current_user.id,
    )
    return {"session_id": session_id, "deleted_messages": int(deleted_messages or 0)}


def rename_session(
    db: Session, current_user: User, session_id: str, *, title: str
) -> dict[str, Any]:
    """Rename one chat session.

    Ownership is checked first, same as ``delete_session``: another user's session
    reads as not found. The returned shape matches one entry of ``list_sessions``
    so the client can drop it straight into an already-rendered list.
    """
    ensure_data_chat_tables()

    new_title = title.strip()
    if not new_title:
        raise error_response(status_code=400, detail="Title cannot be empty")

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

    session.title = new_title
    db.commit()
    db.refresh(session)

    logger.info(
        "Renamed data chat session_id=%s for user_id=%s", session_id, current_user.id
    )
    return {
        "session_id": session.id,
        "title": session.title,
        "dataset_name": session.dataset_name,
        "is_clean": session.is_clean,
        "created_at": session.created_at.isoformat() if session.created_at else None,
        "updated_at": session.updated_at.isoformat() if session.updated_at else None,
    }


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
