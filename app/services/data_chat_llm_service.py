"""LLM prompts for natural-language data chat: schema grounding, NL->SQL, and answer+chart."""

from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import HumanMessage, SystemMessage

from app.config.config import settings
from app.services.data_chat_chart_service import CHART_SELECTION_GUIDE
from app.services.data_chat_query_engine import DATASET_TABLE_NAME
from app.utils.openai_utils import get_openai_client
from app.utils.token_usage import TokenUsageLogger, _extract_usage

MAX_SAMPLE_VALUES = 6
LOW_CARDINALITY_LIMIT = 25
SUMMARY_SAMPLE_ROWS = 20
INSIGHT_SAMPLE_ROWS = 30

_SQL_SYSTEM_PROMPT = (
    "You are an expert data analyst that translates a natural-language question into a single "
    "DuckDB SQL query over ONE table named \"{table}\".\n"
    "RULES:\n"
    "- Output JSON only: {{\"sql\": \"...\", \"needs_clarification\": false, \"clarification\": null}}.\n"
    "- The query MUST be a single read-only SELECT (or WITH ... SELECT). Never write/modify data.\n"
    "- Always quote column identifiers with double quotes exactly as given in the schema.\n"
    "- Infer which columns the user means from the schema and sample values; the user may not name "
    "columns exactly. Map synonyms (revenue->amount/price, customer->name, etc.) to real columns.\n"
    "- For aggregations, GROUP BY the right dimension and ORDER BY the metric. Add LIMIT for 'top N'.\n"
    "- Cast text columns to appropriate types when doing math (TRY_CAST(\"col\" AS DOUBLE)).\n"
    "- Dates and times usually arrive as text. Convert one with TRY_CAST(\"col\" AS TIMESTAMP) "
    "or TRY_CAST(\"col\" AS DATE). Never write TIMESTAMP \"col\" or DATE \"col\": that form "
    "introduces a literal and is a syntax error when applied to a column.\n"
    "- To group by day use CAST(TRY_CAST(\"col\" AS TIMESTAMP) AS DATE); for a wider bucket use "
    "date_trunc('month', TRY_CAST(\"col\" AS TIMESTAMP)). Extract parts with "
    "EXTRACT(YEAR FROM TRY_CAST(\"col\" AS TIMESTAMP)).\n"
    "- If the question only asks to change how the previous answer is displayed (e.g. 'show it as a "
    "pie chart', 'make this a bar graph', 'as a table'), do NOT ask for clarification: re-use the "
    "most recent SQL from the conversation exactly as it is.\n"
    "- If the question cannot be answered from this schema, set needs_clarification=true and put a "
    "short question in 'clarification', and set sql to an empty string."
)

_SUGGESTIONS_SYSTEM_PROMPT = (
    "You propose short natural-language questions a business user could ask about a dataset, "
    "chosen so that answering them would naturally produce a variety of chart types "
    "(counts/KPIs, category breakdowns, trends over time, part-to-whole splits, comparisons).\n"
    "Output JSON only: {\"questions\": [\"...\", \"...\"]}.\n"
    "Rules:\n"
    "- Only ask questions answerable from the given columns.\n"
    "- Keep each question under 15 words.\n"
    "- Make the questions diverse in intent, not variations of the same question.\n"
    "- Return exactly the requested count."
)

_SUMMARY_SYSTEM_PROMPT = (
    "You summarise a SQL query result for a business user and choose the best ECharts-style chart.\n"
    "Output JSON only: {\"answer\": \"one or two sentence plain-language answer\", "
    "\"chart\": {\"type\": \"table|kpi|bar|stacked_bar|smooth_line|area|stacked_line|"
    "stacked_area|waterfall|mixed|doughnut|scatter|bubble|heatmap\", \"x\": null, "
    "\"y\": [], \"series\": null, \"size\": null, \"title\": null, \"orientation\": null}}.\n"
    f"Chart catalogue:\n{CHART_SELECTION_GUIDE}\n"
    "Rules:\n"
    "- If the user names a chart type in the question (pie, bar, line, table, ...), return exactly "
    "that type, even if you would have picked a different one.\n"
    "- Otherwise choose only a chart type that the result columns can genuinely support; "
    "if none fits, use table.\n"
    "- x, y, series, and size must be exact result column names from the SQL result or null.\n"
    "- Use y for numeric measure columns.\n"
    "- Use series for the grouping dimension in stacked charts or heatmaps.\n"
    "- For mixed, return exactly two y columns.\n"
    "- For doughnut, keep it to small part-to-whole outputs.\n"
    "- If the result is a single headline number, prefer kpi.\n"
    "- If a chart would be misleading, return table."
)


_INSIGHT_SYSTEM_PROMPT = (
    "You explain a data result to someone with no background in statistics -- a shop owner, a "
    "teacher, a manager. You are given the question, the result rows, and a block of statistics "
    "that has ALREADY been calculated from those rows.\n"
    "Output JSON only: {\"summary\": \"...\", \"key_findings\": [\"...\"], "
    "\"highs_and_lows\": [\"...\"], \"correlations\": [\"...\"], "
    "\"possible_reasons\": [\"...\"], \"caveats\": [\"...\"]}.\n"
    "HARD RULES:\n"
    "- Never invent, re-calculate or round a number. Every figure you mention must appear "
    "verbatim in the supplied statistics or rows. If a number is not given, describe the pattern "
    "in words instead.\n"
    "- Do no arithmetic of your own, not even a subtraction that looks trivial. To describe the "
    "gap between the highest and lowest value of a measure, quote that measure's supplied "
    "'range'. To describe a share, quote the supplied 'share_of_total_pct'. If the number you "
    "want was not supplied, say 'far higher' or 'roughly double' in words rather than computing "
    "it.\n"
    "- Separate fact from guess. Findings state what the data shows. 'possible_reasons' holds "
    "explanations the data cannot prove, and every one of them must start with a hedge such as "
    "'This could be because' or 'One likely reason is'.\n"
    "- Correlation is not causation. When two columns move together, say they move together and "
    "offer causes only under possible_reasons.\n"
    "WRITING STYLE:\n"
    "- Short everyday sentences. No jargon. If you must use a term like 'median' or "
    "'correlation', explain it in the same sentence in plain words.\n"
    "- Write numbers as digits with thousands separators (985,000), never spelled out in "
    "words. Percentages as plain digits such as 34%. Adding separators to a supplied "
    "number is fine; changing its value is not.\n"
    "- Never attach a currency symbol or a unit that the data did not state. If a column is "
    "just called revenue, write 985,000, not $985,000.\n"
    "- Address the reader as 'you'. Never mention SQL, queries, columns as 'fields', or the model.\n"
    "CONTENT:\n"
    "- summary: 2 to 4 sentences answering what this result actually shows overall.\n"
    "- key_findings: 3 to 6 concrete observations, each one sentence, each tied to a real number.\n"
    "- highs_and_lows: name the highest and lowest performers and say how far apart they are, and "
    "whether the gap is large compared with the rest.\n"
    "- correlations: for each supplied pair, say in plain words what moving together means here. "
    "Return an empty list if no pairs were supplied.\n"
    "- possible_reasons: 2 to 4 hedged, plausible business explanations for why the high values "
    "are high and the low ones are low.\n"
    "- caveats: anything that should stop the reader over-trusting this -- a lopsided mix of "
    "values, a metric that cannot be compared fairly, a category doing the work for the rest. "
    "Do NOT write a caveat about the row count, the sample being small, or the result being cut "
    "short; those are added separately and yours would repeat them. Empty list if none.\n"
    "- Any list may be empty when the data genuinely does not support it. Never pad."
)

class _TokenCapture(BaseCallbackHandler):
    """Records total tokens for a single LLM call on the instance."""

    def __init__(self) -> None:
        self.total_tokens = 0

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        try:
            self.total_tokens += int(_extract_usage(response).get("total_tokens", 0) or 0)
        except Exception:  # pragma: no cover - token accounting must never break a call
            pass


def _client():
    model = (settings.UAT_ANALYSIS_LLM_MODEL or "gpt-4o-mini").strip()
    return get_openai_client().bind(model=model, temperature=0)


def _invoke(system: str, user: str, *, label: str) -> tuple[str, int]:
    capture = _TokenCapture()
    response = _client().invoke(
        [SystemMessage(content=system), HumanMessage(content=user)],
        config={"callbacks": [TokenUsageLogger(label=label), capture]},
    )
    return str(getattr(response, "content", "") or ""), capture.total_tokens


def _extract_json(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    candidate = match.group(0) if match else text
    return json.loads(candidate)


def build_schema_context(df: pd.DataFrame) -> str:
    """Describe each column with its type and sample/distinct values so the LLM can map intent."""
    lines: list[str] = [f'Table "{DATASET_TABLE_NAME}" has {len(df)} rows and these columns:']
    for column in df.columns:
        series = df[column]
        dtype = str(series.dtype)
        non_null = series.dropna()
        distinct = non_null.unique()
        if 0 < len(distinct) <= LOW_CARDINALITY_LIMIT:
            sample = list(distinct[:LOW_CARDINALITY_LIMIT])
            descriptor = f"categorical values: {sample}"
        else:
            sample = list(non_null.head(MAX_SAMPLE_VALUES))
            descriptor = f"examples: {sample}"
        lines.append(f'- "{column}" ({dtype}) — {descriptor}')
    return "\n".join(lines)


def generate_sql(
    question: str,
    schema_context: str,
    *,
    history: list[dict[str, str]] | None = None,
    error_feedback: str | None = None,
) -> tuple[dict[str, Any], int]:
    """Returns ({sql, needs_clarification, clarification}, tokens_used)."""
    system = _SQL_SYSTEM_PROMPT.format(table=DATASET_TABLE_NAME)
    parts = [schema_context]
    if history:
        convo = "\n".join(f"Q: {h['q']}\nSQL: {h.get('sql', '')}" for h in history[-3:])
        parts.append(f"Recent conversation for context:\n{convo}")
    if error_feedback:
        parts.append(
            f"Your previous SQL failed with this error, fix it:\n{error_feedback}"
        )
    parts.append(f"Question: {question}")
    user = "\n\n".join(parts)

    content, tokens = _invoke(system, user, label="data-chat-sql")
    return _extract_json(content), tokens


def generate_sample_questions(schema_context: str, count: int) -> tuple[list[str], int]:
    """Returns (list of dummy questions covering varied chart types, tokens_used)."""
    system = _SUGGESTIONS_SYSTEM_PROMPT
    user = f"{schema_context}\n\nGenerate exactly {count} questions."
    content, tokens = _invoke(system, user, label="data-chat-suggestions")
    payload = _extract_json(content)
    questions = [str(q).strip() for q in payload.get("questions", []) if str(q).strip()]
    return questions[:count], tokens


def summarize_result(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    total_rows: int | None = None,
) -> tuple[dict[str, Any], int]:
    """Returns ({answer, chart}, tokens_used).

    ``total_rows`` is the real number of rows the query matched; ``rows`` may be capped.
    The model must see the real total, otherwise it reports the capped count as the answer.
    """
    sample = rows[:SUMMARY_SAMPLE_ROWS]
    matched_rows = len(rows) if total_rows is None else int(total_rows)
    truncation_note = (
        f"\nOnly the first {len(rows)} of {matched_rows} matching rows were kept; "
        f"{matched_rows} is the true total, use it in the answer."
        if matched_rows > len(rows)
        else ""
    )
    user = (
        f"Question: {question}\n"
        f"Result columns: {columns}\n"
        f"Result rows ({matched_rows} total, showing {len(sample)}):{truncation_note}\n"
        f"{json.dumps(sample, ensure_ascii=False, default=str)}"
    )
    content, tokens = _invoke(_SUMMARY_SYSTEM_PROMPT, user, label="data-chat-summary")
    return _extract_json(content), tokens


def generate_insight(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    statistics: dict[str, Any],
    total_rows: int | None = None,
) -> tuple[dict[str, Any], int]:
    """Returns ({summary, key_findings, highs_and_lows, correlations, possible_reasons, caveats}, tokens).

    ``statistics`` comes from data_chat_insight_service and is already computed from the
    real values. The model narrates it; it is told not to produce numbers of its own.
    """
    sample = rows[:INSIGHT_SAMPLE_ROWS]
    matched_rows = len(rows) if total_rows is None else int(total_rows)
    # The caller appends a truncation caveat itself, so the model is only told the
    # scope of what it is looking at -- otherwise both add one and they read as duplicates.
    truncation_note = (
        f"\nOnly {len(rows)} of {matched_rows} matching rows were analysed."
        if matched_rows > len(rows)
        else ""
    )
    user = (
        f"Question the user asked: {question}\n"
        f"Result columns: {columns}\n"
        f"Rows matched: {matched_rows}{truncation_note}\n\n"
        f"Statistics already calculated from the full result (use these numbers, do not redo them):\n"
        f"{json.dumps(statistics, ensure_ascii=False, default=str)}\n\n"
        f"Sample of the rows (first {len(sample)}):\n"
        f"{json.dumps(sample, ensure_ascii=False, default=str)}"
    )
    content, tokens = _invoke(_INSIGHT_SYSTEM_PROMPT, user, label="data-chat-insight")
    return _extract_json(content), tokens
