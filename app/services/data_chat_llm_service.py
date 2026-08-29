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
    "- A question asking WHY something is high, low or different, or asking the reason or cause "
    "behind a pattern, counts as answerable whenever what it asks about can be matched to a column "
    "or a value in the schema, even loosely -- a plural, a synonym, or the name of a value. "
    "Return the SQL that retrieves the evidence for it: the grouping the question is about with "
    "its counts and totals, plus any other measure in the schema that could plausibly explain the "
    "difference. Do not ask for clarification merely because no column literally records a reason "
    "-- the explanation is written later from the numbers this query returns.\n"
    "- If the question cannot be answered from this schema, set needs_clarification=true and put a "
    "short question in 'clarification', and set sql to an empty string. This still applies to a "
    "question that points at no column at all, such as 'tell me about it' or 'what do you think', "
    "however it is phrased."
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
    "stacked_area|waterfall|mixed|doughnut|pie|scatter|bubble|heatmap\", \"x\": null, "
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
    "- Use pie only when the user actually says pie; for part-to-whole you pick yourself, "
    "return doughnut. Doughnut and pie are separate types, so never answer a pie request "
    "with doughnut or the other way round.\n"
    "- If the result is a single headline number, prefer kpi.\n"
    "- If a chart would be misleading, return table."
)


_INSIGHT_SYSTEM_PROMPT = (
    "You explain a data result to someone with no background in statistics -- a shop owner, a "
    "teacher, a manager. You are given the question, the result rows, and a block of statistics "
    "that has ALREADY been calculated from those rows.\n"
    "Output JSON only, with exactly these six keys:\n"
    "{\"executive_summary\": \"...\", \"data_observations\": [\"...\"], "
    "\"important_patterns\": [\"...\"], \"comparative_analysis\": [\"...\"], "
    "\"correlation_insights\": [\"...\"], \"actionable_recommendations\": [\"...\"]}\n"
    "Every list entry is a plain sentence string. Never emit an object, a key/value pair or a "
    "copy of the supplied statistics as a list entry.\n"
    "Keep every point to one sentence, two at most. State the fact and what it means; do not "
    "elaborate beyond that.\n"
    "WHAT EACH SECTION HOLDS:\n"
    "- executive_summary: 2 to 3 sentences. How much the main measure varies across categories, "
    "and which category leads.\n"
    "- data_observations: 3 to 4 points. The highest value with its share of the total, the "
    "lowest with its share, and how many distinct categories there are.\n"
    "- important_patterns: 1 to 2 points. Whether the spread is dominated by a single category "
    "or fairly balanced, using the supplied 'dominated_by_one' flag and never your own judgement.\n"
    "- comparative_analysis: 1 to 2 points. How many times bigger the leader is than the lowest, "
    "quoting the supplied 'ratio_top_to_bottom', and what that gap means in practice.\n"
    "- correlation_insights: one point per supplied correlation pair. Say in plain words that the "
    "two move together (or in opposite directions), quote its coefficient and strength, and quote "
    "'variance_explained_pct' as how much of one column's variation the other accounts for. If "
    "cross-category links were supplied, add which two values occur together most often and the "
    "co-occurrence percentage. Return an EMPTY list if neither was supplied.\n"
    "- actionable_recommendations: 2 to 3 concrete next steps -- what to focus on, what to "
    "investigate further, what decision this supports.\n"
    "HARD RULES:\n"
    "- Never invent, re-calculate or round a number. Every figure you mention must appear "
    "verbatim in the supplied statistics or rows.\n"
    "- Do no arithmetic of your own, not even a subtraction that looks trivial. Gaps come from "
    "the supplied 'range', shares from 'share_pct', multiples from 'ratio_top_to_bottom', and "
    "variance from 'variance_explained_pct'. If a number was not supplied, describe the pattern "
    "in words instead of computing it.\n"
    "- Return an empty list for any section the supplied data cannot support. Never pad, never "
    "invent a relationship, never guess a category that is not there.\n"
    "- Correlation is not causation. Say two things move together; offer causes only as clearly "
    "hedged suggestions inside actionable_recommendations.\n"
    "WRITING STYLE:\n"
    "- Short everyday sentences. No jargon. If you must use a term like 'median', 'correlation' "
    "or 'variance', explain it in the same sentence in plain words.\n"
    "- Write numbers as digits with thousands separators (985,000), never spelled out in words. "
    "Percentages as plain digits such as 34%. Adding separators to a supplied number is fine; "
    "changing its value is not.\n"
    "- Never attach a currency symbol or a unit that the data did not state. If a column is "
    "just called revenue, write 985,000, not $985,000.\n"
    "- Address the reader as 'you'. Never mention SQL, queries, columns as 'fields', or the model."
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


def _invoke(
    system: str,
    user: str,
    *,
    label: str,
    max_tokens: int | None = None,
) -> tuple[str, int]:
    capture = _TokenCapture()
    client = _client()
    if max_tokens:
        client = client.bind(max_tokens=int(max_tokens))
    response = client.invoke(
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
    content, tokens = _invoke(
        _INSIGHT_SYSTEM_PROMPT,
        user,
        label="data-chat-insight",
        max_tokens=settings.UAT_DATA_CHAT_INSIGHT_MAX_TOKENS,
    )
    return _extract_json(content), tokens
