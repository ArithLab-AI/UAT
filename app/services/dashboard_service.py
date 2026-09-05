"""Persistence + retrieval for dashboard charts.

Charts come only from the *manual* Basic Analysis flow: the frontend runs
``POST /basic-analysis/run`` and then saves the result via
``POST /basic-analysis/charts``. The dashboard itself never creates charts — it
only lists, refreshes, renames and deletes them. AI / natural-language charts
from Data Chat are not handled here.

The chart payload is stored exactly as sent and served back unchanged; nothing
is recomputed on save or on read.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.enum.analysis_type_enum import AnalysisType
from app.enum.chart_type_enum import ChartType
from app.models.auth_models import User
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.models.dashboard_models import Dashboard, SavedChart
from app.schemas.dashboard_schema import CreateDashboardRequest, SaveChartRequest
from app.utils.responses import error_response


def _request_fingerprint(request: SaveChartRequest) -> str:
    """Stable id for one analysis config, so re-saving the same chart updates it
    in place instead of adding a duplicate. Uses the original run request when
    provided; otherwise the chart's identifying fields. ``analysis_name`` /
    ``title`` are excluded so renaming only updates the label."""
    if request.request_payload:
        basis: dict[str, Any] = {
            k: v for k, v in request.request_payload.items() if k != "analysis_name"
        }
    else:
        basis = {
            "dataset_id": request.dataset_id,
            "dataset_type": request.dataset_type,
            "is_clean": request.is_clean,
            "analysis_type": request.analysis_type,
            "chart_type": request.chart_type,
        }
    blob = json.dumps(basis, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _serialize_chart(chart: SavedChart) -> dict[str, Any]:
    return {
        "id": chart.id,
        "title": chart.title,
        "analysis_type": chart.analysis_type,
        "chart_type": chart.chart_type,
        "source_dataset_id": chart.source_dataset_id,
        "source_type": chart.source_type,
        "is_clean": chart.is_clean,
        "dataset_name": chart.dataset_name,
        "file_name": chart.file_name,
        "row_count_used": chart.row_count_used,
        "created_at": chart.created_at,
        "updated_at": chart.updated_at,
    }


def _serialize_chart_detail(chart: SavedChart) -> dict[str, Any]:
    return {
        **_serialize_chart(chart),
        "request_payload": chart.request_payload or {},
        "chart": chart.chart_data,
        "summary": chart.summary or {},
        "warnings": chart.warnings or [],
    }


def _existing_dataset_names(db: Session, *, current_user: User) -> dict[tuple[str, int], dict[str, str]]:
    """Map (source_type, dataset_id) -> current name/file_name for datasets the
    user still owns. Charts whose dataset was deleted are dropped from the
    dashboard listings."""
    names: dict[tuple[str, int], dict[str, str]] = {}

    uploaded = (
        db.query(CsvUploadedDataset.id, CsvUploadedDataset.name, CsvUploadedDataset.file_name)
        .filter(CsvUploadedDataset.created_by_user_id == current_user.id)
        .all()
    )
    for dataset_id, name, file_name in uploaded:
        names[("uploaded", dataset_id)] = {"dataset_name": name, "file_name": file_name}

    merged = (
        db.query(CsvMergedDataset.id, CsvMergedDataset.name)
        .filter(CsvMergedDataset.created_by_user_id == current_user.id)
        .all()
    )
    for dataset_id, name in merged:
        names[("merged", dataset_id)] = {"dataset_name": name, "file_name": f"{name}.csv"}

    return names


def save_chart(db: Session, *, current_user: User, request: SaveChartRequest) -> dict[str, Any]:
    """Store an already-prepared chart from the manual Basic Analysis flow.

    No recomputation — the payload is kept as sent. Re-saving the same analysis
    updates the existing row instead of inserting a duplicate.
    """
    if not request.chart:
        raise error_response(status_code=400, detail="chart payload is required")

    try:
        analysis_type = AnalysisType(request.analysis_type).value
    except ValueError:
        raise error_response(
            status_code=400, detail=f"Unknown analysis_type: {request.analysis_type}"
        )
    try:
        chart_type = ChartType(request.chart_type).value
    except ValueError:
        raise error_response(
            status_code=400, detail=f"Unknown chart_type: {request.chart_type}"
        )

    # Confirm the dataset exists and belongs to the caller; take the name from the
    # DB rather than trusting the client.
    live_names = _existing_dataset_names(db, current_user=current_user)
    dataset_meta = live_names.get((request.dataset_type, request.dataset_id))
    if dataset_meta is None:
        raise error_response(
            status_code=404,
            detail=f"{request.dataset_type.capitalize()} dataset not found",
        )

    title = (request.title or request.analysis_name or "").strip()
    if not title:
        title = f"{analysis_type} - {chart_type}"

    fingerprint = _request_fingerprint(request)
    values = {
        "source_dataset_id": request.dataset_id,
        "source_type": request.dataset_type,
        "is_clean": bool(request.is_clean),
        "dataset_name": dataset_meta["dataset_name"],
        "file_name": dataset_meta["file_name"],
        "title": title[:255],
        "analysis_type": analysis_type,
        "chart_type": chart_type,
        "row_count_used": int(request.row_count_used or 0),
        "request_payload": request.request_payload or {},
        "chart_data": request.chart,
        "summary": request.summary or {},
        "warnings": request.warnings or [],
    }

    saved = (
        db.query(SavedChart)
        .filter(
            SavedChart.created_by_user_id == current_user.id,
            SavedChart.request_fingerprint == fingerprint,
        )
        .first()
    )
    if saved is None:
        saved = SavedChart(
            created_by_user_id=current_user.id,
            request_fingerprint=fingerprint,
            **values,
        )
        db.add(saved)
    else:
        for key, value in values.items():
            setattr(saved, key, value)

    db.commit()
    db.refresh(saved)
    return _serialize_chart_detail(saved)


def update_saved_chart(
    db: Session,
    *,
    current_user: User,
    chart_id: str,
    request: SaveChartRequest,
) -> dict[str, Any]:
    """Replace a saved chart identified by id (PUT semantics).

    Unlike ``save_chart`` this targets one specific row rather than upserting by
    fingerprint — used when the frontend re-runs an analysis and wants that exact
    dashboard chart refreshed.
    """
    chart = (
        db.query(SavedChart)
        .filter(
            SavedChart.id == chart_id,
            SavedChart.created_by_user_id == current_user.id,
        )
        .first()
    )
    if chart is None:
        raise error_response(status_code=404, detail="Saved chart not found")

    if not request.chart:
        raise error_response(status_code=400, detail="chart payload is required")

    try:
        analysis_type = AnalysisType(request.analysis_type).value
    except ValueError:
        raise error_response(
            status_code=400, detail=f"Unknown analysis_type: {request.analysis_type}"
        )
    try:
        chart_type = ChartType(request.chart_type).value
    except ValueError:
        raise error_response(
            status_code=400, detail=f"Unknown chart_type: {request.chart_type}"
        )

    live_names = _existing_dataset_names(db, current_user=current_user)
    dataset_meta = live_names.get((request.dataset_type, request.dataset_id))
    if dataset_meta is None:
        raise error_response(
            status_code=404,
            detail=f"{request.dataset_type.capitalize()} dataset not found",
        )

    title = (request.title or request.analysis_name or "").strip()
    if not title:
        title = f"{analysis_type} - {chart_type}"

    chart.source_dataset_id = request.dataset_id
    chart.source_type = request.dataset_type
    chart.is_clean = bool(request.is_clean)
    chart.dataset_name = dataset_meta["dataset_name"]
    chart.file_name = dataset_meta["file_name"]
    chart.title = title[:255]
    chart.analysis_type = analysis_type
    chart.chart_type = chart_type
    chart.row_count_used = int(request.row_count_used or 0)
    chart.request_fingerprint = _request_fingerprint(request)
    chart.request_payload = request.request_payload or {}
    chart.chart_data = request.chart
    chart.summary = request.summary or {}
    chart.warnings = request.warnings or []

    db.commit()
    db.refresh(chart)
    return _serialize_chart_detail(chart)


def refresh_saved_chart(
    db: Session, *, current_user: User, chart_id: str
) -> dict[str, Any]:
    """Re-run a saved chart's original analysis against the *current* dataset and
    overwrite the stored snapshot in place.

    The chart keeps its id, title, fingerprint and saved request payload; only
    the computed result (``chart_data`` / ``summary`` / ``warnings`` /
    ``row_count_used``) and the dataset name fields are refreshed.

    Raises 409 if the chart has no usable saved request payload, and re-raises
    the analysis engine's 400/404 (dataset deleted, a column the analysis needs
    is gone after a re-clean, ...) so the existing snapshot is left untouched.
    """
    # Imported here rather than at module scope: basic_analysis_service pulls in
    # the whole analysis stack, and only this one function needs it.
    from app.schemas.basic_analysis_schema import BasicAnalysisRequest
    from app.services.basic_analysis_service import run_basic_analysis

    chart = (
        db.query(SavedChart)
        .filter(
            SavedChart.id == chart_id,
            SavedChart.created_by_user_id == current_user.id,
        )
        .first()
    )
    if chart is None:
        raise error_response(status_code=404, detail="Saved chart not found")

    if not chart.request_payload:
        raise error_response(
            status_code=409,
            detail="This chart has no saved analysis configuration to refresh. "
            "Re-create it from Basic Analysis.",
        )

    try:
        run_request = BasicAnalysisRequest(**chart.request_payload)
    except ValidationError:
        raise error_response(
            status_code=409,
            detail="The saved analysis configuration for this chart is no longer "
            "valid. Re-create it from Basic Analysis.",
        )

    try:
        result = run_basic_analysis(db, current_user=current_user, request=run_request)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, str) else "Analysis could not be re-run."
        raise error_response(
            status_code=exc.status_code,
            detail=f"Chart could not be refreshed: {detail}",
        )

    chart.chart_data = result["chart"].model_dump(mode="json")
    chart.summary = result.get("summary") or {}
    chart.warnings = result.get("warnings") or []
    chart.row_count_used = int(result.get("row_count_used") or 0)
    chart.chart_type = ChartType(result["chart_type"]).value
    chart.dataset_name = result["dataset_name"]
    chart.file_name = result["file_name"]

    db.commit()
    db.refresh(chart)
    return _serialize_chart_detail(chart)


def list_dashboard_datasets(db: Session, *, current_user: User) -> list[dict[str, Any]]:
    """Datasets the user has at least one saved chart for — feeds the dataset dropdown."""
    charts = (
        db.query(SavedChart)
        .filter(SavedChart.created_by_user_id == current_user.id)
        .order_by(SavedChart.created_at.desc())
        .all()
    )
    live_names = _existing_dataset_names(db, current_user=current_user)

    grouped: dict[tuple[str, int], dict[str, Any]] = {}
    for chart in charts:
        key = (chart.source_type, chart.source_dataset_id)
        if key not in live_names:
            continue
        bucket = grouped.get(key)
        if bucket is None:
            grouped[key] = {
                "source_dataset_id": chart.source_dataset_id,
                "source_type": chart.source_type,
                "dataset_name": live_names[key]["dataset_name"],
                "file_name": live_names[key]["file_name"],
                "chart_count": 1,
                "last_chart_created_at": chart.created_at,
            }
        else:
            bucket["chart_count"] += 1
            if chart.created_at > bucket["last_chart_created_at"]:
                bucket["last_chart_created_at"] = chart.created_at

    return sorted(
        grouped.values(),
        key=lambda item: item["last_chart_created_at"],
        reverse=True,
    )


def list_saved_charts(
    db: Session,
    *,
    current_user: User,
    source_dataset_id: Optional[int] = None,
    source_type: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Saved charts, optionally filtered to one dataset — feeds the chart dropdown."""
    query = db.query(SavedChart).filter(SavedChart.created_by_user_id == current_user.id)
    if source_dataset_id is not None:
        query = query.filter(SavedChart.source_dataset_id == source_dataset_id)
    if source_type is not None:
        query = query.filter(SavedChart.source_type == source_type)

    charts = query.order_by(SavedChart.created_at.desc()).all()
    live_names = _existing_dataset_names(db, current_user=current_user)
    return [
        _serialize_chart(chart)
        for chart in charts
        if (chart.source_type, chart.source_dataset_id) in live_names
    ]


def get_dashboard_overview(db: Session, *, current_user: User) -> list[dict[str, Any]]:
    """Every dataset with its charts nested — one call to populate both dropdowns."""
    datasets = list_dashboard_datasets(db, current_user=current_user)
    charts = (
        db.query(SavedChart)
        .filter(SavedChart.created_by_user_id == current_user.id)
        .order_by(SavedChart.created_at.desc())
        .all()
    )
    charts_by_key: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for chart in charts:
        charts_by_key.setdefault(
            (chart.source_type, chart.source_dataset_id), []
        ).append(_serialize_chart(chart))

    for dataset in datasets:
        key = (dataset["source_type"], dataset["source_dataset_id"])
        dataset["charts"] = charts_by_key.get(key, [])
    return datasets


def get_saved_chart(db: Session, *, current_user: User, chart_id: str) -> dict[str, Any]:
    chart = (
        db.query(SavedChart)
        .filter(
            SavedChart.id == chart_id,
            SavedChart.created_by_user_id == current_user.id,
        )
        .first()
    )
    if chart is None:
        raise error_response(status_code=404, detail="Saved chart not found")
    return _serialize_chart_detail(chart)


def delete_saved_chart(db: Session, *, current_user: User, chart_id: str) -> None:
    chart = (
        db.query(SavedChart)
        .filter(
            SavedChart.id == chart_id,
            SavedChart.created_by_user_id == current_user.id,
        )
        .first()
    )
    if chart is None:
        raise error_response(status_code=404, detail="Saved chart not found")
    db.delete(chart)
    db.commit()


# ---------------------------------------------------------------------------
# Dashboard Builder boards (a named grid of widgets), distinct from the single
# SavedChart rows above. Widgets/layout/render_state are frontend-owned JSON,
# stored and served back exactly as sent.
# ---------------------------------------------------------------------------


def _serialize_dashboard_summary(dashboard: Dashboard) -> dict[str, Any]:
    return {
        "id": dashboard.id,
        "client_generated_id": dashboard.client_generated_id,
        "name": dashboard.name,
        "description": dashboard.description,
        "source_dataset_id": dashboard.source_dataset_id,
        "source_type": dashboard.source_type,
        "source_dataset_name": dashboard.source_dataset_name,
        "widget_count": len(dashboard.widgets or []),
        "created_at": dashboard.created_at,
        "updated_at": dashboard.updated_at,
    }


def _serialize_dashboard_detail(dashboard: Dashboard) -> dict[str, Any]:
    return {
        **_serialize_dashboard_summary(dashboard),
        "schema_version": dashboard.schema_version,
        "source_dataset_columns": dashboard.source_dataset_columns or [],
        "layout_engine": dashboard.layout_engine,
        "widgets": dashboard.widgets or [],
        "render_state": dashboard.render_state,
        "selected_widget_id": dashboard.selected_widget_id,
        "created_from": dashboard.created_from,
    }


def save_dashboard(
    db: Session, *, current_user: User, request: CreateDashboardRequest
) -> dict[str, Any]:
    """Create a Dashboard Builder board, or update it in place when
    ``client_generated_id`` matches one this user already saved.

    Mirrors ``save_chart``: the dataset is confirmed to exist and belong to the
    caller (name taken from the DB, not trusted from the client); everything
    else — widgets, layout, colors, UI state — is stored exactly as sent.
    """
    live_names = _existing_dataset_names(db, current_user=current_user)
    dataset_meta = live_names.get(
        (request.source_dataset.type, request.source_dataset.id)
    )
    if dataset_meta is None:
        raise error_response(
            status_code=404,
            detail=f"{request.source_dataset.type.capitalize()} dataset not found",
        )

    values = {
        "schema_version": request.schema_version,
        "name": request.name.strip()[:255],
        "description": request.description,
        "source_dataset_id": request.source_dataset.id,
        "source_type": request.source_dataset.type,
        "source_dataset_name": dataset_meta["dataset_name"],
        "source_dataset_columns": request.source_dataset.columns or [],
        "layout_engine": request.layout_engine,
        "widgets": request.widgets or [],
        "render_state": request.render_state,
        "selected_widget_id": request.selected_widget_id,
        "created_from": request.created_from,
        "client_saved_at": request.saved_at,
    }

    dashboard = (
        db.query(Dashboard)
        .filter(
            Dashboard.created_by_user_id == current_user.id,
            Dashboard.client_generated_id == request.client_generated_id,
        )
        .first()
    )
    if dashboard is None:
        dashboard = Dashboard(
            created_by_user_id=current_user.id,
            client_generated_id=request.client_generated_id,
            **values,
        )
        db.add(dashboard)
    else:
        for key, value in values.items():
            setattr(dashboard, key, value)

    db.commit()
    db.refresh(dashboard)
    return _serialize_dashboard_detail(dashboard)


def list_dashboards(db: Session, *, current_user: User) -> list[dict[str, Any]]:
    """Every board the user has saved, most recently updated first."""
    dashboards = (
        db.query(Dashboard)
        .filter(Dashboard.created_by_user_id == current_user.id)
        .order_by(Dashboard.updated_at.desc())
        .all()
    )
    return [_serialize_dashboard_summary(dashboard) for dashboard in dashboards]


def get_dashboard(db: Session, *, current_user: User, dashboard_id: str) -> dict[str, Any]:
    dashboard = (
        db.query(Dashboard)
        .filter(
            Dashboard.id == dashboard_id,
            Dashboard.created_by_user_id == current_user.id,
        )
        .first()
    )
    if dashboard is None:
        raise error_response(status_code=404, detail="Dashboard not found")
    return _serialize_dashboard_detail(dashboard)
