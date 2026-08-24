"""Module 8: Geospatial & Location Analysis.

No geocoding and no GeoJSON boundary matching happens on this backend — there is
no bundled worldwide city/zip -> lat/long reference table and no geocoding API
configured, so "resolving a city name to a map location" is left to the
frontend's map library (which already owns boundary/centroid data, matched by
location name or ISO code — the same way Plotly/ECharts/Mapbox choropleths work).

What this module *does* do:
  - Detect whether the selected location column(s) are a Latitude+Longitude pair
    or a named region (City/State/Country/Zip, via simple name-hint heuristics),
    reported as ``location_type``.
  - For coordinates: return sampled, ready-to-plot {lat, lng, value} points plus
    a bounding box/center for the frontend to auto-fit the map, and a rounded-
    coordinate "cluster" leaderboard.
  - For named regions: aggregate the metric per location (Sum/Average/Count/
    Median/Min/Max), including each location's percentage share of the total,
    plus a Top 10 leaderboard.
  - Snap the requested chart_type to something actually renderable for the
    detected location type (see _snap_chart_type), with a warning explaining why.
"""

from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from app.enum.aggregation_type_enum import AggregationType
from app.enum.chart_type_enum import ChartType
from app.enum.geospatial_enum import LocationType
from app.schemas.basic_analysis_schema import BasicAnalysisRequest, ChartPayload
from app.services.basic_analysis_helpers import (
    apply_groupby_aggregation as _apply_groupby_aggregation,
    is_numeric_column as _is_numeric_column,
    numeric_series as _numeric_series,
    require_column as _require_column,
    round_value as _round,
)
from app.utils.responses import error_response

MAX_MAP_POINTS = 3000       # cap for coordinate-based pin/heatmap/bubble points
MAX_LOCATIONS = 200         # cap for named-region groups returned in `extra.locations`
LEADERBOARD_SIZE = 10       # spec: Top Locations Leaderboard lists top 10
CLUSTER_PRECISION = 3       # decimal places (~110m) for clustering coordinate points into a leaderboard

_ZIP_HINTS = ("zip", "postal", "pincode", "pin_code", "postcode")
_COUNTRY_HINTS = ("country", "nation")
_STATE_HINTS = ("state", "province", "territory")
_CITY_HINTS = ("city", "town", "municipality", "district")
# "lat"/"lng"/"lon" are too short/generic to substring-match safely (e.g. "lat" inside
# "calculation") — these are matched as whole name tokens instead, see _name_tokens.
_LATITUDE_TOKENS = ("latitude", "lat")
_LONGITUDE_TOKENS = ("longitude", "lng", "lon")

_COORD_CHART_TYPES = (ChartType.PIN_MAP, ChartType.HEATMAP_MAP, ChartType.BUBBLE_MAP)


def _name_tokens(column_name: str) -> set[str]:
    return {tok for tok in re.split(r"[^a-z0-9]+", column_name.strip().lower()) if tok}


def geographic_hint(column_name: str, is_numeric: bool) -> LocationType | None:
    """Best-effort, name-based geographic classification for the column-picker endpoint.

    Unlike ``_classify_location_type`` (used once a column is *already* chosen as the
    Geospatial location column, where a generic "region" fallback is reasonable), this
    returns ``None`` when nothing specific matches — tagging every categorical column as
    "geographic" would make the column-picker filter useless.
    """
    tokens = _name_tokens(column_name)
    if is_numeric:
        if tokens & set(_LATITUDE_TOKENS):
            return LocationType.LATITUDE
        if tokens & set(_LONGITUDE_TOKENS):
            return LocationType.LONGITUDE
        return None

    lowered = column_name.strip().lower()
    if any(hint in lowered for hint in _ZIP_HINTS):
        return LocationType.ZIP_CODE
    if any(hint in lowered for hint in _COUNTRY_HINTS):
        return LocationType.COUNTRY
    if any(hint in lowered for hint in _STATE_HINTS):
        return LocationType.STATE
    if any(hint in lowered for hint in _CITY_HINTS):
        return LocationType.CITY
    return None


def _classify_location_type(column_name: str) -> LocationType:
    return geographic_hint(column_name, is_numeric=False) or LocationType.REGION


def _snap_chart_type(chart_type: ChartType, location_type: LocationType, warnings: list[str]) -> ChartType:
    if location_type == LocationType.COORDINATES:
        if chart_type not in _COORD_CHART_TYPES:
            warnings.append(
                "Choropleth needs a named region column (city/state/country); showing a pin map "
                "for this latitude/longitude data instead."
            )
            return ChartType.PIN_MAP
        return chart_type

    if chart_type != ChartType.CHOROPLETH_MAP:
        warnings.append(
            "Pin/Heatmap/Bubble maps need a Latitude + Longitude column pair; showing a choropleth "
            "map for this named-location data instead. Select location_column_2 (Longitude) for "
            "point-based maps."
        )
        return ChartType.CHOROPLETH_MAP
    return chart_type


def _compute_coordinate_geospatial(
    df: pd.DataFrame,
    req: BasicAnalysisRequest,
    chart_type: ChartType,
    lat_col: str,
    lng_col: str,
    warnings: list[str],
) -> tuple[ChartPayload, dict[str, Any]]:
    metric_col = req.metric_column or None
    if metric_col is not None:
        metric_col = _require_column(df, metric_col, "metric")
        if not _is_numeric_column(df, metric_col):
            raise error_response(status_code=400, detail=f"'{metric_col}' must be numeric for Geospatial analysis.")

    working = pd.DataFrame({"lat": _numeric_series(df, lat_col), "lng": _numeric_series(df, lng_col)})
    if metric_col is not None:
        working["value"] = _numeric_series(df, metric_col)
        working = working.dropna(subset=["lat", "lng", "value"])
    else:
        working["value"] = 1.0
        working = working.dropna(subset=["lat", "lng"])

    rows_before = len(working)
    working = working[working["lat"].between(-90, 90) & working["lng"].between(-180, 180)]
    out_of_range = rows_before - len(working)
    if out_of_range:
        warnings.append(f"Excluded {out_of_range} row(s) with latitude/longitude outside valid range.")

    if working.empty:
        raise error_response(status_code=400, detail="No valid latitude/longitude points found.")

    agg = req.aggregation or (AggregationType.COUNT if metric_col is None else AggregationType.SUM)
    if metric_col is None:
        agg = AggregationType.COUNT

    sample = working.sample(MAX_MAP_POINTS, random_state=42) if len(working) > MAX_MAP_POINTS else working
    if len(working) > MAX_MAP_POINTS:
        warnings.append(f"Map sampled down to {MAX_MAP_POINTS} points for performance.")
    points = [
        {"lat": _round(float(row["lat"]), 6), "lng": _round(float(row["lng"]), 6), "value": _round(float(row["value"]))}
        for _, row in sample.iterrows()
    ]

    bounds = {
        "min_lat": _round(float(working["lat"].min()), 6),
        "max_lat": _round(float(working["lat"].max()), 6),
        "min_lng": _round(float(working["lng"].min()), 6),
        "max_lng": _round(float(working["lng"].max()), 6),
        "center": {
            "lat": _round(float((working["lat"].min() + working["lat"].max()) / 2), 6),
            "lng": _round(float((working["lng"].min() + working["lng"].max()) / 2), 6),
        },
    }

    # Leaderboard: cluster nearby points (rounded coordinates) since raw lat/lng pairs
    # are rarely exact duplicates, then aggregate + rank the clusters.
    clustered = working.copy()
    clustered["cluster"] = (
        clustered["lat"].round(CLUSTER_PRECISION).astype(str) + ", " + clustered["lng"].round(CLUSTER_PRECISION).astype(str)
    )
    grouped = _apply_groupby_aggregation(clustered, "cluster", "value", agg).dropna().sort_values(ascending=False)
    total = float(grouped.sum())
    leaderboard = [
        {
            "rank": i + 1,
            "location": str(cluster),
            "value": _round(float(value)),
            "percentage_share": _round(float(value) / total * 100, 2) if total else None,
        }
        for i, (cluster, value) in enumerate(grouped.head(LEADERBOARD_SIZE).items())
    ]

    extra: dict[str, Any] = {
        "location_type": LocationType.COORDINATES.value,
        "map_bounds": bounds,
        "aggregation": agg.value,
        "total_points": int(len(working)),
    }
    chart = ChartPayload(chart_type=chart_type, points=points, table=leaderboard, extra=extra)
    summary = {
        "location_column": lat_col,
        "location_column_2": lng_col,
        "location_type": LocationType.COORDINATES.value,
        "metric_column": metric_col,
        "aggregation": agg.value,
        "total_points": int(len(working)),
    }
    return chart, summary


def _compute_region_geospatial(
    df: pd.DataFrame,
    req: BasicAnalysisRequest,
    chart_type: ChartType,
    location_col: str,
    warnings: list[str],
) -> tuple[ChartPayload, dict[str, Any]]:
    metric_col = req.metric_column or None
    if metric_col is not None:
        metric_col = _require_column(df, metric_col, "metric")
        if not _is_numeric_column(df, metric_col):
            raise error_response(status_code=400, detail=f"'{metric_col}' must be numeric for Geospatial analysis.")

    working = pd.DataFrame({location_col: df[location_col].astype(str).str.strip().replace("", np.nan)})
    if metric_col is not None:
        working[metric_col] = _numeric_series(df, metric_col)
    working = working.dropna(subset=[location_col])
    if working.empty:
        raise error_response(status_code=400, detail=f"No valid values found in '{location_col}'.")

    agg = req.aggregation or (AggregationType.COUNT if metric_col is None else AggregationType.SUM)
    if metric_col is None:
        agg = AggregationType.COUNT

    grouped = _apply_groupby_aggregation(working, location_col, metric_col, agg).dropna().sort_values(ascending=False)

    truncated = False
    if len(grouped) > MAX_LOCATIONS:
        warnings.append(f"Truncated to top {MAX_LOCATIONS} of {len(grouped)} locations.")
        grouped = grouped.iloc[:MAX_LOCATIONS]
        truncated = True

    # Percentage share is well-defined as "share of the sum" for Sum/Count; for
    # Average/Median/Min/Max it's still each location's share of the total across the
    # (possibly truncated) returned locations, per the spec's hover-card requirement —
    # there's no more rigorous universal definition across every aggregation choice.
    total = float(grouped.sum())
    locations = [
        {
            "location": str(loc),
            "value": _round(float(value)),
            "percentage_share": _round(float(value) / total * 100, 2) if total else None,
        }
        for loc, value in grouped.items()
    ]
    leaderboard = [
        {"rank": i + 1, "location": item["location"], "value": item["value"], "percentage_share": item["percentage_share"]}
        for i, item in enumerate(locations[:LEADERBOARD_SIZE])
    ]

    location_type = _classify_location_type(location_col)
    extra: dict[str, Any] = {
        "locations": locations,
        "location_type": location_type.value,
        "aggregation": agg.value,
        "total_locations": int(len(grouped)),
    }
    chart = ChartPayload(chart_type=chart_type, labels=[loc["location"] for loc in locations], table=leaderboard, extra=extra)
    summary = {
        "location_column": location_col,
        "location_type": location_type.value,
        "metric_column": metric_col,
        "aggregation": agg.value,
        "total_locations": int(len(grouped)),
        "truncated": truncated,
    }
    return chart, summary


def _compute_geospatial(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    location_col = _require_column(df, req.location_column, "location")
    warnings: list[str] = []

    if req.location_column_2:
        lng_col = _require_column(df, req.location_column_2, "location (longitude)")
        if not _is_numeric_column(df, location_col) or not _is_numeric_column(df, lng_col):
            raise error_response(
                status_code=400,
                detail="location_column and location_column_2 must both be numeric (Latitude, Longitude).",
            )
        resolved_chart_type = _snap_chart_type(chart_type, LocationType.COORDINATES, warnings)
        chart, summary = _compute_coordinate_geospatial(
            df, req, resolved_chart_type, location_col, lng_col, warnings
        )
    else:
        resolved_chart_type = _snap_chart_type(chart_type, LocationType.REGION, warnings)
        chart, summary = _compute_region_geospatial(df, req, resolved_chart_type, location_col, warnings)

    return chart, summary, warnings
