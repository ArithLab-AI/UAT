"""Module 1: Predictive Regression Analysis.

Trains a regression model (Linear Regression, Decision Tree, Random Forest,
XGBoost, or Auto ML — whichever of those four scores best) to predict a numeric
target column from a set of numeric & categorical predictor columns, then
reports fit metrics plus an Actual-vs-Predicted plot and a feature-importance
ranking.

No imputation is performed: rows missing a value in the target or any selected
predictor are dropped before training (reported back as ``rows_dropped_incomplete``)
rather than silently fabricated. Categorical predictors are one-hot encoded.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split as sk_train_test_split
from sklearn.tree import DecisionTreeRegressor

from app.enum.chart_type_enum import ChartType
from app.enum.regression_enum import RegressionModelType, TrainTestSplitType
from app.schemas.basic_analysis_schema import BasicAnalysisRequest, ChartPayload
from app.services.basic_analysis_helpers import (
    is_numeric_column as _is_numeric_column,
    numeric_series as _numeric_series,
    require_column as _require_column,
    round_value as _round,
)
from app.utils.responses import error_response

MAX_TREND_POINTS = 2000            # cap for the Actual vs Predicted scatter
MAX_FEATURE_IMPORTANCE_ROWS = 30   # cap for the Feature Impact bar chart
MIN_TRAINING_ROWS = 20             # floor so even a 90/10 split leaves >=2 test rows
MAX_CATEGORICAL_CARDINALITY = 50   # reject rather than silently drop a huge one-hot blowup

_AUTO_ML_CANDIDATES = (
    RegressionModelType.LINEAR_REGRESSION,
    RegressionModelType.DECISION_TREE,
    RegressionModelType.RANDOM_FOREST,
    RegressionModelType.XGBOOST,
)

# Spec doesn't define a distinct ratio for Time-based Split, so it reuses the 80/20
# split point but slices by existing row order instead of shuffling (see below).
_SPLIT_TEST_SIZE: dict[TrainTestSplitType, float] = {
    TrainTestSplitType.SPLIT_80_20: 0.2,
    TrainTestSplitType.SPLIT_70_30: 0.3,
    TrainTestSplitType.SPLIT_90_10: 0.1,
    TrainTestSplitType.TIME_BASED: 0.2,
}


def _build_model(kind: RegressionModelType):
    if kind == RegressionModelType.LINEAR_REGRESSION:
        return LinearRegression()
    if kind == RegressionModelType.DECISION_TREE:
        return DecisionTreeRegressor(random_state=42)
    if kind == RegressionModelType.RANDOM_FOREST:
        return RandomForestRegressor(random_state=42)
    if kind == RegressionModelType.XGBOOST:
        try:
            from xgboost import XGBRegressor
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise error_response(
                status_code=500,
                detail="xgboost is required for the XGBoost model. Add 'xgboost' to requirements.",
            ) from exc
        return XGBRegressor(random_state=42, verbosity=0)
    raise ValueError(f"Unsupported regression model: {kind}")  # pragma: no cover - exhaustive enum


def _fit_and_score(
    kind: RegressionModelType, X_train: pd.DataFrame, y_train: pd.Series, X_test: pd.DataFrame, y_test: pd.Series
) -> tuple[Any, np.ndarray, float]:
    model = _build_model(kind)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    r2 = float(r2_score(y_test, y_pred))
    return model, y_pred, r2


def _feature_importance(
    model: Any, model_kind: RegressionModelType, feature_names: list[str], X_train: pd.DataFrame
) -> list[dict[str, Any]]:
    if model_kind == RegressionModelType.LINEAR_REGRESSION:
        # Raw coefficients aren't comparable across features on different scales;
        # weighting by each feature's train-set std puts them on a common footing.
        coefs = np.asarray(model.coef_, dtype=float)
        stds = X_train.std(ddof=0).to_numpy()
        raw_importance = np.abs(coefs * stds)
    elif hasattr(model, "feature_importances_"):
        raw_importance = np.asarray(model.feature_importances_, dtype=float)
    else:  # pragma: no cover - every supported model has one of the above
        raw_importance = np.zeros(len(feature_names))

    total = float(raw_importance.sum())
    normalized = (raw_importance / total) if total > 0 else raw_importance

    ranked = sorted(
        (
            {"feature": name, "importance": _round(float(value), 6)}
            for name, value in zip(feature_names, normalized)
        ),
        key=lambda item: item["importance"] or 0,
        reverse=True,
    )
    return ranked


def _compute_predictive_regression(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    target_col = _require_column(df, req.target_column, "target")

    predictors = [c.strip() for c in (req.predictor_columns or []) if c and c.strip()]
    if not predictors:
        raise error_response(
            status_code=400,
            detail="'predictors' column(s) are required for Predictive Regression Analysis.",
        )
    missing = [c for c in predictors if c not in df.columns]
    if missing:
        raise error_response(status_code=400, detail=f"Column(s) not found: {missing}")
    if target_col in predictors:
        raise error_response(
            status_code=400,
            detail=f"Target column '{target_col}' cannot also be used as a predictor.",
        )

    if not _is_numeric_column(df, target_col):
        raise error_response(status_code=400, detail=f"'{target_col}' must be numeric to be a prediction target.")

    numeric_predictors = [c for c in predictors if _is_numeric_column(df, c)]
    categorical_predictors = [c for c in predictors if c not in numeric_predictors]

    # Build a clean working frame: coerce numeric predictors/target, normalize categorical
    # predictors to trimmed strings (blank -> missing), then drop incomplete rows. No
    # imputation — a fabricated value would quietly distort the trained model.
    working = pd.DataFrame({target_col: _numeric_series(df, target_col)})
    for col in numeric_predictors:
        working[col] = _numeric_series(df, col)
    for col in categorical_predictors:
        cleaned = df[col].astype(str).str.strip()
        working[col] = cleaned.replace("", np.nan)

    rows_before = len(working)
    working = working.dropna(subset=[target_col] + predictors)
    rows_dropped = rows_before - len(working)

    if len(working) < MIN_TRAINING_ROWS:
        raise error_response(
            status_code=400,
            detail=(
                f"Need at least {MIN_TRAINING_ROWS} complete rows (target + all predictors "
                f"non-missing) to train a regression model; found {len(working)}."
            ),
        )

    high_cardinality = [
        col for col in categorical_predictors if working[col].nunique(dropna=True) > MAX_CATEGORICAL_CARDINALITY
    ]
    if high_cardinality:
        raise error_response(
            status_code=400,
            detail=(
                f"Column(s) have too many distinct categories for one-hot encoding "
                f"(max {MAX_CATEGORICAL_CARDINALITY}): {high_cardinality}. Remove them or pick a "
                "lower-cardinality column."
            ),
        )

    X = working[predictors].copy()
    if categorical_predictors:
        X = pd.get_dummies(X, columns=categorical_predictors, drop_first=False)
    X = X.astype(float)
    y = working[target_col].astype(float)
    feature_names = list(X.columns)

    test_size = _SPLIT_TEST_SIZE[req.train_test_split]
    split_note: str | None = None
    if req.train_test_split == TrainTestSplitType.TIME_BASED:
        # No dedicated date-column step in the spec for this analysis, so "time-based"
        # is approximated by the dataset's existing row order: earliest rows train,
        # latest rows test, no shuffling.
        split_idx = int(round(len(working) * (1 - test_size)))
        split_idx = max(1, min(split_idx, len(working) - 1))
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        split_note = (
            "Time-based split approximated using the dataset's existing row order "
            "(no shuffling): earliest rows trained, latest rows tested."
        )
    else:
        X_train, X_test, y_train, y_test = sk_train_test_split(
            X, y, test_size=test_size, random_state=42
        )

    if len(X_test) < 2:
        raise error_response(
            status_code=400,
            detail="Test split has fewer than 2 rows — use more data or a different split ratio.",
        )

    warnings: list[str] = []

    if req.regression_model == RegressionModelType.AUTO_ML:
        results: list[tuple[RegressionModelType, Any, np.ndarray, float]] = []
        for kind in _AUTO_ML_CANDIDATES:
            try:
                model, y_pred, r2 = _fit_and_score(kind, X_train, y_train, X_test, y_test)
                results.append((kind, model, y_pred, r2))
            except Exception as exc:  # noqa: BLE001 - one bad candidate shouldn't sink Auto ML
                warnings.append(f"Skipped {kind.value} during Auto ML: {exc}")
        if not results:
            raise error_response(status_code=500, detail="Auto ML failed to train any candidate model.")
        results.sort(key=lambda item: item[3], reverse=True)
        model_kind, model, y_pred, r2 = results[0]
        warnings.append(
            f"Auto ML selected {model_kind.value} (R²={round(r2, 4)}) among "
            f"{', '.join(k.value for k, *_ in results)}."
        )
    else:
        model_kind = req.regression_model
        model, y_pred, r2 = _fit_and_score(model_kind, X_train, y_train, X_test, y_test)

    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    mae = float(mean_absolute_error(y_test, y_pred))

    n = len(y_test)
    p = X_test.shape[1]
    adjusted_r2 = None
    if n - p - 1 > 0 and not (np.isnan(r2) or np.isinf(r2)):
        adjusted_r2 = 1 - (1 - r2) * (n - 1) / (n - p - 1)

    actual = pd.Series(np.asarray(y_test, dtype=float)).reset_index(drop=True)
    predicted = pd.Series(np.asarray(y_pred, dtype=float)).reset_index(drop=True)
    pair_df = pd.DataFrame({"actual": actual, "predicted": predicted})
    sample = (
        pair_df.sample(MAX_TREND_POINTS, random_state=42) if len(pair_df) > MAX_TREND_POINTS else pair_df
    )
    if len(pair_df) > MAX_TREND_POINTS:
        warnings.append(f"Actual vs Predicted plot sampled down to {MAX_TREND_POINTS} points for performance.")
    points = [
        {"x": _round(float(row["actual"])), "y": _round(float(row["predicted"]))}
        for _, row in sample.iterrows()
    ]

    finite_actual = actual[np.isfinite(actual)]
    finite_predicted = predicted[np.isfinite(predicted)]
    reference_line = None
    if not finite_actual.empty and not finite_predicted.empty:
        lo = float(min(finite_actual.min(), finite_predicted.min()))
        hi = float(max(finite_actual.max(), finite_predicted.max()))
        reference_line = {"start": {"x": _round(lo), "y": _round(lo)}, "end": {"x": _round(hi), "y": _round(hi)}}

    feature_importance = _feature_importance(model, model_kind, feature_names, X_train)
    truncated_feature_importance = feature_importance[:MAX_FEATURE_IMPORTANCE_ROWS]
    if len(feature_importance) > MAX_FEATURE_IMPORTANCE_ROWS:
        warnings.append(
            f"Feature Impact chart truncated to top {MAX_FEATURE_IMPORTANCE_ROWS} of "
            f"{len(feature_importance)} encoded features (categorical predictors are one-hot encoded)."
        )

    extra: dict[str, Any] = {
        "metrics": {
            "r2": _round(r2),
            "adjusted_r2": _round(adjusted_r2),
            "rmse": _round(rmse),
            "mae": _round(mae),
        },
        "reference_line": reference_line,
        "actual_vs_predicted": points,
        "feature_importance": truncated_feature_importance,
        "model_used": model_kind.value,
    }

    if chart_type == ChartType.FEATURE_IMPORTANCE_BAR:
        chart = ChartPayload(
            chart_type=chart_type,
            labels=[item["feature"] for item in truncated_feature_importance],
            series=[{"name": "importance", "data": [item["importance"] for item in truncated_feature_importance]}],
            extra=extra,
        )
    else:
        chart = ChartPayload(chart_type=ChartType.ACTUAL_VS_PREDICTED_SCATTER, points=points, extra=extra)

    summary: dict[str, Any] = {
        "target_column": target_col,
        "predictor_columns": predictors,
        "numeric_predictors": numeric_predictors,
        "categorical_predictors": categorical_predictors,
        "encoded_feature_count": len(feature_names),
        "model_requested": req.regression_model.value,
        "model_used": model_kind.value,
        "train_test_split": req.train_test_split.value,
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "rows_used": int(len(working)),
        "rows_dropped_incomplete": int(rows_dropped),
        "r2": _round(r2),
        "adjusted_r2": _round(adjusted_r2),
        "rmse": _round(rmse),
        "mae": _round(mae),
    }
    if split_note:
        summary["split_note"] = split_note

    return chart, summary, warnings
