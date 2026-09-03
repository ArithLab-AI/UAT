# Dashboard Charts — Frontend Integration Guide

How the manual Basic Analysis flow feeds the dashboard, and every endpoint the
frontend needs to build, list, render, refresh, rename and delete saved charts.

---

## Mental model

- **Charts are created in the Basic Analysis flow, never in the dashboard.**
  `POST /basic-analysis/run` computes a chart; `POST /basic-analysis/charts`
  pins it. The dashboard screen only **reads, refreshes, renames and deletes**.
- **`/run` persists nothing.** It is a pure compute endpoint. Call it as often
  as you like while the user tweaks analysis type, chart type, aggregation,
  columns, N, granularity, etc. Nothing lands on the dashboard until the user
  hits **“Add to dashboard”**.
- **A saved chart is a snapshot.** The chart payload is stored exactly as `/run`
  returned it and served straight back — nothing is recomputed on read. If the
  underlying dataset later changes (re-clean, new rows), the saved chart keeps
  showing the old numbers until it is **refreshed**.
- **Re-saving the same analysis updates in place.** The backend fingerprints the
  analysis config (everything except the title); saving the same config again
  updates the existing row instead of creating a duplicate.
- **Ownership + liveness are enforced server-side.** All list/read endpoints are
  scoped to the current user and automatically hide charts whose source dataset
  has been deleted.

---

## Response envelope

Every success response:

```json
{ "status_code": 200, "message": "…", "data": <payload> }
```

Every error response:

```json
{ "status_code": 409, "fields": [], "message": "human-readable reason" }
```

Validation errors are `422` with `fields` naming the offending keys.

---

## The end-to-end flow

```
Basic Analysis screen                         Dashboard screen
────────────────────                         ────────────────
1. GET  /basic-analysis/types                 A. GET  /dashboard/overview
2. POST /basic-analysis/run   (repeat freely) B. GET  /dashboard/charts/{id}
3. POST /basic-analysis/charts  ← "Add to        C. POST /dashboard/charts/{id}/refresh
                                   dashboard"    D. PUT  /dashboard/charts/{id}   (rename)
                                                 E. DELETE /dashboard/charts/{id}
```

1. **Load pickers** — `GET /basic-analysis/types` for analysis types, their
   default/supported chart types, supported aggregations and required columns.
2. **Explore** — on every parameter change call `POST /basic-analysis/run` and
   render `data.chart`. No persistence, so no cleanup needed.
3. **Pin** — when the user clicks *Add to dashboard*, call
   `POST /basic-analysis/charts` with the fields from the last `/run` response
   plus a `title`. Store the returned `id`.
4. **Dashboard load** — `GET /dashboard/overview` gives every dataset with its
   charts nested; enough to populate both the dataset dropdown and the chart
   dropdown in one call.
5. **Render a chart** — `GET /dashboard/charts/{id}` returns the full
   `chart` payload to draw.
6. **Refresh** — `POST /dashboard/charts/{id}/refresh` re-runs the saved
   analysis server-side against current data and overwrites the snapshot.
7. **Rename** — `PUT /dashboard/charts/{id}` with a changed `title`.
8. **Delete** — `DELETE /dashboard/charts/{id}`.

---

## Endpoints

### 1. `GET /basic-analysis/types`

Metadata for the analysis-type and chart-type pickers. No params.

`data`: array of

| field | type | notes |
|---|---|---|
| `analysis_type` | string | enum value |
| `label`, `tagline` | string | display text |
| `default_chart_type` | string | preselect this |
| `supported_chart_types` | string[] | chart-type dropdown options |
| `supported_aggregations` | string[] | aggregation dropdown options |
| `column_requirements` | object[] | `{ role, required, data_type, label, example }` |

---

### 2. `POST /basic-analysis/run`

Compute a chart. **Persists nothing.**

**Request body** (`BasicAnalysisRequest`):

| field | type | required | notes |
|---|---|---|---|
| `analysis_name` | string | ✅ | 1–200 chars |
| `dataset_id` | int | ✅ | |
| `dataset_type` | `"uploaded" \| "merged"` | ✅ | |
| `is_clean` | bool | | default `false`; `true` = run against the cleaned dataset |
| `analysis_type` | enum | ✅ | e.g. `top_n`, `time_series`, `correlation` |
| `chart_type` | enum | | omit to use the analysis type’s default |
| `x_column` | string | conditional | categorical for most, date for time series |
| `y_column` | string | conditional | numeric; optional for Top/Bottom N and Time Series |
| `columns` | string[] | conditional | Correlation: 2+ numeric columns |
| `aggregation` | enum | | Simple/Advanced Distribution, Top/Bottom N, Time Series, Geospatial |
| `n` | int | | 1–10, default 10 (Top/Bottom N) |
| `granularity` | enum | | default `monthly` (Time Series) |
| `target_column`, `predictor_columns`, `regression_model`, `train_test_split` | | conditional | Predictive Regression |
| `location_column`, `location_column_2`, `metric_column` | | conditional | Geospatial |

Which column fields are required depends on `analysis_type` — drive this off
`column_requirements` from `GET /basic-analysis/types`.

**Response `data`** (`BasicAnalysisResponse`):

```json
{
  "analysis_type": "top_n",
  "chart_type": "bar",
  "dataset_id": 42,
  "dataset_type": "uploaded",
  "dataset_name": "Sales 2024",
  "file_name": "sales_2024.csv",
  "row_count_used": 12045,
  "chart": {
    "chart_type": "bar",
    "labels": ["North", "South", "West"],
    "series": [{ "name": "sum(revenue)", "data": [91234.5, 80122.0, 65540.2] }],
    "points": null,
    "table": null,
    "extra": {}
  },
  "summary": { "x_column": "region", "y_column": "revenue", "aggregation": "sum", "n": 3 },
  "warnings": []
}
```

`chart` is the generic envelope — only the fields relevant to `chart_type` are
populated, the rest are `null`:

| chart family | populated fields |
|---|---|
| bar / line / pie / area | `labels`, `series` |
| scatter / scatter+trend | `points`, `extra` (`r`, `r_squared`, `trend_line`, …) |
| correlation heatmap / pair plot | `labels`, `extra` (`matrix`, `pairs`, `pair_plot_data`, …) |
| descriptive / any table view | `table` |

---

### 3. `POST /basic-analysis/charts`  — “Add to dashboard”

Pin the last `/run` result to the dashboard. `201 Created`.

> Re-saving the same analysis config (same body minus `title`) **updates the
> existing chart in place** and returns its existing `id` — it does not create a
> duplicate. So this endpoint is safe to call again after the user re-runs and
> re-pins.

**Request body** (`SaveChartRequest`) — copy straight from the `/run` response:

| field | type | required | source |
|---|---|---|---|
| `dataset_id` | int ≥ 1 | ✅ | `run.data.dataset_id` |
| `dataset_type` | `"uploaded" \| "merged"` | ✅ | `run.data.dataset_type` |
| `is_clean` | bool | | the `is_clean` you sent to `/run` |
| `analysis_type` | string | ✅ | `run.data.analysis_type` |
| `chart_type` | string | ✅ | `run.data.chart_type` |
| `row_count_used` | int ≥ 0 | | `run.data.row_count_used` |
| `title` | string ≤ 255 | | user input; falls back to `analysis_name`, then `"<analysis_type> - <chart_type>"` |
| `analysis_name` | string ≤ 200 | | `run` request `analysis_name` |
| `chart` | object | ✅ | `run.data.chart` |
| `summary` | object | | `run.data.summary` |
| `warnings` | string[] | | `run.data.warnings` |
| `request_payload` | object | | **the full body you sent to `/run`** — required for server-side refresh to work later |

⚠️ **Always send `request_payload`.** It is the only record of how the chart was
built; without it `POST /dashboard/charts/{id}/refresh` returns `409` and the
user can only rebuild the chart from scratch.

**Response `data`** (`SavedChartDetailResponse`):

```json
{
  "id": "b1f2…",
  "title": "Top 3 regions by revenue",
  "analysis_type": "top_n",
  "chart_type": "bar",
  "source_dataset_id": 42,
  "source_type": "uploaded",
  "is_clean": false,
  "dataset_name": "Sales 2024",
  "file_name": "sales_2024.csv",
  "row_count_used": 12045,
  "created_at": "2026-09-03T10:15:00Z",
  "updated_at": "2026-09-03T10:15:00Z",
  "request_payload": { "...": "the /run body" },
  "chart": { "chart_type": "bar", "labels": [ ], "series": [ ] },
  "summary": { },
  "warnings": [ ]
}
```

---

### 4. `GET /dashboard/overview`

Everything needed to populate the dashboard in one call. No params.

`data`: array, one entry per dataset the user has charts for:

```json
[
  {
    "source_dataset_id": 42,
    "source_type": "uploaded",
    "dataset_name": "Sales 2024",
    "file_name": "sales_2024.csv",
    "chart_count": 3,
    "last_chart_created_at": "2026-09-03T10:15:00Z",
    "charts": [
      {
        "id": "b1f2…",
        "title": "Top 3 regions by revenue",
        "analysis_type": "top_n",
        "chart_type": "bar",
        "source_dataset_id": 42,
        "source_type": "uploaded",
        "is_clean": false,
        "dataset_name": "Sales 2024",
        "file_name": "sales_2024.csv",
        "row_count_used": 12045,
        "created_at": "2026-09-03T10:15:00Z",
        "updated_at": "2026-09-03T10:15:00Z"
      }
    ]
  }
]
```

The nested `charts` are **summaries** — no `chart` payload. Fetch the payload
with `GET /dashboard/charts/{id}` when the user selects a chart.

---

### 5. `GET /dashboard/datasets`

Just the dataset dropdown — same as `overview` entries but **without** the
`charts` array. Use when you don’t need the charts yet.

---

### 6. `GET /dashboard/charts`

Chart list, optionally scoped to one dataset. Query params:

| param | type | notes |
|---|---|---|
| `source_dataset_id` | int | optional filter |
| `source_type` | `"uploaded" \| "merged"` | optional filter |

`data`: array of chart **summaries** (same shape as the nested `charts` above).

---

### 7. `GET /dashboard/charts/{chart_id}`

Full chart to render. `data` is `SavedChartDetailResponse` (same shape as the
`POST /basic-analysis/charts` response). Draw `data.chart`; show
`data.warnings` if non-empty.

`404` if the chart doesn’t exist or isn’t the caller’s.

---

### 8. `POST /dashboard/charts/{chart_id}/refresh`  — pull fresh data

Re-runs the chart’s **saved** analysis config against the **current** dataset,
server-side, and overwrites the stored snapshot. **No request body.**

- The chart keeps its `id`, `title` and position.
- `chart`, `summary`, `warnings`, `row_count_used`, `dataset_name`, `file_name`
  and `updated_at` are refreshed.
- On success: `200`, `data` = the refreshed `SavedChartDetailResponse`.

**Failure modes — the old snapshot is left intact:**

| status | `message` (abridged) | meaning | suggested UI |
|---|---|---|---|
| `409` | “no saved analysis configuration to refresh” | chart saved without `request_payload` | disable Refresh; tell user to rebuild from Basic Analysis |
| `409` | “saved analysis configuration … no longer valid” | stored config fails validation | same as above |
| `400` | “Chart could not be refreshed: …” | e.g. a column the analysis needs was dropped/renamed by a re-clean | toast the message; keep showing the existing chart |
| `404` | “Chart could not be refreshed: … dataset not found” | source dataset deleted | toast; the chart will also drop out of the next `overview` |

> Use this instead of the `GET → run → PUT` dance. It is atomic (a failed
> refresh never half-writes), needs no body, and can’t be fed a stale or
> mismatched payload.

---

### 9. `PUT /dashboard/charts/{chart_id}`  — rename / manual replace

Replaces a chart with a **client-supplied** payload. Body is `SaveChartRequest`
(same as `POST /basic-analysis/charts`). **Nothing is recomputed.**

Primary use: renaming. Send the chart’s current fields with a changed `title`.
It also supports pushing a fresh client-side `/run` result if you ever need to,
but prefer `…/refresh` for that.

`404` if the chart isn’t the caller’s.

---

### 10. `DELETE /dashboard/charts/{chart_id}`

Removes the chart. `data` is `null`. `404` if not the caller’s.

---

## Worked example — two datasets

```
1. Upload dataset A and B, clean both.

2. Basic Analysis screen, dataset A:
   POST /basic-analysis/run  (analysis_type=advanced_distribution, chart_type=bar, is_clean=true)
   → tweak to line, then pie, then aggregation avg   (3 more /run calls, nothing saved)
   → user likes the bar version, clicks "Add to dashboard"
   POST /basic-analysis/charts  { ...run.data, title: "Revenue by region",
                                  request_payload: <the bar /run body> }
   → 201, id = "aaa"
   Repeat for two more charts on A → ids "bbb", "ccc".

3. Same for dataset B → ids "ddd", "eee".

4. Dashboard screen:
   GET /dashboard/overview
   → [ { dataset A, chart_count 3, charts:[aaa,bbb,ccc] },
       { dataset B, chart_count 2, charts:[ddd,eee] } ]

5. User selects chart "aaa":
   GET /dashboard/charts/aaa → render data.chart

6. User re-cleans dataset A (adds rows). Back on the dashboard they click Refresh on "aaa":
   POST /dashboard/charts/aaa/refresh
   → 200, data.chart now reflects the new rows, data.row_count_used bumped,
     data.updated_at newer.

7. User renames "bbb":
   PUT /dashboard/charts/bbb  { ...current fields, title: "Q3 revenue by region" }

8. User deletes "eee":
   DELETE /dashboard/charts/eee
```

---

## Migration note

**`POST /dashboard/charts` has been removed.** The create endpoint is now
**`POST /basic-analysis/charts`** — identical request and response, different
path. Update the “Add to dashboard” call site.

Unchanged: `GET /dashboard/overview`, `GET /dashboard/datasets`,
`GET /dashboard/charts`, `GET /dashboard/charts/{id}`,
`PUT /dashboard/charts/{id}`, `DELETE /dashboard/charts/{id}`.

New: `POST /dashboard/charts/{id}/refresh`.
