import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from app.db.database import Base


class SavedChart(Base):
    """A visualization chart the user explicitly saved to their dashboard.

    Produced by ``/basic-analysis/run`` and persisted here so the dashboard page
    can list every chart a user built, grouped by the dataset it came from, and
    replay its chart-ready payload without recomputing.
    """

    __tablename__ = "saved_charts"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    # Dataset the chart was built from. Plain columns (no FK) to mirror
    # DatasetAnalysis / DataChatSession — uploaded and merged datasets live in
    # separate tables, so source_type disambiguates source_dataset_id.
    source_dataset_id = Column(Integer, nullable=False, index=True)
    source_type = Column(String(20), nullable=False, index=True)  # uploaded | merged
    is_clean = Column(Boolean, nullable=False, default=False)
    dataset_name = Column(String(255), nullable=False)
    file_name = Column(String(255), nullable=False)

    title = Column(String(255), nullable=False)
    analysis_type = Column(String(50), nullable=False, index=True)
    chart_type = Column(String(50), nullable=False)
    row_count_used = Column(Integer, nullable=False, default=0)

    # sha256 of the analysis request (minus analysis_name). Re-running the same
    # analysis updates the stored chart in place instead of adding a duplicate.
    request_fingerprint = Column(String(64), nullable=False, index=True)
    # The BasicAnalysisRequest used to build the chart, so it can be re-run later.
    request_payload = Column(JSON, nullable=False)
    # The chart-ready envelope (ChartPayload) the frontend renders.
    chart_data = Column(JSON, nullable=False)
    summary = Column(JSON, nullable=True)
    warnings = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Dashboard(Base):
    """A saved Dashboard Builder board: a named grid of widgets built from one
    dataset, with each widget's chart, layout and per-widget style.

    Unlike ``SavedChart`` (a single analysis result), a widget here also carries
    UI-only state (colors, sizing, resize handles) the builder needs to reopen
    the board exactly as the user left it. That JSON is frontend-owned, so it is
    stored and served back exactly as sent — nothing here is recomputed.
    """

    __tablename__ = "dashboards"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)

    # Id the frontend generates client-side (e.g. "dashboard-<timestamp>").
    # Saving again with the same value updates this board instead of duplicating it.
    client_generated_id = Column(String(100), nullable=False, index=True)
    schema_version = Column(Integer, nullable=False, default=1)

    name = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)

    # Dataset the board was built from. Plain columns (no FK) to mirror SavedChart —
    # uploaded and merged datasets live in separate tables.
    source_dataset_id = Column(Integer, nullable=False, index=True)
    source_type = Column(String(20), nullable=False, index=True)  # uploaded | merged
    source_dataset_name = Column(String(255), nullable=True)
    source_dataset_columns = Column(JSON, nullable=True)

    layout_engine = Column(JSON, nullable=True)
    # The widgets array exactly as the builder produced it (chart_spec, layout,
    # per-widget settings, ...). One dict per widget; shape is frontend-owned.
    widgets = Column(JSON, nullable=False, default=list)
    # Opaque builder UI state (e.g. rendered widget snapshots) kept so the board
    # reopens instantly without recomputing anything.
    render_state = Column(JSON, nullable=True)
    selected_widget_id = Column(String(100), nullable=True)

    created_from = Column(String(50), nullable=True)
    # "saved_at" from the client payload, kept alongside our own updated_at.
    client_saved_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    user = relationship("User")

    __table_args__ = (
        UniqueConstraint(
            "created_by_user_id",
            "client_generated_id",
            name="uq_dashboards_user_client_id",
        ),
    )
