import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, Text

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
