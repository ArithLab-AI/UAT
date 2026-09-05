from sqlalchemy import inspect, text

from app.db.database import Base
from app.models.dashboard_models import Dashboard, SavedChart


def ensure_dashboard_schema(engine) -> None:
    """Create saved_charts if missing and backfill columns added later."""
    Base.metadata.create_all(bind=engine, tables=[SavedChart.__table__])

    inspector = inspect(engine)
    if not inspector.has_table("saved_charts"):
        return

    columns = {column["name"] for column in inspector.get_columns("saved_charts")}
    with engine.begin() as connection:
        if "request_fingerprint" not in columns:
            connection.execute(
                text("ALTER TABLE saved_charts ADD COLUMN request_fingerprint VARCHAR(64)")
            )
            connection.execute(
                text(
                    "UPDATE saved_charts SET request_fingerprint = id "
                    "WHERE request_fingerprint IS NULL"
                )
            )


def ensure_dashboard_builder_schema(engine) -> None:
    """Create the Dashboard Builder ``dashboards`` table if missing.

    Separate from ``ensure_dashboard_schema`` (which owns ``saved_charts``) so
    each table's setup evolves independently; both are idempotent, `checkfirst`
    creates, run every startup like the other ``ensure_*_schema`` helpers.
    """
    Dashboard.__table__.create(bind=engine, checkfirst=True)
