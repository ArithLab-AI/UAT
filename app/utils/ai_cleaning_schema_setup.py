from sqlalchemy import inspect, text

from app.db.database import Base
from app.models.ai_cleaning_models import AICleaningJobDetail


def ensure_ai_cleaning_schema(engine) -> None:
    Base.metadata.create_all(bind=engine, tables=[AICleaningJobDetail.__table__])

    inspector = inspect(engine)
    if not inspector.has_table("cleaning_jobs"):
        return

    columns = {column["name"] for column in inspector.get_columns("cleaning_jobs")}
    with engine.begin() as connection:
        if "ai_cleaning_type" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE cleaning_jobs "
                    "ADD COLUMN ai_cleaning_type BOOLEAN NOT NULL DEFAULT FALSE"
                )
            )
        if "source_dataset_id" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE cleaning_jobs "
                    "ADD COLUMN source_dataset_id INTEGER NULL"
                )
            )
        if "columns_after_names" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE cleaning_jobs "
                    "ADD COLUMN columns_after_names JSON"
                )
            )

    if inspector.has_table("ai_cleaning_job_details"):
        ai_cleaning_columns = {column["name"] for column in inspector.get_columns("ai_cleaning_job_details")}
        with engine.begin() as connection:
            if "source_suggestion_id" not in ai_cleaning_columns:
                connection.execute(
                    text(
                        "ALTER TABLE ai_cleaning_job_details "
                        "ADD COLUMN source_suggestion_id VARCHAR(36)"
                    )
                )
            if "source_suggestion_priority" not in ai_cleaning_columns:
                connection.execute(
                    text(
                        "ALTER TABLE ai_cleaning_job_details "
                        "ADD COLUMN source_suggestion_priority VARCHAR(20)"
                    )
                )
            if "cleaned_columns" not in ai_cleaning_columns:
                connection.execute(
                    text(
                        "ALTER TABLE ai_cleaning_job_details "
                        "ADD COLUMN cleaned_columns JSON"
                    )
                )
