from sqlalchemy import inspect, text

from app.enum.user_role_enum import DEFAULT_USER_ROLE
from app.models.subscription_models import UserUploadStorageUsage


def ensure_subscription_schema(engine) -> None:
    UserUploadStorageUsage.__table__.create(bind=engine, checkfirst=True)
    inspector = inspect(engine)

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO user_upload_storage_usage (
                    user_id,
                    uploaded_dataset_id,
                    file_size_bytes,
                    file_name,
                    sheet_name,
                    created_at
                )
                SELECT
                    created_by_user_id,
                    id,
                    file_size,
                    file_name,
                    sheet_name,
                    created_at
                FROM csv_uploaded_datasets uploaded
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM user_upload_storage_usage usage
                    WHERE usage.uploaded_dataset_id = uploaded.id
                )
                """
            )
        )

    if not inspector.has_table("subscription_plans"):
        return

    plan_columns = {column["name"] for column in inspector.get_columns("subscription_plans")}
    if "user_role" not in plan_columns:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE subscription_plans "
                    f"ADD COLUMN user_role INTEGER NOT NULL DEFAULT {DEFAULT_USER_ROLE}"
                )
            )
