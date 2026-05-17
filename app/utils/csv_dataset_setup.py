from sqlalchemy import inspect, text
from app.utils.object_storage import get_object_storage_service


def ensure_csv_dataset_schema(engine) -> None:
    inspector = inspect(engine)
    bucket_name = get_object_storage_service().bucket_name

    if inspector.has_table("csv_uploaded_datasets"):
        uploaded_columns = {column["name"] for column in inspector.get_columns("csv_uploaded_datasets")}
        _ensure_retention_columns(engine, "csv_uploaded_datasets", uploaded_columns)
        if "table_name" not in uploaded_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_uploaded_datasets ADD COLUMN table_name VARCHAR"))
                connection.execute(
                    text(
                        "UPDATE csv_uploaded_datasets "
                        "SET table_name = CONCAT('upload_legacy_', id) "
                        "WHERE table_name IS NULL"
                    )
                )
        if "internal_columns" not in uploaded_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_uploaded_datasets ADD COLUMN internal_columns JSON"))
                connection.execute(
                    text(
                        "UPDATE csv_uploaded_datasets "
                        "SET internal_columns = columns "
                        "WHERE internal_columns IS NULL"
                    )
                )
        if "file_size" not in uploaded_columns:
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "ALTER TABLE csv_uploaded_datasets "
                        "ADD COLUMN file_size INTEGER NOT NULL DEFAULT 0"
                    )
                )
        if "storage_key" not in uploaded_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_uploaded_datasets ADD COLUMN storage_key VARCHAR"))
        if "file_url" not in uploaded_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_uploaded_datasets ADD COLUMN file_url VARCHAR"))
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE csv_uploaded_datasets "
                    "SET storage_key = CONCAT('csv_datasets/', table_name, '.csv') "
                    "WHERE storage_key IS NULL AND table_name IS NOT NULL"
                )
            )
            if bucket_name:
                connection.execute(
                    text(
                        "UPDATE csv_uploaded_datasets "
                        "SET file_url = CONCAT('s3://', :bucket_name, '/', storage_key) "
                        "WHERE file_url IS NULL AND storage_key IS NOT NULL"
                    ),
                    {"bucket_name": bucket_name},
                )

    if inspector.has_table("csv_merged_datasets"):
        merged_columns = {column["name"] for column in inspector.get_columns("csv_merged_datasets")}
        if "table_name" not in merged_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_merged_datasets ADD COLUMN table_name VARCHAR"))
                connection.execute(
                    text(
                        "UPDATE csv_merged_datasets "
                        "SET table_name = CONCAT('merged_legacy_', id) "
                        "WHERE table_name IS NULL"
                    )
                )
        if "internal_columns" not in merged_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_merged_datasets ADD COLUMN internal_columns JSON"))
                connection.execute(
                    text(
                        "UPDATE csv_merged_datasets "
                        "SET internal_columns = columns "
                        "WHERE internal_columns IS NULL"
                    )
                )
        if "storage_key" not in merged_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_merged_datasets ADD COLUMN storage_key VARCHAR"))
        if "file_url" not in merged_columns:
            with engine.begin() as connection:
                connection.execute(text("ALTER TABLE csv_merged_datasets ADD COLUMN file_url VARCHAR"))
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE csv_merged_datasets "
                    "SET storage_key = CONCAT('csv_datasets/', table_name, '.csv') "
                    "WHERE storage_key IS NULL AND table_name IS NOT NULL"
                )
            )
            if bucket_name:
                connection.execute(
                    text(
                        "UPDATE csv_merged_datasets "
                        "SET file_url = CONCAT('s3://', :bucket_name, '/', storage_key) "
                        "WHERE file_url IS NULL AND storage_key IS NOT NULL"
                    ),
                    {"bucket_name": bucket_name},
                )


def _ensure_retention_columns(engine, table_name: str, columns: set[str]) -> None:
    with engine.begin() as connection:
        if "is_retention" not in columns:
            connection.execute(
                text(
                    f"ALTER TABLE {table_name} "
                    "ADD COLUMN is_retention BOOLEAN NOT NULL DEFAULT FALSE"
                )
            )
        if "retention_until" not in columns:
            connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN retention_until TIMESTAMP"))
        if "retention_at" not in columns:
            connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN retention_at TIMESTAMP"))
