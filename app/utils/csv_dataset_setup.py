from sqlalchemy import inspect, text


def ensure_csv_dataset_schema(engine) -> None:
    inspector = inspect(engine)

    if inspector.has_table("csv_uploaded_datasets"):
        uploaded_columns = {column["name"] for column in inspector.get_columns("csv_uploaded_datasets")}
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
