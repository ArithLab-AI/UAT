from sqlalchemy import inspect, text


def ensure_file_upload_schema(engine) -> None:
    inspector = inspect(engine)

    if not inspector.has_table("uploaded_files"):
        return

    columns = {column["name"] for column in inspector.get_columns("uploaded_files")}

    with engine.begin() as connection:
        if "storage_key" not in columns:
            connection.execute(text("ALTER TABLE uploaded_files ADD COLUMN storage_key VARCHAR"))

        if "status" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE uploaded_files "
                    "ADD COLUMN status VARCHAR NOT NULL DEFAULT 'uploaded'"
                )
            )
