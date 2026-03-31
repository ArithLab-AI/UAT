from sqlalchemy import inspect, text

def ensure_auth_schema(engine) -> None:
    inspector = inspect(engine)

    if not inspector.has_table("users"):
        return

    user_columns = {column["name"] for column in inspector.get_columns("users")}
    if "user_role" in user_columns:
        return

    with engine.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE users "
                "ADD COLUMN user_role INTEGER NOT NULL DEFAULT 2"
            )
        )
