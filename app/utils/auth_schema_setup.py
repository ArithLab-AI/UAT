from sqlalchemy import inspect, text

from app.enum.user_role_enum import DEFAULT_USER_ROLE

def ensure_auth_schema(engine) -> None:
    inspector = inspect(engine)

    if not inspector.has_table("users"):
        return

    user_columns = {column["name"] for column in inspector.get_columns("users")}
    with engine.begin() as connection:
        if "user_role" not in user_columns:
            connection.execute(
                text(
                    "ALTER TABLE users "
                    f"ADD COLUMN user_role INTEGER NOT NULL DEFAULT {DEFAULT_USER_ROLE}"
                )
            )
        if "google_subject" not in user_columns:
            connection.execute(
                text("ALTER TABLE users ADD COLUMN google_subject VARCHAR")
            )
            connection.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_users_google_subject "
                    "ON users (google_subject)"
                )
            )
