from sqlalchemy import inspect, text

from app.enum.user_role_enum import DEFAULT_USER_ROLE


def ensure_subscription_schema(engine) -> None:
    inspector = inspect(engine)

    if not inspector.has_table("subscription_plans"):
        return

    plan_columns = {column["name"] for column in inspector.get_columns("subscription_plans")}
    if "user_role" in plan_columns:
        return

    with engine.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE subscription_plans "
                f"ADD COLUMN user_role INTEGER NOT NULL DEFAULT {DEFAULT_USER_ROLE}"
            )
        )
