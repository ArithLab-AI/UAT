FREE_USER = 1
LITE_USER = 2
PRO_USER = 3
ENTERPRISE_USER = 4
DEFAULT_USER_ROLE = FREE_USER


def normalize_user_role(value: int | str | None) -> int:
    if value is None:
        return DEFAULT_USER_ROLE
    if isinstance(value, int):
        if value in {FREE_USER, LITE_USER, PRO_USER, ENTERPRISE_USER}:
            return value
        raise ValueError("user_role must be 1, 2, 3, or 4")
    if isinstance(value, str):
        stripped_value = value.strip().lower()
        if stripped_value in {"1", "free", "free_user"}:
            return FREE_USER
        if stripped_value in {"2", "lite", "lite_user"}:
            return LITE_USER
        if stripped_value in {"3", "pro", "pro_user"}:
            return PRO_USER
        if stripped_value in {"4", "enterprise_user"}:
            return ENTERPRISE_USER
        raise ValueError("User Role must be 1, 2, 3, or 4")
    raise ValueError("User Role must be 1, 2, 3, or 4")
