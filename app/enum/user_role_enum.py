from enum import IntEnum

class UserRoleEnum(IntEnum):
    buisiness = 1
    customer_segmant = 2

def normalize_user_role(value: int | str | UserRoleEnum) -> int:
    if isinstance(value, UserRoleEnum):
        return value.value
    if isinstance(value, int):
        return UserRoleEnum(value).value
    if isinstance(value, str):
        stripped_value = value.strip()
        if stripped_value in {"1", "2"}:
            return UserRoleEnum(int(stripped_value)).value
        if stripped_value == "buisiness":
            return UserRoleEnum.buisiness.value
        if stripped_value == "customer_segmant":
            return UserRoleEnum.customer_segmant.value
    raise ValueError("user_role must be 1 for buisiness or 2 for customer_segmant")
