import logging
from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.models.auth_models import User
from app.models.csv_dataset_models import CsvUploadedDataset
from app.models.subscription_models import SubscriptionPlan, UserSubscription

logger = logging.getLogger(__name__)


PLAN_CAPABILITIES = {
    "free": {
        "max_file_size_bytes": 5 * 1024 * 1024,
        "max_active_datasets": 1,
        "max_merge_sources": 0,
        "can_merge": False,
    },
    "lite": {
        "max_file_size_bytes": 50 * 1024 * 1024,
        "max_active_datasets": 5,
        "max_merge_sources": 2,
        "can_merge": True,
    },
    "pro": {
        "max_file_size_bytes": 500 * 1024 * 1024,
        "max_active_datasets": None,
        "max_merge_sources": None,
        "can_merge": True,
    },
    "enterprise": {
        "max_file_size_bytes": None,
        "max_active_datasets": None,
        "max_merge_sources": None,
        "can_merge": True,
    },
}


def normalize_plan_tier(plan_name: str | None) -> str:
    normalized_name = (plan_name or "").strip().lower()

    if "enterprise" in normalized_name or "premium" in normalized_name:
        return "enterprise"
    if "pro" in normalized_name:
        return "pro"
    if "lite" in normalized_name:
        return "lite"
    return "free"


def get_plan_capabilities(plan_name: str | None) -> dict:
    return PLAN_CAPABILITIES[normalize_plan_tier(plan_name)]


def get_user_storage_summary(db: Session, user_id: int, plan_name: str | None) -> dict:
    plan_capabilities = get_plan_capabilities(plan_name)
    total_file_size_bytes = plan_capabilities["max_file_size_bytes"]
    used_file_size_bytes = (
        db.query(func.coalesce(func.sum(CsvUploadedDataset.file_size), 0))
        .filter(CsvUploadedDataset.created_by_user_id == user_id)
        .scalar()
    ) or 0

    remaining_file_size_bytes = None
    if total_file_size_bytes is not None:
        remaining_file_size_bytes = max(total_file_size_bytes - used_file_size_bytes, 0)

    return {
        "total_file_size_bytes": total_file_size_bytes,
        "used_file_size_bytes": used_file_size_bytes,
        "remaining_file_size_bytes": remaining_file_size_bytes
    }


def get_active_subscription(db: Session, user_id: int) -> UserSubscription | None:
    active_subscriptions = (
        db.query(UserSubscription)
        .filter(
            UserSubscription.user_id == user_id,
            UserSubscription.status == "active",
        )
        .order_by(UserSubscription.id.desc())
        .all()
    )

    current_time = datetime.utcnow()
    valid_active_subscription = None

    for subscription in active_subscriptions:
        if subscription.end_date and subscription.end_date >= current_time:
            valid_active_subscription = subscription
            break
        subscription.status = "expired"

    return valid_active_subscription


def get_user_plan_capabilities(db: Session, user: User) -> dict:
    active_subscription = get_active_subscription(db, user.id)
    plan_name = active_subscription.plan.name if active_subscription and active_subscription.plan else None
    return get_plan_capabilities(plan_name)


def ensure_default_free_subscription(db: Session, user_id: int) -> UserSubscription | None:
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        logger.warning("Cannot assign default subscription: user_id=%s not found", user_id)
        return None

    current_time = datetime.utcnow()
    valid_active_subscription = get_active_subscription(db, user_id)

    if valid_active_subscription:
        logger.info(
            "Active subscription already exists for user_id=%s subscription_id=%s",
            user_id,
            valid_active_subscription.id,
        )
        return valid_active_subscription

    free_plan = (
        db.query(SubscriptionPlan)
        .filter(
            SubscriptionPlan.is_active == True,
        )
        .order_by(SubscriptionPlan.id.asc())
        .all()
    )

    free_plan = next(
        (plan for plan in free_plan if normalize_plan_tier(plan.name) == "free"),
        None,
    )

    if not free_plan:
        logger.warning(
            "Default free plan not found for user_id=%s",
            user_id,
        )
        return None

    new_subscription = UserSubscription(
        user_id=user_id,
        plan_id=free_plan.id,
        start_date=current_time,
        end_date=current_time + timedelta(days=free_plan.duration_days),
        status="active",
    )
    db.add(new_subscription)
    logger.info(
        "Assigned default free plan id=%s to user_id=%s",
        free_plan.id,
        user_id,
    )
    return new_subscription
