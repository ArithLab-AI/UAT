import logging
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.models.subscription_models import SubscriptionPlan, UserSubscription

logger = logging.getLogger(__name__)


def ensure_default_free_subscription(db: Session, user_id: int) -> UserSubscription | None:
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
            SubscriptionPlan.name == "Free",
            SubscriptionPlan.is_active == True,
        )
        .first()
    )

    if not free_plan:
        logger.warning("Free plan not found for user_id=%s", user_id)
        return None

    new_subscription = UserSubscription(
        user_id=user_id,
        plan_id=free_plan.id,
        start_date=current_time,
        end_date=current_time + timedelta(days=free_plan.duration_days),
        status="active",
    )
    db.add(new_subscription)
    logger.info("Assigned free plan id=%s to user_id=%s", free_plan.id, user_id)
    return new_subscription
