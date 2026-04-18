import logging
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.db.database import get_db
from app.models.subscription_models import SubscriptionPlan, UserSubscription
from app.models.auth_models import User
from app.schemas import subscription_schema
from app.schemas.common_schema import MessageSuccessResponse
from app.schemas.subscription_schema import SubscribeRequest
from app.config.deps import get_current_user
from app.services.subscription_service import get_user_storage_summary, normalize_plan_tier
from app.utils.responses import error_response, success_response

router = APIRouter(prefix="/subscriptions", tags=["Subscriptions"])
logger = logging.getLogger(__name__)

PLAN_SORT_ORDER = {
    "free": 0,
    "lite": 1,
    "pro": 2,
    "enterprise": 3,
}


def _serialize_subscription_with_storage(db: Session, subscription: UserSubscription) -> dict:
    storage_summary = get_user_storage_summary(
        db,
        subscription.user_id,
        subscription.plan.name if subscription.plan else None,
    )
    plan = subscription.plan
    return {
        "id": plan.id,
        "name": plan.name,
        "user_role": plan.user_role,
        "price": plan.price,
        "duration_days": plan.duration_days,
        "start_date": subscription.start_date,
        "end_date": subscription.end_date,
        "status": subscription.status,
        **storage_summary,
    }

@router.get("/plans", response_model=subscription_schema.PlanListSuccessResponse)
def get_plans(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    plans = db.query(SubscriptionPlan).filter(
        SubscriptionPlan.is_active == True
    ).all()
    plans.sort(key=lambda plan: (PLAN_SORT_ORDER.get(normalize_plan_tier(plan.name), 99), plan.id))
    logger.info(
        "Fetched %s active subscription plans for user_id=%s",
        len(plans),
        current_user.id,
    )
    return success_response("Plans fetched successfully", data=plans)

@router.post("/subscribe", response_model=subscription_schema.SubscriptionSuccessResponse)
def subscribe(
    payload: SubscribeRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    logger.info("Subscribe requested by user_id=%s for plan_id=%s", current_user.id, payload.plan_id)
    plan = db.query(SubscriptionPlan).filter(
        SubscriptionPlan.id == payload.plan_id,
        SubscriptionPlan.is_active == True
    ).first()

    if not plan:
        logger.warning(
            "Subscribe failed for user_id=%s: plan_id=%s not found or inactive",
            current_user.id,
            payload.plan_id,
        )
        raise error_response(status_code=404, detail="Plan not found")

    # Expire old subscription if exists
    old_subscriptions = db.query(UserSubscription).filter(
        UserSubscription.user_id == current_user.id,
        UserSubscription.status == "active"
    ).all()

    if old_subscriptions:
        for old_subscription in old_subscriptions:
            old_subscription.status = "expired"
            logger.info(
                "Expired previous subscription id=%s for user_id=%s",
                old_subscription.id,
                current_user.id,
            )

    start_date = datetime.utcnow()
    end_date = start_date + timedelta(days=plan.duration_days)

    new_subscription = UserSubscription(
        user_id=current_user.id,
        plan_id=plan.id,
        start_date=start_date,
        end_date=end_date,
        status="active"
    )

    db.add(new_subscription)
    db.commit()
    db.refresh(new_subscription)
    logger.info("Subscription created id=%s for user_id=%s", new_subscription.id, current_user.id)

    return success_response(
        "Subscription created successfully",
        data=_serialize_subscription_with_storage(db, new_subscription),
    )


@router.get("/my-subscription", response_model=subscription_schema.SubscriptionSuccessResponse)
def my_subscription(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    logger.info("Fetching active subscription for user_id=%s", current_user.id)
    subscription = db.query(UserSubscription).filter(
        UserSubscription.user_id == current_user.id,
        UserSubscription.status == "active"
    ).order_by(UserSubscription.id.desc()).first()

    if not subscription:
        logger.warning("No active subscription for user_id=%s", current_user.id)
        raise error_response(status_code=404, detail="No active subscription")

    # Auto-expire if past date
    if subscription.end_date < datetime.utcnow():
        subscription.status = "expired"
        db.commit()
        logger.warning("Subscription id=%s auto-expired for user_id=%s", subscription.id, current_user.id)
        raise error_response(status_code=400, detail="Subscription expired")

    logger.info("Active subscription id=%s returned for user_id=%s", subscription.id, current_user.id)
    return success_response(
        "Subscription fetched successfully",
        data=_serialize_subscription_with_storage(db, subscription),
    )

@router.post("/cancel", response_model=MessageSuccessResponse)
def cancel_subscription(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    logger.info("Cancel subscription requested by user_id=%s", current_user.id)
    subscription = db.query(UserSubscription).filter(
        UserSubscription.user_id == current_user.id,
        UserSubscription.status == "active"
    ).order_by(UserSubscription.id.desc()).first()

    if not subscription:
        logger.warning("Cancel subscription failed: no active subscription for user_id=%s", current_user.id)
        raise error_response(status_code=404, detail="No active subscription")

    subscription.status = "canceled"
    db.commit()
    logger.info("Subscription id=%s canceled for user_id=%s", subscription.id, current_user.id)

    return success_response("Subscription canceled successfully", data=None)
