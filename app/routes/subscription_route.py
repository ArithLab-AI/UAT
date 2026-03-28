import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from app.db.database import get_db
from app.models.subscription_models import SubscriptionPlan, UserSubscription
from app.models.auth_models import User
from app.schemas.subscription_schema import (
    PlanResponse,
    SubscribeRequest,
    SubscriptionResponse
)
from app.config.deps import get_current_user

router = APIRouter(prefix="/subscriptions", tags=["Subscriptions"])
logger = logging.getLogger(__name__)

@router.get("/plans", response_model=list[PlanResponse])
def get_plans(db: Session = Depends(get_db)):
    plans = db.query(SubscriptionPlan).filter(
        SubscriptionPlan.is_active == True
    ).all()
    logger.info("Fetched %s active subscription plans", len(plans))
    return plans

@router.post("/subscribe", response_model=SubscriptionResponse)
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
        raise HTTPException(status_code=404, detail="Plan not found")

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

    return new_subscription


# 🔹 3. My Subscription
@router.get("/my-subscription", response_model=SubscriptionResponse)
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
        raise HTTPException(status_code=404, detail="No active subscription")

    # Auto-expire if past date
    if subscription.end_date < datetime.utcnow():
        subscription.status = "expired"
        db.commit()
        logger.warning("Subscription id=%s auto-expired for user_id=%s", subscription.id, current_user.id)
        raise HTTPException(status_code=400, detail="Subscription expired")

    logger.info("Active subscription id=%s returned for user_id=%s", subscription.id, current_user.id)
    return subscription


# 🔹 4. Cancel Subscription
@router.post("/cancel")
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
        raise HTTPException(status_code=404, detail="No active subscription")

    subscription.status = "canceled"
    db.commit()
    logger.info("Subscription id=%s canceled for user_id=%s", subscription.id, current_user.id)

    return {"message": "Subscription canceled successfully"}
