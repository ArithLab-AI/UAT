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

def build_subscription_response(subscription: UserSubscription):
    remaining_days = (subscription.end_date - datetime.utcnow()).days

    return {
        "id": subscription.id,
        "plan": subscription.plan,
        "start_date": subscription.start_date,
        "end_date": subscription.end_date,
        "status": subscription.status,
        "remaining_days": max(remaining_days, 0)
    }

def get_active_subscription(user_id: int, db: Session) -> UserSubscription:
    subscription = db.query(UserSubscription).filter(
        UserSubscription.user_id == user_id,
        UserSubscription.status == "active"
    ).first()

    if not subscription:
        raise HTTPException(status_code=404, detail="No active subscription")

    # Auto-expire if needed
    if subscription.end_date < datetime.utcnow():
        subscription.status = "expired"
        db.commit()
        raise HTTPException(status_code=400, detail="Subscription expired")

    return subscription

@router.get("/plans", response_model=list[PlanResponse])
def get_plans(db: Session = Depends(get_db)):
    plans = db.query(SubscriptionPlan).filter(
        SubscriptionPlan.is_active == True
    ).all()
    return plans

@router.post("/subscribe", response_model=SubscriptionResponse)
def subscribe(
    payload: SubscribeRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    plan = db.query(SubscriptionPlan).filter(
        SubscriptionPlan.id == payload.plan_id,
        SubscriptionPlan.is_active == True
    ).first()

    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")

    if plan.name.lower() == "enterprise":
        raise HTTPException(
            status_code=403,
            detail="Please contact sales for Enterprise plan"
        )

    old_subscription = db.query(UserSubscription).filter(
        UserSubscription.user_id == current_user.id,
        UserSubscription.status == "active"
    ).first()

    if old_subscription:
        if old_subscription.plan_id == plan.id:
            raise HTTPException(
                status_code=400,
                detail="You are already subscribed to this plan"
            )

        old_subscription.status = "expired"

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

    return build_subscription_response(new_subscription)


@router.get("/my-subscription", response_model=SubscriptionResponse)
def my_subscription(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    subscription = db.query(UserSubscription).filter(
        UserSubscription.user_id == current_user.id,
        UserSubscription.status == "active"
    ).first()

    if not subscription:
        raise HTTPException(status_code=404, detail="No active subscription")

    if subscription.end_date < datetime.utcnow():
        subscription.status = "expired"
        db.commit()
        raise HTTPException(status_code=400, detail="Subscription expired")

    return build_subscription_response(subscription)

@router.post("/cancel")
def cancel_subscription(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    subscription = db.query(UserSubscription).filter(
        UserSubscription.user_id == current_user.id,
        UserSubscription.status == "active"
    ).first()

    if not subscription:
        raise HTTPException(status_code=404, detail="No active subscription")

    subscription.status = "canceled"
    db.commit()

    return {"message": "Subscription canceled successfully"}