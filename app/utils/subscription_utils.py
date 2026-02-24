from fastapi import HTTPException
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.subscription_models import UserSubscription


def get_active_subscription(user_id: int, db: Session):
    subscription = db.query(UserSubscription).filter(
        UserSubscription.user_id == user_id,
        UserSubscription.status == "active"
    ).first()

    if not subscription:
        raise HTTPException(status_code=403, detail="No active subscription")

    if subscription.end_date < datetime.utcnow():
        subscription.status = "expired"
        db.commit()
        raise HTTPException(status_code=403, detail="Subscription expired")

    return subscription