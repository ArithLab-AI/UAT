from sqlalchemy.orm import Session
from app.enum.user_role_enum import ENTERPRISE_USER, FREE_USER, LITE_USER, PRO_USER
from app.models.subscription_models import SubscriptionPlan

def seed_subscription_plans(db: Session):
    plans = [
        {
            "name": "Free",
            "user_role": FREE_USER,
            "price": 0,
            "duration_days": 30
        },
        {
            "name": "Lite",
            "user_role": LITE_USER,
            "price": 16,
            "duration_days": 30
        },
        {
            "name": "Pro",
            "user_role": PRO_USER,
            "price": 30,
            "duration_days": 30
        },
        {
            "name": "Enterprise",
            "user_role": ENTERPRISE_USER,
            "price": 0,
            "duration_days": 30
        },
    ]

    legacy_name_map = {
        "Customer Segmant Free": "Free",
        "Customer Segmant Lite": "Lite",
        "Customer Segmant Pro": "Pro",
        "Customer Segmant Premium": "Enterprise",
        "Premium": "Enterprise",
        "Business Free": "Free",
        "Business Lite": "Lite",
        "Business Pro": "Pro",
        "Business Premium": "Enterprise",
    }

    for legacy_name, current_name in legacy_name_map.items():
        legacy_plan = db.query(SubscriptionPlan).filter_by(name=legacy_name).first()
        current_plan = db.query(SubscriptionPlan).filter_by(name=current_name).first()

        if legacy_plan and not current_plan:
            legacy_plan.name = current_name
        elif legacy_plan and current_plan and legacy_plan.is_active:
            legacy_plan.is_active = False

    for plan in plans:
        existing = db.query(SubscriptionPlan).filter_by(name=plan["name"]).first()
        if not existing:
            db.add(SubscriptionPlan(**plan))
            continue

        if (
            existing.user_role != plan["user_role"]
            or existing.price != plan["price"]
            or existing.duration_days != plan["duration_days"]
            or not existing.is_active
        ):
            existing.user_role = plan["user_role"]
            existing.price = plan["price"]
            existing.duration_days = plan["duration_days"]
            existing.is_active = True

    db.commit()
