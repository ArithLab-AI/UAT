from sqlalchemy.orm import Session
from app.models.subscription_models import SubscriptionPlan

def seed_subscription_plans(db: Session):
    plans = [
        {
            "name": "Free",
            "price": 0,
            "duration_days": 30,
            # "max_projects": 1,
            # "max_requests_per_month": 50,
            # "priority_support": False
        },
        {
            "name": "Basic",
            "price":399.99,
            "duration_days": 30,
            # "max_projects": 5,
            # "max_requests_per_month": 500,
            # "priority_support": False
        },
        {
            "name": "Pro",
            "price": 599.99,
            "duration_days": 30,
            # "max_projects": 20,
            # "max_requests_per_month": 5000,
            # "priority_support": True
        },
        {
            "name": "Premium",
            "price": 999.99,
            "duration_days": 30,
            # "max_projects": -1,  # unlimited
            # "max_requests_per_month": -1,
            # "priority_support": True
        },
    ]

    for plan in plans:
        existing = db.query(SubscriptionPlan).filter_by(name=plan["name"]).first()
        if not existing:
            db.add(SubscriptionPlan(**plan))

    db.commit()
