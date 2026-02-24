from sqlalchemy.orm import Session
from app.models.subscription_models import SubscriptionPlan

def seed_subscription_plans(db: Session):
    plans = [
        {
            "name": "Free",
            "price": 0.0,
            "duration_days": 30,
            "max_file_uploads": 2,
            "ai_queries_per_month": 5,
            "data_retention_days": 2,
            "has_advanced_analytics": False,
        },
        {
            "name": "Lite",
            "price": 14.99,
            "duration_days": 30,
            "max_file_uploads": 20,
            "ai_queries_per_month": 100,
            "data_retention_days": 90,
            "has_advanced_analytics": True,
            "has_custom_dashboards": True,
            "has_external_connections": True,
        },
        {
            "name": "Pro",
            "price": 29.99,
            "duration_days": 30,
            "max_file_uploads": 100,
            "ai_queries_per_month": 500,
            "data_retention_days": 365,
            "has_advanced_analytics": True,
            "has_priority_support": True,
            "has_custom_branding": True,
        },
        {
            "name": "Enterprise",
            "price": 0.0,
            "duration_days": 365,
            "max_file_uploads": -1,
            "ai_queries_per_month": 1000,
            "data_retention_days": 730,
            "has_advanced_analytics": True,
            "has_priority_support": True,
            "has_team_features": True,
        },
    ]

    for plan in plans:
        existing = db.query(SubscriptionPlan).filter_by(name=plan["name"]).first()
        if not existing:
            db.add(SubscriptionPlan(**plan))

    db.commit()