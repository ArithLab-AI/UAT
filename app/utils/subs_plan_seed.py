from sqlalchemy.orm import Session
from app.enum.user_role_enum import UserRoleEnum
from app.models.subscription_models import SubscriptionPlan

def seed_subscription_plans(db: Session):
    legacy_plan_names = {"Free", "Lite", "Pro", "Premium"}
    business_plans = [
        {
            "name": "Business Free",
            "user_role": UserRoleEnum.buisiness.value,
            "price": 0,
            "duration_days": 30,
            # "max_projects": 1,
            # "max_requests_per_month": 50,
            # "priority_support": False
        },
        {
            "name": "Business Lite",
            "user_role": UserRoleEnum.buisiness.value,
            "price": 399.99,
            "duration_days": 30,
            # "max_projects": 5,
            # "max_requests_per_month": 500,
            # "priority_support": False
        },
        {
            "name": "Business Pro",
            "user_role": UserRoleEnum.buisiness.value,
            "price": 599.99,
            "duration_days": 30,
            # "max_projects": 20,
            # "max_requests_per_month": 5000,
            # "priority_support": True
        },
        {
            "name": "Business Premium",
            "user_role": UserRoleEnum.buisiness.value,
            "price": 999.99,
            "duration_days": 30,
            # "max_projects": -1,  # unlimited
            # "max_requests_per_month": -1,
            # "priority_support": True
        },
    ]

    customer_segmant_plans = [
        {
            "name": "Customer Segmant Free",
            "user_role": UserRoleEnum.customer_segmant.value,
            "price": 0,
            "duration_days": 30,
            # "max_projects": 1,
            # "max_requests_per_month": 50,
            # "priority_support": False
        },
        {
            "name": "Customer Segmant Lite",
            "user_role": UserRoleEnum.customer_segmant.value,
            "price": 299.99,
            "duration_days": 30,
            # "max_projects": 5,
            # "max_requests_per_month": 500,
            # "priority_support": False
        },
        {
            "name": "Customer Segmant Pro",
            "user_role": UserRoleEnum.customer_segmant.value,
            "price": 499.99,
            "duration_days": 30,
            # "max_projects": 20,
            # "max_requests_per_month": 5000,
            # "priority_support": True
        },
        {
            "name": "Customer Segmant Premium",
            "user_role": UserRoleEnum.customer_segmant.value,
            "price": 899.99,
            "duration_days": 30,
            # "max_projects": -1,  # unlimited
            # "max_requests_per_month": -1,
            # "priority_support": True
        },
    ]

    plans = business_plans + customer_segmant_plans

    inserted_count = 0
    deactivated_count = 0
    for plan in plans:
        existing = db.query(SubscriptionPlan).filter_by(name=plan["name"]).first()
        if not existing:
            db.add(SubscriptionPlan(**plan))
            inserted_count += 1

    legacy_plans = (
        db.query(SubscriptionPlan)
        .filter(SubscriptionPlan.name.in_(legacy_plan_names))
        .all()
    )
    for legacy_plan in legacy_plans:
        if legacy_plan.is_active:
            legacy_plan.is_active = False
            deactivated_count += 1

    db.commit()
