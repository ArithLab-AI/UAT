from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.database import Base

class SubscriptionPlan(Base):
    __tablename__ = "subscription_plans"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    price = Column(Float, nullable=False)
    duration_days = Column(Integer, nullable=False)
    is_active = Column(Boolean, default=True)

    max_file_uploads = Column(Integer, default=0)
    ai_queries_per_month = Column(Integer, default=0)
    data_retention_days = Column(Integer, default=2)

    has_advanced_analytics = Column(Boolean, default=False)
    has_priority_support = Column(Boolean, default=False)
    has_custom_dashboards = Column(Boolean, default=False)
    has_external_connections = Column(Boolean, default=False)
    has_custom_branding = Column(Boolean, default=False)
    has_team_features = Column(Boolean, default=False)


class UserSubscription(Base):
    __tablename__ = "user_subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    plan_id = Column(Integer, ForeignKey("subscription_plans.id"), nullable=False)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime, nullable=False)
    status = Column(String, default="active")
    user = relationship("User")
    plan = relationship("SubscriptionPlan")
