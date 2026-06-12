from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.database import Base
from app.enum.user_role_enum import DEFAULT_USER_ROLE

class SubscriptionPlan(Base):
    __tablename__ = "subscription_plans"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    user_role = Column(Integer, nullable=False, server_default=str(DEFAULT_USER_ROLE))
    price = Column(Float, nullable=False)
    duration_days = Column(Integer, nullable=False)
    is_active = Column(Boolean, default=True)

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


class UserUploadStorageUsage(Base):
    __tablename__ = "user_upload_storage_usage"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    subscription_id = Column(Integer,ForeignKey("user_subscriptions.id"),nullable=True,)
    uploaded_dataset_id = Column(Integer, nullable=True, index=True)
    file_size_bytes = Column(Integer, nullable=False, default=0)
    file_name = Column(String, nullable=True)
    sheet_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    user = relationship("User")
