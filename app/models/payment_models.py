from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.db.database import Base
from app.enum.payment_status_enum import PaymentGateway, PaymentStatus


class PaymentTransaction(Base):
    """One checkout attempt for a paid subscription plan.

    A row is created when the gateway order is created, and moves to ``paid`` once the
    payment is verified — at which point ``subscription_id`` points to the
    UserSubscription it activated. ``amount`` is in the currency's smallest unit
    (paise for INR), exactly as sent to the gateway.
    """

    __tablename__ = "payment_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    plan_id = Column(Integer, ForeignKey("subscription_plans.id"), nullable=False)
    subscription_id = Column(Integer, ForeignKey("user_subscriptions.id"), nullable=True)

    gateway = Column(String(30), nullable=False, default=PaymentGateway.RAZORPAY.value)
    gateway_order_id = Column(String(64), nullable=False, unique=True, index=True)
    gateway_payment_id = Column(String(64), nullable=True, unique=True, index=True)
    gateway_signature = Column(String(256), nullable=True)
    receipt = Column(String(40), nullable=False, unique=True)

    amount = Column(Integer, nullable=False)
    currency = Column(String(3), nullable=False)
    status = Column(String(20), nullable=False, default=PaymentStatus.CREATED.value, index=True)
    payment_method = Column(String(30), nullable=True)
    failure_reason = Column(String(500), nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    paid_at = Column(DateTime, nullable=True)

    user = relationship("User")
    plan = relationship("SubscriptionPlan")
    subscription = relationship("UserSubscription")
