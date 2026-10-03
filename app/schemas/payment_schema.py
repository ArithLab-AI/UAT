from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.schemas.common_schema import SuccessResponse
from app.schemas.subscription_schema import SubscriptionResponse


class CreateOrderRequest(BaseModel):
    plan_id: int = Field(..., ge=1)


class OrderPlanResponse(BaseModel):
    id: int
    name: str
    price: float
    duration_days: int


class CheckoutPrefillResponse(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None


class CreateOrderResponse(BaseModel):
    """Everything the frontend needs to open Razorpay Checkout."""

    transaction_id: int
    key_id: str = Field(description="Public Razorpay key for Checkout (`key` option).")
    order_id: str = Field(description="Razorpay order id (`order_id` option).")
    amount: int = Field(description="Amount in the smallest currency unit (paise for INR).")
    currency: str
    receipt: str
    merchant_name: str
    description: str
    plan: OrderPlanResponse
    prefill: CheckoutPrefillResponse


class VerifyPaymentRequest(BaseModel):
    """The three values Razorpay Checkout passes to its success `handler`."""

    razorpay_order_id: str = Field(..., min_length=1, max_length=64)
    razorpay_payment_id: str = Field(..., min_length=1, max_length=64)
    razorpay_signature: str = Field(..., min_length=1, max_length=256)


class PaymentTransactionResponse(BaseModel):
    id: int
    plan_id: int
    plan_name: Optional[str] = None
    gateway: str
    order_id: str
    payment_id: Optional[str] = None
    amount: int
    currency: str
    status: str
    payment_method: Optional[str] = None
    failure_reason: Optional[str] = None
    subscription_id: Optional[int] = None
    created_at: datetime
    paid_at: Optional[datetime] = None


class VerifyPaymentResponse(BaseModel):
    payment: PaymentTransactionResponse
    subscription: Optional[SubscriptionResponse] = None


CreateOrderSuccessResponse = SuccessResponse[CreateOrderResponse]
VerifyPaymentSuccessResponse = SuccessResponse[VerifyPaymentResponse]
PaymentTransactionSuccessResponse = SuccessResponse[PaymentTransactionResponse]
PaymentHistorySuccessResponse = SuccessResponse[list[PaymentTransactionResponse]]
