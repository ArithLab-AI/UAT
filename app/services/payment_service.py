"""Paid subscription checkout via Razorpay (Orders API + Standard Checkout).

Flow:
  1. ``create_checkout_order``  - user picks a paid plan; a Razorpay order is created
     server-side for the plan's price and stored as a ``created`` PaymentTransaction.
  2. Frontend opens Razorpay Checkout with the returned order_id/key_id.
  3. ``verify_checkout_payment`` - Checkout's success handler posts the order id,
     payment id and signature; the signature is verified, the payment is fetched
     from Razorpay to confirm amount/status, and the plan is activated.
  4. ``handle_webhook`` - Razorpay's server-to-server events activate the plan too,
     so a user who closes the tab before step 3 still gets what they paid for.

Activation is idempotent (the transaction row is locked and checked), so steps 3 and
4 can both arrive for the same payment without creating two subscriptions.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from sqlalchemy.orm import Session

from app.config.config import settings
from app.enum.payment_status_enum import PaymentGateway, PaymentStatus
from app.models.auth_models import User
from app.models.payment_models import PaymentTransaction
from app.models.subscription_models import SubscriptionPlan, UserSubscription
from app.services import razorpay_client
from app.services.file_retention_service import sync_user_dataset_retention_expiries
from app.services.subscription_service import get_user_storage_summary
from app.utils.responses import error_response

logger = logging.getLogger(__name__)

# Razorpay statuses for a payment that has gone through.
_AUTHORIZED = "authorized"
_CAPTURED = "captured"


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def serialize_transaction(txn: PaymentTransaction) -> dict[str, Any]:
    return {
        "id": txn.id,
        "plan_id": txn.plan_id,
        "plan_name": txn.plan.name if txn.plan else None,
        "gateway": txn.gateway,
        "order_id": txn.gateway_order_id,
        "payment_id": txn.gateway_payment_id,
        "amount": txn.amount,
        "currency": txn.currency,
        "status": txn.status,
        "payment_method": txn.payment_method,
        "failure_reason": txn.failure_reason,
        "subscription_id": txn.subscription_id,
        "created_at": txn.created_at,
        "paid_at": txn.paid_at,
    }


def _serialize_subscription(db: Session, subscription: UserSubscription | None) -> dict[str, Any] | None:
    """Same shape as /subscriptions/my-subscription (SubscriptionResponse)."""
    if subscription is None or subscription.plan is None:
        return None
    plan = subscription.plan
    return {
        "id": plan.id,
        "name": plan.name,
        "user_role": plan.user_role,
        "price": plan.price,
        "duration_days": plan.duration_days,
        "start_date": subscription.start_date,
        "end_date": subscription.end_date,
        "status": subscription.status,
        **get_user_storage_summary(db, subscription.user_id, plan.name),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_gateway() -> None:
    if not razorpay_client.is_configured():
        logger.error("Razorpay checkout requested but RAZORPAY_KEY_ID/RAZORPAY_KEY_SECRET are not set")
        raise error_response(status_code=503, detail="Payment gateway is not configured")


def _to_minor_units(price: float) -> int:
    """Plan price (e.g. 16 or 16.50) -> smallest currency unit (1600 / 1650 paise)."""
    return int((Decimal(str(price)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _gateway_error(exc: razorpay_client.RazorpayAPIError, action: str) -> Exception:
    logger.error("Razorpay %s failed: %s (status=%s code=%s)", action, exc, exc.status_code, exc.code)
    return error_response(status_code=502, detail=f"Payment gateway error: {exc}")


def _locked_transaction(db: Session, order_id: str, user_id: int | None = None) -> PaymentTransaction | None:
    query = db.query(PaymentTransaction).filter(PaymentTransaction.gateway_order_id == order_id)
    if user_id is not None:
        query = query.filter(PaymentTransaction.user_id == user_id)
    # Row lock: the checkout callback and the webhook can race for the same order.
    return query.with_for_update().first()


def _activate_subscription(db: Session, txn: PaymentTransaction) -> UserSubscription:
    """Same activation rules as POST /subscriptions/subscribe: expire the current
    active plan(s) and start the paid plan from now for its duration."""
    plan = txn.plan
    for old in (
        db.query(UserSubscription)
        .filter(UserSubscription.user_id == txn.user_id, UserSubscription.status == "active")
        .all()
    ):
        old.status = "expired"
        logger.info("Expired subscription id=%s for user_id=%s (paid upgrade)", old.id, txn.user_id)

    start_date = datetime.utcnow()
    subscription = UserSubscription(
        user_id=txn.user_id,
        plan_id=plan.id,
        start_date=start_date,
        end_date=start_date + timedelta(days=plan.duration_days),
        status="active",
    )
    db.add(subscription)
    db.flush()
    sync_user_dataset_retention_expiries(db, txn.user_id)
    return subscription


def _mark_paid(db: Session, txn: PaymentTransaction, payment: dict[str, Any], signature: str | None) -> None:
    subscription = _activate_subscription(db, txn)
    txn.status = PaymentStatus.PAID.value
    txn.gateway_payment_id = payment.get("id")
    txn.payment_method = payment.get("method")
    if signature:
        txn.gateway_signature = signature
    txn.failure_reason = None
    txn.paid_at = datetime.utcnow()
    txn.subscription_id = subscription.id
    logger.info(
        "Payment %s for order %s paid; subscription id=%s activated for user_id=%s",
        txn.gateway_payment_id, txn.gateway_order_id, subscription.id, txn.user_id,
    )


def _check_payment_matches(txn: PaymentTransaction, payment: dict[str, Any]) -> None:
    if payment.get("order_id") != txn.gateway_order_id:
        raise error_response(status_code=400, detail="Payment does not belong to this order")
    if int(payment.get("amount") or 0) != txn.amount or (payment.get("currency") or "").upper() != txn.currency:
        logger.error(
            "Amount mismatch for order %s: expected %s %s, got %s %s",
            txn.gateway_order_id, txn.amount, txn.currency, payment.get("amount"), payment.get("currency"),
        )
        raise error_response(status_code=400, detail="Payment amount does not match the order")


def _ensure_captured(txn: PaymentTransaction, payment: dict[str, Any]) -> dict[str, Any]:
    status = payment.get("status")
    if status == _CAPTURED:
        return payment
    if status == _AUTHORIZED:
        # Account has automatic capture off: capture it now so the money settles.
        try:
            return razorpay_client.capture_payment(payment["id"], amount=txn.amount, currency=txn.currency)
        except razorpay_client.RazorpayAPIError as exc:
            raise _gateway_error(exc, "capture") from exc
    raise error_response(status_code=400, detail=f"Payment is not complete (status: {status})")


# ---------------------------------------------------------------------------
# 1. Create order
# ---------------------------------------------------------------------------


def create_checkout_order(db: Session, *, user: User, plan_id: int) -> dict[str, Any]:
    _require_gateway()

    plan = (
        db.query(SubscriptionPlan)
        .filter(SubscriptionPlan.id == plan_id, SubscriptionPlan.is_active == True)  # noqa: E712
        .first()
    )
    if not plan:
        raise error_response(status_code=404, detail="Plan not found")
    if not plan.price or plan.price <= 0:
        raise error_response(status_code=400, detail="This plan does not require an online payment")

    amount = _to_minor_units(plan.price)
    currency = settings.RAZORPAY_CURRENCY.upper()
    receipt = f"rcpt_{user.id}_{uuid.uuid4().hex[:20]}"  # Razorpay limit: 40 chars

    try:
        order = razorpay_client.create_order(
            amount=amount,
            currency=currency,
            receipt=receipt,
            notes={"user_id": str(user.id), "plan_id": str(plan.id), "plan_name": plan.name},
        )
    except razorpay_client.RazorpayAPIError as exc:
        raise _gateway_error(exc, "order creation") from exc

    txn = PaymentTransaction(
        user_id=user.id,
        plan_id=plan.id,
        gateway=PaymentGateway.RAZORPAY.value,
        gateway_order_id=order["id"],
        receipt=receipt,
        amount=amount,
        currency=currency,
        status=PaymentStatus.CREATED.value,
    )
    db.add(txn)
    db.commit()
    db.refresh(txn)
    logger.info("Razorpay order %s created for user_id=%s plan_id=%s amount=%s %s",
                order["id"], user.id, plan.id, amount, currency)

    full_name = " ".join(part for part in (user.first_name, user.last_name) if part) or user.username
    return {
        "transaction_id": txn.id,
        "key_id": settings.RAZORPAY_KEY_ID,
        "order_id": order["id"],
        "amount": amount,
        "currency": currency,
        "receipt": receipt,
        "merchant_name": settings.RAZORPAY_MERCHANT_NAME,
        "description": f"{plan.name} plan - {plan.duration_days} days",
        "plan": {"id": plan.id, "name": plan.name, "price": plan.price, "duration_days": plan.duration_days},
        "prefill": {"name": full_name, "email": user.email},
    }


# ---------------------------------------------------------------------------
# 2. Verify checkout payment
# ---------------------------------------------------------------------------


def verify_checkout_payment(
    db: Session, *, user: User, order_id: str, payment_id: str, signature: str
) -> dict[str, Any]:
    _require_gateway()

    txn = _locked_transaction(db, order_id, user_id=user.id)
    if txn is None:
        raise error_response(status_code=404, detail="Payment order not found")

    if txn.status == PaymentStatus.PAID.value:
        # Already activated (webhook got there first, or the client retried).
        if txn.gateway_payment_id != payment_id:
            raise error_response(status_code=409, detail="This order has already been paid with a different payment")
        db.commit()
        return {"payment": serialize_transaction(txn), "subscription": _serialize_subscription(db, txn.subscription)}

    if not razorpay_client.verify_checkout_signature(order_id, payment_id, signature):
        db.commit()  # release the row lock
        logger.warning("Invalid Razorpay signature for order %s (user_id=%s)", order_id, user.id)
        raise error_response(status_code=400, detail="Payment verification failed: invalid signature")

    try:
        payment = razorpay_client.fetch_payment(payment_id)
    except razorpay_client.RazorpayAPIError as exc:
        db.rollback()
        raise _gateway_error(exc, "payment fetch") from exc

    try:
        _check_payment_matches(txn, payment)
        payment = _ensure_captured(txn, payment)
    except Exception:
        db.rollback()
        raise

    _mark_paid(db, txn, payment, signature)
    db.commit()
    db.refresh(txn)
    return {"payment": serialize_transaction(txn), "subscription": _serialize_subscription(db, txn.subscription)}


# ---------------------------------------------------------------------------
# 3. Webhook
# ---------------------------------------------------------------------------


def handle_webhook(db: Session, *, raw_body: bytes, signature: str | None) -> dict[str, Any]:
    if not settings.RAZORPAY_WEBHOOK_SECRET:
        logger.error("Razorpay webhook received but RAZORPAY_WEBHOOK_SECRET is not set")
        raise error_response(status_code=503, detail="Webhook is not configured")
    if not razorpay_client.verify_webhook_signature(raw_body, signature or ""):
        logger.warning("Rejected Razorpay webhook with invalid signature")
        raise error_response(status_code=400, detail="Invalid webhook signature")

    try:
        event = json.loads(raw_body.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error_response(status_code=400, detail="Invalid webhook payload") from exc

    event_type = event.get("event")
    payment = ((event.get("payload") or {}).get("payment") or {}).get("entity") or {}
    order_id = payment.get("order_id")
    logger.info("Razorpay webhook %s for order %s payment %s", event_type, order_id, payment.get("id"))

    if event_type not in ("payment.authorized", "payment.captured", "order.paid", "payment.failed") or not order_id:
        return {"received": True, "handled": False}

    txn = _locked_transaction(db, order_id)
    if txn is None:
        db.commit()
        logger.warning("Razorpay webhook for unknown order %s ignored", order_id)
        return {"received": True, "handled": False}

    if event_type == "payment.failed":
        if txn.status == PaymentStatus.CREATED.value:
            txn.status = PaymentStatus.FAILED.value
            txn.gateway_payment_id = txn.gateway_payment_id or payment.get("id")
            txn.failure_reason = (payment.get("error_description") or payment.get("error_reason") or "Payment failed")[:500]
        db.commit()
        return {"received": True, "handled": True}

    if txn.status == PaymentStatus.PAID.value:
        db.commit()
        return {"received": True, "handled": True}

    try:
        _check_payment_matches(txn, payment)
        payment = _ensure_captured(txn, payment)
    except Exception:
        db.rollback()
        logger.exception("Razorpay webhook for order %s could not be applied", order_id)
        # 200 so Razorpay doesn't retry a payload that will never match; it's logged.
        return {"received": True, "handled": False}

    # A retry after a failed attempt (status 'failed') can still succeed.
    _mark_paid(db, txn, payment, signature=None)
    db.commit()
    return {"received": True, "handled": True}


# ---------------------------------------------------------------------------
# 4. Lookups
# ---------------------------------------------------------------------------


def get_user_transaction(db: Session, *, user: User, order_id: str) -> dict[str, Any]:
    txn = (
        db.query(PaymentTransaction)
        .filter(PaymentTransaction.gateway_order_id == order_id, PaymentTransaction.user_id == user.id)
        .first()
    )
    if txn is None:
        raise error_response(status_code=404, detail="Payment order not found")
    return serialize_transaction(txn)


def list_user_transactions(db: Session, *, user: User, limit: int = 50) -> list[dict[str, Any]]:
    rows = (
        db.query(PaymentTransaction)
        .filter(PaymentTransaction.user_id == user.id)
        .order_by(PaymentTransaction.created_at.desc())
        .limit(limit)
        .all()
    )
    return [serialize_transaction(txn) for txn in rows]
