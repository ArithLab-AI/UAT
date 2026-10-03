import logging

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.payment_schema import (
    CreateOrderRequest,
    CreateOrderSuccessResponse,
    PaymentHistorySuccessResponse,
    PaymentTransactionSuccessResponse,
    VerifyPaymentRequest,
    VerifyPaymentSuccessResponse,
)
from app.services.payment_service import (
    create_checkout_order,
    get_user_transaction,
    handle_webhook,
    list_user_transactions,
    verify_checkout_payment,
)
from app.utils.responses import success_response

router = APIRouter(prefix="/payments", tags=["Payments"])
logger = logging.getLogger(__name__)


@router.post(
    "/razorpay/orders",
    response_model=CreateOrderSuccessResponse,
    status_code=201,
)
def create_razorpay_order(
    payload: CreateOrderRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Step 1 — create a Razorpay order for a paid plan.

    Pass the returned ``key_id``, ``order_id``, ``amount``, ``currency``, ``merchant_name``,
    ``description`` and ``prefill`` straight into Razorpay Checkout. The amount comes from
    the plan on the server; the client never sends a price."""
    data = create_checkout_order(db, user=current_user, plan_id=payload.plan_id)
    return success_response("Payment order created successfully", status_code=201, data=data)


@router.post(
    "/razorpay/verify",
    response_model=VerifyPaymentSuccessResponse,
)
def verify_razorpay_payment(
    payload: VerifyPaymentRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Step 2 — call from Checkout's success ``handler`` with its three values.

    Verifies the signature, confirms the payment with Razorpay, and activates the plan.
    Safe to retry: an already-verified order returns the same result."""
    data = verify_checkout_payment(
        db,
        user=current_user,
        order_id=payload.razorpay_order_id,
        payment_id=payload.razorpay_payment_id,
        signature=payload.razorpay_signature,
    )
    return success_response("Payment verified and subscription activated", data=data)


@router.post("/razorpay/webhook", include_in_schema=True)
async def razorpay_webhook(
    request: Request,
    x_razorpay_signature: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    """Razorpay server-to-server events (no user auth; verified by X-Razorpay-Signature).

    Configure in Razorpay Dashboard -> Settings -> Webhooks with the events
    ``payment.authorized``, ``payment.captured``, ``payment.failed`` and ``order.paid``."""
    raw_body = await request.body()  # signature is computed over the exact raw bytes
    data = await run_in_threadpool(handle_webhook, db, raw_body=raw_body, signature=x_razorpay_signature)
    return success_response("Webhook processed", data=data)


@router.get(
    "/razorpay/orders/{order_id}",
    response_model=PaymentTransactionSuccessResponse,
)
def get_razorpay_order_status(
    order_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Status of one of the current user's orders (created / paid / failed)."""
    data = get_user_transaction(db, user=current_user, order_id=order_id)
    return success_response("Payment fetched successfully", data=data)


@router.get(
    "/history",
    response_model=PaymentHistorySuccessResponse,
)
def get_payment_history(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """The current user's payment attempts, newest first."""
    data = list_user_transactions(db, user=current_user, limit=limit)
    return success_response("Payment history fetched successfully", data=data)
