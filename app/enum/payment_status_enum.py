from enum import Enum


class PaymentStatus(str, Enum):
    """Lifecycle of a payment_transactions row.

    CREATED -> PAID     : payment verified (checkout callback or webhook) and plan activated.
    CREATED -> FAILED   : Razorpay reported the payment as failed (webhook).
    """

    CREATED = "created"
    PAID = "paid"
    FAILED = "failed"


class PaymentGateway(str, Enum):
    RAZORPAY = "razorpay"
