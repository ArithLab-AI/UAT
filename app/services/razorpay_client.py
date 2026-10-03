"""Thin Razorpay REST client + signature helpers.

Uses only the standard library (no SDK dependency): Razorpay's REST API is plain JSON
over HTTPS with Basic auth (key_id:key_secret). Only the three calls the checkout flow
needs are implemented: create order, fetch payment, capture payment.

Docs: https://razorpay.com/docs/api/orders/ and
https://razorpay.com/docs/payments/server-integration/python/payment-gateway/build-integration/
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import urllib.error
import urllib.request
from typing import Any

from app.config.config import settings

logger = logging.getLogger(__name__)


class RazorpayNotConfiguredError(RuntimeError):
    """RAZORPAY_KEY_ID / RAZORPAY_KEY_SECRET are missing from the environment."""


class RazorpayAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, code: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


def is_configured() -> bool:
    return bool(settings.RAZORPAY_KEY_ID and settings.RAZORPAY_KEY_SECRET)


def _auth_header() -> str:
    if not is_configured():
        raise RazorpayNotConfiguredError("Razorpay keys are not configured")
    token = f"{settings.RAZORPAY_KEY_ID}:{settings.RAZORPAY_KEY_SECRET}".encode()
    return "Basic " + base64.b64encode(token).decode()


def _request(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = settings.RAZORPAY_API_BASE_URL.rstrip("/") + path
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Authorization": _auth_header()},
    )
    try:
        with urllib.request.urlopen(request, timeout=settings.RAZORPAY_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        description, code = f"HTTP {exc.code}", None
        try:
            error = json.loads(exc.read().decode()).get("error", {})
            description = error.get("description") or description
            code = error.get("code")
        except Exception:  # noqa: BLE001 - error body is best-effort
            pass
        logger.warning("Razorpay %s %s failed: %s %s", method, path, exc.code, description)
        raise RazorpayAPIError(description, status_code=exc.code, code=code) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        logger.warning("Razorpay %s %s unreachable: %s", method, path, exc)
        raise RazorpayAPIError("Payment gateway is unreachable") from exc


def create_order(*, amount: int, currency: str, receipt: str, notes: dict[str, str]) -> dict[str, Any]:
    """POST /orders. ``amount`` is in the smallest currency unit (paise)."""
    return _request(
        "POST",
        "/orders",
        {"amount": amount, "currency": currency, "receipt": receipt, "notes": notes},
    )


def fetch_payment(payment_id: str) -> dict[str, Any]:
    return _request("GET", f"/payments/{payment_id}")


def capture_payment(payment_id: str, *, amount: int, currency: str) -> dict[str, Any]:
    """Needed only when the Razorpay account has automatic capture turned off."""
    return _request("POST", f"/payments/{payment_id}/capture", {"amount": amount, "currency": currency})


def _hmac_sha256(secret: str, message: bytes) -> str:
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def verify_checkout_signature(order_id: str, payment_id: str, signature: str) -> bool:
    """Checkout success signature = HMAC_SHA256(order_id + "|" + payment_id, key_secret)."""
    if not settings.RAZORPAY_KEY_SECRET:
        raise RazorpayNotConfiguredError("Razorpay keys are not configured")
    expected = _hmac_sha256(settings.RAZORPAY_KEY_SECRET, f"{order_id}|{payment_id}".encode())
    return hmac.compare_digest(expected, signature)


def verify_webhook_signature(raw_body: bytes, signature: str) -> bool:
    """Webhook signature = HMAC_SHA256(raw request body, webhook secret)."""
    if not settings.RAZORPAY_WEBHOOK_SECRET:
        raise RazorpayNotConfiguredError("Razorpay webhook secret is not configured")
    expected = _hmac_sha256(settings.RAZORPAY_WEBHOOK_SECRET, raw_body)
    return hmac.compare_digest(expected, signature or "")
