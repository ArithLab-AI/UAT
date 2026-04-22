import logging
import math
import threading
from collections import defaultdict
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.db.database import SessionLocal
from app.models.auth_models import User
from app.models.csv_dataset_models import CsvUploadedDataset
from app.models.subscription_models import UserSubscription
from app.services.csv_service import delete_uploaded_dataset
from app.services.subscription_service import get_active_subscription, normalize_plan_tier
from app.utils.email_utils import send_plain_email, smtp_settings_complete
from app.utils.mail_body import retention_warning_mail_body
from app.utils.responses import error_response

logger = logging.getLogger(__name__)

FREE_RETENTION_HOURS = 24
RETENTION_WARNING_DAYS = 2
RETENTION_JOB_INTERVAL_SECONDS = 12 * 60 * 60
DEFAULT_PAID_RETENTION_DAYS = 30
PAID_RETENTION_EXTENSION_DAYS = 2
ANONYMOUS_UPLOAD_EMAIL = "guest-upload@local.invalid"

_retention_stop_event = threading.Event()
_retention_thread: threading.Thread | None = None


def _send_email(recipient: str, subject: str, body: str) -> bool:
    if not smtp_settings_complete():
        logger.warning("Skipping retention email for %s because SMTP is not fully configured", recipient)
        return False

    try:
        send_plain_email(recipient=recipient, subject=subject, body=body)
    except Exception:
        logger.exception("Failed to send retention email to recipient=%s", recipient)
        return False

    logger.info("Sent retention email to recipient=%s", recipient)
    return True


def _send_retention_warning_email(
    *,
    recipient: str,
    dataset_names: list[str],
    expires_at: datetime,
    plan_name: str,
) -> None:
    body = retention_warning_mail_body(
        dataset_names=dataset_names,
        expires_at=expires_at,
        plan_name=plan_name,
    )
    _send_email(
        recipient,
        subject="Airthlab File Retention Reminder",
        body=body,
    )


def _get_latest_subscription(db: Session, user_id: int) -> UserSubscription | None:
    return (
        db.query(UserSubscription)
        .filter(UserSubscription.user_id == user_id)
        .order_by(UserSubscription.id.desc())
        .first()
    )


def _get_paid_retention_days(subscription: UserSubscription | None) -> int:
    return (
        subscription.plan.duration_days
        if subscription and subscription.plan and subscription.plan.duration_days
        else DEFAULT_PAID_RETENTION_DAYS
    )


def _get_base_retention_deadline(
    *,
    created_at: datetime,
    tier: str,
    subscription: UserSubscription | None,
) -> datetime | None:
    if tier == "free":
        return created_at + timedelta(hours=FREE_RETENTION_HOURS)
    if tier in {"lite", "pro"}:
        return created_at + timedelta(days=_get_paid_retention_days(subscription))
    return None


def _get_paid_retention_limit(
    *,
    created_at: datetime,
    subscription: UserSubscription | None,
) -> datetime:
    return created_at + timedelta(
        days=_get_paid_retention_days(subscription) + PAID_RETENTION_EXTENSION_DAYS
    )


def get_retention_expiry_for_user(
    *,
    db: Session,
    user_id: int,
    created_at: datetime,
) -> datetime | None:
    subscription = get_active_subscription(db, user_id)
    plan_name = subscription.plan.name if subscription and subscription.plan else None
    tier = normalize_plan_tier(plan_name)
    return _get_base_retention_deadline(
        created_at=created_at,
        tier=tier,
        subscription=subscription,
    )


def set_dataset_retention_expiry(
    *,
    db: Session,
    dataset,
    user_id: int,
) -> datetime | None:
    if not dataset.created_at:
        dataset.created_at = datetime.utcnow()

    expiry_at = get_retention_expiry_for_user(
        db=db,
        user_id=user_id,
        created_at=dataset.created_at,
    )
    dataset.retention_until = expiry_at
    dataset.retention_at = datetime.utcnow() if expiry_at else None
    dataset.is_retention = expiry_at is not None
    return expiry_at


def _get_retention_deadline(
    *,
    dataset,
    tier: str,
    subscription: UserSubscription | None,
) -> datetime | None:
    deadline = _get_base_retention_deadline(
        created_at=dataset.created_at,
        tier=tier,
        subscription=subscription,
    )
    if deadline is None:
        return None

    if not dataset.retention_until:
        return deadline

    if tier in {"lite", "pro"}:
        return min(
            dataset.retention_until,
            _get_paid_retention_limit(
                created_at=dataset.created_at,
                subscription=subscription,
            ),
        )

    deadline = min(deadline, dataset.retention_until)

    return deadline


def retention_dataset_for_user(
    *,
    db: Session,
    dataset,
    user_id: int,
) -> datetime:
    subscription = get_active_subscription(db, user_id)
    plan_name = subscription.plan.name if subscription and subscription.plan else None
    tier = normalize_plan_tier(plan_name)

    if tier not in {"lite", "pro"}:
        raise error_response(
            status_code=403,
            detail="File retention is available only for Lite and Pro plans.",
        )

    now = datetime.utcnow()
    retention_until = _get_paid_retention_limit(
        created_at=dataset.created_at,
        subscription=subscription,
    )
    if retention_until <= now:
        raise error_response(
            status_code=400,
            detail=(
                "This file can no longer be retained. Retention is limited to "
                f"your subscription period plus {PAID_RETENTION_EXTENSION_DAYS} days."
            ),
        )

    dataset.is_retention = True
    dataset.retention_until = retention_until
    dataset.retention_at = now
    return retention_until


def _get_user_retention_policy(db: Session, user_id: int) -> tuple[str, str, UserSubscription | None]:
    subscription = _get_latest_subscription(db, user_id)
    plan_name = subscription.plan.name if subscription and subscription.plan else "Free"
    return normalize_plan_tier(plan_name), plan_name, subscription


def get_user_retention_summary(db: Session, user_id: int) -> dict:
    now = datetime.utcnow()
    tier, plan_name, subscription = _get_user_retention_policy(db, user_id)
    datasets: list[tuple[str, datetime]] = []

    uploaded_datasets = (
        db.query(CsvUploadedDataset)
        .filter(CsvUploadedDataset.created_by_user_id == user_id)
        .all()
    )

    for dataset in uploaded_datasets:
        deadline = _get_retention_deadline(
            dataset=dataset,
            tier=tier,
            subscription=subscription,
        )
        if deadline is not None and deadline > now:
            datasets.append((dataset.name, deadline))

    if not datasets:
        return {
            "retention_plan": plan_name,
            "retention_pending_days": None,
            "retention_pending_hours": None,
            "next_file_expiry_at": None,
            "retention_file_count": 0,
        }

    next_file_name, next_expiry_at = min(datasets, key=lambda item: item[1])
    remaining_seconds = max((next_expiry_at - now).total_seconds(), 0)

    return {
        "retention_plan": plan_name,
        "retention_pending_days": math.ceil(remaining_seconds / 86400),
        "retention_pending_hours": math.ceil(remaining_seconds / 3600),
        "next_file_expiry_at": next_expiry_at,
        "next_expiring_file_name": next_file_name,
        "retention_file_count": len(datasets),
    }


def _get_cached_retention_policy(
    db: Session,
    user_id: int,
    retention_cache: dict[int, tuple[str, str, UserSubscription | None]],
) -> tuple[str, str, UserSubscription | None]:
    cached_policy = retention_cache.get(user_id)
    if cached_policy is not None:
        return cached_policy

    cached_policy = _get_user_retention_policy(db, user_id)
    retention_cache[user_id] = cached_policy
    return cached_policy


def _maybe_add_warning(
    *,
    warning_groups: dict[str, dict],
    email: str,
    plan_name: str,
    deadline: datetime,
    dataset_name: str,
) -> None:
    warning_entry = warning_groups[email]
    warning_entry["plan_name"] = plan_name
    warning_entry["expires_at"] = min(
        warning_entry["expires_at"] or deadline,
        deadline,
    )
    warning_entry["datasets"].append(dataset_name)


def _process_retention_datasets(
    *,
    db: Session,
    datasets: list,
    now: datetime,
    retention_cache: dict[int, tuple[str, str, UserSubscription | None]],
    warning_groups: dict[str, dict],
    delete_callback,
    log_label: str,
) -> None:
    for dataset in datasets:
        user = dataset.created_by
        if not user or not user.email:
            continue

        tier, plan_name, subscription = _get_cached_retention_policy(
            db,
            dataset.created_by_user_id,
            retention_cache,
        )
        deadline = _get_retention_deadline(
            dataset=dataset,
            tier=tier,
            subscription=subscription,
        )
        if deadline is None:
            continue

        if deadline <= now:
            delete_callback(dataset=dataset)
            logger.info(
                "Auto-deleted %s_id=%s for user_id=%s plan=%s",
                log_label,
                dataset.id,
                dataset.created_by_user_id,
                plan_name,
            )
            continue

        if tier in {"lite", "pro"} and deadline - now <= timedelta(days=RETENTION_WARNING_DAYS):
            _maybe_add_warning(
                warning_groups=warning_groups,
                email=user.email,
                plan_name=plan_name,
                deadline=deadline,
                dataset_name=dataset.name,
            )


def run_file_retention_cycle() -> None:
    now = datetime.utcnow()
    db = SessionLocal()
    warning_groups: dict[str, dict] = defaultdict(
        lambda: {"plan_name": "", "expires_at": None, "datasets": []}
    )

    try:
        uploaded_datasets = (
            db.query(CsvUploadedDataset)
            .join(User, CsvUploadedDataset.created_by_user_id == User.id)
            .filter(User.email != ANONYMOUS_UPLOAD_EMAIL)
            .order_by(CsvUploadedDataset.created_at.asc())
            .all()
        )
        retention_cache: dict[int, tuple[str, str, UserSubscription | None]] = {}

        _process_retention_datasets(
            db=db,
            datasets=uploaded_datasets,
            now=now,
            retention_cache=retention_cache,
            warning_groups=warning_groups,
            delete_callback=delete_uploaded_dataset,
            log_label="uploaded dataset",
        )

        db.commit()

        for recipient, warning_entry in warning_groups.items():
            _send_retention_warning_email(
                recipient=recipient,
                dataset_names=sorted(set(warning_entry["datasets"])),
                expires_at=warning_entry["expires_at"],
                plan_name=warning_entry["plan_name"],
            )
    except Exception:
        db.rollback()
        logger.exception("File retention cycle failed")
    finally:
        db.close()


def _retention_worker() -> None:
    logger.info("File retention worker started")
    while not _retention_stop_event.is_set():
        run_file_retention_cycle()
        _retention_stop_event.wait(RETENTION_JOB_INTERVAL_SECONDS)
    logger.info("File retention worker stopped")


def start_file_retention_scheduler() -> None:
    global _retention_thread

    if _retention_thread and _retention_thread.is_alive():
        return

    _retention_stop_event.clear()
    _retention_thread = threading.Thread(
        target=_retention_worker,
        name="file-retention-worker",
        daemon=True,
    )
    _retention_thread.start()


def stop_file_retention_scheduler() -> None:
    _retention_stop_event.set()
