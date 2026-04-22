import logging
import smtplib
import ssl
from email.mime.text import MIMEText

from app.config.config import settings

logger = logging.getLogger(__name__)
SMTP_TIMEOUT_SECONDS = 20


def smtp_settings_complete() -> bool:
    return not missing_smtp_settings()


def missing_smtp_settings() -> list[str]:
    required_settings = {
        "SMTP_HOST": settings.SMTP_HOST,
        "SMTP_PORT": settings.SMTP_PORT,
        "SMTP_EMAIL": settings.SMTP_EMAIL,
        "SMTP_PASSWORD": settings.SMTP_PASSWORD,
    }
    return [key for key, value in required_settings.items() if not value]


def _smtp_host() -> str:
    return str(settings.SMTP_HOST).strip()


def _smtp_email() -> str:
    return str(settings.SMTP_EMAIL).strip()


def _smtp_password() -> str:
    password = str(settings.SMTP_PASSWORD).strip()
    if _is_gmail_smtp():
        return password.replace(" ", "")
    return password


def _is_gmail_smtp() -> bool:
    return "gmail.com" in _smtp_host().lower()


def _send_email_with_ssl(*, recipient: str, message: MIMEText, port: int = 465) -> None:
    with smtplib.SMTP_SSL(
        _smtp_host(),
        port,
        timeout=SMTP_TIMEOUT_SECONDS,
        context=ssl.create_default_context(),
    ) as server:
        server.login(_smtp_email(), _smtp_password())
        server.sendmail(_smtp_email(), recipient, message.as_string())


def _send_email_with_starttls(*, recipient: str, message: MIMEText) -> None:
    with smtplib.SMTP(_smtp_host(), settings.SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as server:
        server.ehlo()
        if server.has_extn("starttls"):
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        server.login(_smtp_email(), _smtp_password())
        server.sendmail(_smtp_email(), recipient, message.as_string())


def send_plain_email(*, recipient: str, subject: str, body: str) -> None:
    message = MIMEText(body)
    message["Subject"] = subject
    message["From"] = _smtp_email()
    message["To"] = recipient

    if settings.SMTP_PORT == 465:
        _send_email_with_ssl(recipient=recipient, message=message)
        return

    try:
        _send_email_with_starttls(recipient=recipient, message=message)
    except (OSError, smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected):
        if not _is_gmail_smtp() or settings.SMTP_PORT == 465:
            raise
        logger.warning("SMTP STARTTLS failed for host=%s; retrying Gmail SMTP over SSL", _smtp_host())
        _send_email_with_ssl(recipient=recipient, message=message)
