import logging
import smtplib
import ssl
from email.mime.text import MIMEText

from app.config.config import settings

logger = logging.getLogger(__name__)


def smtp_settings_complete() -> bool:
    required_settings = (
        settings.SMTP_HOST,
        settings.SMTP_PORT,
        settings.SMTP_EMAIL,
        settings.SMTP_PASSWORD,
    )
    return all(required_settings)


def missing_smtp_settings() -> list[str]:
    required_settings = {
        "SMTP_HOST": settings.SMTP_HOST,
        "SMTP_PORT": settings.SMTP_PORT,
        "SMTP_EMAIL": settings.SMTP_EMAIL,
        "SMTP_PASSWORD": settings.SMTP_PASSWORD,
    }
    return [key for key, value in required_settings.items() if not value]


def send_plain_email(*, recipient: str, subject: str, body: str) -> None:
    message = MIMEText(body)
    message["Subject"] = subject
    message["From"] = settings.SMTP_EMAIL
    message["To"] = recipient

    if settings.SMTP_PORT == 465:
        with smtplib.SMTP_SSL(
            settings.SMTP_HOST,
            settings.SMTP_PORT,
            context=ssl.create_default_context(),
        ) as server:
            server.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
            server.sendmail(settings.SMTP_EMAIL, recipient, message.as_string())
        return

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.ehlo()
        if server.has_extn("starttls"):
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        server.login(settings.SMTP_EMAIL, settings.SMTP_PASSWORD)
        server.sendmail(settings.SMTP_EMAIL, recipient, message.as_string())
