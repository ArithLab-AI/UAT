import textwrap
from datetime import datetime

from app.config.config import settings

def mail_body(otp: int) -> str:
    body = textwrap.dedent(f"""\
        Hello,

        Welcome to Airthlab – your real-time analytics platform for intelligent data insights.

        Your One-Time Password (OTP) for secure authentication is:

        OTP: {otp}

        This OTP is valid for the next {settings.OTP_EXPIRE_MINUTES} minutes.

        Airthlab enables powerful real-time data analytics, advanced monitoring, and actionable insights to help you make smarter decisions faster.

        If you did not request this code, please ignore this email. Your account remains secure.

        Best regards,
        The Airthlab Team
    """)
    
    return body.strip()


def retention_warning_mail_body(
    *,
    dataset_names: list[str],
    expires_at: datetime,
    plan_name: str,
) -> str:
    dataset_lines = "\n".join(f"- {name}" for name in dataset_names)
    body = textwrap.dedent(f"""\
        Hello,

        Your {plan_name} plan files listed below are scheduled for automatic deletion on
        {expires_at.strftime('%Y-%m-%d %H:%M:%S UTC')}.

        {dataset_lines}

        Please retain or download them before the retention window ends.

        Best regards,
        The Airthlab Team
    """)

    return body.strip()
