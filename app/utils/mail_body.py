from app.config.config import settings
import textwrap

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
