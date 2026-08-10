"""
Simple email notification for pipeline failures.
Usage: python aws_mail.py "Subject line" "Body message"
"""
import smtplib
import sys
from email.mime.text import MIMEText
from datetime import datetime

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "joelfiddes@gmail.com"
SMTP_PASS = "mwgz dmco utpr igyi"  # Gmail App Password
TO_EMAIL = "joelfiddes@gmail.com"

def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, TO_EMAIL, msg.as_string())
        print(f"Email sent: {subject}")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    subject = sys.argv[1] if len(sys.argv) > 1 else "SnowMapper Alert"
    body = sys.argv[2] if len(sys.argv) > 2 else "No details provided."
    body += f"\n\nTimestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    body += "\nHost: snowmapper EC2 (13.50.55.27)"
    send_email(subject, body)
