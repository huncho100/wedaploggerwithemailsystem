import azure.functions as func
import datetime
import logging
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

app = func.FunctionApp()

# Environment variables
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
TO_EMAIL = os.getenv("TO_EMAIL")
FROM_EMAIL = os.getenv("FROM_EMAIL", "torkumamatthew@outlook.com")
EMAIL_TIMER_SCHEDULE = os.getenv("EMAIL_TIMER_SCHEDULE", "0 0 5 * * *")  # Every day at 5 AM

@app.timer_trigger(
    schedule=EMAIL_TIMER_SCHEDULE,
    arg_name="emailTimer",
    run_on_startup=True,
    use_monitor=True
)
def email_notify(emailTimer: func.TimerRequest) -> None:
    """Send daily weather summary email."""
    
    logging.info("Starting email notification function...")

    try:
        now = datetime.datetime.now()
        subject = f"Weather Update - {now.strftime('%Y-%m-%d')}"

        # Example message (you can customize to read blob data instead)
        content = f"""
        Good morning Chief!

        Here is your weather update for today:
        - City: Lagos
        - Date: {now.strftime('%A, %d %B %Y')}
        - Time: {now.strftime('%H:%M')}
        
        Please check your dashboard or logs for detailed info.
        """

        message = Mail(
            from_email=FROM_EMAIL,
            to_emails=TO_EMAIL,
            subject=subject,
            plain_text_content=content
        )

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        logging.info(f"Email sent! Status code: {response.status_code}")

    except Exception as e:
        logging.error(f"Failed to send email: {e}")
