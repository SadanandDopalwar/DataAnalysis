import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
# Email credentials
sender_email = "@gmail.com"
sender_password = ""  # Use the App Password, not the regular password
receiver_email = "@gmail.com"
cc_emails = ""


async def send_emaill(body, subject, logger, isattachments):
# Send the email
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Cc"] = cc_emails
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))
    if isattachments:
        for filename in os.listdir('.'):
                if filename.endswith('.png'):
                    with open(filename, 'rb') as attachment:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename="{filename}"',
                    )
                    message.attach(part)
    try:   
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(message)
            logger.info("Email sent successfully!")
    except Exception as e:
        logger.error("Failed to send email: %s", e)
