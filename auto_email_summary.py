import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Email configuration
EMAIL_SENDER = "ishanjain256@gmail.com"
EMAIL_PASSWORD = "aluikldmmiaklotw"  # App password from Google
EMAIL_RECEIVER = "ishanjain169@gmail.com"

# Create the email
message = MIMEMultipart()
message["From"] = EMAIL_SENDER
message["To"] = EMAIL_RECEIVER
message["Subject"] = "🚨 Delta Pipeline Alert - Job Executed"

# Customize the email content
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
body = f"""
Hello,

✅ This is an automated alert from your Delta Pipeline System.

📅 Timestamp: {current_time}

Status: ✔️ Job executed successfully.

Regards,  
Delta Pipeline System
"""
message.attach(MIMEText(body, "plain"))

# Send the email via Gmail SMTP
try:
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(EMAIL_SENDER, EMAIL_PASSWORD)
    server.send_message(message)
    server.quit()
    print("✅ Email sent successfully.")
except Exception as e:
    print(f"❌ Failed to send email: {e}")
