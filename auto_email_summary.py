import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pytz

# Email configuration
EMAIL_SENDER = "ishanjain256@gmail.com"
EMAIL_PASSWORD = "aluikldmmiaklotw"  # App password from Google
EMAIL_RECEIVER = "ishanjain169@gmail.com"

def create_enhanced_html_alert():
    """Create enhanced HTML alert email"""
    
    # Get current time in IST
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S IST")
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                margin: 0; 
                padding: 20px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            }}
            .container {{ 
                background-color: white; 
                padding: 30px; 
                border-radius: 15px; 
                box-shadow: 0 10px 30px rgba(0,0,0,0.2); 
                max-width: 600px; 
                margin: 0 auto; 
            }}
            .header {{ 
                background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%); 
                color: white; 
                padding: 25px; 
                border-radius: 10px; 
                text-align: center; 
                margin-bottom: 25px; 
            }}
            .alert-icon {{ 
                font-size: 48px; 
                margin-bottom: 10px; 
            }}
            .status-box {{ 
                background: linear-gradient(135deg, #00d2ff 0%, #3a7bd5 100%); 
                color: white; 
                padding: 20px; 
                border-radius: 10px; 
                text-align: center; 
                margin: 20px 0; 
                font-size: 18px; 
                font-weight: bold; 
            }}
            .info-grid {{ 
                display: grid; 
                grid-template-columns: 1fr 1fr; 
                gap: 15px; 
                margin: 20px 0; 
            }}
            .info-card {{ 
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); 
                padding: 15px; 
                border-radius: 8px; 
                border-left: 4px solid #007bff; 
                text-align: center; 
            }}
            .info-value {{ 
                font-size: 24px; 
                font-weight: bold; 
                color: #007bff; 
                margin: 5px 0; 
            }}
            .info-label {{ 
                font-size: 12px; 
                color: #6c757d; 
                text-transform: uppercase; 
            }}
            .footer {{ 
                margin-top: 30px; 
                padding: 20px; 
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); 
                border-radius: 8px; 
                font-size: 14px; 
                color: #6c757d; 
                text-align: center; 
            }}
            .success-badge {{ 
                background: linear-gradient(135deg, #28a745 0%, #20c997 100%); 
                color: white; 
                padding: 8px 16px; 
                border-radius: 20px; 
                font-weight: bold; 
                display: inline-block; 
                margin: 10px 0; 
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="alert-icon">🚨</div>
                <h1>Delta Pipeline Alert</h1>
                <p>Automated System Notification</p>
            </div>
            
            <div class="status-box">
                <div class="success-badge">✅ JOB EXECUTED SUCCESSFULLY</div>
            </div>
            
            <div class="info-grid">
                <div class="info-card">
                    <div class="info-value">📅</div>
                    <div class="info-label">Current Date</div>
                    <div style="font-weight: bold; margin-top: 5px;">{current_time.split()[0]}</div>
                </div>
                <div class="info-card">
                    <div class="info-value">🕒</div>
                    <div class="info-label">Execution Time</div>
                    <div style="font-weight: bold; margin-top: 5px;">{current_time.split()[1]} IST</div>
                </div>
                <div class="info-card">
                    <div class="info-value">⚡</div>
                    <div class="info-label">System Status</div>
                    <div style="font-weight: bold; margin-top: 5px; color: #28a745;">ACTIVE</div>
                </div>
                <div class="info-card">
                    <div class="info-value">🔄</div>
                    <div class="info-label">Pipeline Mode</div>
                    <div style="font-weight: bold; margin-top: 5px;">AUTOMATED</div>
                </div>
            </div>
            
            <div style="background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); padding: 20px; border-radius: 10px; border-left: 5px solid #2196f3; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #1976d2;">📋 Alert Details</h3>
                <p><strong>🕒 Timestamp:</strong> {current_time}</p>
                <p><strong>🎯 Status:</strong> <span style="color: #28a745; font-weight: bold;">Job executed successfully</span></p>
                <p><strong>🌐 Timezone:</strong> Asia/Kolkata (IST)</p>
                <p><strong>💻 System:</strong> Enhanced Delta Lake Pipeline</p>
            </div>
            
            <div class="footer">
                <p>🤖 This is an automated alert from your <strong>Delta Pipeline System</strong></p>
                <p>📧 If you have any concerns, please check the system logs</p>
                <p>🔔 Alert generated at: {current_time}</p>
            </div>
        </div>
    </body>
    </html>"""
    
    return html_content

def send_enhanced_alert():
    """Send enhanced HTML alert email"""
    try:
        # Create the email message
        message = MIMEMultipart("alternative")
        message["From"] = EMAIL_SENDER
        message["To"] = EMAIL_RECEIVER
        
        # Get current time in IST for subject
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
        message["Subject"] = f"🚨 Delta Pipeline Alert - Job Executed Successfully - {current_time} IST"
        
        # Create HTML content
        html_content = create_enhanced_html_alert()
        
        # Create plain text version as fallback
        current_time_full = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S IST")
        plain_text = f"""
        ===============================================
        🚨 DELTA PIPELINE SYSTEM ALERT
        ===============================================
        
        Hello,
        
        ✅ This is an automated alert from your Enhanced Delta Pipeline System.
        
        📅 Timestamp: {current_time_full}
        🎯 Status: ✔️ Job executed successfully
        🌐 Timezone: Asia/Kolkata (IST)
        💻 System: Enhanced Delta Lake Pipeline
        
        📋 SYSTEM STATUS: ACTIVE ⚡
        🔄 PIPELINE MODE: AUTOMATED
        
        ===============================================
        
        Regards,  
        Enhanced Delta Pipeline System
        
        🤖 This is an automated notification
        ===============================================
        """
        
        # Attach both plain text and HTML versions
        part1 = MIMEText(plain_text, "plain")
        part2 = MIMEText(html_content, "html")
        
        message.attach(part1)
        message.attach(part2)
        
        # Send email via Gmail SMTP
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(message)
        
        print("✅ Enhanced email alert sent successfully.")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send enhanced email alert: {e}")
        return False

def main():
    """Main function to send alert"""
    print("🚀 Sending Enhanced Delta Pipeline Alert...")
    print("-" * 50)
    
    # Get current time in IST
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"📅 Current Time: {current_time}")
    
    # Send the enhanced alert
    success = send_enhanced_alert()
    
    if success:
        print("✅ Alert sent successfully!")
        print("📧 Check your email for the enhanced alert notification")
    else:
        print("❌ Failed to send alert")
    
    print("-" * 50)

if __name__ == "__main__":
    main()