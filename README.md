# 🚀 Real-time-data-ingestion-project

### There is a .mp4 file in this project that will make you understand whole project


This project demonstrates an automated data ingestion pipeline using **Apache Spark**, **Delta Lake**, and **Faker**. It simulates periodic data ingestion, tracks versioned records, and sends email summaries after each execution.

---

## 📌 Features

- ✅ **Generate Fake Data** using the Faker library (Name, Address, Email)
- ✅ **Append to Delta Table** with automatic schema management
- ✅ **Version Tracking** using Delta Table API with `versionAsOf` and `timestampAsOf`
- ✅ **Time Zone Support** (Asia/Kolkata)
- ✅ **Scheduled Appending** every 5 minutes (configurable)
- ✅ **Email Notification** after each append with an HTML summary of changes

---

## 🛠️ Tech Stack

- **Apache Spark** (v3.4.0+)
- **Delta Lake** (v2.3+)
- **Python 3.11**
- **Faker**
- **Gmail SMTP API**
- **VS Code + PowerShell**

---

## 📁 Project Structure

📦 Delta-Pipeline-Project/
├── delta-tables/ # Delta storage location
├── output/ # Optional output/logs
├── generate_and_append.py # Manual data generation & append
├── append_with_deltatable_api.py # Delta API usage example
├── version_tracking.py # Track and retrieve versions
├── auto_email_summary.py # Standalone email sender
├── scheduled_append.py # Main automated pipeline script
├── run_pipeline.bat # Optional runner script (Windows)

yaml
Copy
Edit

---

## ⚙️ Setup Instructions

### 🔧 Prerequisites

- Python 3.11+
- Apache Spark with Delta Lake JARs
- Java 8 or 11
- Gmail account with **App Password** enabled

### 📦 Install Dependencies

```bash
pip install pyspark faker delta-spark pytz
✅ Make sure your spark-submit includes Delta Lake JARs.

📨 Email Configuration
Edit the following in scheduled_append.py and auto_email_summary.py:

python
Copy
Edit
EMAIL_SENDER = "your_email@gmail.com"
EMAIL_PASSWORD = "your_app_password"  # Not your Gmail password!
EMAIL_RECEIVER = "destination_email@gmail.com"
🚀 Running the Pipeline
▶️ Option 1: One-time Append
bash
Copy
Edit
python generate_and_append.py
▶️ Option 2: Append with Delta API + Version View
bash
Copy
Edit
python append_with_deltatable_api.py
▶️ Option 3: Automated Scheduled Pipeline (Every 5 Minutes)
bash
Copy
Edit
python scheduled_append.py
Email notification will be sent after each execution.

▶️ Option 4: Track Table Version
bash
Copy
Edit
python version_tracking.py

🧠 Learning Goals
This project demonstrates:

Using Delta Lake's time travel and version control features

Automating ETL-style ingestion pipelines

Sending real-time email reports from Spark pipelines

Handling schema evolution and batch processing

📜 License
MIT License © 2025 [Ishan Jain]

🙌 Acknowledgements
Delta Lake by Databricks

Apache Spark

Faker Python Library

