# ================= IMPORTS =================
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, desc
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from faker import Faker
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import time
import os
import sys

# ================= CONFIGURATION =================
num_rows_per_append = 5
append_interval_seconds = 300  # 5 minutes
delta_path = "delta-tables/people"

EMAIL_SENDER = "ishanjain256@gmail.com"
EMAIL_PASSWORD = "aluikldmmiaklotw"  # Use App Password
EMAIL_RECEIVER = "ishanjain169@gmail.com"

# ================= INITIALIZE SPARK =================
def initialize_spark():
    try:
        # Download Delta Lake JARs automatically
        spark = SparkSession.builder \
            .appName("DeltaPipeline") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.session.timeZone", "Asia/Kolkata") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("[INFO] Spark session initialized with Delta support.")
        return spark
    except Exception as e:
        print(f"[ERROR] Spark init failed: {e}")
        sys.exit(1)

# ================= FAKE DATA =================
def generate_fake_data(batch_size):
    fake = Faker("en_IN")
    return [{
        "name": fake.name(),
        "address": fake.address().replace("\n", ", "),
        "email": fake.email()
    } for _ in range(batch_size)]

# ================= CREATE / LOAD DELTA =================
def create_or_get_delta_table(spark, delta_path):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    os.makedirs(os.path.dirname(delta_path) or ".", exist_ok=True)

    try:
        # Try to read existing Delta table
        spark.read.format("delta").load(delta_path)
        print("[INFO] Delta table loaded.")
        return True
    except:
        print("[WARN] Delta table not found. Creating new one...")
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").save(delta_path)
        print("[INFO] Delta table created.")
        return True

# ================= APPEND TO DELTA =================
def append_to_delta_table(spark, data):
    try:
        # Ensure table exists
        create_or_get_delta_table(spark, delta_path)
        
        df = spark.createDataFrame(data)
        df = df.withColumn("created_at", current_timestamp())
        df.write.format("delta").mode("append").save(delta_path)
        print("[INFO] Data appended to Delta table.")
        return df
    except Exception as e:
        print(f"[ERROR] Append failed: {e}")
        return None

# ================= FETCH TABLE INFO =================
def get_table_info(spark):
    try:
        df = spark.read.format("delta").load(delta_path)
        total = df.count()
        latest = df.orderBy(desc("created_at")).limit(5)
        print(f"[INFO] Total records: {total}")
        latest.show(truncate=False)
        return total, latest
    except Exception as e:
        print(f"[ERROR] Cannot read delta table: {e}")
        return 0, None

# ================= EMAIL =================
def send_email(subject, html):
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = subject

        msg.attach(MIMEText("Plain text fallback", "plain"))
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print("[INFO] Email sent.")
    except Exception as e:
        print(f"[ERROR] Email failed: {e}")

# ================= MAIN LOOP =================
def main():
    print("[🚀] Starting Delta Pipeline with Spark + Delta Lake")
    spark = initialize_spark()
    iteration = 1

    while True:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[INFO] Iteration {iteration} started at {now}")

        data = generate_fake_data(num_rows_per_append)
        df = append_to_delta_table(spark, data)

        if df:
            total, latest = get_table_info(spark)
            html = f"""
            <html><body>
            <h2>Delta Pipeline Execution Report</h2>
            <p><b>Time:</b> {now}</p>
            <p><b>Total Records:</b> {total}</p>
            <p><b>Latest Appended:</b></p><ul>
            """
            if latest:
                for row in latest.collect():
                    html += f"<li>{row['name']} | {row['email']} | {row['created_at']}</li>"
            html += "</ul></body></html>"

            send_email("Delta Pipeline Update", html)
        else:
            print("[WARN] No data appended, skipping email.")

        iteration += 1
        print(f"[INFO] Sleeping for {append_interval_seconds} seconds...\n")
        time.sleep(append_interval_seconds)

if __name__ == "__main__":
    main()