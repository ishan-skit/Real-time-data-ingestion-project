from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, desc
from faker import Faker
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os
import sys

# ========== CONFIGURATION PARAMETERS ==========
# Pipeline Configuration
num_rows_per_append = 5
append_interval_seconds = 300  # 5 minutes (300 seconds)
num_iterations = None  # Set to None for infinite loop, or specify number like 10

# Email Configuration
EMAIL_SENDER = "ishanjain256@gmail.com"
EMAIL_PASSWORD = "aluikldmmiaklotw"  # App password from Google
EMAIL_RECEIVER = "ishanjain169@gmail.com"

# Delta Configuration
delta_path = "delta-tables/people"

def initialize_spark():
    """Initialize Spark Session with Delta Lake support"""
    try:
        spark = SparkSession.builder \
            .appName("EnhancedDeltaScheduledPipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.session.timeZone", "Asia/Kolkata") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        print("SUCCESS: Spark session initialized successfully")
        return spark
    except Exception as e:
        print(f"ERROR: Failed to initialize Spark session: {str(e)}")
        sys.exit(1)

def generate_fake_data(batch_size=5):
    """Generate fake data using Faker library"""
    try:
        fake = Faker()
        data = []
        for _ in range(batch_size):
            # Clean address by replacing problematic characters
            clean_address = fake.address().replace("\n", ", ").replace("\r", "").strip()
            data.append((fake.name(), clean_address, fake.email()))
        return data
    except Exception as e:
        print(f"ERROR: Failed to generate fake data: {str(e)}")
        return []

def append_to_delta_table(spark, data):
    """Append data to Delta table with error handling"""
    try:
        # Create DataFrame
        df = spark.createDataFrame(data, ["name", "address", "email"])
        df_with_timestamp = df.withColumn("created_at", current_timestamp())
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(delta_path) if os.path.dirname(delta_path) else ".", exist_ok=True)
        
        # Append to Delta table
        df_with_timestamp.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .save(delta_path)
        
        return df_with_timestamp, True
    except Exception as e:
        print(f"ERROR: Failed to append data to Delta table: {str(e)}")
        return None, False

def get_table_statistics(spark):
    """Get comprehensive table statistics"""
    try:
        full_table = spark.read.format("delta").load(delta_path)
        total_count = full_table.count()
        
        # Get latest 5 records
        latest_records = full_table.orderBy(desc("created_at")).limit(5)
        
        return {
            'total_count': total_count,
            'latest_records': latest_records.collect(),
            'success': True
        }
    except Exception as e:
        print(f"ERROR: Failed to get table statistics: {str(e)}")
        return {'success': False, 'error': str(e)}

def create_html_email_content(appended_data, table_stats, execution_time, iteration_num):
    """Create HTML formatted email content"""
    
    # HTML template with inline CSS for better compatibility
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .container {{ background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; margin-bottom: 20px; }}
            .status-success {{ color: #28a745; font-weight: bold; }}
            .stats-box {{ background-color: #e8f5e8; border-left: 4px solid #28a745; padding: 15px; margin: 15px 0; border-radius: 4px; }}
            .data-table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
            .data-table th, .data-table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            .data-table th {{ background-color: #f8f9fa; font-weight: bold; }}
            .data-table tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .footer {{ margin-top: 30px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; font-size: 12px; color: #666; }}
            .execution-details {{ background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0; border-radius: 4px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Delta Lake Pipeline Execution Report</h1>
                <p>Automated Data Ingestion Summary</p>
            </div>
            
            <div class="execution-details">
                <h3>📋 Execution Details</h3>
                <p><strong>Execution Time:</strong> {execution_time}</p>
                <p><strong>Iteration Number:</strong> {iteration_num}</p>
                <p><strong>Status:</strong> <span class="status-success">✅ SUCCESS</span></p>
                <p><strong>Records Added:</strong> {len(appended_data.collect()) if appended_data else 0}</p>
            </div>
            
            <div class="stats-box">
                <h3>📊 Table Statistics</h3>
                <p><strong>Total Records in Delta Table:</strong> {table_stats.get('total_count', 'N/A')}</p>
                <p><strong>Table Path:</strong> {delta_path}</p>
            </div>
            
            <h3>📝 Newly Added Records</h3>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Address</th>
                        <th>Email</th>
                        <th>Created At</th>
                    </tr>
                </thead>
                <tbody>"""
    
    # Add newly appended records
    if appended_data:
        for row in appended_data.collect():
            html_content += f"""
                    <tr>
                        <td>{row['name']}</td>
                        <td>{row['address']}</td>
                        <td>{row['email']}</td>
                        <td>{row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}</td>
                    </tr>"""
    
    html_content += """
                </tbody>
            </table>
            
            <h3>🕒 Latest 5 Records from Table</h3>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Address</th>
                        <th>Email</th>
                        <th>Created At</th>
                    </tr>
                </thead>
                <tbody>"""
    
    # Add latest records from table
    if table_stats.get('success') and 'latest_records' in table_stats:
        for row in table_stats['latest_records']:
            html_content += f"""
                    <tr>
                        <td>{row['name']}</td>
                        <td>{row['address']}</td>
                        <td>{row['email']}</td>
                        <td>{row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}</td>
                    </tr>"""
    
    html_content += f"""
                </tbody>
            </table>
            
            <div class="footer">
                <p>📧 This is an automated email from the Delta Lake Pipeline System.</p>
                <p>🔄 Next execution scheduled in {append_interval_seconds // 60} minutes.</p>
                <p>Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}</p>
            </div>
        </div>
    </body>
    </html>"""
    
    return html_content

def send_email_notification(appended_data, table_stats, execution_time, iteration_num):
    """Send HTML email notification with execution summary"""
    try:
        # Create the email message
        message = MIMEMultipart("alternative")
        message["From"] = EMAIL_SENDER
        message["To"] = EMAIL_RECEIVER
        message["Subject"] = f"🚀 Delta Pipeline Report - Iteration #{iteration_num} - {execution_time}"
        
        # Create HTML content
        html_content = create_html_email_content(appended_data, table_stats, execution_time, iteration_num)
        
        # Create plain text version as fallback
        plain_text = f"""
        Delta Lake Pipeline Execution Report
        ====================================
        
        Execution Time: {execution_time}
        Iteration: #{iteration_num}
        Status: SUCCESS
        Records Added: {len(appended_data.collect()) if appended_data else 0}
        Total Records: {table_stats.get('total_count', 'N/A')}
        
        This is an automated notification from your Delta Pipeline System.
        """
        
        # Attach both plain text and HTML versions
        part1 = MIMEText(plain_text, "plain")
        part2 = MIMEText(html_content, "html")
        
        message.attach(part1)
        message.attach(part2)
        
        # Send email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(message)
        
        print("SUCCESS: Email notification sent successfully")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to send email notification: {str(e)}")
        return False

def main():
    """Main pipeline execution function"""
    print("=" * 60)
    print("🚀 ENHANCED DELTA LAKE SCHEDULED PIPELINE STARTING")
    print("=" * 60)
    
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        iteration = 1
        
        # Infinite loop or fixed iterations
        while True:
            execution_start_time = datetime.now()
            execution_time_str = execution_start_time.strftime('%Y-%m-%d %H:%M:%S IST')
            
            print(f"\n📅 Starting Iteration #{iteration} at {execution_time_str}")
            print("-" * 50)
            
            # Generate fake data
            print(f"📊 Generating {num_rows_per_append} fake records...")
            fake_data = generate_fake_data(num_rows_per_append)
            
            if not fake_data:
                print("ERROR: No data generated, skipping iteration")
                continue
            
            # Append to Delta table
            print("💾 Appending data to Delta table...")
            appended_df, append_success = append_to_delta_table(spark, fake_data)
            
            if not append_success:
                print("ERROR: Failed to append data, skipping email notification")
                continue
            
            # Get table statistics
            print("📈 Collecting table statistics...")
            table_stats = get_table_statistics(spark)
            
            # Show current batch data
            print(f"✅ Successfully appended {num_rows_per_append} rows:")
            appended_df.show(truncate=False)
            
            if table_stats.get('success'):
                print(f"📊 Total records in Delta table: {table_stats['total_count']}")
            
            # Send email notification
            print("📧 Sending email notification...")
            email_success = send_email_notification(appended_df, table_stats, execution_time_str, iteration)
            
            if not email_success:
                print("WARNING: Email notification failed, but data was successfully appended")
            
            print(f"✅ Iteration #{iteration} completed successfully")
            
            # Check if we should continue
            if num_iterations is not None and iteration >= num_iterations:
                print(f"\n🎯 Completed all {num_iterations} iterations. Pipeline finished.")
                break
            
            # Wait for next iteration
            print(f"⏳ Waiting {append_interval_seconds} seconds ({append_interval_seconds//60} minutes) before next execution...")
            print("=" * 60)
            
            time.sleep(append_interval_seconds)
            iteration += 1
            
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrupted by user (Ctrl+C)")
        print("🔄 Gracefully shutting down...")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        print("💥 Pipeline terminated unexpectedly")
        
    finally:
        # Always stop Spark session
        try:
            spark.stop()
            print("✅ Spark session stopped successfully")
        except:
            pass
        
        print("🏁 Pipeline execution completed")
        print("=" * 60)

if __name__ == "__main__":
    main()