from pyspark.sql import SparkSession
from faker import Faker
import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DeltaLakeFakeDataPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

try:
    # Initialize Faker
    fake = Faker()
    
    # Generate fake data (configurable batch size)
    batch_size = 10  # Increased from 5 for better testing
    print(f"Generating {batch_size} fake records...")
    
    data = []
    for _ in range(batch_size):
        # Clean address by replacing newlines with commas
        clean_address = fake.address().replace("\n", ", ").replace("\r", "")
        data.append((fake.name(), clean_address, fake.email()))
    
    # Create DataFrame with proper schema
    df = spark.createDataFrame(data, ["name", "address", "email"])
    
    # Add timestamp for tracking when data was inserted
    from pyspark.sql.functions import current_timestamp
    df_with_timestamp = df.withColumn("created_at", current_timestamp())
    
    # Save to Delta Lake with schema merge option
    print("Appending data to Delta table...")
    df_with_timestamp.write.format("delta").option("mergeSchema", "true").mode("append").save("delta-tables/people")
    
    print("[SUCCESS] Successfully appended data!")
    print("New Records Added:")
    df_with_timestamp.show(truncate=False)
    
    # Show total record count
    full_table = spark.read.format("delta").load("delta-tables/people")
    total_count = full_table.count()
    print(f"\n[INFO] Total records in Delta table: {total_count}")
    
    # Show latest 10 records
    print("\nLatest 10 records:")
    full_table.orderBy("created_at", ascending=False).show(10, truncate=False)

except Exception as e:
    print(f"[ERROR] Error occurred: {str(e)}")
    
finally:
    # Always stop Spark session
    spark.stop()
    print("Spark session stopped.")