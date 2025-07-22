from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import pytz

# ✅ Initialize Spark Session with proper Delta configs
spark = SparkSession.builder \
    .appName("DeltaVersioning") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.session.timeZone", "Asia/Kolkata") \
    .getOrCreate()

# ✅ Define Delta table path
delta_path = "delta-tables/people"

# ✅ Load Delta table
delta_table = DeltaTable.forPath(spark, delta_path)

# ✅ Show table version history
print("📜 Delta Table History:")
history_df = delta_table.history()
history_df.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# ✅ Extract latest version info
latest_version_row = history_df.orderBy("version", ascending=False).first()
latest_version = latest_version_row["version"]
latest_timestamp = latest_version_row["timestamp"]

print(f"\n✅ Latest Version: {latest_version} at {latest_timestamp}")

# ✅ Load data from latest version
print("\n📄 Data from Latest Version (versionAsOf):")
latest_df = spark.read.format("delta") \
    .option("versionAsOf", latest_version) \
    .load(delta_path)
latest_df.show(truncate=False)

# ✅ Load data 1 minute before latest timestamp
one_min_ago = latest_timestamp - timedelta(minutes=1)
print(f"\n🕒 Data using timestampAsOf = {one_min_ago}")
timestamp_df = spark.read.format("delta") \
    .option("timestampAsOf", one_min_ago.isoformat()) \
    .load(delta_path)
timestamp_df.show(truncate=False)

# ✅ Stop Spark session
spark.stop()

print("Script completed successfully")