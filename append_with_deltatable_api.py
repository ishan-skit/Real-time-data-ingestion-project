from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from faker import Faker
import os

# ✅ Spark Session setup
spark = SparkSession.builder \
    .appName("DeltaLakeAppendDemo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# ✅ Delta path
delta_path = "delta-tables/people"
os.makedirs("delta-tables", exist_ok=True)

# ✅ Generate fake data
fake = Faker()
records = [(fake.name(), fake.address().replace("\n", ", "), fake.email()) for _ in range(5)]
df = spark.createDataFrame(records, ["name", "address", "email"])

# ✅ Check if Delta table exists
if not DeltaTable.isDeltaTable(spark, delta_path):
    print("📦 Delta table not found. Creating a new one...")
    df.write.format("delta").mode("overwrite").save(delta_path)
else:
    print("📥 Delta table found. Appending new data...")
    df.write.format("delta").mode("append").save(delta_path)

print("✅ New data added:")
df.show(truncate=False)

# ✅ Load Delta Table & show contents
delta_table = DeltaTable.forPath(spark, delta_path)
print("\n📄 Full Delta Table Content:")
delta_table.toDF().show(truncate=False)

print("\n🕓 Delta Table History:")
delta_table.history().select("version", "timestamp", "operation").show(truncate=False)

spark.stop()
