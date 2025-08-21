from pyspark.sql import SparkSession

# ----------------------------
# 1️⃣ Initialize Spark Session
# ----------------------------
spark = SparkSession.builder \
    .appName("S3ToParquet") \
    .getOrCreate()

# ----------------------------
# 2️⃣ S3 Configuration
# ----------------------------
# Option 1: Using environment variables or IAM role
input_bucket_path = "s3://lordvishnubucket1/customers.csv"
output_bucket_path = "s3://lordvishnubucket1/parquet-output"

# Option 2: Using explicit AWS credentials (not recommended for production)
# spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY")
# spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY")
# spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

# ----------------------------
# 3️⃣ Read Input File from S3
# ----------------------------
# Supports CSV, JSON, Parquet, etc.
df = spark.read.option("header", True).csv(input_bucket_path)

print("✅ Input Data Sample:")
df.show(5)

# ----------------------------
# 4️⃣ Write DataFrame to S3 in Parquet format
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .parquet(output_bucket_path)

# ----------------------------


print(f"✅ Data successfully written to {output_bucket_path}")

# ----------------------------
# 5️⃣ Stop Spark Session
# ----------------------------
spark.stop()
