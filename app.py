# -*- coding: utf-8 -*-
"""
Created on Thu Aug 21 20:27:51 2025

@author: vij_c
"""
from pyspark.sql import SparkSession

# Start Spark

spark = SparkSession.builder \
    .appName("ParquetExample") \
           .getOrCreate()

# Sample DataFrame
data = [
    (1, "Alice", 29),
    (2, "Bob", 35),
    (3, "Charlie", 50)
]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

# Write DataFrame to Parquet
df.write.mode("overwrite").parquet("output/customers.parquet")
print("✅ Parquet file written to output/customers.parquet")

# Read it back
parquet_df = spark.read.parquet("output/customers.parquet")
parquet_df.show()
