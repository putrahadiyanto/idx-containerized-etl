from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# MongoDB config variables
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_USERNAME = os.getenv("MONGO_USERNAME", "root")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_AUTH_DB = os.getenv("MONGO_AUTH_DB", "admin")
DB_NAME = os.getenv("DB_NAME", "idx_etl")

# Construct MongoDB URI with authentication
mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{DB_NAME}.stock_data?authSource={MONGO_AUTH_DB}"

# Inisialisasi Spark session
spark = SparkSession.builder \
    .appName("YFinance Data Transformation") \
    .config("spark.mongodb.input.uri", mongo_uri) \
    .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,/opt/spark/jars/mongodb-driver-3.12.10.jar,/opt/spark/jars/bson-3.12.10.jar,/opt/spark/jars/mongodb-driver-core-3.12.10.jar") \
    .getOrCreate()

# Membaca data dari MongoDB koleksi
logging.info("Reading data from MongoDB...")
df = spark.read.format("mongo").load()

if df.count() == 0:
    logging.warning("No data found in MongoDB collection")
    spark.stop()
    sys.exit(1)

logging.info(f"Successfully loaded {df.count()} records from MongoDB")

# Mengonversi kolom 'Date' menjadi tipe tanggal yang benar
df = df.withColumn("Date", F.to_date(F.col("Date")))

# Create output directory
output_dir = "/data"
os.makedirs(output_dir, exist_ok=True)

# 1. Agregasi Harian (per hari), berdasarkan ticker
df_daily = df.groupBy("Date", "ticker").agg(
    F.avg("Open").alias("avg_open"),
    F.avg("High").alias("avg_high"),
    F.avg("Low").alias("avg_low"),
    F.avg("Close").alias("avg_close"),
    F.avg("Volume").alias("avg_volume"),
    F.avg("Dividends").alias("avg_dividends"),
    F.avg("Stock Splits").alias("avg_stock_splits")
)

# Menyimpan hasil agregasi harian ke JSON
logging.info("Saving daily aggregation to JSON...")
df_daily.coalesce(1).write.mode("overwrite").json(f"{output_dir}/daily_aggregation")

# print("Harian agregasi per ticker berhasil disimpan ke JSON!")

# 2. Agregasi Bulanan (per bulan), berdasarkan ticker
df_monthly = df.withColumn("Year", F.year(F.col("Date"))) \
               .withColumn("Month", F.month(F.col("Date"))) \
               .groupBy("Year", "Month", "ticker").agg(
                   F.avg("Open").alias("avg_open"),
                   F.avg("High").alias("avg_high"),
                   F.avg("Low").alias("avg_low"),
                   F.avg("Close").alias("avg_close"),
                   F.avg("Volume").alias("avg_volume"),
                   F.avg("Dividends").alias("avg_dividends"),
                   F.avg("Stock Splits").alias("avg_stock_splits")
               )

# Menyimpan hasil agregasi bulanan ke JSON
logging.info("Saving monthly aggregation to JSON...")
df_monthly.coalesce(1).write.mode("overwrite").json(f"{output_dir}/monthly_aggregation")

# print("Bulanan agregasi per ticker berhasil disimpan ke JSON!")

# 3. Agregasi Tahunan (per tahun), berdasarkan ticker
df_yearly = df.withColumn("Year", F.year(F.col("Date"))) \
              .groupBy("Year", "ticker").agg(
                  F.avg("Open").alias("avg_open"),
                  F.avg("High").alias("avg_high"),
                  F.avg("Low").alias("avg_low"),
                  F.avg("Close").alias("avg_close"),
                  F.avg("Volume").alias("avg_volume"),
                  F.avg("Dividends").alias("avg_dividends"),
                  F.avg("Stock Splits").alias("avg_stock_splits")
              )

# Menyimpan hasil agregasi tahunan ke JSON
logging.info("Saving yearly aggregation to JSON...")
df_yearly.coalesce(1).write.mode("overwrite").json(f"{output_dir}/yearly_aggregation")

# print("Tahunan agregasi per ticker berhasil disimpan ke JSON!")

# Stop Spark session
logging.info("All aggregations completed successfully!")
logging.info(f"Output files saved to: {output_dir}")
spark.stop()
logging.info(f"Output files saved to: {output_dir}")
spark.stop()