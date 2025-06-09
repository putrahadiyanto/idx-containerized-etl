"""
Load script for IDX Laporan Keuangan ETL process.

This script handles the loading of transformed financial data
from parquet files into MongoDB.
"""

import logging
import os
from pyspark.sql import SparkSession

def get_current_quarter():
    """Get current year and quarter for collection naming."""
    from datetime import datetime
    now = datetime.now()
    quarter = (now.month - 1) // 3 + 1
    return str(now.year), quarter

def create_spark_session():
    """Create and configure Spark session."""
    spark = SparkSession.builder \
        .appName("IDX Financial Data Load") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar") \
        .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar") \
        .getOrCreate()
    return spark

def load():
    """
    Load task - Reads transformed data from parquet and writes it to MongoDB collection.
    """
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting load process...")
    
    spark = None
    try:
        spark = create_spark_session()
        
        # Get current year and quarter
        CURRENT_YEAR_STR, QUARTER = get_current_quarter()
        FINAL_COLLECTION = "idx_lapkeu_" + CURRENT_YEAR_STR + "TW" + str(QUARTER) + "_transformed"
        
        logging.info("Reading transformed data from parquet file...")
        
        parquet_path = "/data/data.parquet"
        if not os.path.exists(parquet_path):
            logging.warning(f"Parquet data path {parquet_path} does not exist. Skipping load.")
            return

        df = spark.read.format("parquet").load(parquet_path)
        
        if df.count() == 0:
            logging.warning("Transformed DataFrame is empty. No data will be saved to MongoDB.")
            return
        
        df = df.orderBy("emiten", ascending=True)
        
        logging.info(f"Writing data to MongoDB collection {FINAL_COLLECTION}...")
        df.write.format("mongo") \
            .option("uri", "mongodb://host.docker.internal:27017") \
            .option("database", "idx_lapkeu_transformed") \
            .option("collection", FINAL_COLLECTION) \
            .mode("overwrite") \
            .save()
        
        logging.info(f"Successfully wrote {df.count()} records to MongoDB collection {FINAL_COLLECTION}")
        
        # Clean up the parquet file after successful load
        logging.info("Cleaning up parquet file...")
        try:
            os.remove(parquet_path)
            logging.info("Successfully removed parquet file")
        except Exception as e_cleanup:
            logging.warning(f"Failed to clean up parquet file: {str(e_cleanup)}")
            
    except Exception as e:
        logging.error(f"An error occurred during the load process: {str(e)}", exc_info=True)
        raise
    finally:
        if spark: 
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    load()
