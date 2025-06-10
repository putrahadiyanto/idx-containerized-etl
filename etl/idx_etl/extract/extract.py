"""
Extract module for IDX Laporan Keuangan ETL process.

This module handles the extraction of financial data from IDX website
using a Selenium-based scraper and stores it in MongoDB.
"""

import logging
import os
from scraper import run_scraper
from utils import get_current_quarter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_financial_data():
    """
    Extract financial data from IDX website and store it in MongoDB.
    
    Uses Selenium-based scraping to fetch financial data from IDX website
    and stores it in the appropriate MongoDB collection.
    """
    logging.info("Starting financial data extraction using Selenium scraper...")
    
    # Define paths and configurations for the scraper
    csv_file = "/app/emiten.csv"
    download_folder = "/tmp/idx_scraper_downloads"
    json_folder = None  # Set to a path to save JSONs, else None
    
    # Get current year and quarter
    CURRENT_YEAR_STR, QUARTER = get_current_quarter()
    
    # MongoDB configuration - use environment variables with defaults
    mongo_db_uri = os.getenv("MONGO_URI", "mongodb://host.docker.internal:27017/")
    mongo_database_name = os.getenv("MONGO_DATABASE", "idx_lapkeu")
    mongo_target_collection_name = os.getenv(
        "MONGO_COLLECTION", 
        f"idx_lapkeu{CURRENT_YEAR_STR}TW{QUARTER}"
    )
    
    # Selenium configuration
    selenium_hub = os.getenv("SELENIUM_HUB_URL", None)

    try:
        run_scraper(
            csv_file_path=csv_file,
            download_dir=download_folder,
            json_output_dir=json_folder,
            mongo_uri=mongo_db_uri,
            mongo_db_name=mongo_database_name,
            mongo_collection_name=mongo_target_collection_name,
            selenium_hub_url=selenium_hub
        )
        logging.info("Financial data extraction completed successfully.")
    except Exception as e:
        logging.error(f"Error during financial data extraction: {str(e)}", exc_info=True)
        raise


def main():
    """
    Main extract task entry point.
    """
    extract_financial_data()


if __name__ == "__main__":
    main()
