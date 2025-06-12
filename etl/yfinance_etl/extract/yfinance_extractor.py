#!/usr/bin/env python3
"""
Standalone YFinance Data Extractor
Extracts stock data from Yahoo Finance and stores it in MongoDB
"""

import yfinance as yf
from datetime import datetime
import uuid
import time
import os
import logging
import sys
import pandas as pd
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# MongoDB config variables
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))

def get_mongo_client():
    """
    Create and return a MongoDB client
    """
    try:
        client = MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
        # Test connection
        client.admin.command('ping')
        logging.info(f"Successfully connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {str(e)}")
        sys.exit(1)

def load_tickers(csv_file='emiten.csv'):
    """
    Load ticker symbols from CSV file
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, csv_file)
        logging.info(f"Reading tickers from: {csv_path}")
        
        if not os.path.exists(csv_path):
            logging.error(f"File {csv_path} not found")
            return []
        
        ticker_list = []
        with open(csv_path, 'r') as file:
            # Skip header line
            next(file)
            for line in file:
                ticker = line.strip()
                if ticker:
                    ticker_list.append(ticker)
        
        logging.info(f"Successfully loaded {len(ticker_list)} tickers from {csv_file}")
        return ticker_list
    except Exception as e:
        logging.error(f"Error reading file {csv_file}: {str(e)}")
        return []

def extract_and_save_data():
    """
    Main function to extract stock data and save to MongoDB
    """
    # Load tickers
    ticker_list = load_tickers()
    if not ticker_list:
        logging.warning("No tickers found. Make sure emiten.csv is available and properly formatted.")
        return 0
    logging.info(f"Processing {len(ticker_list)} tickers: {ticker_list}")
    # Create MongoDB client and get database/collection
    client = get_mongo_client()
    
    # Use single database and collection
    db_name = "yfinance_raw_data"
    collection_name = "stock_data"
    
    db = client[db_name]
    collection = db[collection_name]
    
    total_documents = 0
    
    for ticker_symbol in ticker_list:
        try:
            ticker_yf = f"{ticker_symbol}.JK"
            logging.info(f"Downloading latest data for {ticker_symbol}...")
            
            # Get data for the last 30 days to ensure we get recent data
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now().replace(day=1)).strftime("%Y-%m-%d")
            
            stock_data = yf.download(
                ticker_yf,
                start=start_date,
                end=end_date,
                progress=False
            )
            
            if stock_data.empty:
                logging.warning(f"No data available for {ticker_symbol}")
                continue
            
            logging.info(f"Downloaded {len(stock_data)} rows of data for {ticker_symbol}")            # Process and insert each row directly into MongoDB
            documents_for_ticker = []
            for index, row in stock_data.iterrows():
                try:                    # Helper function to safely convert values
                    def safe_float(value, default=0.0):
                        try:
                            if value is None or str(value).lower() in ['nan', 'none', '']:
                                return default
                            # Handle pandas Series by getting the first element
                            if hasattr(value, 'iloc'):
                                return float(value.iloc[0])
                            return float(value)
                        except (ValueError, TypeError, IndexError):
                            return default
                    
                    def safe_int(value, default=0):
                        try:
                            if value is None or str(value).lower() in ['nan', 'none', '']:
                                return default
                            # Handle pandas Series by getting the first element
                            if hasattr(value, 'iloc'):
                                return int(float(value.iloc[0]))
                            return int(float(value))
                        except (ValueError, TypeError, IndexError):
                            return default
                    
                    document = {
                        "_id": str(uuid.uuid4()).replace("-", "")[:24],
                        "Date": index.isoformat(),
                        "Open": safe_float(row['Open']),
                        "High": safe_float(row['High']),
                        "Low": safe_float(row['Low']),
                        "Close": safe_float(row['Close']),
                        "Volume": safe_int(row['Volume']),
                        "Dividends": safe_float(row.get('Dividends', 0)),
                        "Stock Splits": safe_float(row.get('Stock Splits', 0)),
                        "ticker": ticker_symbol,
                        "fetch_timestamp": datetime.now().isoformat()
                    }
                    documents_for_ticker.append(document)
                except Exception as e:
                    logging.warning(f"Error processing row for {ticker_symbol}: {str(e)}")
                    continue
            
            # Insert batch for better performance
            if documents_for_ticker:
                collection.insert_many(documents_for_ticker)
                total_documents += len(documents_for_ticker)
                
            logging.info(f"Successfully processed {ticker_symbol}: {len(documents_for_ticker)} documents")
            time.sleep(2)  # Add delay to avoid rate limiting
            
        except Exception as e:
            logging.error(f"Error downloading {ticker_symbol}: {str(e)}")
    
    logging.info(f"Total {total_documents} documents successfully saved to MongoDB database: {db_name}, collection: {collection_name}")
    
    # Create indexes for better query performance
    if total_documents > 0:
        collection.create_index("ticker")
        collection.create_index("Date")
        logging.info("Created indexes on ticker and Date fields")
        
    client.close()
    return total_documents

if __name__ == "__main__":
    logging.info("Starting YFinance data extraction...")
    start_time = datetime.now()
    
    try:
        total_docs = extract_and_save_data()
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info(f"Extraction completed successfully!")
        logging.info(f"Total documents processed: {total_docs}")
        logging.info(f"Total time taken: {duration}")
        
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        sys.exit(1)
