#!/usr/bin/env python3
"""
YFinance Data Loader
Loads transformed JSON data from transform step into MongoDB
"""

import json
import os
import logging
import sys
import glob
import shutil
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

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
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_USERNAME = os.getenv("MONGO_USERNAME", "root")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_AUTH_DB = os.getenv("MONGO_AUTH_DB", "admin")
DB_NAME = os.getenv("DB_NAME", "idx_etl")

def get_mongo_client():
    """
    Create and return a MongoDB client with authentication
    """
    try:
        # Construct MongoDB URI with authentication
        mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_AUTH_DB}"
        
        client = MongoClient(mongo_uri)
        # Test connection
        client.admin.command('ping')
        logging.info(f"Successfully connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {str(e)}")
        sys.exit(1)

def find_json_files(data_dir="/data"):
    """
    Find all JSON files in the data directory
    """
    json_files = {}
    
    # Look for specific aggregation directories
    aggregation_types = ["daily_aggregation", "monthly_aggregation", "yearly_aggregation"]
    
    for agg_type in aggregation_types:
        agg_dir = os.path.join(data_dir, agg_type)
        if os.path.exists(agg_dir):
            # Find JSON files in the directory (Spark creates part-*.json files)
            json_pattern = os.path.join(agg_dir, "part-*.json")
            found_files = glob.glob(json_pattern)
            if found_files:
                json_files[agg_type] = found_files
                logging.info(f"Found {len(found_files)} JSON files for {agg_type}")
            else:
                logging.warning(f"No JSON files found in {agg_dir}")
        else:
            logging.warning(f"Directory {agg_dir} does not exist")
    
    return json_files

def load_json_data(json_files):
    """
    Load and parse JSON data from files
    """
    all_data = {}
    
    for agg_type, file_list in json_files.items():
        data_list = []
        
        for json_file in file_list:
            try:
                logging.info(f"Reading {json_file}...")
                with open(json_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:  # Skip empty lines
                            try:
                                data = json.loads(line)
                                # Add metadata
                                data['aggregation_type'] = agg_type
                                data['load_timestamp'] = datetime.now().isoformat()
                                data_list.append(data)
                            except json.JSONDecodeError as e:
                                logging.warning(f"Failed to parse JSON line in {json_file}: {e}")
                                continue
                
                logging.info(f"Successfully loaded {len(data_list)} records from {json_file}")
                
            except Exception as e:
                logging.error(f"Error reading file {json_file}: {str(e)}")
                continue
        
        if data_list:
            all_data[agg_type] = data_list
            logging.info(f"Total {len(data_list)} records loaded for {agg_type}")
    
    return all_data

def load_data_to_mongodb():
    """
    Main function to load JSON data into MongoDB
    """
    logging.info("Starting YFinance data loading process...")
    
    # Find JSON files
    json_files = find_json_files()
    
    if not json_files:
        logging.error("No JSON files found. Make sure transform step completed successfully.")
        return 0
    
    # Load JSON data
    all_data = load_json_data(json_files)
    
    if not all_data:
        logging.error("No data could be loaded from JSON files.")
        return 0
    
    # Connect to MongoDB
    client = get_mongo_client()
    
    try:
        db = client[DB_NAME]
        total_inserted = 0
          # Process data for each aggregation type with different strategies
        for agg_type, data_list in all_data.items():
            if not data_list:
                continue
            if agg_type == "daily_aggregation":
                # Daily: Simple insert into single collection
                collection_name = "daily_aggregation"  # Single collection for all daily data
                collection = db[collection_name]
                
                try:
                    logging.info(f"Inserting {len(data_list)} daily records into collection '{collection_name}'...")
                    
                    # Insert in batches for better performance
                    batch_size = 1000
                    inserted_count = 0
                    
                    for i in range(0, len(data_list), batch_size):
                        batch = data_list[i:i + batch_size]
                        try:
                            result = collection.insert_many(batch, ordered=False)
                            inserted_count += len(result.inserted_ids)
                        except BulkWriteError as e:
                            # Handle duplicate key errors gracefully
                            inserted_count += len(e.details.get('writeErrors', []))
                            logging.warning(f"Bulk write error (some duplicates may have been skipped): {len(e.details.get('writeErrors', []))} errors")
                    
                    logging.info(f"Successfully inserted {inserted_count} daily records into '{collection_name}'")
                    total_inserted += inserted_count
                    
                except Exception as e:
                    logging.error(f"Failed to insert daily data into '{collection_name}': {str(e)}")
                    continue
                    
            elif agg_type in ["monthly_aggregation", "yearly_aggregation"]:
                # Monthly/Yearly: Use single collection and update based on time period
                collection_name = agg_type  # No date suffix for aggregated data
                collection = db[collection_name]
                
                try:
                    logging.info(f"Upserting {len(data_list)} {agg_type} records into collection '{collection_name}'...")
                    
                    upserted_count = 0
                    updated_count = 0
                    
                    for record in data_list:
                        try:
                            if agg_type == "monthly_aggregation":
                                # Update based on Year, Month, and ticker
                                filter_query = {
                                    "Year": record.get("Year"),
                                    "Month": record.get("Month"),
                                    "ticker": record.get("ticker")
                                }
                            else:  # yearly_aggregation
                                # Update based on Year and ticker
                                filter_query = {
                                    "Year": record.get("Year"),
                                    "ticker": record.get("ticker")
                                }
                            
                            # Perform upsert (update if exists, insert if not)
                            result = collection.replace_one(
                                filter_query,
                                record,
                                upsert=True
                            )
                            
                            if result.upserted_id:
                                upserted_count += 1
                            elif result.modified_count > 0:
                                updated_count += 1
                                
                        except Exception as e:
                            logging.warning(f"Failed to upsert record for {agg_type}: {str(e)}")
                            continue
                    
                    logging.info(f"Successfully processed {agg_type}: {upserted_count} new records, {updated_count} updated records")
                    total_inserted += upserted_count + updated_count
                    
                except Exception as e:
                    logging.error(f"Failed to process {agg_type} data: {str(e)}")
                    continue        # Create indexes for all collections
        try:
            # Daily aggregation indexes (single collection)
            if "daily_aggregation" in db.list_collection_names():
                daily_collection = db["daily_aggregation"]
                daily_collection.create_index([("Date", 1), ("ticker", 1)])
                daily_collection.create_index("ticker")
                logging.info("Created indexes for daily_aggregation collection")
            
            # Monthly aggregation indexes (single collection)
            if "monthly_aggregation" in db.list_collection_names():
                monthly_collection = db["monthly_aggregation"]
                monthly_collection.create_index([("Year", 1), ("Month", 1), ("ticker", 1)], unique=True)
                monthly_collection.create_index("ticker")
                logging.info("Created indexes for monthly_aggregation collection")
            
            # Yearly aggregation indexes (single collection)
            if "yearly_aggregation" in db.list_collection_names():
                yearly_collection = db["yearly_aggregation"]
                yearly_collection.create_index([("Year", 1), ("ticker", 1)], unique=True)
                yearly_collection.create_index("ticker")
                logging.info("Created indexes for yearly_aggregation collection")
                
        except Exception as e:
            logging.warning(f"Failed to create some indexes: {str(e)}")
        logging.info(f"Data loading completed! Total {total_inserted} documents inserted into database '{DB_NAME}'")
        
        # Cleanup: Delete processed JSON files after successful loading
        if total_inserted > 0:
            try:
                logging.info("Starting JSON file cleanup...")
                deleted_files_count = 0
                
                for agg_type, file_list in json_files.items():
                    for json_file in file_list:
                        try:
                            os.remove(json_file)
                            deleted_files_count += 1
                            logging.info(f"Deleted processed file: {json_file}")
                        except Exception as e:
                            logging.warning(f"Failed to delete {json_file}: {str(e)}")
                
                logging.info(f"Cleanup completed! Deleted {deleted_files_count} JSON files")
                
            except Exception as e:
                logging.warning(f"JSON cleanup failed: {str(e)}")
        else:
            logging.info("Skipping cleanup - no data was successfully loaded")
        
        return total_inserted
        
    except Exception as e:
        logging.error(f"Error during MongoDB operations: {str(e)}")
        return 0
    finally:
        client.close()

if __name__ == "__main__":
    logging.info("Starting YFinance JSON to MongoDB loader...")
    start_time = datetime.now()
    
    try:
        total_docs = load_data_to_mongodb()
        end_time = datetime.now()
        duration = end_time - start_time
        
        if total_docs > 0:
            logging.info(f"Loading completed successfully!")
            logging.info(f"Total documents processed: {total_docs}")
            logging.info(f"Total time taken: {duration}")
        else:
            logging.error("No documents were loaded.")
            sys.exit(1)
        
    except Exception as e:
        logging.error(f"Loading failed: {str(e)}")
        sys.exit(1)
