"""
Load script for IDX Laporan Keuangan ETL process.

This script handles the loading of transformed financial data
from JSON files to MongoDB using upsert strategy.
"""

import logging
import os
import json
import glob
from datetime import datetime
from pymongo import MongoClient
from pymongo.operations import UpdateOne

def get_current_quarter():
    """
    Determine the current quarter based on the current month
    
    Returns:
        tuple: (year, quarter) tuple where year is a string and quarter is an integer
    """
    CURRENT_YEAR_STR = str(datetime.now().year)
    CURRENT_MONTH = datetime.now().month
    QUARTER = 0

    if CURRENT_MONTH <= 3:
        QUARTER = 4
        CURRENT_YEAR_STR = str(int(CURRENT_YEAR_STR) - 1)
    elif CURRENT_MONTH <= 6:
        QUARTER = 1
    elif CURRENT_MONTH <= 9:
        QUARTER = 2
    else:
        QUARTER = 3
    
    return (CURRENT_YEAR_STR, QUARTER)

def read_json_data(json_directory):
    """
    Read JSON data from PySpark output directory.
    PySpark saves JSON as a directory with part files.
    """
    logging.info(f"Reading JSON data from directory: {json_directory}")
    
    if not os.path.exists(json_directory):
        logging.error(f"JSON directory does not exist: {json_directory}")
        return []
    
    # Find all part-*.json files in the directory
    json_files = glob.glob(os.path.join(json_directory, "part-*.json"))
    
    if not json_files:
        logging.warning(f"No JSON part files found in directory: {json_directory}")
        return []
    
    all_records = []
    
    for json_file in json_files:
        logging.info(f"Reading JSON file: {json_file}")
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:  # Skip empty lines
                        record = json.loads(line)
                        all_records.append(record)
        except Exception as e:
            logging.error(f"Error reading JSON file {json_file}: {str(e)}")
            continue
    
    logging.info(f"Successfully read {len(all_records)} records from JSON files")
    return all_records

def create_upsert_operations(records):
    """
    Create MongoDB UpdateOne operations for upsert strategy.
    Filter is based on composite key: emiten, tahun, triwulan
    """
    logging.info("Creating upsert operations...")
    
    operations = []
    
    for record in records:
        # Ensure required fields exist
        if not all(key in record for key in ['emiten', 'tahun', 'triwulan']):
            logging.warning(f"Skipping record missing required fields: {record}")
            continue
          # Keep data types exactly as they come from transform (no type conversion)
        # This preserves the exact format from the transform output
        
        # Create filter for the composite unique key (emiten, tahun, triwulan)
        filter_doc = {
            "emiten": record['emiten'],
            "tahun": record['tahun'], 
            "triwulan": record['triwulan']
        }
        
        # Create ordered update document to match transform output order
        ordered_record = {}
        field_order = [
            'entity_name', 'emiten', 'tahun', 'triwulan', 'report_date', 
            'satuan', 'pembulatan', 'revenue', 'gross_profit', 'operating_profit', 
            'net_profit', 'cash', 'total_assets', 'short_term_borrowing', 
            'long_term_borrowing', 'total_equity', 'liabilities', 
            'cash_dari_operasi', 'cash_dari_investasi', 'cash_dari_pendanaan'
        ]
        
        # Add fields in the specified order
        for field in field_order:
            if field in record:
                ordered_record[field] = record[field]
        
        # Add any additional fields that weren't in the order list
        for field, value in record.items():
            if field not in ordered_record:
                ordered_record[field] = value
        
        # Create update document
        update_doc = {"$set": ordered_record}
        
        # Create UpdateOne operation with upsert=True
        operation = UpdateOne(
            filter=filter_doc,
            update=update_doc,
            upsert=True
        )
        
        operations.append(operation)
    
    logging.info(f"Created {len(operations)} upsert operations")
    return operations

def load():
    """
    Load task - Reads transformed data from JSON and performs MongoDB upserts.
    """
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting load process...")
    
    try:
        # Get current quarter for file naming
        CURRENT_YEAR_STR, QUARTER = get_current_quarter()
        json_directory = f"/data/transformed_data_{CURRENT_YEAR_STR}TW{QUARTER}"
        
        # Read JSON data
        records = read_json_data(json_directory)
        
        if not records:
            logging.warning("No records found in JSON files. Skipping load.")
            return
        
        # Create MongoDB connection
        logging.info("Connecting to MongoDB...")
        client = MongoClient("mongodb://host.docker.internal:27017/")
        
        # Database and collection setup
        db_name = "idx_lapkeu_final"
        collection_name = f"idx_lapkeu_{CURRENT_YEAR_STR}TW{QUARTER}_final"
        
        db = client[db_name]
        collection = db[collection_name]
        
        logging.info(f"Using database: {db_name}, collection: {collection_name}")
        
        # Create upsert operations
        operations = create_upsert_operations(records)
        
        if not operations:
            logging.warning("No valid operations to execute. Skipping load.")
            return        # Execute bulk write with upsert operations
        logging.info(f"Executing bulk write with {len(operations)} operations...")
        result = collection.bulk_write(operations)
        
        # Log results
        logging.info(f"Bulk write completed successfully:")
        logging.info(f"  - Matched documents: {result.matched_count}")
        logging.info(f"  - Modified documents: {result.modified_count}")
        logging.info(f"  - Upserted documents: {result.upserted_count}")
        logging.info(f"  - Total operations: {len(operations)}")
        
        # Try to enforce field ordering by recreating documents with ordered structure
        logging.info("Attempting to enforce field ordering in stored documents...")
        try:
            # Define the exact field order we want
            field_order = [
                'entity_name', 'emiten', 'tahun', 'triwulan', 'report_date',
                'satuan', 'pembulatan', 'revenue', 'gross_profit', 'operating_profit',
                'net_profit', 'cash', 'total_assets', 'short_term_borrowing',
                'long_term_borrowing', 'total_equity', 'liabilities',
                'cash_dari_operasi', 'cash_dari_investasi', 'cash_dari_pendanaan'
            ]
            
            # Get all documents from the collection
            documents = list(collection.find())
            
            if documents:
                # Create new operations to replace each document with ordered structure
                reorder_operations = []
                
                for doc in documents:
                    # Create ordered document
                    ordered_doc = {}
                    
                    # Add fields in the specified order
                    for field in field_order:
                        if field in doc:
                            ordered_doc[field] = doc[field]
                    
                    # Add any additional fields that weren't in the order list
                    for field, value in doc.items():
                        if field not in ordered_doc and field != '_id':
                            ordered_doc[field] = value
                    
                    # Create replace operation
                    from pymongo.operations import ReplaceOne
                    reorder_operation = ReplaceOne(
                        filter={"_id": doc["_id"]},
                        replacement=ordered_doc
                    )
                    reorder_operations.append(reorder_operation)
                
                # Execute bulk replace to reorder fields
                if reorder_operations:
                    logging.info(f"Executing field reordering for {len(reorder_operations)} documents...")
                    reorder_result = collection.bulk_write(reorder_operations)
                    logging.info(f"Field reordering completed: {reorder_result.modified_count} documents updated")
                
        except Exception as e_reorder:
            logging.warning(f"Could not enforce field ordering: {str(e_reorder)}")
            # This is not critical, so we continue
        
        # Clean up JSON files after successful load
        logging.info("Cleaning up JSON files...")
        try:
            if os.path.exists(json_directory):
                import shutil
                shutil.rmtree(json_directory)
                logging.info(f"Successfully removed JSON directory: {json_directory}")
        except Exception as e_cleanup:
            logging.warning(f"Failed to clean up JSON directory: {str(e_cleanup)}")
            
    except Exception as e:
        logging.error(f"An error occurred during the load process: {str(e)}", exc_info=True)
        raise
    finally:
        try:
            if 'client' in locals():
                client.close()
                logging.info("MongoDB connection closed.")
        except:
            pass

if __name__ == "__main__":
    load()
