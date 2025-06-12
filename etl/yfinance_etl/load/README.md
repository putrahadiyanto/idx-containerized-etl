# YFinance Load Service

This service loads transformed JSON data from the transform step into MongoDB.

## Overview

The load service reads JSON files produced by the transform step and inserts them into MongoDB with different strategies:

- **Database**: `yfinance_processed_data`
- **Collections**: 
  - `daily_aggregation` (single collection for all daily data)
  - `monthly_aggregation` (single collection, upsert by Year/Month/ticker)
  - `yearly_aggregation` (single collection, upsert by Year/ticker)

## Data Storage Strategy

- **Daily Aggregation**: Uses single collection with simple inserts for all daily data
- **Monthly Aggregation**: Uses single collection with upsert based on Year, Month, and ticker combination
- **Yearly Aggregation**: Uses single collection with upsert based on Year and ticker combination

## Features

- Reads JSON data from shared Docker volume
- Handles multiple aggregation types (daily, monthly, yearly)
- Batch processing for better performance
- Automatic indexing for optimal query performance
- Error handling and logging
- Connects to MongoDB on host machine

## Configuration

Environment variables:
- `MONGO_HOST`: MongoDB host (default: host.docker.internal)
- `MONGO_PORT`: MongoDB port (default: 27017)
- `DB_NAME`: Target database name (default: yfinance_processed_data)

## Usage

### Option 1: Run after Transform Step
```bash
# First run the transform step
cd ../transform
docker compose up

# Then run the load step
cd ../load
docker compose up
```

### Option 2: Build and Run Standalone
```bash
# Build the image
docker build -t yfinance-load .

# Run with shared volume
docker run --rm \
  -v yfinance-data:/data \
  -e MONGO_HOST=host.docker.internal \
  -e MONGO_PORT=27017 \
  -e DB_NAME=yfinance_processed_data \
  --add-host host.docker.internal:host-gateway \
  yfinance-load
```

## Data Structure

Each document in MongoDB will have:
- Original transformed data fields
- `aggregation_type`: Type of aggregation (daily/monthly/yearly)
- `load_timestamp`: When the data was loaded

## Indexes Created

- **Daily aggregation**: `(Date, ticker)` and `ticker` (on single collection)
- **Monthly aggregation**: `(Year, Month, ticker)` unique index and `ticker` (on single collection)
- **Yearly aggregation**: `(Year, ticker)` unique index and `ticker` (on single collection)

## Prerequisites

1. MongoDB running on host machine
2. Transform step completed successfully with JSON output
3. Shared Docker volume `yfinance-data` containing the JSON files

## Logging

The service provides detailed logging for:
- MongoDB connection status
- JSON file discovery and parsing
- Data insertion progress
- Error handling
- Performance metrics
