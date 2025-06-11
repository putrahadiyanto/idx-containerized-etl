# Dockerized YFinance Data Extractor

This application extracts stock data from Yahoo Finance and stores it in MongoDB running on your host machine.

## Prerequisites

1. **MongoDB** must be running on your Windows 11 machine on port 27017
2. **Docker Desktop** must be installed and running
3. **emiten.csv** file with stock ticker symbols (one per line, with header)

## Files Structure

```
extract/
├── Dockerfile              # Docker configuration
├── docker-compose.yml      # Docker Compose configuration
├── requirements.txt        # Python dependencies
├── yfinance_extractor.py   # Main extraction script
├── emiten.csv              # Stock ticker symbols
└── README.md               # This file
```

## Quick Start

1. **Ensure MongoDB is running** on your Windows machine:
   ```bash
   # Check if MongoDB is running
   mongo --eval "db.adminCommand('ping')"
   ```

2. **Build and run the container**:
   ```bash
   # Using Docker Compose (recommended)
   docker-compose up --build

   # Or using Docker directly
   docker build -t yfinance-extractor .
   docker run --rm yfinance-extractor
   ```

3. **View logs** in the container output

## Configuration

### Environment Variables

- `MONGO_HOST`: MongoDB host (default: host.docker.internal)
- `MONGO_PORT`: MongoDB port (default: 27017)

### MongoDB Connection

The container connects to MongoDB on your host machine using `host.docker.internal`, which is Docker's way to access the host machine from within a container on Windows.

## Data Storage

- Database: `yfinance_data_{year}` (e.g., yfinance_data_2025)
- Collection: `stock_data_{YYYYMMDD}` (e.g., stock_data_20250611)
- Indexes: Created on `ticker` and `Date` fields for better performance

## Stock Data Format

Each document contains:
```json
{
  "_id": "unique_id",
  "Date": "2025-06-11T00:00:00",
  "Open": 1000.0,
  "High": 1050.0,
  "Low": 990.0,
  "Close": 1020.0,
  "Volume": 1000000,
  "Dividends": 0.0,
  "Stock Splits": 0.0,
  "ticker": "BBCA",
  "fetch_timestamp": "2025-06-11T10:30:00"
}
```

## Troubleshooting

### MongoDB Connection Issues

1. **Ensure MongoDB is running** on your host machine
2. **Check firewall settings** - MongoDB port 27017 should be accessible
3. **Verify Docker Desktop** is running and has access to host network

### No Data Retrieved

1. **Check emiten.csv format** - should have ticker symbols, one per line
2. **Verify ticker symbols** are valid Indonesian stock symbols
3. **Check Yahoo Finance availability** for the specified date range

## Manual Execution

You can also run the script manually without Docker:

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (optional)
set MONGO_HOST=localhost
set MONGO_PORT=27017

# Run the script
python yfinance_extractor.py
```
