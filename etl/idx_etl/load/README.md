# ETL Load Service

This containerized service handles the loading phase of the IDX Laporan Keuangan ETL pipeline using PySpark and MongoDB.

## What it does

1. **Reads transformed data** from `/data/data.parquet` (output from Transform service)
2. **Loads data into MongoDB** using Spark MongoDB connector
3. **Cleans up** the parquet file after successful load

## Architecture

- **Single Container**: Self-contained with Spark and MongoDB connector
- **Airflow Compatible**: Designed to run as a single task in Airflow DAGs
- **Shared Volume**: `etl-data-volume` mounted at `/data` for inter-service communication

## Files

- `load.py` - Main loading script with PySpark and MongoDB integration
- `Dockerfile` - Container definition with Spark and MongoDB connector
- `requirements.txt` - Python dependencies
- `docker-compose.yaml` - Single service orchestration
- `README.md` - This documentation

## Running the service

```bash
# Build and run the load service
docker-compose up --build

# Run in detached mode
docker-compose up -d --build

# Check logs
docker-compose logs load-service

# Clean up
docker-compose down -v
```

## Prerequisites

- Docker and Docker Compose installed
- MongoDB running on host machine at port 27017
- `/data/data.parquet` file from Transform service
- Sufficient memory for Spark operations

## Output

The service loads data into MongoDB collection named `idx_lapkeu_{YEAR}TW{QUARTER}_transformed` in the `idx_lapkeu_transformed` database.

## Configuration

The service is configured to:
- Use Spark 3.3.2 with MongoDB connector
- Connect to MongoDB at `host.docker.internal:27017`
- Generate quarter-based collection names automatically
- Run once and exit (not a long-running service)
- Clean up parquet file after successful load

## Environment Variables

- `PYTHONUNBUFFERED=1` - Ensures immediate output logging
- `PYSPARK_PYTHON=python3` - Python executable for Spark workers
- `PYSPARK_DRIVER_PYTHON=python3` - Python executable for Spark driver

## MongoDB Collection Structure

Data is stored in collections with the following naming pattern:
- Database: `idx_lapkeu_transformed`
- Collection: `idx_lapkeu_{YEAR}TW{QUARTER}_transformed`
- Example: `idx_lapkeu_2025TW2_transformed` for Q2 2025
