# IDX Containerized ETL Pipeline

This directory contains the complete containerized ETL pipeline for IDX Laporan Keuangan (Financial Reports) processing, split into three independent services that communicate via shared volumes.

## Architecture Overview

```
┌─────────────┐    ┌───────────────┐    ┌─────────────┐
│   Extract   │───▶│   Transform   │───▶│    Load     │
│   Service   │    │    Service    │    │   Service   │
└─────────────┘    └───────────────┘    └─────────────┘
       │                    │                    │
       ▼                    ▼                    ▼
/data/data.parquet   /data/data.parquet   MongoDB
```

## Services

### 1. Extract Service (`extract/`)
- **Technology**: Python + Selenium + Chrome
- **Input**: IDX website scraping
- **Output**: `/data/data.parquet`
- **Function**: Scrapes financial data from IDX website and exports to parquet

### 2. Transform Service (`transform/`)
- **Technology**: PySpark
- **Input**: `/data/data.parquet`
- **Output**: `/data/data.parquet` (transformed)
- **Function**: Applies sector-specific business logic and transformations

### 3. Load Service (`load/`)
- **Technology**: PySpark + MongoDB
- **Input**: `/data/data.parquet`
- **Output**: MongoDB collection
- **Function**: Loads transformed data into MongoDB and cleans up files

## Running the Complete Pipeline

### Option 1: Run All Services in Sequence
```bash
# From the etl/ directory
docker-compose up --build

# Run in background
docker-compose up -d --build

# Check logs
docker-compose logs -f
```

### Option 2: Run Individual Services
```bash
# Extract only
cd extract && docker-compose up --build

# Transform only (requires data from extract)
cd transform && docker-compose up --build

# Load only (requires data from transform)
cd load && docker-compose up --build
```

### Option 3: Run for Airflow Integration
Each service can be run individually as Airflow tasks:
```python
# In your Airflow DAG
extract_task = DockerOperator(
    task_id='extract_data',
    image='extract-extract-service:latest',
    volumes=['etl-data-volume:/data']
)

transform_task = DockerOperator(
    task_id='transform_data',
    image='transform-transform-service:latest',
    volumes=['etl-data-volume:/data']
)

load_task = DockerOperator(
    task_id='load_data',
    image='load-load-service:latest',
    volumes=['etl-data-volume:/data']
)
```

## Prerequisites

- **Docker and Docker Compose** installed
- **MongoDB** running on host machine at port 27017
- **Internet connection** for IDX website access
- **Sufficient memory** for Spark operations (recommended: 4GB+ RAM)

## Data Flow

1. **Extract**: Raw financial data → `/data/data.parquet`
2. **Transform**: Raw parquet → Processed parquet (overwrite)
3. **Load**: Processed parquet → MongoDB + cleanup

## Configuration

### MongoDB Connection
All services connect to MongoDB at `host.docker.internal:27017`:
- Extract: Temporary storage during scraping
- Load: Final data destination

### Shared Volume
- **Volume Name**: `etl-data-volume`
- **Mount Point**: `/data`
- **Purpose**: Inter-service communication via parquet files

### Environment Variables
Each service supports environment variables for configuration:
- `PYTHONUNBUFFERED=1`: Immediate logging output
- Spark services: `PYSPARK_PYTHON=python3`
- Extract service: `CHROMEDRIVER_EXECUTABLE_PATH=/usr/local/bin/chromedriver`

## Monitoring and Debugging

### Check Service Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs extract-service
docker-compose logs transform-service
docker-compose logs load-service
```

### Inspect Shared Volume
```bash
# List files in shared volume
docker run --rm -v etl-data-volume:/data alpine ls -la /data

# Check parquet file
docker run --rm -v etl-data-volume:/data alpine cat /data/data.parquet
```

### Debug Individual Services
```bash
# Interactive shell in extract service
docker-compose run --rm extract-service bash

# Interactive shell in transform service  
docker-compose run --rm transform-service bash
```

## Directory Structure

```
etl/
├── docker-compose.yaml          # Master orchestration
├── README.md                    # This file
├── extract/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── extract.py
│   ├── app_scraper.py
│   ├── emiten.csv
│   ├── requirements.txt
│   ├── test_extract.py
│   └── README.md
├── transform/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── transform.py
│   ├── requirements.txt
│   └── README.md
└── load/
    ├── Dockerfile
    ├── docker-compose.yaml
    ├── load.py
    ├── requirements.txt
    └── README.md
```

## Cleanup

```bash
# Stop all services and remove containers
docker-compose down

# Stop and remove containers + volumes
docker-compose down -v

# Remove all ETL images
docker rmi $(docker images "*extract*" "*transform*" "*load*" -q)
```

This containerized architecture provides flexibility for both standalone execution and Airflow orchestration while maintaining clear separation of concerns between ETL stages.
