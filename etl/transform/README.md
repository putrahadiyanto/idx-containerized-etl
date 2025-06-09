# Transform Service

This service handles the transformation of financial data using PySpark. It reads data from the shared `/data/data.parquet` file created by the Extract service and writes the transformed data back to parquet format for the Load service.

## Features

- **PySpark Processing**: Uses PySpark for distributed data processing
- **MongoDB Spark Connector**: Includes MongoDB Spark connector for compatibility
- **Multiple Financial Sectors**: Transforms data for banks, financing services, securities, insurance, and investment funds
- **Calculated Fields**: Adds computed metrics and standardized financial fields
- **Shared Volume**: Reads/writes data via shared `/data` volume

## Architecture

The Transform service is designed to work in a containerized ETL pipeline:

```
Extract Service → /data/data.parquet → Transform Service → /data/data.parquet → Load Service
```

## Dependencies

- **Java 11**: Required for PySpark
- **PySpark 3.3.2**: For distributed data processing
- **MongoDB Spark Connector**: For MongoDB compatibility
- **Python 3**: Runtime environment

## Usage

### Docker Compose (Recommended)

```bash
# Run transform service with extract dependency
docker-compose up

# Run only transform service (assuming data.parquet exists)
docker-compose up transform
```

### Docker Build and Run

```bash
# Build the image
docker build -t idx-transform-service .

# Run with shared volume
docker run -v $(pwd)/../../../data:/data idx-transform-service
```

## Configuration

The service expects:

- **Input**: `/data/data.parquet` file from Extract service
- **Output**: Transformed `/data/data.parquet` file for Load service
- **Spark Configuration**: Optimized for MongoDB Spark connector

## Transformations Applied

1. **Banks (G1. Banks)**
   - Interest income and Sharia income aggregation
   - Borrowings calculation (short-term and long-term)
   - Standard financial metrics

2. **Financing Services**
   - Revenue from financing activities
   - Fee and commission income aggregation
   - Operating profit calculations

3. **Securities Companies**
   - Trading income and commission revenue
   - Securities-specific financial metrics

4. **Insurance Companies**
   - Premium income calculations
   - Insurance-specific transformations

5. **Investment Funds**
   - Net asset value calculations
   - Fund-specific metrics

## Environment Variables

- `JAVA_HOME`: Java installation path
- `SPARK_HOME`: Spark installation path
- `PYTHONPATH`: Python path including Spark libraries
- `SPARK_CLASSPATH`: MongoDB Spark connector classpath
- `PYSPARK_SUBMIT_ARGS`: Spark submission arguments

## Logging

The service provides detailed logging for:
- Transformation progress
- Record counts processed
- Error handling and debugging
- Performance metrics

## Error Handling

- Input validation for empty datasets
- Graceful error handling with detailed logging
- Proper Spark session cleanup
- Exception propagation for pipeline awareness

## Data Flow

1. Reads financial data from `/data/data.parquet`
2. Applies sector-specific transformations
3. Calculates derived financial metrics
4. Writes transformed data back to `/data/data.parquet`
5. Logs transformation results and statistics

This service is designed to be stateless and can be orchestrated by Airflow or other workflow management systems.
