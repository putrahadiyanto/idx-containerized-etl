# IDX Extract Service

This service extracts financial data from the IDX (Indonesia Stock Exchange) website using Selenium web scraping and stores the data in MongoDB.

## Features

- Web scraping of financial reports from IDX website
- Chrome browser automation with Selenium
- XBRL to JSON conversion
- MongoDB data storage
- Docker containerization

## Files

- `Dockerfile` - Container configuration with Chrome and ChromeDriver
- `docker-compose.yaml` - Multi-service configuration with MongoDB
- `extract.py` - Main extraction script
- `scraper.py` - Core scraping functionality
- `utils.py` - Utility functions for date/quarter calculations
- `emiten.csv` - List of stock tickers to process
- `requirements.txt` - Python dependencies

## Environment Variables

- `MONGO_URI` - MongoDB connection string (default: mongodb://host.docker.internal:27017/)
- `MONGO_DATABASE` - Database name (default: idx_lapkeu)
- `MONGO_COLLECTION` - Collection name (auto-generated based on year/quarter)
- `SELENIUM_HUB_URL` - Optional Selenium Grid URL
- `CHROMEDRIVER_EXECUTABLE_PATH` - ChromeDriver path (default: /usr/local/bin/chromedriver)

## Prerequisites

- Docker and Docker Compose installed
- MongoDB running on the host machine (port 27017)
- Chrome browser dependencies

## Usage

### Build and Run with Docker Compose

```bash
# Build and start the service
docker-compose up --build

# Run in detached mode
docker-compose up -d --build

# View logs
docker-compose logs -f idx-extract

# Stop service
docker-compose down
```

### Build and Run with Docker

```bash
# Build the image
docker build -t idx-extract .

# Run the container (connects to host MongoDB)
docker run --rm \
  -e MONGO_URI=mongodb://host.docker.internal:27017/ \
  -e MONGO_DATABASE=idx_lapkeu \
  --name idx-extract \
  idx-extract
```

## Data Flow

1. Reads stock tickers from `emiten.csv`
2. For each ticker, downloads financial report ZIP files from IDX
3. Extracts XBRL files from ZIP archives
4. Converts XBRL data to JSON format
5. Stores processed data in MongoDB with year/quarter-based collection names

## Collection Naming

Collections are automatically named based on the current quarter:
- Format: `idx_lapkeu{YEAR}TW{QUARTER}`
- Example: `idx_lapkeu2025TW2` for Q2 2025

## Quarter Logic

- Q1 (Jan-Mar): Reports from Q4 of previous year
- Q2 (Apr-Jun): Reports from Q1 of current year  
- Q3 (Jul-Sep): Reports from Q2 of current year
- Q4 (Oct-Dec): Reports from Q3 of current year

## Error Handling

- Failed downloads are logged and skipped
- ZIP extraction errors are handled gracefully
- MongoDB connection issues will cause the process to fail
- Screenshots are saved for debugging failed downloads

## Monitoring

Check the logs for:
- Successful ticker processing
- Download failures
- MongoDB upload status
- WebDriver initialization issues
