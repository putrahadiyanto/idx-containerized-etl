# YFinance Load Service Dockerfile
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy load script
COPY load_json_to_mongo.py .

# Create data directory for JSON input
RUN mkdir -p /data

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the load script
CMD ["python", "load_json_to_mongo.py"]
