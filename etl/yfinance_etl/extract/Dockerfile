# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    MONGO_HOST=host.docker.internal \
    MONGO_PORT=27017

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY yfinance_extractor.py .
COPY emiten.csv .

# Command to run the application
CMD ["python", "yfinance_extractor.py"]
