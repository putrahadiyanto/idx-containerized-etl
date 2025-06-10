FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application files
COPY load.py .

# Create data directory for input
RUN mkdir -p /data

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the load script
CMD ["python", "load.py"]
