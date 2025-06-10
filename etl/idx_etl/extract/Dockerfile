FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome and get matching ChromeDriver
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Get Chrome version and install matching ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d'.' -f1) && \
    echo "Chrome version: $CHROME_VERSION" && \
    # For Chrome 115+, use Chrome for Testing API
    if [ "$CHROME_VERSION" -ge "115" ]; then \
        CHROMEDRIVER_URL="https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"; \
        CHROMEDRIVER_VERSION=$(curl -s "$CHROMEDRIVER_URL" | python3 -c "import sys, json; data=json.load(sys.stdin); versions=[v for v in data['versions'] if v['version'].startswith('$CHROME_VERSION.')]; print(versions[-1]['version'] if versions else '$CHROME_VERSION.0.0.0')"); \
        wget -O chromedriver-linux64.zip "https://storage.googleapis.com/chrome-for-testing-public/$CHROMEDRIVER_VERSION/linux64/chromedriver-linux64.zip"; \
    else \
        # For older Chrome versions, use the old API
        CHROMEDRIVER_VERSION=$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROME_VERSION); \
        wget -O chromedriver-linux64.zip "https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"; \
    fi && \
    unzip chromedriver-linux64.zip && \
    if [ -d "chromedriver-linux64" ]; then \
        mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver; \
    else \
        mv chromedriver /usr/local/bin/chromedriver; \
    fi && \
    rm -rf chromedriver-linux64.zip chromedriver-linux64 && \
    chmod +x /usr/local/bin/chromedriver && \
    /usr/local/bin/chromedriver --version

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the necessary application files
COPY extract.py .
COPY scraper.py .
COPY utils.py .
COPY emiten.csv .

# Create necessary directories
RUN mkdir -p /tmp/idx_scraper_downloads

# Set environment variables
ENV CHROMEDRIVER_EXECUTABLE_PATH=/usr/local/bin/chromedriver
ENV PYTHONPATH=/app

# Run the extract script
CMD ["python", "extract.py"]
