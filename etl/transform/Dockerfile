# Transform Service Dockerfile
# Using Ubuntu base image which has Java 11 available
FROM ubuntu:20.04

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies including Python 3.9 and Java 11
RUN apt-get update && apt-get install -y \
    python3.9 \
    python3.9-dev \
    python3.9-distutils \
    python3-pip \
    wget \
    curl \
    openjdk-11-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Python as default
RUN ln -sf /usr/bin/python3.9 /usr/bin/python && \
    ln -sf /usr/bin/python3.9 /usr/bin/python3

# Set working directory
WORKDIR /app

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Download and install Spark
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN mkdir -p /opt/spark && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/spark --strip-components=1 && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Download MongoDB Spark connector JARs
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar \
      https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar && \
    wget -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.12.10/mongodb-driver-3.12.10.jar && \
    wget -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/mongodb/bson/3.12.10/bson-3.12.10.jar && \
    wget -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/3.12.10/mongodb-driver-core-3.12.10.jar

# Set Spark environment variables
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV SPARK_CLASSPATH="/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar"
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar pyspark-shell"
ENV PYTHONUNBUFFERED=1
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_DRIVER_HOST=127.0.0.1
ENV SPARK_DRIVER_BIND_ADDRESS=127.0.0.1
ENV SPARK_NO_DAEMONIZE=true
ENV _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy transform script and diagnostic script
COPY transform.py .
COPY check_data.py .

# Create data directory for JSON output
RUN mkdir -p /data

# Run the transform script
CMD ["python", "transform.py"]
