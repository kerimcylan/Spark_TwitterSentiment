# Python base image
FROM python:3.9-slim

# working directory
WORKDIR /app

# dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# PostgreSQL JDBC driver
ENV POSTGRES_JDBC_VERSION 42.2.18
RUN apt-get update && apt-get install -y curl && \
    curl -o /app/postgresql-$POSTGRES_JDBC_VERSION.jar \
    https://jdbc.postgresql.org/download/postgresql-$POSTGRES_JDBC_VERSION.jar && \
    rm -rf /var/lib/apt/lists/*

COPY . .


# Spark script
CMD ["python", "/app/SparkSentiment.py"]
