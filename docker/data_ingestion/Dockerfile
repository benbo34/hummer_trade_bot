# Dockerfile for Data Ingestion Container
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the data ingestion script
COPY src/data_ingestion/ingest_data.py .

# Run the data ingestion script
CMD ["python", "ingest_data.py"]