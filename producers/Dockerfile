# =============================================================================
# Dockerfile for Python Kafka Producers (stock & crypto)
# =============================================================================
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer source code
COPY producers/ .

# Default: run the entrypoint that dispatches to the correct producer
CMD ["python", "main.py"]
