FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required for psycopg2 logical replication
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Ensure we use the binary version to avoid compilation headaches
RUN pip install --no-cache-dir psycopg2-binary meilisearch requests

COPY . .

# Force unbuffered output so we see errors immediately
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]an