FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
# (Ensure you have a requirements.txt file)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the API code
COPY . .

# Command to run the API (adjust 'main:app' to your file name)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]