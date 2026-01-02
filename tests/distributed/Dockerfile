FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install chronis with redis support
RUN pip install --no-cache-dir -e ".[redis]"

# Default command
CMD ["python", "-u", "tests/distributed/worker.py"]
