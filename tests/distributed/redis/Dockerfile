FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy only necessary files for installation
COPY pyproject.toml README.md ./
COPY chronis/ ./chronis/

# Install chronis with redis support
RUN pip install --no-cache-dir -e ".[redis]"

# Copy test files
COPY tests/distributed/redis/ ./tests/distributed/redis/

# Default command
CMD ["python", "-u", "tests/distributed/redis/worker.py"]
