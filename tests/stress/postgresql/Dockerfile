FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only necessary files for installation
COPY pyproject.toml README.md ./
COPY chronis/ ./chronis/

# Install chronis with postgres and redis support
RUN pip install --no-cache-dir -e ".[postgres,redis]"

# Copy test files
COPY tests/stress/postgresql/ ./tests/stress/postgresql/

# Default command
CMD ["python", "-u", "tests/stress/postgresql/worker.py"]
