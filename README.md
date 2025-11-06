# Chronis - Distributed Scheduler Framework

**Chronis** (크로노스) - Python distributed scheduler framework for multi-container environments

## Features

- ✅ **Adapter**: Support for DynamoDB, PostgreSQL, SQLite, InMemory storage
- ✅ **Distributed Locks**: Redis and InMemory lock adapters
- ✅ **Polling-based**: Non-blocking scheduling using APScheduler
- ✅ **High Availability**: Prevents duplicate execution in multi-pod environments
- ✅ **Timezone Support**: IANA timezone-aware scheduling with automatic DST handling
- ✅ **Retry Logic**: Automatic retry with exponential backoff

## Installation

```bash
# Basic installation
pip install chronis

# With optional dependencies
pip install chronis[aws]      # DynamoDB support
pip install chronis[postgres]  # PostgreSQL support
pip install chronis[redis]     # Redis lock support
pip install chronis[all]       # All adapters
```

## Quick Start

```python
from chronis import (
    PollingScheduler,
    JobDefinition,
    TriggerType,
    InMemoryStorageAdapter,
    InMemoryLockAdapter,
)

# 1. Create adapters
storage = InMemoryStorageAdapter()
lock = InMemoryLockAdapter()

# 2. Create scheduler
scheduler = PollingScheduler(
    storage_adapter=storage,
    lock_adapter=lock,
    polling_interval_seconds=10,
)

# 3. Define job function
def send_daily_email():
    print("Sending daily email...")

# 4. Register function
scheduler.register_job_function("send_daily_email", send_daily_email)

# 5. Create job
job = JobDefinition(
    job_id="email-001",
    name="Daily Email",
    trigger_type=TriggerType.CRON,
    trigger_args={"hour": 9, "minute": 0},
    func="send_daily_email",
    timezone="Asia/Seoul",  # Run at 9 AM Seoul time
)
scheduler.create_job(job)

# 6. Start scheduler
scheduler.start()
```

## Core Concepts

### Trigger Types

- **INTERVAL**: Execute periodically (e.g., every 5 minutes)
- **CRON**: Execute based on cron expression
- **DATE**: One-time execution at specific time

### Timezone Support

```python
# Korean time
job_kr = JobDefinition(
    job_id="korea-report",
    trigger_type=TriggerType.CRON,
    trigger_args={"hour": 9, "minute": 0},
    func="generate_report",
    timezone="Asia/Seoul"  # Runs at 9 AM KST
)

# US Eastern time (DST auto-handled)
job_us = JobDefinition(
    job_id="us-report",
    trigger_type=TriggerType.CRON,
    trigger_args={"hour": 8, "minute": 0},
    func="generate_report",
    timezone="America/New_York"  # Runs at 8 AM EST/EDT
)
```

### Retry Logic

```python
job = JobDefinition(
    job_id="api-sync",
    trigger_type=TriggerType.INTERVAL,
    trigger_args={"minutes": 30},
    func="sync_api_data",
    max_retries=5,  # Retry up to 5 times
    retry_delay_seconds=60,  # Wait 60 seconds
    retry_exponential_backoff=True  # Use exponential backoff
)
```

## Production Usage

### DynamoDB + Redis

```python
from chronis.adapters.storage import DynamoDBAdapter
from chronis.adapters.locks import RedisLockAdapter

storage = DynamoDBAdapter(
    table_name="scheduled_jobs",
    region="us-east-1"
)
lock = RedisLockAdapter(host="redis.example.com")

scheduler = PollingScheduler(
    storage_adapter=storage,
    lock_adapter=lock,
    polling_interval_seconds=10,
)
```

### PostgreSQL + Redis

```python
from chronis.adapters.storage import PostgreSQLAdapter
from chronis.adapters.locks import RedisLockAdapter

storage = PostgreSQLAdapter(
    host="localhost",
    database="scheduler",
    user="postgres",
    password="secret"
)
lock = RedisLockAdapter(host="localhost")

scheduler = PollingScheduler(
    storage_adapter=storage,
    lock_adapter=lock
)
```

## Examples

See the [examples/](examples/) directory for more examples:

- `quickstart.py` - Basic usage example

Run examples:

```bash
uv run python examples/quickstart.py
```

## Testing

```bash
# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=chronis --cov-report=html

# Run specific test file
uv run pytest tests/test_basic.py -v
```

## Development

```bash
# Clone repository
git clone https://github.com/yourusername/chronis.git
cd chronis

# Install dependencies
uv sync

# Run linter
uv run ruff check chronis/

# Format code
uv run ruff format chronis/

# Run tests
uv run pytest
```
