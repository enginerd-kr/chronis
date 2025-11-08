# Chronis - Distributed Scheduler Framework

**Chronis** - Python distributed scheduler framework for multi-container environments

## Features

- **Distributed Scheduling**: Prevents duplicate execution in multi-container environments
- **Multiple Storage Adapters**: DynamoDB, PostgreSQL, SQLite, InMemory
- **Distributed Locks**: Redis and InMemory lock adapters
- **Timezone Support**: IANA timezone-aware scheduling with automatic DST handling
- **Async Support**: Native support for both sync and async job functions

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
from chronis import PollingScheduler
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.adapters.locks import InMemoryLockAdapter

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

# 5. Create jobs using simplified API

# Interval job - runs every 30 seconds
scheduler.create_interval_job(
    job_id="heartbeat",
    name="System Heartbeat",
    func="send_daily_email",
    seconds=30
)

# Cron job - runs daily at 9 AM Seoul time
scheduler.create_cron_job(
    job_id="daily-report",
    name="Daily Report",
    func="send_daily_email",
    hour=9,
    minute=0,
    timezone="Asia/Seoul"
)

# Date job - runs once at specific time
scheduler.create_date_job(
    job_id="welcome-email",
    name="Welcome Email",
    func="send_daily_email",
    run_date="2025-11-08 10:00:00",
    timezone="Asia/Seoul"
)

# 6. Start scheduler
scheduler.start()
```

## Core Concepts

### Simplified Job Creation API

Chronis provides three intuitive methods for creating jobs without needing to understand internal implementation details:

#### Interval Jobs

Execute jobs repeatedly at fixed intervals:

```python
# Run every 30 seconds
scheduler.create_interval_job(
    job_id="heartbeat",
    name="System Heartbeat",
    func=send_heartbeat,
    seconds=30
)

# Run every 2 hours
scheduler.create_interval_job(
    job_id="cleanup",
    name="Cleanup Task",
    func=cleanup_old_data,
    hours=2,
    timezone="Asia/Seoul"
)
```

**Parameters**: `seconds`, `minutes`, `hours`, `days`, `weeks`

#### Cron Jobs

Execute jobs based on cron-style patterns:

```python
# Run every day at 9 AM
scheduler.create_cron_job(
    job_id="daily-report",
    name="Daily Report",
    func=generate_report,
    hour=9,
    minute=0,
    timezone="Asia/Seoul"
)

# Run every Monday at 6 PM
scheduler.create_cron_job(
    job_id="weekly-summary",
    name="Weekly Summary",
    func=send_summary,
    day_of_week="mon",
    hour=18,
    minute=0
)
```

**Parameters**: `year`, `month`, `day`, `week`, `day_of_week`, `hour`, `minute`, `second`

#### Date Jobs

Execute jobs once at a specific date/time:

```python
# Run once at specific time
scheduler.create_date_job(
    job_id="welcome-email",
    name="Send Welcome Email",
    func=send_welcome_email,
    run_date="2025-11-08 10:00:00",
    timezone="Asia/Seoul",
    kwargs={"user_id": 123}
)

# Run once using datetime object
from datetime import datetime, timedelta
run_time = datetime.now() + timedelta(hours=1)
scheduler.create_date_job(
    job_id="reminder",
    name="Reminder",
    func=send_reminder,
    run_date=run_time
)
```

### Timezone Support

All job types support IANA timezones with automatic DST handling:

```python
# Korean time
scheduler.create_cron_job(
    job_id="korea-report",
    name="Korea Report",
    func=generate_report,
    hour=9,
    minute=0,
    timezone="Asia/Seoul"  # Runs at 9 AM KST
)

# US Eastern time (DST auto-handled)
scheduler.create_cron_job(
    job_id="us-report",
    name="US Report",
    func=generate_report,
    hour=8,
    minute=0,
    timezone="America/New_York"  # Runs at 8 AM EST/EDT
)
```

### Job State Management

Manage job lifecycle with pause, resume, and cancel operations:

```python
from chronis import JobStatus

# Pause a running job
scheduler.pause_job("daily-report")

# Resume a paused job
scheduler.resume_job("daily-report")

# Cancel a job
scheduler.cancel_job("daily-report")
```

### Async Function Support

Chronis natively supports both synchronous and asynchronous job functions:

```python
import asyncio

# Async job function
async def async_send_email():
    await asyncio.sleep(1)
    print("Async email sent!")

# Sync job function
def sync_send_email():
    print("Sync email sent!")

# Register both
scheduler.register_job_function("async_email", async_send_email)
scheduler.register_job_function("sync_email", sync_send_email)

# Create jobs - Chronis handles async automatically
scheduler.create_interval_job(
    job_id="async-job",
    name="Async Job",
    func="async_email",
    seconds=30
)

scheduler.create_interval_job(
    job_id="sync-job",
    name="Sync Job",
    func="sync_email",
    seconds=30
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

See the [examples/](examples/) directory for complete examples:

```bash
uv run python examples/quickstart.py
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing guidelines, and how to submit pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
