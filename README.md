# Chronis - Distributed Scheduler Framework

**Chronis** - Python distributed scheduler framework for multi-container environments

## Features

- **Distributed Scheduling**: Prevents duplicate execution in multi-container environments
- **Pluggable Storage**: Implement custom storage adapters for any database
- **Pluggable Locks**: Implement custom lock adapters for distributed locking
- **Timezone Support**: IANA timezone-aware scheduling with automatic DST handling
- **Async Support**: Native support for both sync and async job functions

## Installation

```bash
pip install chronis
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

Query and manage your scheduled jobs:

```python
# Get a specific job
job = scheduler.get_job("daily-report")
print(f"Status: {job.status}, Next run: {job.next_run_time}")

# Query all jobs
all_jobs = scheduler.query_jobs()
for job in all_jobs:
    print(f"{job.job_id}: {job.status} - {job.trigger_type}")

# Query scheduled jobs only
scheduled_jobs = scheduler.query_jobs(filters={"status": "scheduled"})

# Query failed jobs
failed_jobs = scheduler.query_jobs(filters={"status": "failed"})

# Query with limit
recent_jobs = scheduler.query_jobs(limit=10)

# Delete a job
scheduler.delete_job("daily-report")
```

### Job States

Chronis uses a simple state model:

- **PENDING**: Job created, waiting for first run
- **SCHEDULED**: Job is scheduled for next execution
- **RUNNING**: Job is currently executing
- **FAILED**: Job execution failed (can be retried)

Note: One-time jobs (DATE trigger) are automatically deleted after successful execution.

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

## Multi-Tenancy Support (Optional)

Chronis supports multi-tenancy through the `metadata` field, allowing you to isolate jobs by tenant, user, or organization:

```python
# Create tenant-specific job
scheduler.create_interval_job(
    job_id="daily-report",
    name="Daily Report",
    func=generate_report,
    hours=24,
    metadata={"tenant_id": "acme"}
)

# Query jobs for specific tenant
tenant_jobs = scheduler.query_jobs(
    filters={"metadata.tenant_id": "acme", "status": "scheduled"}
)

# FastAPI integration example
from fastapi import FastAPI, Depends

app = FastAPI()

def get_current_tenant() -> str:
    """Extract tenant from JWT token."""
    return "tenant:acme"

@app.post("/jobs/create")
def create_job(
    job_id: str,
    schedule_hours: int,
    current_tenant: str = Depends(get_current_tenant)
):
    job = scheduler.create_interval_job(
        job_id=job_id,
        name="Tenant Job",
        func=process_job,
        hours=schedule_hours,
        metadata={"tenant_id": current_tenant}
    )
    return {"status": "created", "job": job}

@app.get("/jobs")
def list_my_jobs(current_tenant: str = Depends(get_current_tenant)):
    """Users can only see their own tenant's jobs."""
    jobs = scheduler.query_jobs(
        filters={"metadata.tenant_id": current_tenant},
        limit=100
    )
    return {"jobs": jobs}
```

### Metadata Patterns

```python
# Simple tenant ID
metadata = {"tenant_id": "acme"}

# Hierarchical organization
metadata = {
    "org_id": "acme",
    "team_id": "engineering",
    "project_id": "website"
}

# Custom tags
metadata = {
    "priority": "high",
    "environment": "production",
    "region": "us-east-1"
}
```

## Custom Adapters

Want to implement your own storage adapter for a specific database? See the [Adapter Implementation Guide](docs/ADAPTER_GUIDE.md) for:

- Interface contract and required methods
- Reference implementations (PostgreSQL, DynamoDB, Redis)
- Performance optimization guidelines
- Testing patterns

## Examples

See the [examples/](examples/) directory for complete examples:

```bash
uv run python examples/quickstart.py
uv run python examples/multitenant.py
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing guidelines, and how to submit pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
