# Chronis - AI Agent-Friendly Distributed Scheduler

**Chronis** - Python scheduler designed for AI agents, LLM workflows, and multi-container environments

## Why Chronis?

**Agentic AI needs autonomous scheduling.** For AI agents to truly work independently, they must manage their own schedules - setting reminders, scheduling follow-ups, and orchestrating time-based workflows without human intervention. Traditional schedulers require complex configuration and aren't built for this. Chronis makes it possible with a simple, LLM-friendly API designed for autonomous operation.

**AI-optimized real-time scheduling.** Traditional schedulers optimize for millisecond precision, but when an AI agent schedules "30 minutes from now," human perception doesn't distinguish between 30:00 and 30:08—both feel like "30 minutes later."

This insight drives our architecture:

- **AI Agent Ready**: Simple API perfect for LLM function calling and agent workflows
- **Human-Scale Timing**: Configurable polling intervals deliver the responsiveness AI agents need
- **Architectural Simplicity**: No message brokers or event streams—just configurable polling with your database

Chronis also provides:

- **Pluggable Everything**: Bring your own storage and locks (PostgreSQL, Redis, or custom adapters)
- **Distributed by Default**: No duplicate executions across containers or processes
- **Timezone Aware**: IANA timezones with automatic DST handling
- **Async Native**: Built for modern Python with full async/await support

## Installation

```bash
pip install chronis

# Optional: Install with adapter dependencies
pip install chronis[redis]      # Redis storage and locking
pip install chronis[postgres]   # PostgreSQL storage
pip install chronis[all]        # All adapters
```

## Quick Start

```python
from chronis import PollingScheduler, InMemoryStorageAdapter, InMemoryLockAdapter

# Setup
scheduler = PollingScheduler(
    storage_adapter=InMemoryStorageAdapter(),
    lock_adapter=InMemoryLockAdapter(),
)

def send_email():
    print("Sending email...")

scheduler.register_job_function("send_email", send_email)

# Create jobs with simple fluent API
scheduler.every(minutes=5).run("send_email")                          # Every 5 minutes
scheduler.on(hour=9, minute=30).run("send_email")                     # Daily at 9:30
scheduler.on(day_of_week="mon", hour=9).run("send_email")             # Every Monday at 9:00
scheduler.once(when="2025-12-25T09:00:00").run("send_email")          # Once at specific time

# With options
scheduler.every(hours=1).config(retry=3, timeout=300).run("send_email")
scheduler.on(hour=9).config(timezone="Asia/Seoul").run("send_email")

scheduler.start()
```

## Fluent API

```python
# every() - Interval scheduling
scheduler.every(seconds=30).run("task")
scheduler.every(minutes=5).run("task")
scheduler.every(hours=1, minutes=30).run("task")

# on() - Cron scheduling (specific times)
scheduler.on(minute=5).run("task")                      # Every hour at :05
scheduler.on(hour=9, minute=30).run("task")             # Daily at 9:30
scheduler.on(day_of_week="mon", hour=9).run("task")     # Weekly on Monday
scheduler.on(day=1, hour=0, minute=0).run("task")       # Monthly on 1st
scheduler.on(month="jan", day=1).run("task")            # Yearly on Jan 1st

# once() - One-time scheduling
scheduler.once(when="2025-12-25T09:00:00").run("task")
scheduler.once(when=datetime.now() + timedelta(hours=1)).run("task")

# config() - Options (can be chained in any order before run())
scheduler.every(minutes=5).config(
    retry=3,                    # Max retry attempts
    timeout=300,                # Timeout in seconds
    timezone="Asia/Seoul",      # Timezone
    metadata={"env": "prod"},   # Custom metadata
).run("task")

# config() first is also valid
scheduler.config(retry=3).every(minutes=5).run("task")
```

## Job Management

```python
# Query and manage jobs
jobs = scheduler.query_jobs(filters={"status": "scheduled"})
job = scheduler.get_job(job_id)

scheduler.pause_job(job_id)
scheduler.resume_job(job_id)
scheduler.delete_job(job_id)
```

## Callbacks

Monitor and react to job execution results:

```python
# Global handlers for all jobs
def on_job_failure(job_id: str, error: Exception, job_info):
    logger.error(f"Job {job_id} failed: {error}")
    send_alert(job_id, error)

def on_job_success(job_id: str, job_info):
    logger.info(f"Job {job_id} completed successfully")

scheduler = PollingScheduler(
    storage_adapter=storage,
    lock_adapter=lock,
    on_failure=on_job_failure,
    on_success=on_job_success,
)

# Job-specific handlers
def on_critical_failure(job_id: str, error: Exception, job_info):
    send_urgent_alert(error)

def on_critical_success(job_id: str, job_info):
    update_dashboard(job_id)

scheduler.every(hours=1).config(
    on_failure=on_critical_failure,
    on_success=on_critical_success,
).run("critical_task")
```

## AI Agent Example

```python
# LLM function calling - minimal parameters!
def schedule_reminder(message: str, hours_from_now: int):
    """AI agent schedules a reminder."""
    job = scheduler.once(
        when=datetime.now() + timedelta(hours=hours_from_now)
    ).run("send_notification", message=message)
    return f"Reminder scheduled: {job.job_id}"

schedule_reminder("Check on customer", 24)
```

## Direct API

For advanced use cases, the direct API with full parameter control is also available. Use these methods when you need explicit control over all job parameters or when integrating programmatically:

```python
scheduler.create_interval_job(func="task", seconds=30, max_retries=3)
scheduler.create_cron_job(func="task", hour=9, minute=0, timezone="UTC")
scheduler.create_date_job(func="task", run_date="2025-12-25T09:00:00")
```

These methods accept all configuration options as explicit parameters, making them suitable for dynamic job creation where parameters are determined at runtime.

## Advanced Options

### Misfire Handling

When a scheduler is down or busy, jobs may miss their scheduled execution time. The `if_missed` option controls how Chronis handles these misfired jobs when the scheduler recovers:

- `skip`: Ignore missed executions entirely (default)
- `run_once`: Execute the job once to catch up, regardless of how many executions were missed
- `run_all`: Execute all missed runs (use with caution for interval jobs)

```python
scheduler.on(hour=9).config(if_missed="run_once").run("daily_report")
scheduler.every(hours=1).config(if_missed="skip").run("cleanup")
```

### Timeout & Retry

Jobs can fail due to network issues, external service outages, or long-running operations. Configure timeout and retry behavior to handle these scenarios gracefully:

```python
scheduler.every(minutes=5).config(
    timeout=60,           # Kill job if it exceeds 60 seconds
    retry=3,              # Retry up to 3 times on failure
    retry_delay=60,       # Wait 60 seconds between retries
).run("sync_external_api")
```

### Multi-Tenancy & Metadata

Store custom key-value pairs with jobs using the `metadata` option. This is useful for multi-tenancy (tag jobs by tenant/environment), filtering (query jobs by metadata fields), and passing additional context for logging or debugging.

```python
scheduler.every(hours=1).config(metadata={"tenant_id": "acme", "env": "prod"}).run("task")
jobs = scheduler.query_jobs(filters={"metadata.tenant_id": "acme"})
```

## Learn More

- [Adapter Implementation Guide](docs/ADAPTER_GUIDE.md) - Build custom storage/lock adapters
- [Examples](examples/) - Complete working examples
- [Contributing](CONTRIBUTING.md) - Development setup and guidelines

## License

MIT License - see [LICENSE](LICENSE) for details.
