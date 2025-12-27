# Chronis - AI Agent-Friendly Distributed Scheduler

**Chronis** - Python scheduler designed for AI agents, LLM workflows, and multi-container environments

## Why Chronis?

**Agentic AI needs autonomous scheduling.** For AI agents to truly work independently, they must manage their own schedules - setting reminders, scheduling follow-ups, and orchestrating time-based workflows without human intervention. Chronis makes this possible with a simple, LLM-friendly API designed for autonomous operation.

Traditional schedulers require complex configuration and aren't built for AI agents to self-manage. Chronis provides:

- **AI Agent Ready**: Simple API perfect for LLM function calling and agent workflows
- **Autonomous Scheduling**: Agents can create, modify, and manage their own jobs
- **Distributed by Default**: No duplicate executions across containers or processes
- **Pluggable Everything**: Bring your own database and distributed locks
- **Timezone Aware**: IANA timezones with automatic DST handling
- **Async Native**: Built for modern Python with full async/await support

## Installation

```bash
pip install chronis
```

## Quick Start

```python
from chronis import PollingScheduler
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.adapters.lock import InMemoryLockAdapter

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

# 5. Create jobs

# Interval - runs every 30 seconds
job1 = scheduler.create_interval_job(
    func="send_daily_email",
    seconds=30
)

# Cron - runs daily at 9 AM Seoul time
job2 = scheduler.create_cron_job(
    func="send_daily_email",
    hour=9,
    minute=0,
    timezone="Asia/Seoul"
)

# Date - runs once at specific time
job3 = scheduler.create_date_job(
    func="send_daily_email",
    run_date="2025-11-08 10:00:00",
    timezone="Asia/Seoul"
)

# 6. Start scheduler
scheduler.start()
```

## Job Management

```python
# Create job
job = scheduler.create_interval_job(func=my_func, hours=1)

# Query jobs
jobs = scheduler.query_jobs(filters={"status": "scheduled"}, limit=10)

# Get specific job
job = scheduler.get_job(saved_id)

# Pause/Resume jobs
scheduler.pause_job(saved_id)   # Temporarily suspend execution
scheduler.resume_job(saved_id)  # Resume execution

# Delete job
scheduler.delete_job(saved_id)
```

**Job States**: `PENDING` → `SCHEDULED` → `RUNNING` / `PAUSED` → `FAILED` (one-time jobs auto-delete after success)

**Custom IDs** (optional): You can provide explicit IDs if needed:

```python
scheduler.create_interval_job(
    func=my_func,
    job_id="my-custom-id",  # Explicit ID
    name="My Job",
    hours=1
)
```

## Multi-Tenancy & Metadata

Perfect for SaaS applications and multi-agent systems:

```python
# Create job with metadata
job = scheduler.create_interval_job(
    func=generate_report,
    hours=24,
    metadata={"tenant_id": "acme", "priority": "high"}
)

# Query by metadata
jobs = scheduler.query_jobs(filters={"metadata.tenant_id": "acme"})
```

## Callbacks (Success & Failure Handlers)

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
    on_failure=on_job_failure,  # Global failure handler
    on_success=on_job_success,  # Global success handler
)

# Job-specific handlers (override global)
scheduler.create_interval_job(
    func=critical_task,
    hours=1,
    on_failure=lambda job_id, err, info: send_urgent_alert(err),
    on_success=lambda job_id, info: update_dashboard(job_id)
)
```

## AI Agent Example

```python
# LLM function calling - minimal parameters!
def schedule_reminder(message: str, hours_from_now: int):
    """AI agent schedules a reminder."""
    job = scheduler.create_date_job(
        func="send_notification",
        run_date=datetime.now() + timedelta(hours=hours_from_now),
        kwargs={"message": message}
    )
    return f"Reminder scheduled with ID: {job.job_id}"

# Agent can now manage its own schedule
schedule_reminder("Check on customer", 24)
```

## Learn More

- [Adapter Implementation Guide](docs/ADAPTER_GUIDE.md) - Build custom storage/lock adapters
- [Examples](examples/) - Complete working examples
- [Contributing](CONTRIBUTING.md) - Development setup and guidelines

## License

MIT License - see [LICENSE](LICENSE) for details.
