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

## Three Simple Job Types

**Interval** - Repeat at fixed intervals (every 30 seconds, every 2 hours, etc.)

```python
scheduler.create_interval_job(job_id="heartbeat", name="Heartbeat", func=check_health, seconds=30)
```

**Cron** - Run on schedule (daily at 9 AM, every Monday, etc.)

```python
scheduler.create_cron_job(job_id="daily-report", name="Report", func=generate_report, hour=9, timezone="Asia/Seoul")
```

**Date** - Run once at specific time (one-time tasks, reminders, etc.)

```python
scheduler.create_date_job(job_id="reminder", name="Reminder", func=send_reminder, run_date="2025-12-25 09:00:00")
```

## Job Management

```python
# Query jobs
jobs = scheduler.query_jobs(filters={"status": "scheduled"}, limit=10)

# Get specific job
job = scheduler.get_job("daily-report")

# Delete job
scheduler.delete_job("daily-report")
```

**Job States**: `PENDING` → `SCHEDULED` → `RUNNING` → `FAILED` (one-time jobs auto-delete after success)

## Multi-Tenancy & Metadata

Perfect for SaaS applications and multi-agent systems:

```python
# Create job with metadata
scheduler.create_interval_job(
    job_id="daily-report",
    name="Daily Report",
    func=generate_report,
    hours=24,
    metadata={"tenant_id": "acme", "priority": "high"}
)

# Query by metadata
jobs = scheduler.query_jobs(filters={"metadata.tenant_id": "acme"})
```

## Learn More

- [Adapter Implementation Guide](docs/ADAPTER_GUIDE.md) - Build custom storage/lock adapters
- [Examples](examples/) - Complete working examples
- [Contributing](CONTRIBUTING.md) - Development setup and guidelines

## License

MIT License - see [LICENSE](LICENSE) for details.
