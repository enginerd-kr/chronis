"""
Chronis - Distributed Scheduler Framework for Multi-Container Environments

Usage:
    from chronis import PollingScheduler
    from chronis.adapters.storage import InMemoryStorageAdapter
    from chronis.adapters.lock import InMemoryLockAdapter

    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    # Register job function
    scheduler.register_function("send_daily_report", send_daily_report)

    # Create jobs using simplified API (TriggerType hidden)

    # Interval job - runs every 30 seconds
    scheduler.create_interval_job(
        job_id="heartbeat",
        name="System Heartbeat",
        func="send_daily_report",
        seconds=30
    )

    # Cron job - runs daily at 9 AM Seoul time
    scheduler.create_cron_job(
        job_id="daily-report",
        name="Daily Report",
        func="send_daily_report",
        hour=9,
        minute=0,
        timezone="Asia/Seoul"
    )

    # Date job - runs once at specific time
    scheduler.create_date_job(
        job_id="welcome-email",
        name="Welcome Email",
        func="send_daily_report",
        run_date="2025-11-08 10:00:00",
        timezone="Asia/Seoul"
    )

    scheduler.start()

    # Manage job state
    scheduler.pause_job("daily-report")
    scheduler.resume_job("daily-report")
    scheduler.cancel_job("daily-report")
"""

# Core classes
from chronis.adapters.lock import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core import (
    ConnectionError,
    JobAlreadyExistsError,
    JobInfo,
    JobNotFoundError,
    JobStatus,
    PollingScheduler,
    SchedulerError,
    ValidationError,
)

__all__ = [
    # Core
    "PollingScheduler",
    "JobInfo",
    "JobStatus",
    # Exceptions
    "SchedulerError",
    "JobAlreadyExistsError",
    "JobNotFoundError",
    "ValidationError",
    "ConnectionError",
    # Storage Adapters
    "InMemoryStorageAdapter",
    # Lock Adapters
    "InMemoryLockAdapter",
]
