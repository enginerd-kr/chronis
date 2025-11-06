"""
Chronis - Distributed Scheduler Framework for Multi-Container Environments

Usage:
    from chronis import PollingScheduler, JobDefinition, TriggerType
    from chronis.adapters.storage import InMemoryStorageAdapter
    from chronis.adapters.locks import InMemoryLockAdapter

    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    # Create a job with timezone support and retry logic
    job = JobDefinition(
        job_id="daily-report",
        name="Daily Report (Seoul Time)",
        trigger_type=TriggerType.CRON,
        trigger_args={"hour": 9, "minute": 0},
        func="send_daily_report",
        timezone="Asia/Seoul",  # Run at 9 AM Seoul time
        max_retries=5,  # Retry up to 5 times on failure
        retry_delay_seconds=60,  # Wait 60 seconds between retries
        retry_exponential_backoff=True  # Use exponential backoff
    )
    scheduler.create_job(job)
    scheduler.start()
"""

# Core classes
from chronis.adapters.locks import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.enums import TriggerType
from chronis.core.exceptions import (
    ConnectionError,
    JobAlreadyExistsError,
    JobNotFoundError,
    SchedulerError,
    ValidationError,
)
from chronis.core.job import JobDefinition, JobInfo
from chronis.core.scheduler import PollingScheduler

__all__ = [
    # Core
    "PollingScheduler",
    "JobDefinition",
    "JobInfo",
    "TriggerType",
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
