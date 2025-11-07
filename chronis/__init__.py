"""
Chronis - Distributed Scheduler Framework for Multi-Container Environments

Usage:
    from chronis import PollingScheduler, JobDefinition, TriggerType, JobStatus
    from chronis.adapters.storage import InMemoryStorageAdapter
    from chronis.adapters.locks import InMemoryLockAdapter

    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    # Create a job with timezone support and state management
    job = JobDefinition(
        job_id="daily-report",
        name="Daily Report (Seoul Time)",
        trigger_type=TriggerType.CRON,
        trigger_args={"hour": 9, "minute": 0},
        func="send_daily_report",
        timezone="Asia/Seoul",  # Run at 9 AM Seoul time
        status=JobStatus.SCHEDULED  # Initial state
    )
    scheduler.create_job(job)
    scheduler.start()

    # Manage job state
    scheduler.pause_job("daily-report")
    scheduler.resume_job("daily-report")
    scheduler.cancel_job("daily-report")
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
from chronis.core.state import JobStatus

__all__ = [
    # Core
    "PollingScheduler",
    "JobDefinition",
    "JobInfo",
    "TriggerType",
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
