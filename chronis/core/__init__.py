"""Core scheduler components."""

from chronis.core.common import (
    JobAlreadyExistsError,
    JobNotFoundError,
    SchedulerError,
    TriggerType,
)
from chronis.core.execution.coordinator import ExecutionCoordinator
from chronis.core.jobs import JobDefinition, JobInfo
from chronis.core.query import (
    combine_filters,
    jobs_after_time,
    jobs_before_time,
    jobs_by_metadata,
    jobs_by_status,
    jobs_by_trigger_type,
    jobs_ready_before,
    scheduled_jobs,
)
from chronis.core.schedulers.next_run_calculator import NextRunTimeCalculator
from chronis.core.schedulers.polling_scheduler import PollingScheduler
from chronis.core.state import JobStatus

__all__ = [
    # Common Types
    "TriggerType",
    "JobStatus",
    # Exceptions
    "SchedulerError",
    "JobAlreadyExistsError",
    "JobNotFoundError",
    # Job Management
    "JobDefinition",
    "JobInfo",
    # Scheduling
    "NextRunTimeCalculator",
    # Query helpers
    "scheduled_jobs",
    "jobs_ready_before",
    "jobs_by_status",
    "jobs_by_trigger_type",
    "jobs_by_metadata",
    "jobs_before_time",
    "jobs_after_time",
    "combine_filters",
    # Services
    "ExecutionCoordinator",
    # Scheduler
    "PollingScheduler",
]
