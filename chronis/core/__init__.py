"""Core scheduler components."""

from chronis.core.common import (
    JobAlreadyExistsError,
    JobNotFoundError,
    SchedulerError,
    TriggerType,
)
from chronis.core.execution import AsyncExecutor
from chronis.core.jobs import JobDefinition, JobInfo
from chronis.core.lifecycle import (
    can_execute,
    determine_next_status_after_execution,
    is_ready_for_execution,
)
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
from chronis.core.scheduler import PollingScheduler
from chronis.core.scheduling import NextRunTimeCalculator
from chronis.core.services import ExecutionCoordinator
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
    "can_execute",
    "determine_next_status_after_execution",
    "is_ready_for_execution",
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
    # Execution
    "AsyncExecutor",
    # Scheduler
    "PollingScheduler",
]
