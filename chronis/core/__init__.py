"""Core scheduler components."""

from chronis.core.common import (
    ConnectionError,
    JobAlreadyExistsError,
    JobNotFoundError,
    SchedulerError,
    TriggerType,
    ValidationError,
)
from chronis.core.execution import AsyncExecutor
from chronis.core.jobs import JobDefinition, JobInfo
from chronis.core.lifecycle import JobLifecycleManager
from chronis.core.scheduling import NextRunTimeCalculator
from chronis.core.services import ExecutionCoordinator, JobService, SchedulingOrchestrator
from chronis.core.query import (
    JobQuery,
    JobQuerySpec,
    MetadataSpec,
    NextRunBeforeSpec,
    StatusSpec,
    TriggerTypeSpec,
    jobs_ready_before,
    scheduled_jobs,
)
from chronis.core.scheduler import PollingScheduler
from chronis.core.state import JobStatus

__all__ = [
    # Common Types
    "TriggerType",
    "JobStatus",
    # Exceptions
    "SchedulerError",
    "JobAlreadyExistsError",
    "JobNotFoundError",
    "ValidationError",
    "ConnectionError",
    # Job Management
    "JobDefinition",
    "JobInfo",
    "JobLifecycleManager",
    # Scheduling
    "NextRunTimeCalculator",
    # Query
    "JobQuery",
    "JobQuerySpec",
    "StatusSpec",
    "MetadataSpec",
    "NextRunBeforeSpec",
    "TriggerTypeSpec",
    "scheduled_jobs",
    "jobs_ready_before",
    # Services
    "JobService",
    "SchedulingOrchestrator",
    "ExecutionCoordinator",
    # Execution
    "AsyncExecutor",
    # Scheduler
    "PollingScheduler",
]
