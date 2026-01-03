"""Job status and trigger type enumerations."""

from enum import Enum


class TriggerType(Enum):
    """Trigger type for job scheduling."""

    INTERVAL = "interval"  # Periodic execution
    CRON = "cron"  # Cron expression
    DATE = "date"  # One-time execution


class JobStatus(str, Enum):
    """Job status enum."""

    PENDING = "pending"  # Created, waiting for first run
    SCHEDULED = "scheduled"  # Next run time is set
    RUNNING = "running"  # Currently executing
    PAUSED = "paused"  # Temporarily suspended
    FAILED = "failed"  # Execution failed

    def __str__(self) -> str:
        return self.value

    def can_execute(self) -> bool:
        """Check if job can be executed in this status."""
        return self not in (JobStatus.RUNNING, JobStatus.PAUSED)
