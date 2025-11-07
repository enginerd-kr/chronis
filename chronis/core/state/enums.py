"""Job status enumeration."""

from enum import Enum


class JobStatus(str, Enum):
    """Job status enum."""

    PENDING = "pending"  # Created, waiting for first run
    SCHEDULED = "scheduled"  # Next run time is set
    RUNNING = "running"  # Currently executing
    COMPLETED = "completed"  # One-time job completed successfully
    PAUSED = "paused"  # Temporarily paused
    CANCELLED = "cancelled"  # Cancelled by user

    def __str__(self) -> str:
        return self.value
