"""Job state management using State Pattern."""

from abc import ABC, abstractmethod
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


class JobState(ABC):
    """Abstract base class for job states."""

    @abstractmethod
    def can_execute(self) -> bool:
        """Check if job can be executed in this state."""
        pass

    @abstractmethod
    def can_pause(self) -> bool:
        """Check if job can be paused in this state."""
        pass

    @abstractmethod
    def can_cancel(self) -> bool:
        """Check if job can be cancelled in this state."""
        pass

    @abstractmethod
    def can_resume(self) -> bool:
        """Check if job can be resumed in this state."""
        pass

    @abstractmethod
    def get_status(self) -> JobStatus:
        """Get the current status."""
        pass


class PendingState(JobState):
    """Job is created and waiting for first run."""

    def can_execute(self) -> bool:
        return True

    def can_pause(self) -> bool:
        return True

    def can_cancel(self) -> bool:
        return True

    def can_resume(self) -> bool:
        return False

    def get_status(self) -> JobStatus:
        return JobStatus.PENDING


class ScheduledState(JobState):
    """Job is scheduled for next run."""

    def can_execute(self) -> bool:
        return True

    def can_pause(self) -> bool:
        return True

    def can_cancel(self) -> bool:
        return True

    def can_resume(self) -> bool:
        return False

    def get_status(self) -> JobStatus:
        return JobStatus.SCHEDULED


class RunningState(JobState):
    """Job is currently executing."""

    def can_execute(self) -> bool:
        return False  # Cannot execute while already running

    def can_pause(self) -> bool:
        return False  # Cannot pause while running

    def can_cancel(self) -> bool:
        return True  # Can cancel running job

    def can_resume(self) -> bool:
        return False

    def get_status(self) -> JobStatus:
        return JobStatus.RUNNING


class CompletedState(JobState):
    """One-time job completed successfully."""

    def can_execute(self) -> bool:
        return False

    def can_pause(self) -> bool:
        return False

    def can_cancel(self) -> bool:
        return False

    def can_resume(self) -> bool:
        return False

    def get_status(self) -> JobStatus:
        return JobStatus.COMPLETED


class PausedState(JobState):
    """Job is temporarily paused."""

    def can_execute(self) -> bool:
        return False

    def can_pause(self) -> bool:
        return False

    def can_cancel(self) -> bool:
        return True

    def can_resume(self) -> bool:
        return True

    def get_status(self) -> JobStatus:
        return JobStatus.PAUSED


class CancelledState(JobState):
    """Job is cancelled."""

    def can_execute(self) -> bool:
        return False

    def can_pause(self) -> bool:
        return False

    def can_cancel(self) -> bool:
        return False

    def can_resume(self) -> bool:
        return False

    def get_status(self) -> JobStatus:
        return JobStatus.CANCELLED


class StateFactory:
    """Factory for creating state instances."""

    _states: dict[JobStatus, JobState] = {
        JobStatus.PENDING: PendingState(),
        JobStatus.SCHEDULED: ScheduledState(),
        JobStatus.RUNNING: RunningState(),
        JobStatus.COMPLETED: CompletedState(),
        JobStatus.PAUSED: PausedState(),
        JobStatus.CANCELLED: CancelledState(),
    }

    @classmethod
    def get_state(cls, status: JobStatus | str) -> JobState:
        """
        Get state instance for given status.

        Args:
            status: Job status (enum or string)

        Returns:
            JobState instance

        Raises:
            ValueError: If status is invalid
        """
        if isinstance(status, str):
            try:
                status = JobStatus(status)
            except ValueError as e:
                raise ValueError(f"Invalid job status: {status}") from e

        state = cls._states.get(status)
        if not state:
            raise ValueError(f"No state found for status: {status}")

        return state
