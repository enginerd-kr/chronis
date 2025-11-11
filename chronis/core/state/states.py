"""Job state pattern implementation."""

from abc import ABC, abstractmethod

from chronis.core.state.enums import JobStatus


class JobState(ABC):
    """Abstract base class for job states."""

    @abstractmethod
    def can_execute(self) -> bool:
        """Check if job can be executed in this state."""
        pass

    @abstractmethod
    def get_status(self) -> JobStatus:
        """Get the current status."""
        pass


class PendingState(JobState):
    """Job is created and waiting for first run."""

    def can_execute(self) -> bool:
        return True

    def get_status(self) -> JobStatus:
        return JobStatus.PENDING


class ScheduledState(JobState):
    """Job is scheduled for next run."""

    def can_execute(self) -> bool:
        return True

    def get_status(self) -> JobStatus:
        return JobStatus.SCHEDULED


class RunningState(JobState):
    """Job is currently executing."""

    def can_execute(self) -> bool:
        return False  # Cannot execute while already running

    def get_status(self) -> JobStatus:
        return JobStatus.RUNNING


class FailedState(JobState):
    """Job execution failed."""

    def can_execute(self) -> bool:
        return True  # Can retry failed jobs

    def get_status(self) -> JobStatus:
        return JobStatus.FAILED


class StateFactory:
    """Factory for creating state instances."""

    _states: dict[JobStatus, JobState] = {
        JobStatus.PENDING: PendingState(),
        JobStatus.SCHEDULED: ScheduledState(),
        JobStatus.RUNNING: RunningState(),
        JobStatus.FAILED: FailedState(),
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
