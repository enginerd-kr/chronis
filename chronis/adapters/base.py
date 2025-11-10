"""Abstract base classes for adapters."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class JobStorageAdapter(ABC):
    """Job storage adapter abstract class."""

    @abstractmethod
    def create_job(self, job_data: dict[str, Any]) -> dict[str, Any]:
        """Create a job."""
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Get a job."""
        pass

    @abstractmethod
    def update_job(self, job_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        """Update a job."""
        pass

    @abstractmethod
    def delete_job(self, job_id: str) -> bool:
        """Delete a job."""
        pass

    @abstractmethod
    def query_ready_jobs(
        self, current_time: datetime, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Query jobs ready for execution (next_run_time <= current_time).

        Args:
            current_time: Current time for comparison
            limit: Maximum number of jobs to return (None for unlimited)

        Returns:
            List of job dictionaries, sorted by next_run_time (oldest first)
        """
        pass

    @abstractmethod
    def get_all_jobs(self) -> list[dict[str, Any]]:
        """
        Get all jobs.

        Returns:
            List of all job dictionaries
        """
        pass


class LockAdapter(ABC):
    """Distributed lock adapter abstract class."""

    @abstractmethod
    def acquire(self, lock_key: str, ttl_seconds: int, blocking: bool = False) -> bool:
        """Acquire lock."""
        pass

    @abstractmethod
    def release(self, lock_key: str) -> bool:
        """Release lock."""
        pass
