"""Abstract base classes for adapters."""

from abc import ABC, abstractmethod
from typing import Any

from chronis.type_defs import JobStorageData, JobUpdateData


class JobStorageAdapter(ABC):
    """Job storage adapter abstract class."""

    @abstractmethod
    def create_job(self, job_data: JobStorageData) -> JobStorageData:
        """
        Create a job.

        Required fields in job_data:
            - job_id: str
            - status: str
            - next_run_time: str (ISO format, UTC)

        Optional fields:
            - metadata: dict (user-defined, for extensions like multi-tenancy)
            - Any other adapter-specific fields

        Returns:
            Created job data
        """
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> JobStorageData | None:
        """Get a job by ID."""
        pass

    @abstractmethod
    def update_job(self, job_id: str, updates: JobUpdateData) -> JobStorageData:
        """Update a job."""
        pass

    @abstractmethod
    def delete_job(self, job_id: str) -> bool:
        """Delete a job."""
        pass

    @abstractmethod
    def query_jobs(
        self,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[JobStorageData]:
        """
        Query jobs with flexible filters.

        Common filter patterns:
            - {"status": "scheduled"}
            - {"status": "scheduled", "next_run_time_lte": "2025-01-01T00:00:00Z"}
            - {"metadata.tenant_id": "acme"}  # Multi-tenancy example
            - Custom filters based on metadata or other fields

        Filter format and interpretation is adapter-specific.

        Args:
            filters: Dictionary of filter conditions
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip (for pagination)

        Returns:
            List of jobs matching filters, sorted by next_run_time (oldest first)
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
