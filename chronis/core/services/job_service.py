"""Job CRUD application service."""

from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.core.common.exceptions import JobAlreadyExistsError, JobNotFoundError
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.state import JobStatus
from chronis.utils.logging import ContextLogger
from chronis.utils.time import utc_now


class JobService:
    """
    Application service for job CRUD operations.

    This service orchestrates job creation, retrieval, updates, and deletion
    by coordinating between the storage adapter and domain logic.

    Responsibilities:
    - Job lifecycle management (CRUD)
    - Data transformation (JobDefinition -> dict -> JobInfo)
    - Exception translation
    - Logging

    This is an application service that sits between the domain layer and
    infrastructure layer, providing a clean API for job management.
    """

    def __init__(
        self,
        storage: JobStorageAdapter,
        logger: ContextLogger | None = None,
        verbose: bool = False,
    ) -> None:
        """
        Initialize job service.

        Args:
            storage: Storage adapter for persistence
            logger: Context logger for structured logging
            verbose: Enable verbose logging
        """
        self.storage = storage
        self.logger = logger
        self.verbose = verbose

    def create(self, job: JobDefinition) -> JobInfo:
        """
        Create a new job.

        Args:
            job: Job definition

        Returns:
            Created job info

        Raises:
            JobAlreadyExistsError: If job with same ID already exists
        """
        job_data = job.to_dict()

        try:
            result = self.storage.create_job(job_data)
            if self.verbose and self.logger:
                self.logger.info("Job created", job_id=job.job_id)
            return JobInfo.from_dict(result)
        except ValueError as e:
            raise JobAlreadyExistsError(str(e)) from e

    def get(self, job_id: str) -> JobInfo | None:
        """
        Get job by ID.

        Args:
            job_id: Job ID

        Returns:
            Job info or None if not found
        """
        job_data = self.storage.get_job(job_id)
        return JobInfo.from_dict(job_data) if job_data else None

    def query(
        self,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[JobInfo]:
        """
        Query jobs using filter dictionary.

        Args:
            filters: Filter dictionary (e.g., from scheduled_jobs() helper)
            limit: Maximum number of results

        Returns:
            List of matching jobs

        Example:
            >>> from chronis.core.query import scheduled_jobs
            >>> jobs = service.query(filters=scheduled_jobs())
        """
        jobs_data = self.storage.query_jobs(filters=filters, limit=limit)
        return [JobInfo.from_dict(job_data) for job_data in jobs_data]

    def update(
        self,
        job_id: str,
        name: str | None = None,
        trigger_type: str | None = None,
        trigger_args: dict[str, Any] | None = None,
        status: JobStatus | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> JobInfo:
        """
        Update job fields.

        Args:
            job_id: Job ID
            name: New name (optional)
            trigger_type: New trigger type (optional)
            trigger_args: New trigger parameters (optional)
            status: New status (optional)
            metadata: New metadata (optional)

        Returns:
            Updated job info

        Raises:
            JobNotFoundError: If job doesn't exist
        """
        # Build updates dictionary from non-None parameters
        updates = {
            k: v
            for k, v in {
                "name": name,
                "trigger_type": trigger_type,
                "trigger_args": trigger_args,
                "status": status.value if status else None,
                "metadata": metadata,
            }.items()
            if v is not None
        }

        if not updates:
            # No updates provided, just fetch and return current state
            current = self.get(job_id)
            if not current:
                raise JobNotFoundError(f"Job {job_id} not found")
            return current

        updates["updated_at"] = utc_now().isoformat()

        try:
            result = self.storage.update_job(job_id, updates)
            return JobInfo.from_dict(result)
        except ValueError as e:
            raise JobNotFoundError(str(e)) from e

    def delete(self, job_id: str) -> bool:
        """
        Delete job.

        Args:
            job_id: Job ID

        Returns:
            True if deleted, False if not found
        """
        return self.storage.delete_job(job_id)

    def exists(self, job_id: str) -> bool:
        """
        Check if job exists.

        Args:
            job_id: Job ID

        Returns:
            True if job exists
        """
        return self.get(job_id) is not None

    def count(self, filters: dict[str, Any] | None = None) -> int:
        """
        Count jobs matching filters.

        Args:
            filters: Filter dictionary (None = count all)

        Returns:
            Number of matching jobs
        """
        return self.storage.count_jobs(filters=filters)
