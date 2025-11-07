"""Job manager for CRUD operations on jobs."""

from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.core.common.exceptions import JobAlreadyExistsError, JobNotFoundError
from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.state import JobStatus
from chronis.utils.logging import ContextLogger
from chronis.utils.time import utc_now


class JobManager:
    """
    Manages job CRUD operations.

    Provides a clean interface for creating, reading, updating, and deleting jobs,
    as well as state transition operations (pause, resume, cancel).

    Usage:
        >>> manager = JobManager(storage, logger)
        >>> job_info = manager.create_job(job_definition)
        >>> manager.pause_job("job-001")
    """

    def __init__(self, storage: JobStorageAdapter, logger: ContextLogger):
        """
        Initialize JobManager.

        Args:
            storage: Storage adapter for job persistence
            logger: Context logger for logging operations
        """
        self.storage = storage
        self.logger = logger

    def create_job(self, job: JobDefinition) -> JobInfo:
        """
        Create new job.

        Args:
            job: Job definition

        Returns:
            Created job info

        Raises:
            JobAlreadyExistsError: If job already exists
            ValidationError: If parameters are invalid
        """
        self.logger.info(
            "Creating job",
            job_id=job.job_id,
            job_name=job.name,
            trigger_type=job.trigger_type.value,
            timezone=job.timezone,
        )

        job_data = job.to_dict()
        try:
            result = self.storage.create_job(job_data)
            self.logger.info(
                "Job created successfully",
                job_id=job.job_id,
                next_run_time=result.get("next_run_time"),
            )
            return JobInfo(result)
        except ValueError as e:
            self.logger.error(
                f"Failed to create job: {e}",
                job_id=job.job_id,
                error_type="JobAlreadyExists",
            )
            raise JobAlreadyExistsError(str(e)) from e
        except Exception as e:
            self.logger.error(
                f"Unexpected error creating job: {e}",
                exc_info=True,
                job_id=job.job_id,
                error_type=type(e).__name__,
            )
            raise

    def get_job(self, job_id: str) -> JobInfo | None:
        """
        Get job by ID.

        Args:
            job_id: Job ID

        Returns:
            Job info or None if not found
        """
        job_data = self.storage.get_job(job_id)
        return JobInfo(job_data) if job_data else None

    def update_job(
        self,
        job_id: str,
        name: str | None = None,
        trigger_type: TriggerType | None = None,
        trigger_args: dict[str, Any] | None = None,
        status: JobStatus | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> JobInfo:
        """
        Update job.

        Args:
            job_id: Job ID
            name: New name (optional)
            trigger_type: New trigger type (optional)
            trigger_args: New trigger parameters (optional)
            status: Job status (optional)
            metadata: Metadata (optional)

        Returns:
            Updated job info

        Raises:
            JobNotFoundError: If job not found
        """
        # Build updates dictionary
        updates = {
            k: v
            for k, v in {
                "name": name,
                "trigger_type": trigger_type.value if trigger_type else None,
                "trigger_args": trigger_args,
                "status": status.value if status else None,
                "metadata": metadata,
            }.items()
            if v is not None
        }

        if not updates:
            # No updates provided, just fetch and return current state
            current = self.get_job(job_id)
            if not current:
                raise JobNotFoundError(f"Job {job_id} not found")
            return current

        updates["updated_at"] = utc_now().isoformat()

        try:
            result = self.storage.update_job(job_id, updates)
            return JobInfo(result)
        except ValueError as e:
            raise JobNotFoundError(str(e)) from e

    def delete_job(self, job_id: str) -> bool:
        """
        Delete job.

        Args:
            job_id: Job ID

        Returns:
            True if deleted, False if not found
        """
        self.logger.info("Deleting job", job_id=job_id)

        try:
            success = self.storage.delete_job(job_id)
            if success:
                self.logger.info("Job deleted successfully", job_id=job_id)
            else:
                self.logger.warning("Job not found for deletion", job_id=job_id)
            return success
        except Exception as e:
            self.logger.error(
                f"Error deleting job: {e}",
                exc_info=True,
                job_id=job_id,
                error_type=type(e).__name__,
            )
            raise

    def list_jobs(
        self,
        status: JobStatus | None = None,
        limit: int = 100,
    ) -> list[JobInfo]:
        """
        List jobs with optional filtering.

        Args:
            status: Status filter (None=all)
            limit: Maximum count

        Returns:
            List of job info objects
        """
        jobs_data = self.storage.list_jobs(limit=limit)

        # Filter by status if specified
        if status:
            jobs_data = [j for j in jobs_data if j.get("status") == status.value]

        return [JobInfo(job_data) for job_data in jobs_data]

    def pause_job(self, job_id: str) -> JobInfo:
        """
        Pause a job.

        Args:
            job_id: Job ID

        Returns:
            Updated job info

        Raises:
            JobNotFoundError: If job not found
            ValueError: If job cannot be paused in current state
        """
        job = self.get_job(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} not found")

        if not job.can_pause():
            raise ValueError(f"Job cannot be paused in {job.status} state")

        return self.update_job(job_id, status=JobStatus.PAUSED)

    def resume_job(self, job_id: str) -> JobInfo:
        """
        Resume a paused job.

        Args:
            job_id: Job ID

        Returns:
            Updated job info

        Raises:
            JobNotFoundError: If job not found
            ValueError: If job cannot be resumed in current state
        """
        job = self.get_job(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} not found")

        if not job.can_resume():
            raise ValueError(f"Job cannot be resumed in {job.status} state")

        return self.update_job(job_id, status=JobStatus.SCHEDULED)

    def cancel_job(self, job_id: str) -> JobInfo:
        """
        Cancel a job.

        Args:
            job_id: Job ID

        Returns:
            Updated job info

        Raises:
            JobNotFoundError: If job not found
            ValueError: If job cannot be cancelled in current state
        """
        job = self.get_job(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} not found")

        if not job.can_cancel():
            raise ValueError(f"Job cannot be cancelled in {job.status} state")

        return self.update_job(job_id, status=JobStatus.CANCELLED)
