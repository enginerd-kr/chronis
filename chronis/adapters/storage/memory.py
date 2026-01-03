"""In-memory storage adapter for testing."""

import logging
from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.type_defs import JobStorageData, JobUpdateData
from chronis.utils.time import utc_now

logger = logging.getLogger(__name__)


class InMemoryStorageAdapter(JobStorageAdapter):
    """
    In-memory storage adapter for testing and local development.

    WARNING: Not for production use. Data is not persisted and not distributed.
    Use RedisStorageAdapter or PostgreSQLStorageAdapter in production.
    """

    def __init__(self) -> None:
        """Initialize in-memory storage adapter."""
        self._jobs: dict[str, JobStorageData] = {}

        logger.warning(
            "InMemoryStorageAdapter is for testing/development only. "
            "Use RedisStorageAdapter, PostgreSQLStorageAdapter, or DynamoDBStorageAdapter in production."
        )

    def create_job(self, job_data: JobStorageData) -> JobStorageData:
        """Create a job."""
        job_id = job_data["job_id"]
        if job_id in self._jobs:
            raise ValueError(f"Job {job_id} already exists")
        self._jobs[job_id] = job_data.copy()  # type: ignore[typeddict-item]
        return job_data

    def get_job(self, job_id: str) -> JobStorageData | None:
        """Get a job."""
        return self._jobs.get(job_id)

    def update_job(self, job_id: str, updates: JobUpdateData) -> JobStorageData:
        """Update a job."""
        if job_id not in self._jobs:
            raise ValueError(f"Job {job_id} not found")
        self._jobs[job_id].update(updates)  # type: ignore[typeddict-item]
        self._jobs[job_id]["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]
        return self._jobs[job_id]

    def compare_and_swap_job(
        self,
        job_id: str,
        expected_values: dict[str, Any],
        updates: JobUpdateData,
    ) -> tuple[bool, JobStorageData | None]:
        """
        Atomically update a job only if current values match expected values.

        This implements the Compare-and-Swap (CAS) pattern for optimistic concurrency control.

        Args:
            job_id: Job ID to update
            expected_values: Dictionary of field-value pairs that must match current values
            updates: Dictionary of fields to update (only applied if all expectations match)

        Returns:
            Tuple of (success, updated_job_data):
                - success: True if update succeeded, False if expectations didn't match
                - updated_job_data: Updated job data if success=True, None if success=False

        Raises:
            ValueError: If job_id not found
        """
        if job_id not in self._jobs:
            raise ValueError(f"Job {job_id} not found")

        current_job = self._jobs[job_id]

        # Compare: Check if all expected values match current values
        for field, expected_value in expected_values.items():
            current_value = current_job.get(field)
            if current_value != expected_value:
                # Mismatch - return failure without updating
                return (False, None)

        # Swap: All expectations matched, apply updates
        self._jobs[job_id].update(updates)  # type: ignore[typeddict-item]
        self._jobs[job_id]["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]
        return (True, self._jobs[job_id])

    def delete_job(self, job_id: str) -> bool:
        """Delete a job."""
        if job_id in self._jobs:
            del self._jobs[job_id]
            return True
        return False

    def query_jobs(
        self,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[JobStorageData]:
        """
        Query jobs with flexible filters.

        Args:
            filters: Dictionary of filter conditions
            limit: Maximum number of jobs to return
            offset: Number of jobs to skip

        Returns:
            List of jobs matching filters, sorted by next_run_time
        """
        jobs = list(self._jobs.values())

        if filters:
            if "status" in filters:
                jobs = [j for j in jobs if j.get("status") == filters["status"]]

            if "next_run_time_lte" in filters:
                jobs = [
                    j
                    for j in jobs
                    if j.get("next_run_time") is not None
                    and j.get("next_run_time") <= filters["next_run_time_lte"]
                ]

            for key, value in filters.items():
                if key.startswith("metadata."):
                    metadata_key = key.replace("metadata.", "")
                    jobs = [j for j in jobs if j.get("metadata", {}).get(metadata_key) == value]

        jobs.sort(key=lambda j: j.get("next_run_time") or "")

        if offset:
            jobs = jobs[offset:]
        if limit:
            jobs = jobs[:limit]

        return jobs
