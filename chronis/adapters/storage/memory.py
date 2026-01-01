"""In-memory storage adapter for testing."""

import logging
from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.type_defs import JobStorageData, JobUpdateData
from chronis.utils.time import utc_now

logger = logging.getLogger(__name__)


class InMemoryStorageAdapter(JobStorageAdapter):
    """
    In-memory storage adapter (for testing and local development only).

    ⚠️ WARNING:
        This adapter is for testing and local development ONLY.
        - Data is NOT persisted (lost on restart)
        - NOT distributed (single process only)
        - NOT production-ready

        For production use, switch to:
        - RedisStorageAdapter (recommended)
        - PostgreSQLStorageAdapter
        - DynamoDBStorageAdapter
        - Or implement a custom adapter
    """

    def __init__(self) -> None:
        """
        Initialize in-memory storage adapter.

        Logs a warning to remind developers this is not for production use.
        """
        self._jobs: dict[str, JobStorageData] = {}

        # Warn about production usage
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
            # Status filter
            if "status" in filters:
                jobs = [j for j in jobs if j.get("status") == filters["status"]]

            # Time filter
            if "next_run_time_lte" in filters:
                jobs = [
                    j
                    for j in jobs
                    if j.get("next_run_time") is not None
                    and j.get("next_run_time") <= filters["next_run_time_lte"]
                ]

            # Metadata filters (support nested keys like "metadata.tenant_id")
            for key, value in filters.items():
                if key.startswith("metadata."):
                    metadata_key = key.replace("metadata.", "")
                    jobs = [j for j in jobs if j.get("metadata", {}).get(metadata_key) == value]

        # Sort by next_run_time (oldest first)
        jobs.sort(key=lambda j: j.get("next_run_time") or "")

        # Apply offset and limit
        if offset:
            jobs = jobs[offset:]
        if limit:
            jobs = jobs[:limit]

        return jobs

    def count_jobs(self, filters: dict[str, Any] | None = None) -> int:
        """Count jobs matching filters."""
        return len(self.query_jobs(filters=filters))

    def update_job_run_times(
        self,
        job_id: str,
        scheduled_time: str,
        actual_time: str,
        next_run_time: str | None,
    ) -> JobStorageData:
        """Update job run times after execution."""
        if job_id not in self._jobs:
            raise ValueError(f"Job {job_id} not found")

        self._jobs[job_id].update(  # type: ignore[typeddict-item]
            {
                "last_scheduled_time": scheduled_time,
                "last_run_time": actual_time,
                "next_run_time": next_run_time,
                "updated_at": utc_now().isoformat(),
            }
        )

        return self._jobs[job_id]
