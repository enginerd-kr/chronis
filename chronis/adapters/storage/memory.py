"""In-memory storage adapter for testing."""

from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.utils.time import utc_now


class InMemoryStorageAdapter(JobStorageAdapter):
    """In-memory storage adapter (for testing)."""

    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}

    def create_job(self, job_data: dict[str, Any]) -> dict[str, Any]:
        """Create a job."""
        job_id = job_data["job_id"]
        if job_id in self._jobs:
            raise ValueError(f"Job {job_id} already exists")
        self._jobs[job_id] = job_data.copy()
        return job_data

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Get a job."""
        return self._jobs.get(job_id)

    def update_job(self, job_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        """Update a job."""
        if job_id not in self._jobs:
            raise ValueError(f"Job {job_id} not found")
        self._jobs[job_id].update(updates)
        self._jobs[job_id]["updated_at"] = utc_now().isoformat()
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
    ) -> list[dict[str, Any]]:
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
                    if j.get("next_run_time", "") <= filters["next_run_time_lte"]
                ]

            # Metadata filters (support nested keys like "metadata.tenant_id")
            for key, value in filters.items():
                if key.startswith("metadata."):
                    metadata_key = key.replace("metadata.", "")
                    jobs = [
                        j
                        for j in jobs
                        if j.get("metadata", {}).get(metadata_key) == value
                    ]

        # Sort by next_run_time (oldest first)
        jobs.sort(key=lambda j: j.get("next_run_time", ""))

        # Apply offset and limit
        if offset:
            jobs = jobs[offset:]
        if limit:
            jobs = jobs[:limit]

        return jobs

    def get_all_jobs(self) -> list[dict[str, Any]]:
        """
        Get all jobs.

        Returns:
            List of all job dictionaries
        """
        return list(self._jobs.values())
