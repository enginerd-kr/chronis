"""In-memory storage adapter for testing."""

from datetime import datetime
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

    def query_ready_jobs(
        self, current_time: datetime, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Query jobs ready for execution (next_run_time <= current_time and status is SCHEDULED).

        Args:
            current_time: Current time for comparison
            limit: Maximum number of jobs to return (None for unlimited)

        Returns:
            List of ready jobs, sorted by next_run_time (oldest first)
        """
        ready_jobs = []
        current_iso = current_time.isoformat()

        for job in self._jobs.values():
            # Only SCHEDULED jobs are ready to execute
            if job.get("status") != "scheduled":
                continue

            next_run = job.get("next_run_time")
            if next_run and next_run <= current_iso:
                ready_jobs.append(job)

        # Sort by next_run_time (oldest first)
        ready_jobs.sort(key=lambda j: j.get("next_run_time", ""))

        # Apply limit if specified
        if limit is not None and limit > 0:
            return ready_jobs[:limit]

        return ready_jobs
