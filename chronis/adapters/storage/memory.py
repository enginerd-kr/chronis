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

    def list_jobs(self, is_active: bool | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """List jobs."""
        jobs = list(self._jobs.values())

        # Note: is_active parameter is kept for adapter interface compatibility
        # but is ignored since we now use status field
        return jobs[:limit]

    def query_ready_jobs(self, current_time: datetime) -> list[dict[str, Any]]:
        """Query jobs ready for execution (next_run_time <= current_time and status is SCHEDULED)."""
        ready_jobs = []
        current_iso = current_time.isoformat()
        for job in self._jobs.values():
            # Only SCHEDULED jobs are ready to execute
            if job.get("status") != "scheduled":
                continue

            next_run = job.get("next_run_time")
            if next_run and next_run <= current_iso:
                ready_jobs.append(job)

        return ready_jobs
