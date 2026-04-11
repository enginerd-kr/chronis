"""File-based storage adapter with JSON persistence."""

import json
import logging
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.type_defs import JobStorageData, JobUpdateData
from chronis.utils.time import utc_now

logger = logging.getLogger(__name__)


class FileStorage(JobStorageAdapter):
    """
    File-based storage adapter with JSON persistence.

    Data is stored as a single JSON file on disk. Suitable for local development
    and single-process deployments where persistence across restarts is needed.

    Thread-safe via threading.Lock (single-process only).
    For multi-process or distributed deployments, use RedisStorage or PostgreSQLStorage.
    """

    def __init__(
        self,
        file_path: str | Path = "chronis_jobs.json",
        auto_create: bool = True,
    ) -> None:
        """
        Initialize file-based storage adapter.

        Args:
            file_path: Path to the JSON file (str or Path). Defaults to 'chronis_jobs.json'.
            auto_create: If True (default), create the file and parent directories if they
                don't exist. If False, raise FileNotFoundError if the file is missing.

        Raises:
            FileNotFoundError: If auto_create is False and the file doesn't exist
            ValueError: If the file contains invalid JSON or is not a JSON object
        """
        self._file_path = Path(file_path).resolve()
        self._lock = threading.Lock()

        self._jobs: dict[str, JobStorageData] = {}

        if self._file_path.exists():
            self._jobs = self._load()
        elif auto_create:
            self._file_path.parent.mkdir(parents=True, exist_ok=True)
            self._save()
        else:
            raise FileNotFoundError(
                f"Storage file not found: {self._file_path}\n"
                f"Set auto_create=True to create it automatically."
            )

        logger.info("FileStorage initialized: %s", self._file_path)

    def _load(self) -> dict[str, JobStorageData]:
        """Load jobs from the JSON file."""
        try:
            text = self._file_path.read_text(encoding="utf-8")
        except OSError as e:
            raise ValueError(f"Failed to read storage file {self._file_path}: {e}") from e

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {self._file_path}: {e}") from e

        if not isinstance(data, dict):
            raise ValueError(
                f"Expected JSON object in {self._file_path}, "
                f"got {type(data).__name__}"
            )

        for job_data in data.values():
            if isinstance(job_data.get("args"), list):
                job_data["args"] = tuple(job_data["args"])

        return data  # type: ignore[return-value]

    def _save(self) -> None:
        """Atomically persist jobs to the JSON file."""
        dir_path = self._file_path.parent
        fd, tmp_path = tempfile.mkstemp(dir=str(dir_path), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._jobs, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, str(self._file_path))
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def create_job(self, job_data: JobStorageData) -> JobStorageData:
        """Create a job."""
        with self._lock:
            job_id = job_data["job_id"]
            if job_id in self._jobs:
                raise ValueError(f"Job {job_id} already exists")
            self._jobs[job_id] = job_data.copy()  # type: ignore[typeddict-item]
            self._save()
            return job_data

    def get_job(self, job_id: str) -> JobStorageData | None:
        """Get a job."""
        with self._lock:
            job = self._jobs.get(job_id)
            return job.copy() if job else None  # type: ignore[union-attr]

    def update_job(self, job_id: str, updates: JobUpdateData) -> JobStorageData:
        """Update a job."""
        with self._lock:
            if job_id not in self._jobs:
                raise ValueError(f"Job {job_id} not found")
            self._jobs[job_id].update(updates)  # type: ignore[typeddict-item]
            self._jobs[job_id]["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]
            self._save()
            return self._jobs[job_id].copy()  # type: ignore[return-value]

    def compare_and_swap_job(
        self,
        job_id: str,
        expected_values: dict[str, Any],
        updates: JobUpdateData,
    ) -> tuple[bool, JobStorageData | None]:
        """
        Atomically update a job only if current values match expected values.

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
        with self._lock:
            if job_id not in self._jobs:
                raise ValueError(f"Job {job_id} not found")

            current_job = self._jobs[job_id]

            for field, expected_value in expected_values.items():
                current_value = current_job.get(field)
                if current_value != expected_value:
                    return (False, None)

            self._jobs[job_id].update(updates)  # type: ignore[typeddict-item]
            self._jobs[job_id]["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]
            self._save()
            return (True, self._jobs[job_id].copy())  # type: ignore[return-value]

    def delete_job(self, job_id: str) -> bool:
        """Delete a job."""
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                self._save()
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
        with self._lock:
            jobs = [j.copy() for j in self._jobs.values()]  # type: ignore[misc]

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

            if "updated_at_lte" in filters:
                jobs = [
                    j
                    for j in jobs
                    if j.get("updated_at") is not None
                    and j.get("updated_at") <= filters["updated_at_lte"]
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
