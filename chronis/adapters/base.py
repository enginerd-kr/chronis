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

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer                    │
        │ WHO CALLS:      Chronis Core (JobService)                    │
        │ WHEN CALLED:    When user creates a job via create_*_job()   │
        └──────────────────────────────────────────────────────────────┘

        Required fields in job_data:
            - job_id: str
            - status: str
            - next_run_time: str (ISO format, UTC)

        Optional fields:
            - metadata: dict (user-defined, for extensions like multi-tenancy)
            - Any other adapter-specific fields

        Returns:
            Created job data

        Raises:
            ValueError: If job_id already exists
        """
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> JobStorageData | None:
        """
        Get a job by ID.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer                    │
        │ WHO CALLS:      Chronis Core (JobService, user queries)      │
        │ WHEN CALLED:    When retrieving job details                  │
        └──────────────────────────────────────────────────────────────┘

        Args:
            job_id: Job ID to retrieve

        Returns:
            Job data if found, None if not found
        """
        pass

    @abstractmethod
    def update_job(self, job_id: str, updates: JobUpdateData) -> JobStorageData:
        """
        Update a job.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer                    │
        │ WHO CALLS:      Chronis Core (JobService, state transitions) │
        │ WHEN CALLED:    Status changes, retries, user updates        │
        └──────────────────────────────────────────────────────────────┘

        Args:
            job_id: Job ID to update
            updates: Dictionary of fields to update

        Returns:
            Updated job data

        Raises:
            ValueError: If job_id not found
        """
        pass

    @abstractmethod
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer                    │
        │ WHO CALLS:      Chronis Core, user via delete_job()          │
        │ WHEN CALLED:    One-time jobs after completion, user deletes │
        └──────────────────────────────────────────────────────────────┘

        Args:
            job_id: Job ID to delete

        Returns:
            True if deleted, False if not found
        """
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

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer                    │
        │ WHO CALLS:      Chronis Core (SchedulingOrchestrator)        │
        │ WHEN CALLED:    Every polling cycle to find due jobs         │
        ├──────────────────────────────────────────────────────────────┤
        │ CRITICAL: MUST return misfire fields for detection to work   │
        │  - if_missed                                                 │
        │  - misfire_threshold_seconds                                 │
        │  - last_run_time                                             │
        │  - last_scheduled_time                                       │
        └──────────────────────────────────────────────────────────────┘

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

    @abstractmethod
    def count_jobs(self, filters: dict[str, Any] | None = None) -> int:
        """
        Count jobs with optional filters.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer                    │
        │ WHO CALLS:      Chronis Core, user via count()               │
        │ WHEN CALLED:    Monitoring, status checks, pagination        │
        └──────────────────────────────────────────────────────────────┘

        Args:
            filters: Dictionary of filter conditions (None = count all)

        Returns:
            Number of matching jobs
        """
        pass

    @abstractmethod
    def update_job_run_times(
        self,
        job_id: str,
        scheduled_time: str,
        actual_time: str,
        next_run_time: str | None,
    ) -> JobStorageData:
        """
        Update job execution times after a run.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer                    │
        │ WHO CALLS:      Chronis Core scheduler                       │
        │ WHEN CALLED:    After every job execution                    │
        │                 (both normal and misfired)                   │
        ├──────────────────────────────────────────────────────────────┤
        │ RESPONSIBILITY SPLIT:                                        │
        │                                                              │
        │ Chronis Core (caller):                                       │
        │  ✓ Executes the job                                          │
        │  ✓ Calculates scheduled_time (what was planned)              │
        │  ✓ Calculates actual_time (when it ran)                      │
        │  ✓ Calculates next_run_time (next schedule)                  │
        │  ✓ Calls this method with calculated values                  │
        │                                                              │
        │ Storage Adapter (implementer):                               │
        │  ✓ Receives time values from Core                            │
        │  ✓ Persists to database/storage                              │
        │  ✓ Returns updated job data                                  │
        │                                                              │
        │ Does NOT:                                                    │
        │  ✗ Calculate time values (Core does this)                   │
        │  ✗ Detect misfire (MisfireClassifier does this)             │
        │  ✗ Handle misfire policy (MisfireHandler does this)         │
        └──────────────────────────────────────────────────────────────┘

        Args:
            job_id: Job ID
            scheduled_time: When this run was scheduled for (ISO format, calculated by Core)
            actual_time: When this run actually executed (ISO format, calculated by Core)
            next_run_time: Next scheduled run time (ISO format or None, calculated by Core)

        Returns:
            Updated job data

        Implementation Requirements:
            MUST update these fields in storage:
            - last_scheduled_time = scheduled_time
            - last_run_time = actual_time
            - next_run_time = next_run_time
            - updated_at = current timestamp

        Example implementations:
            InMemory: self._jobs[job_id].update({...})
            Redis: self.redis.hset(f"job:{job_id}", mapping={...})
            PostgreSQL: UPDATE jobs SET ... WHERE job_id = $1

        Raises:
            ValueError: If job_id not found
        """
        pass


class LockAdapter(ABC):
    """Distributed lock adapter abstract class."""

    @abstractmethod
    def acquire(self, lock_key: str, ttl_seconds: int, blocking: bool = False) -> bool:
        """
        Acquire a distributed lock.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Lock adapter developer                       │
        │ WHO CALLS:      Chronis Core (ExecutionCoordinator)          │
        │ WHEN CALLED:    Before job execution to prevent duplicates   │
        └──────────────────────────────────────────────────────────────┘

        Args:
            lock_key: Unique lock identifier (e.g., "scheduler:lock:job-id")
            ttl_seconds: Time-to-live for the lock in seconds
            blocking: If True, wait until lock is available

        Returns:
            True if lock acquired, False otherwise
        """
        pass

    @abstractmethod
    def release(self, lock_key: str) -> bool:
        """
        Release a distributed lock.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Lock adapter developer                       │
        │ WHO CALLS:      Chronis Core (ExecutionCoordinator)          │
        │ WHEN CALLED:    After job execution completes                │
        └──────────────────────────────────────────────────────────────┘

        Args:
            lock_key: Unique lock identifier

        Returns:
            True if lock released, False if lock not held
        """
        pass
