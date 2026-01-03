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

    def compare_and_swap_job(
        self,
        job_id: str,
        expected_values: dict[str, Any],
        updates: JobUpdateData,
    ) -> tuple[bool, JobStorageData | None]:
        """
        Atomically update a job only if current values match expected values.

        This implements the Compare-and-Swap (CAS) pattern for optimistic concurrency control.
        The operation is atomic - either all conditions match and the update succeeds,
        or the update is rejected without any changes.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer (RECOMMENDED)      │
        │ WHO CALLS:      Chronis Core (ExecutionCoordinator)          │
        │ WHEN CALLED:    Job execution with optimistic locking        │
        │ ATOMICITY:      MUST be atomic (single transaction/operation)│
        │                                                              │
        │ DEFAULT IMPLEMENTATION: Non-atomic fallback provided         │
        │ ⚠️  Override for production multi-instance deployments      │
        └──────────────────────────────────────────────────────────────┘

        DEFAULT IMPLEMENTATION:
            This base implementation uses get → check → update pattern which is
            NOT atomic. For production use with multiple instances, override this
            method with an atomic implementation using your storage backend's
            native CAS/optimistic locking.

            Examples of atomic implementations:
            - Redis: Use WATCH/MULTI/EXEC transaction
            - PostgreSQL: Use WHERE conditions in UPDATE statement
            - DynamoDB: Use ConditionExpression in UpdateItem
            - MongoDB: Use findAndModify with query conditions

            ⚠️  WARNING: This default implementation is NOT atomic and NOT safe
            for distributed deployments. It may cause duplicate job execution.

            Use this default only for:
            - Single-instance deployments
            - Testing/development environments
            - Storage backends without atomic operations (at your own risk)

        Example:
            # Only update if status is still SCHEDULED and next_run_time hasn't changed
            success, updated_job = storage.compare_and_swap_job(
                job_id="job-123",
                expected_values={"status": "scheduled", "next_run_time": "2025-01-01T00:00:00Z"},
                updates={"status": "running", "next_run_time": "2025-01-01T01:00:00Z"}
            )
            if success:
                # Job was updated - safe to execute
            else:
                # Another instance already updated it - skip execution

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
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(
            f"Using non-atomic fallback compare_and_swap_job for {self.__class__.__name__}. "
            "This is NOT safe for multi-instance deployments. "
            "Override this method with an atomic implementation for production use."
        )

        # Get current job
        job_data = self.get_job(job_id)
        if job_data is None:
            raise ValueError(f"Job {job_id} not found")

        # Compare: Check if all expected values match
        for field, expected_value in expected_values.items():
            current_value = job_data.get(field)
            if current_value != expected_value:
                return (False, None)

        # Swap: Apply updates (⚠️ non-atomic - TOCTOU race condition possible)
        updated_job = self.update_job(job_id, updates)
        return (True, updated_job)

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

    def count_jobs(self, filters: dict[str, Any] | None = None) -> int:
        """
        Count jobs with optional filters.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Storage adapter developer (OPTIONAL)         │
        │ WHO CALLS:      Chronis Core, user via count()               │
        │ WHEN CALLED:    Monitoring, status checks, pagination        │
        │                                                              │
        │ DEFAULT IMPLEMENTATION: Query and count                      │
        │ Override for optimization (SQL COUNT, Redis SCARD)           │
        └──────────────────────────────────────────────────────────────┘

        DEFAULT IMPLEMENTATION:
            This base implementation queries all matching jobs and returns the count.
            This is fine for:
            - Small datasets (< 10k jobs)
            - Development/testing environments
            - Storage backends without native count operations

            For better performance, override this method with optimized counting:
            - PostgreSQL: SELECT COUNT(*) FROM jobs WHERE ...
            - Redis: SCARD on status index, ZCOUNT on time index
            - DynamoDB: Query with Select='COUNT'
            - MongoDB: countDocuments()

        Args:
            filters: Dictionary of filter conditions (None = count all)

        Returns:
            Number of matching jobs
        """
        return len(self.query_jobs(filters=filters))


class LockAdapter(ABC):
    """
    Distributed lock adapter abstract class.

    Backend-agnostic interface supporting:
    - Lock ownership tracking (token-based)
    - Atomic operations (verify ownership before release/extend)
    - TTL extension for long-running jobs
    - Optional blocking mode with timeout
    """

    @abstractmethod
    def acquire(
        self,
        lock_key: str,
        ttl_seconds: int,
        blocking: bool = False,
        timeout: float | None = None,
        owner_id: str | None = None,
    ) -> bool:
        """
        Acquire a distributed lock.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Lock adapter developer                       │
        │ WHO CALLS:      Chronis Core (ExecutionCoordinator)          │
        │ WHEN CALLED:    Before job execution to prevent duplicates   │
        └──────────────────────────────────────────────────────────────┘

        Implementation Requirements (ALL backends):
        - MUST generate unique owner_id if not provided (e.g., UUID)
        - MUST store owner_id with lock (for release/extend verification)
        - MUST be atomic (no race conditions between check and set)
        - MUST respect TTL (auto-expire after ttl_seconds)
        - SHOULD support blocking mode efficiently (backend-specific)

        Backend-Specific Implementations:
        - Redis: Use SET NX EX with owner_id as value
        - DynamoDB: Use ConditionalPut with OwnerId attribute
        - InMemory: Store (owner_id, expiry_time) tuple with threading.Lock

        Args:
            lock_key: Unique lock identifier (e.g., "scheduler:lock:job-id")
            ttl_seconds: Time-to-live for the lock in seconds
            blocking: If True, wait for lock to become available
            timeout: Max wait time in seconds (None = wait forever)
                     Only used when blocking=True
            owner_id: Optional owner identifier for lock tracking
                      If None, adapter must generate a unique ID (e.g., UUID)

        Returns:
            True if lock acquired, False otherwise

        Example:
            >>> lock_adapter = RedisLockAdapter(redis_client)
            >>> # Non-blocking acquire
            >>> acquired = lock_adapter.acquire("job-123", ttl_seconds=60)
            >>> # Blocking acquire with timeout
            >>> acquired = lock_adapter.acquire(
            ...     "job-123", ttl_seconds=60, blocking=True, timeout=10
            ... )
        """
        pass

    @abstractmethod
    def release(self, lock_key: str, owner_id: str | None = None) -> bool:
        """
        Release a distributed lock.

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Lock adapter developer                       │
        │ WHO CALLS:      Chronis Core (ExecutionCoordinator)          │
        │ WHEN CALLED:    After job execution completes                │
        └──────────────────────────────────────────────────────────────┘

        Implementation Requirements (ALL backends):
        - MUST verify owner_id matches before release
        - MUST be atomic (check owner + delete in single operation)
        - MUST return False if not owner (don't raise exception)
        - SHOULD signal waiting processes (if blocking mode supported)

        Backend-Specific Implementations:
        - Redis: Use Lua script (GET + DEL + LPUSH to signal key)
        - DynamoDB: Use ConditionalDelete with OwnerId check
        - InMemory: Check owner within threading.Lock, then notify Condition

        Args:
            lock_key: Unique lock identifier
            owner_id: Owner identifier (uses instance owner_id if None)
                      Must match the owner_id used in acquire()

        Returns:
            True if lock released, False if:
            - Lock doesn't exist
            - Owner mismatch (not our lock)

        Security Note:
            This prevents a common race condition where Process A's lock
            expires, Process B acquires it, then Process A releases B's lock.

        Example:
            >>> lock_adapter.acquire("job-123", ttl_seconds=60)
            >>> try:
            ...     # Do work
            ...     pass
            ... finally:
            ...     lock_adapter.release("job-123")
        """
        pass

    @abstractmethod
    def extend(
        self,
        lock_key: str,
        ttl_seconds: int,
        owner_id: str | None = None,
    ) -> bool:
        """
        Extend lock TTL (for long-running jobs).

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Lock adapter developer                       │
        │ WHO CALLS:      Chronis Core, user code                      │
        │ WHEN CALLED:    During job execution to prevent TTL expiry   │
        └──────────────────────────────────────────────────────────────┘

        Implementation Requirements (ALL backends):
        - MUST verify owner_id matches before extending
        - MUST be atomic (check owner + extend TTL)
        - MUST return False if not owner

        Backend-Specific Implementations:
        - Redis: Use Lua script (GET + EXPIRE)
        - DynamoDB: Use ConditionalUpdate with OwnerId check
        - InMemory: Check owner within threading.Lock, update expiry

        Args:
            lock_key: Unique lock identifier
            ttl_seconds: New TTL duration in seconds (not additive)
            owner_id: Owner identifier (uses instance owner_id if None)

        Returns:
            True if extended, False if:
            - Lock doesn't exist
            - Owner mismatch (not our lock)

        Use Case:
            Long-running jobs that exceed initial TTL can extend the lock
            to prevent it from expiring while still working.

        Example:
            >>> lock_adapter.acquire("job-123", ttl_seconds=60)
            >>> # After 30 seconds of work...
            >>> lock_adapter.extend("job-123", ttl_seconds=60)  # Extend by 60s
            >>> # Continue working...
            >>> lock_adapter.release("job-123")
        """
        pass

    @abstractmethod
    def reset(self, lock_key: str) -> bool:
        """
        Forcibly delete lock (crash recovery).

        ┌──────────────────────────────────────────────────────────────┐
        │                   IMPLEMENTATION CONTRACT                     │
        ├──────────────────────────────────────────────────────────────┤
        │ WHO IMPLEMENTS: Lock adapter developer                       │
        │ WHO CALLS:      Admin/maintenance tools                      │
        │ WHEN CALLED:    Cleanup of orphaned locks after crashes      │
        └──────────────────────────────────────────────────────────────┘

        Implementation Requirements:
        - MUST delete lock without ownership verification
        - SHOULD be used with extreme caution (bypasses safety)

        ⚠️ WARNING: This bypasses ownership checks!
        Use only for:
        - Cleaning up orphaned locks after process crashes
        - Administrative maintenance
        - Testing/development

        Args:
            lock_key: Unique lock identifier

        Returns:
            True if deleted, False if lock didn't exist

        Example:
            >>> # After application crash, cleanup orphaned locks
            >>> lock_adapter.reset("job-123")
        """
        pass
