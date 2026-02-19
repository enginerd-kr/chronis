"""Base scheduler abstract class."""

import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.common.exceptions import (
    InvalidJobStateError,
    JobAlreadyExistsError,
    JobNotFoundError,
)
from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.execution.coordinator import ExecutionCoordinator
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.state import JobStatus
from chronis.core.state.enums import TriggerType
from chronis.utils.logging import ContextLogger, _default_logger
from chronis.utils.time import utc_now


class BaseScheduler(ABC):
    """
    Abstract base class for all scheduler implementations.

    Provides:
    1. Common job CRUD operations
    2. Job execution coordination
    3. Callback management
    4. Graceful shutdown

    Subclasses must implement:
    - start(): Start the scheduler
    - stop(): Stop the scheduler
    - _schedule_jobs(): Schedule jobs using specific strategy
    """

    DEFAULT_MAX_WORKERS = 20

    def __init__(
        self,
        storage_adapter: JobStorageAdapter,
        lock_adapter: LockAdapter,
        max_workers: int | None = None,
        lock_ttl_seconds: int = 300,
        verbose: bool = False,
        logger: ContextLogger | None = None,
        on_failure: OnFailureCallback | None = None,
        on_success: OnSuccessCallback | None = None,
    ) -> None:
        """
        Initialize base scheduler.

        Args:
            storage_adapter: Job storage adapter (required)
            lock_adapter: Distributed lock adapter (required)
            max_workers: Maximum number of worker threads (default: 20)
            lock_ttl_seconds: Lock TTL (seconds)
            verbose: Enable verbose logging (default: False)
            logger: Custom logger (uses default if None)
            on_failure: Global failure handler for all jobs (optional)
            on_success: Global success handler for all jobs (optional)
        """
        self.storage = storage_adapter
        self.lock = lock_adapter
        self.max_workers = max_workers or self.DEFAULT_MAX_WORKERS
        self.lock_ttl_seconds = lock_ttl_seconds
        self.verbose = verbose
        self.on_failure = on_failure
        self.on_success = on_success

        # Running state
        self._running = False

        # Job function and callback registries
        self._job_registry: dict[str, Callable] = {}
        self._failure_handler_registry: dict[str, OnFailureCallback] = {}
        self._success_handler_registry: dict[str, OnSuccessCallback] = {}
        self._registry_lock = threading.RLock()

        # Initialize structured logger
        base_logger = (
            logger.logger if isinstance(logger, ContextLogger) else (logger or _default_logger)
        )
        self.logger = ContextLogger(base_logger, {"component": self.__class__.__name__})

        # Initialize execution coordinator (shared across all scheduler types)
        self._init_execution_coordinator()

    def _init_execution_coordinator(self) -> None:
        """Initialize execution coordinator with thread pool."""
        from concurrent.futures import ThreadPoolExecutor

        # Initialize ThreadPoolExecutor for job execution
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="chronis-worker-"
        )

        self._execution_coordinator = ExecutionCoordinator(
            storage=self.storage,
            lock=self.lock,
            executor=self._executor,
            function_registry=self._job_registry,
            failure_handler_registry=self._failure_handler_registry,
            success_handler_registry=self._success_handler_registry,
            global_on_failure=self.on_failure,
            global_on_success=self.on_success,
            logger=self.logger,
            lock_ttl_seconds=self.lock_ttl_seconds,
            verbose=self.verbose,
        )

    @abstractmethod
    def start(self) -> None:
        """
        Start scheduler (implementation-specific).

        Subclasses must implement this to start their scheduling mechanism:
        - PollingScheduler: Start APScheduler for periodic polling
        - EventScheduler: Start event listener
        """
        pass

    @abstractmethod
    def stop(self) -> dict[str, Any]:
        """
        Stop scheduler gracefully (implementation-specific).

        Returns:
            Dictionary with shutdown status
        """
        pass

    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running

    def register_job_function(self, name: str, func: Callable) -> None:
        """
        Register job function (thread-safe).

        Args:
            name: Function name (e.g., "my_module.my_job")
            func: Function object
        """
        with self._registry_lock:
            self._job_registry[name] = func

    # ========================================
    # Job CRUD Operations (Common)
    # ========================================

    def create_job(self, job: JobDefinition) -> JobInfo:
        """
        Create new job.

        Args:
            job: Job definition

        Returns:
            Created job info

        Raises:
            JobAlreadyExistsError: Job already exists
        """
        # Register on_failure handler if provided
        if job.on_failure:
            with self._registry_lock:
                self._failure_handler_registry[job.job_id] = job.on_failure

        # Register on_success handler if provided
        if job.on_success:
            with self._registry_lock:
                self._success_handler_registry[job.job_id] = job.on_success

        # Create job in storage
        job_data = job.to_dict()
        try:
            result = self.storage.create_job(job_data)  # type: ignore[arg-type]
            return JobInfo.from_dict(result)  # type: ignore[arg-type]
        except ValueError as e:
            raise JobAlreadyExistsError(str(e)) from e

    def get_job(self, job_id: str) -> JobInfo | None:
        """
        Get job by ID.

        Args:
            job_id: Job ID

        Returns:
            Job info or None if not found
        """
        job_data = self.storage.get_job(job_id)
        return JobInfo.from_dict(job_data) if job_data else None  # type: ignore[arg-type]

    def query_jobs(
        self, filters: dict[str, Any] | None = None, limit: int | None = None
    ) -> list[JobInfo]:
        """
        Query jobs with flexible filters.

        Args:
            filters: Dictionary of filter conditions (None = get all jobs)
            limit: Maximum number of jobs to return

        Returns:
            List of jobs matching filters
        """
        jobs_data = self.storage.query_jobs(filters=filters, limit=limit)
        return [JobInfo.from_dict(job_data) for job_data in jobs_data]  # type: ignore[arg-type]

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
            JobNotFoundError: Job not found
        """
        # Build updates dictionary
        updates_dict: dict[str, Any] = {
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

        if not updates_dict:
            current = self.get_job(job_id)
            if not current:
                raise JobNotFoundError(f"Job {job_id} not found")
            return current

        updates_dict["updated_at"] = utc_now().isoformat()

        try:
            result = self.storage.update_job(job_id, updates_dict)  # type: ignore[arg-type]
            return JobInfo.from_dict(result)  # type: ignore[arg-type]
        except ValueError as e:
            raise JobNotFoundError(str(e)) from e

    def delete_job(self, job_id: str) -> bool:
        """
        Delete job.

        Args:
            job_id: Job ID

        Returns:
            Deletion success
        """
        return self.storage.delete_job(job_id)

    def pause_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job.

        Args:
            job_id: Job ID to pause

        Returns:
            True if job was paused

        Raises:
            JobNotFoundError: Job not found
            InvalidJobStateError: Job not in pausable state
        """
        return self._transition_job_state(
            job_id,
            allowed=(JobStatus.SCHEDULED, JobStatus.PENDING),
            new_status=JobStatus.PAUSED,
            action="paused",
        )

    def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job.

        Args:
            job_id: Job ID to resume

        Returns:
            True if job was resumed

        Raises:
            JobNotFoundError: Job not found
            InvalidJobStateError: Job not paused
        """
        return self._transition_job_state(
            job_id,
            allowed=(JobStatus.PAUSED,),
            new_status=JobStatus.SCHEDULED,
            action="resumed",
        )

    def _transition_job_state(
        self,
        job_id: str,
        allowed: tuple[JobStatus, ...],
        new_status: JobStatus,
        action: str,
    ) -> bool:
        """Validate and apply a job state transition."""
        job = self.get_job(job_id)
        if not job:
            raise JobNotFoundError(
                f"Job '{job_id}' not found. It may have been deleted or never existed. "
                "Use scheduler.query_jobs() to see available jobs."
            )

        if job.status not in allowed:
            allowed_str = " or ".join(s.value.upper() for s in allowed)
            raise InvalidJobStateError(
                f"Cannot {action.rstrip('d')} job '{job_id}' in {job.status.value} state. "
                f"Only {allowed_str} jobs can be {action}."
            )

        self.storage.update_job(
            job_id, {"status": new_status.value, "updated_at": utc_now().isoformat()}
        )
        if self.verbose:
            self.logger.info(f"Job {action}", job_id=job_id)
        return True

