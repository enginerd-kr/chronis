"""Polling-based scheduler implementation."""

import logging
import secrets
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.common.types import TriggerType
from chronis.core.execution.async_loop import AsyncExecutor
from chronis.core.job_queue import JobQueue
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.services import ExecutionCoordinator, JobService, SchedulingOrchestrator
from chronis.core.state import JobStatus
from chronis.utils.logging import ContextLogger, _default_logger
from chronis.utils.time import get_timezone


class PollingScheduler:
    """
    Polling-based scheduler (APScheduler-based non-blocking).

    Uses APScheduler BackgroundScheduler internally for
    non-blocking periodic polling.

    Supports various storage and lock systems through adapter pattern.

    Usage:
        >>> # Production: DynamoDB + Redis
        >>> from chronis.adapters.storage import DynamoDBAdapter
        >>> from chronis.adapters.lock import RedisLockAdapter
        >>> storage = DynamoDBAdapter(table_name="scheduled_jobs")
        >>> lock = RedisLockAdapter(host="localhost")
        >>> scheduler = PollingScheduler(
        ...     storage_adapter=storage,
        ...     lock_adapter=lock,
        ...     polling_interval_seconds=10,
        ...     lock_prefix="myapp:"
        ... )
        >>> scheduler.start()
        >>> scheduler.stop()
    """

    # Constants
    MIN_POLLING_INTERVAL = 1
    MAX_POLLING_INTERVAL = 3600
    DEFAULT_MAX_WORKERS = 20

    def __init__(
        self,
        storage_adapter: JobStorageAdapter,
        lock_adapter: LockAdapter,
        polling_interval_seconds: int = 10,
        lock_ttl_seconds: int = 300,
        lock_prefix: str = "scheduler:lock:",
        max_workers: int | None = None,
        max_queue_size: int | None = None,
        executor_interval_seconds: int | None = None,
        verbose: bool = False,
        logger: logging.Logger | None = None,
        on_failure: OnFailureCallback | None = None,
        on_success: OnSuccessCallback | None = None,
    ) -> None:
        """
        Initialize polling scheduler.

        Args:
            storage_adapter: Job storage adapter (required)
            lock_adapter: Distributed lock adapter (required)
            polling_interval_seconds: Polling interval (seconds)
            lock_ttl_seconds: Lock TTL (seconds)
            lock_prefix: Lock key prefix
            max_workers: Maximum number of worker threads (default: 20)
            max_queue_size: Maximum queue size for backpressure control (default: max_workers * 5)
            executor_interval_seconds: Executor check interval (default: min(1, polling_interval / 2))
            verbose: Enable verbose logging (default: False)
            logger: Custom logger (uses default if None)
            on_failure: Global failure handler for all jobs (optional)
            on_success: Global success handler for all jobs (optional)

        Raises:
            ValueError: If parameters are invalid
        """
        # Validate parameters
        if polling_interval_seconds < self.MIN_POLLING_INTERVAL:
            raise ValueError(f"polling_interval_seconds must be >= {self.MIN_POLLING_INTERVAL}")
        if polling_interval_seconds > self.MAX_POLLING_INTERVAL:
            raise ValueError(
                f"polling_interval_seconds should not exceed {self.MAX_POLLING_INTERVAL}"
            )
        if lock_ttl_seconds < polling_interval_seconds * 2:
            raise ValueError(
                "lock_ttl_seconds should be at least 2x polling_interval_seconds "
                "to prevent premature lock expiration"
            )
        if not lock_prefix:
            raise ValueError("lock_prefix cannot be empty")

        self.storage = storage_adapter
        self.lock = lock_adapter
        self.polling_interval_seconds = polling_interval_seconds
        self.lock_ttl_seconds = lock_ttl_seconds
        self.lock_prefix = lock_prefix
        self.max_workers = max_workers or self.DEFAULT_MAX_WORKERS
        self.max_queue_size = max_queue_size or (self.max_workers * 5)
        # Default executor interval: half of polling interval (min 1 second)
        self.executor_interval_seconds = executor_interval_seconds or max(
            1, polling_interval_seconds / 2
        )
        self.verbose = verbose
        self.on_failure = on_failure
        self.on_success = on_success

        self._running = False
        self._job_registry: dict[str, Callable] = {}
        self._failure_handler_registry: dict[str, OnFailureCallback] = {}
        self._success_handler_registry: dict[str, OnSuccessCallback] = {}
        self._registry_lock = threading.RLock()

        # Initialize structured logger
        base_logger = logger or _default_logger
        self.logger = ContextLogger(base_logger, {"component": "PollingScheduler"})

        # Initialize job queue for backpressure control
        self._job_queue = JobQueue(max_queue_size=self.max_queue_size)

        # Initialize ThreadPoolExecutor for job execution
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="chronis-worker-"
        )

        # Initialize dedicated event loop for async jobs
        self._async_executor = AsyncExecutor()

        # Initialize APScheduler (BackgroundScheduler - non-blocking)
        self._apscheduler = BackgroundScheduler(
            timezone="UTC",
            daemon=True,  # Run as daemon thread
        )
        self._last_poll_time: datetime | None = None

        # Initialize application services (DDD architecture)
        self._job_service = JobService(
            storage=self.storage, logger=self.logger, verbose=self.verbose
        )

        self._scheduling_orchestrator = SchedulingOrchestrator(
            storage=self.storage,
            job_queue=self._job_queue,
            logger=self.logger,
            verbose=self.verbose,
        )

        self._execution_coordinator = ExecutionCoordinator(
            storage=self.storage,
            lock=self.lock,
            executor=self._executor,
            async_executor=self._async_executor,
            function_registry=self._job_registry,
            failure_handler_registry=self._failure_handler_registry,
            success_handler_registry=self._success_handler_registry,
            global_on_failure=self.on_failure,
            global_on_success=self.on_success,
            logger=self.logger,
            lock_prefix=self.lock_prefix,
            lock_ttl_seconds=self.lock_ttl_seconds,
            verbose=self.verbose,
        )

    def register_job_function(self, name: str, func: Callable) -> None:
        """
        Register job function (thread-safe).

        Args:
            name: Function name (e.g., "my_module.my_job")
            func: Function object

        Example:
            >>> def send_email():
            ...     print("Sending email...")
            >>> scheduler.register_job_function("send_email", send_email)
        """
        with self._registry_lock:
            self._job_registry[name] = func

    def start(self) -> None:
        """
        Start scheduler (non-blocking).

        Uses APScheduler BackgroundScheduler to perform
        polling tasks in the background. This method returns immediately.

        Example:
            >>> scheduler.start()
            >>> # Returns immediately - continues running in background
            >>> print("Main thread continues...")
            >>> time.sleep(60)
            >>> scheduler.stop()
        """
        if self._running:
            raise RuntimeError("Scheduler is already running")

        self.logger.info("Starting scheduler")

        # Start dedicated event loop for async jobs
        self._start_async_loop()

        # Register executor job to APScheduler with configurable interval
        executor_trigger = IntervalTrigger(seconds=self.executor_interval_seconds, timezone="UTC")
        self._apscheduler.add_job(
            func=self._execute_queued_jobs,
            trigger=executor_trigger,
            id="executor_job",
            name="Job Executor",
            replace_existing=True,
        )

        # Register polling job to APScheduler
        polling_trigger = IntervalTrigger(seconds=self.polling_interval_seconds, timezone="UTC")
        self._apscheduler.add_job(
            func=self._poll_and_add_to_queue,
            trigger=polling_trigger,
            id="polling_job",
            name="Job Polling",
            replace_existing=True,
        )

        # Start APScheduler (non-blocking)
        self._apscheduler.start()
        self._running = True

    def stop(self) -> None:
        """
        Stop scheduler.

        Terminates APScheduler and cleans up all resources.

        Example:
            >>> scheduler.stop()
        """
        if not self._running:
            return

        # Shutdown APScheduler
        self._apscheduler.shutdown(wait=True)

        # Stop dedicated event loop
        self._stop_async_loop()

        # Shutdown thread pool executor
        self._executor.shutdown(wait=True, cancel_futures=False)

        self._running = False

    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running

    def get_queue_status(self) -> dict[str, Any]:
        """
        Get job queue status for monitoring.

        Returns:
            Dictionary with queue statistics including:
            - pending_jobs: Number of jobs waiting in queue
            - in_flight_jobs: Number of jobs dequeued for execution
            - total_in_flight: Total jobs (pending + in-flight)
            - available_slots: Available queue capacity
            - utilization: Queue utilization ratio (0.0 to 1.0)
            - in_flight_job_ids: List of job IDs currently in-flight
        """
        return self._scheduling_orchestrator.get_queue_status()

    # ------------------------------------------------------------------------
    # Helper Methods for Auto-Generation
    # ------------------------------------------------------------------------

    def _generate_job_name(self, func: Callable | str) -> str:
        """
        Generate human-readable job name from function.

        Args:
            func: Function object or import path string

        Returns:
            Human-readable name (e.g., "Send Email", "Generate Report")
        """
        if isinstance(func, str):
            func_name = func.split(".")[-1]
        else:
            func_name = func.__name__

        # Convert snake_case to Title Case
        # send_email -> Send Email
        return func_name.replace("_", " ").title()

    def _generate_job_id(self, func: Callable | str, name: str) -> str:
        """
        Generate unique job ID.

        Format: {func_name}_{timestamp}_{random}
        Example: send_email_20251226_120530_a1b2c3d4

        Args:
            func: Function object or import path string
            name: Job name (for fallback)

        Returns:
            Unique job identifier
        """
        # Extract function name
        if isinstance(func, str):
            func_name = func.split(".")[-1]
        else:
            func_name = func.__name__

        # Sanitize: only alphanumeric and underscore
        func_name = "".join(c if c.isalnum() or c == "_" else "_" for c in func_name)

        # Truncate if too long
        if len(func_name) > 30:
            func_name = func_name[:30]

        # Timestamp (sortable, UTC)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Random suffix (8 chars) for collision prevention
        random_suffix = secrets.token_hex(4)

        return f"{func_name}_{timestamp}_{random_suffix}"

    # ------------------------------------------------------------------------
    # Async Event Loop Management
    # ------------------------------------------------------------------------

    def _start_async_loop(self) -> None:
        """Start dedicated event loop for async jobs in background thread."""
        # AsyncExecutor manages its own event loop - just ensure it's started
        self._async_executor.start()

    def _stop_async_loop(self) -> None:
        """Stop dedicated event loop."""
        # AsyncExecutor manages its own event loop cleanup
        self._async_executor.stop()

    # ------------------------------------------------------------------------
    # Internal Methods (APScheduler Polling Logic)
    # ------------------------------------------------------------------------

    def _poll_and_add_to_queue(self) -> None:
        """
        Poll ready jobs from storage and add to queue (called periodically by APScheduler).

        This method runs in APScheduler's background thread.
        """
        # Delegate to SchedulingOrchestrator
        self._scheduling_orchestrator.poll_and_enqueue()
        self._last_poll_time = self._scheduling_orchestrator.last_poll_time

    def _execute_queued_jobs(self) -> None:
        """
        Execute jobs from queue (called every 1 second by APScheduler).

        This method runs in APScheduler's background thread.
        """
        try:
            # Execute jobs while queue is not empty
            while not self._scheduling_orchestrator.is_queue_empty():
                job_data = self._scheduling_orchestrator.get_next_job_from_queue()
                if job_data is None:
                    break

                # Try to execute with distributed lock
                executed = self._execution_coordinator.try_execute(
                    job_data, on_complete=self._scheduling_orchestrator.mark_job_completed
                )

                # If not executed (lock failed or wrong status), mark as completed in queue
                if not executed:
                    self._scheduling_orchestrator.mark_job_completed(job_data["job_id"])

        except Exception as e:
            self.logger.error(f"Executor error: {e}", exc_info=True)

    # ------------------------------------------------------------------------
    # Job CRUD Operations (Delegated to JobService)
    # ------------------------------------------------------------------------

    def create_job(self, job: JobDefinition) -> JobInfo:
        """
        Create new job (using adapter).

        Args:
            job: Job definition

        Returns:
            Created job info

        Raises:
            JobAlreadyExistsError: Job already exists

        Example:
            >>> job = JobDefinition(
            ...     job_id="email-001",
            ...     name="Daily Email",
            ...     trigger_type=TriggerType.CRON,
            ...     trigger_args={"hour": "9", "minute": "0"},
            ...     func=send_email,
            ... )
            >>> scheduler.create_job(job)
        """
        # Register on_failure handler if provided
        if job.on_failure:
            with self._registry_lock:
                self._failure_handler_registry[job.job_id] = job.on_failure

        # Register on_success handler if provided
        if job.on_success:
            with self._registry_lock:
                self._success_handler_registry[job.job_id] = job.on_success

        return self._job_service.create(job)

    def get_job(self, job_id: str) -> JobInfo | None:
        """
        Get job (using adapter).

        Args:
            job_id: Job ID

        Returns:
            Job info or None

        Example:
            >>> job = scheduler.get_job("email-001")
            >>> if job:
            ...     print(f"Next run: {job.next_run_time}")
        """
        return self._job_service.get(job_id)

    def query_jobs(
        self, filters: dict[str, Any] | None = None, limit: int | None = None
    ) -> list[JobInfo]:
        """
        Query jobs with flexible filters.

        Args:
            filters: Dictionary of filter conditions (None = get all jobs)
                - {"status": "scheduled"}: Filter by status
                - {"metadata.tenant_id": "acme"}: Filter by tenant (multi-tenancy)
                - {"metadata.priority": "high", "status": "scheduled"}: Multiple filters
            limit: Maximum number of jobs to return

        Returns:
            List of jobs matching filters

        Example:
            >>> # Get all jobs
            >>> all_jobs = scheduler.query_jobs()
            >>>
            >>> # Get all scheduled jobs
            >>> jobs = scheduler.query_jobs(filters={"status": "scheduled"})
            >>>
            >>> # Multi-tenancy: Get tenant-specific jobs
            >>> jobs = scheduler.query_jobs(
            ...     filters={"metadata.tenant_id": "acme", "status": "scheduled"}
            ... )
            >>>
            >>> # Access trigger info from JobInfo
            >>> for job in jobs:
            ...     print(f"{job.job_id}: {job.trigger_type} - {job.next_run_time}")
        """
        return self._job_service.query(filters=filters, limit=limit)

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
        Update job (using adapter).

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

        Example:
            >>> scheduler.update_job(
            ...     "email-001",
            ...     trigger_args={"hour": "10", "minute": "0"},
            ...     status=JobStatus.PAUSED
            ... )
        """
        # Convert TriggerType enum to string if provided
        trigger_type_str = trigger_type.value if trigger_type else None

        return self._job_service.update(
            job_id=job_id,
            name=name,
            trigger_type=trigger_type_str,
            trigger_args=trigger_args,
            status=status,
            metadata=metadata,
        )

    def delete_job(self, job_id: str) -> bool:
        """
        Delete job (using adapter).

        Args:
            job_id: Job ID

        Returns:
            Deletion success

        Example:
            >>> scheduler.delete_job("email-001")
            True
        """
        return self._job_service.delete(job_id)

    def pause_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job.

        Paused jobs will not be polled or executed until resumed.
        Only SCHEDULED or PENDING jobs can be paused.

        Args:
            job_id: Job ID to pause

        Returns:
            True if job was paused, False if job was not in pausable state

        Raises:
            JobNotFoundError: Job not found

        Example:
            >>> # Pause during deployment
            >>> scheduler.pause_job("email-001")
            True
            >>>
            >>> # Bulk pause by tenant
            >>> jobs = scheduler.query_jobs(filters={"metadata.tenant_id": "acme"})
            >>> for job in jobs:
            ...     scheduler.pause_job(job.job_id)
        """
        from chronis.core.common.exceptions import JobNotFoundError

        job = self._job_service.get(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} not found")

        # Can only pause SCHEDULED or PENDING jobs
        if job.status not in (JobStatus.SCHEDULED, JobStatus.PENDING):
            self.logger.warning(
                f"Cannot pause job {job_id} in status {job.status}",
                extra={"job_id": job_id, "status": job.status},
            )
            return False

        self._job_service.update(job_id=job_id, status=JobStatus.PAUSED)

        self.logger.info(f"Job {job_id} paused", extra={"job_id": job_id})
        return True

    def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job.

        Args:
            job_id: Job ID to resume

        Returns:
            True if job was resumed, False if job was not paused

        Raises:
            JobNotFoundError: Job not found

        Example:
            >>> scheduler.resume_job("email-001")
            True
        """
        from chronis.core.common.exceptions import JobNotFoundError

        job = self._job_service.get(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} not found")

        if job.status != JobStatus.PAUSED:
            self.logger.warning(
                f"Cannot resume job {job_id} - not paused (status: {job.status})",
                extra={"job_id": job_id, "status": job.status},
            )
            return False

        self._job_service.update(job_id=job_id, status=JobStatus.SCHEDULED)

        self.logger.info(f"Job {job_id} resumed", extra={"job_id": job_id})
        return True

    # ========================================
    # Simplified Public API (TriggerType hidden)
    # ========================================

    def create_interval_job(
        self,
        func: Callable | str,
        job_id: str | None = None,
        name: str | None = None,
        seconds: int | None = None,
        minutes: int | None = None,
        hours: int | None = None,
        days: int | None = None,
        weeks: int | None = None,
        timezone: str = "UTC",
        args: tuple | None = None,
        kwargs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        on_failure: OnFailureCallback | None = None,
        on_success: OnSuccessCallback | None = None,
        max_retries: int = 0,
        retry_delay_seconds: int = 60,
        timeout_seconds: int | None = None,
        priority: int = 5,
    ) -> JobInfo:
        """
        Create interval job (runs repeatedly at fixed intervals).

        Args:
            func: Function to execute (callable or import path string)
            job_id: Unique job identifier (auto-generated if None)
            name: Human-readable job name (auto-generated if None)
            seconds: Interval in seconds
            minutes: Interval in minutes
            hours: Interval in hours
            days: Interval in days
            weeks: Interval in weeks
            timezone: IANA timezone (e.g., "Asia/Seoul", "UTC")
            args: Positional arguments for func
            kwargs: Keyword arguments for func
            metadata: User-defined metadata (optional)
                - For multi-tenancy: {"tenant_id": "acme"}
                - For custom tags: {"priority": "high", "team": "eng"}
            on_failure: Failure handler for this specific job (optional)
            on_success: Success handler for this specific job (optional)
            max_retries: Maximum number of retry attempts (default: 0, no retry)
            retry_delay_seconds: Base delay between retries in seconds (default: 60)
            timeout_seconds: Job execution timeout in seconds (default: None, no timeout)

        Returns:
            Created job info with generated or provided job_id

        Example:
            >>> # AI-friendly: Auto-generated ID
            >>> job = scheduler.create_interval_job(
            ...     func=send_heartbeat,
            ...     seconds=30
            ... )
            >>> print(job.job_id)  # "send_heartbeat_20251226_120000_abc123"
            >>>
            >>> # Human-friendly: Explicit ID
            >>> scheduler.create_interval_job(
            ...     func=send_heartbeat,
            ...     job_id="heartbeat",
            ...     name="System Heartbeat",
            ...     seconds=30
            ... )
            >>>
            >>> # Multi-tenant job
            >>> scheduler.create_interval_job(
            ...     func=generate_report,
            ...     hours=24,
            ...     metadata={"tenant_id": "acme"}
            ... )
        """
        # Auto-generate name if not provided
        if name is None:
            name = self._generate_job_name(func)

        # Auto-generate job_id if not provided
        if job_id is None:
            job_id = self._generate_job_id(func, name)

        # Build trigger_args from non-None parameters
        trigger_args = {
            k: v
            for k, v in {
                "seconds": seconds,
                "minutes": minutes,
                "hours": hours,
                "days": days,
                "weeks": weeks,
            }.items()
            if v is not None
        }

        if not trigger_args:
            raise ValueError("At least one interval parameter must be specified")

        job = JobDefinition(
            job_id=job_id,
            name=name,
            trigger_type=TriggerType.INTERVAL,
            trigger_args=trigger_args,
            func=func,
            timezone=timezone,
            args=args,
            kwargs=kwargs,
            metadata=metadata,
            on_failure=on_failure,
            on_success=on_success,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            timeout_seconds=timeout_seconds,
            priority=priority,
        )
        return self.create_job(job)

    def create_cron_job(
        self,
        func: Callable | str,
        job_id: str | None = None,
        name: str | None = None,
        year: int | str | None = None,
        month: int | str | None = None,
        day: int | str | None = None,
        week: int | str | None = None,
        day_of_week: int | str | None = None,
        hour: int | str | None = None,
        minute: int | str | None = None,
        timezone: str = "UTC",
        args: tuple | None = None,
        kwargs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        on_failure: OnFailureCallback | None = None,
        on_success: OnSuccessCallback | None = None,
        max_retries: int = 0,
        retry_delay_seconds: int = 60,
        timeout_seconds: int | None = None,
        priority: int = 5,
    ) -> JobInfo:
        """
        Create cron job (runs on specific date/time patterns).

        Args:
            func: Function to execute (callable or import path string)
            job_id: Unique job identifier (auto-generated if None)
            name: Human-readable job name (auto-generated if None)
            year: 4-digit year
            month: Month (1-12)
            day: Day of month (1-31)
            week: ISO week (1-53)
            day_of_week: Day of week (0-6 or mon,tue,wed,thu,fri,sat,sun)
            hour: Hour (0-23)
            minute: Minute (0-59)
            timezone: IANA timezone (e.g., "Asia/Seoul", "UTC")
            args: Positional arguments for func
            kwargs: Keyword arguments for func
            metadata: Additional metadata
            on_failure: Failure handler for this specific job (optional)
            on_success: Success handler for this specific job (optional)
            max_retries: Maximum number of retry attempts (default: 0, no retry)
            retry_delay_seconds: Base delay between retries in seconds (default: 60)
            timeout_seconds: Job execution timeout in seconds (default: None, no timeout)

        Returns:
            Created job info with generated or provided job_id

        Example:
            >>> # AI-friendly: Auto-generated ID
            >>> job = scheduler.create_cron_job(
            ...     func=generate_report,
            ...     hour=9,
            ...     minute=0,
            ...     timezone="Asia/Seoul"
            ... )
            >>> print(job.job_id)  # "generate_report_20251226_120000_abc123"
            >>>
            >>> # Human-friendly: Explicit ID
            >>> scheduler.create_cron_job(
            ...     func=generate_report,
            ...     job_id="daily-report",
            ...     name="Daily Report",
            ...     hour=9,
            ...     minute=0,
            ...     timezone="Asia/Seoul"
            ... )
            >>>
            >>> # Run every Monday at 6 PM
            >>> scheduler.create_cron_job(
            ...     func=send_summary,
            ...     day_of_week="mon",
            ...     hour=18,
            ...     minute=0
            ... )
        """
        # Auto-generate name if not provided
        if name is None:
            name = self._generate_job_name(func)

        # Auto-generate job_id if not provided
        if job_id is None:
            job_id = self._generate_job_id(func, name)

        # Build trigger_args from non-None parameters
        trigger_args = {
            k: v
            for k, v in {
                "year": year,
                "month": month,
                "day": day,
                "week": week,
                "day_of_week": day_of_week,
                "hour": hour,
                "minute": minute,
            }.items()
            if v is not None
        }

        if not trigger_args:
            raise ValueError("At least one cron parameter must be specified")

        job = JobDefinition(
            job_id=job_id,
            name=name,
            trigger_type=TriggerType.CRON,
            trigger_args=trigger_args,
            func=func,
            timezone=timezone,
            args=args,
            kwargs=kwargs,
            metadata=metadata,
            on_failure=on_failure,
            on_success=on_success,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            timeout_seconds=timeout_seconds,
            priority=priority,
        )
        return self.create_job(job)

    def create_date_job(
        self,
        func: Callable | str,
        run_date: str | datetime,
        job_id: str | None = None,
        name: str | None = None,
        timezone: str = "UTC",
        args: tuple | None = None,
        kwargs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        on_failure: OnFailureCallback | None = None,
        on_success: OnSuccessCallback | None = None,
        max_retries: int = 0,
        retry_delay_seconds: int = 60,
        timeout_seconds: int | None = None,
        priority: int = 5,
    ) -> JobInfo:
        """
        Create one-time job (runs once at specific date/time).

        Args:
            func: Function to execute (callable or import path string)
            run_date: ISO format datetime string or datetime object
            job_id: Unique job identifier (auto-generated if None)
            name: Human-readable job name (auto-generated if None)
            timezone: IANA timezone (e.g., "Asia/Seoul", "UTC")
            args: Positional arguments for func
            kwargs: Keyword arguments for func
            metadata: Additional metadata
            on_failure: Failure handler for this specific job (optional)
            on_success: Success handler for this specific job (optional)
            max_retries: Maximum number of retry attempts (default: 0, no retry)
            retry_delay_seconds: Base delay between retries in seconds (default: 60)
            timeout_seconds: Job execution timeout in seconds (default: None, no timeout)

        Returns:
            Created job info with generated or provided job_id

        Example:
            >>> # AI-friendly: Auto-generated ID
            >>> from datetime import datetime, timedelta
            >>> job = scheduler.create_date_job(
            ...     func=send_welcome_email,
            ...     run_date=datetime.now() + timedelta(hours=1),
            ...     kwargs={"user_id": 123}
            ... )
            >>> print(job.job_id)  # "send_welcome_email_20251226_120000_abc123"
            >>>
            >>> # Human-friendly: Explicit ID
            >>> scheduler.create_date_job(
            ...     func=send_reminder,
            ...     run_date="2025-11-08 10:00:00",
            ...     job_id="reminder",
            ...     name="Important Reminder",
            ...     timezone="Asia/Seoul"
            ... )
        """
        # Auto-generate name if not provided
        if name is None:
            name = self._generate_job_name(func)

        # Auto-generate job_id if not provided
        if job_id is None:
            job_id = self._generate_job_id(func, name)

        if isinstance(run_date, datetime):
            if run_date.tzinfo is None:
                # Naive datetime is assumed to be in local system timezone
                # Convert to aware datetime in local timezone, then to UTC

                run_date = run_date.astimezone(get_timezone("UTC"))
            run_date = run_date.isoformat()

        job = JobDefinition(
            job_id=job_id,
            name=name,
            trigger_type=TriggerType.DATE,
            trigger_args={"run_date": run_date},
            func=func,
            timezone=timezone,
            args=args,
            kwargs=kwargs,
            metadata=metadata,
            on_failure=on_failure,
            on_success=on_success,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            timeout_seconds=timeout_seconds,
            priority=priority,
        )
        return self.create_job(job)
