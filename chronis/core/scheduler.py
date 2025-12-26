"""Polling-based scheduler implementation."""

import asyncio
import inspect
import logging
import secrets
import threading
import time
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.common.exceptions import JobAlreadyExistsError, JobNotFoundError
from chronis.core.common.types import TriggerType
from chronis.core.job_queue import JobQueue
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.state import JobStatus
from chronis.core.triggers import TriggerFactory
from chronis.utils.logging import ContextLogger, _default_logger
from chronis.utils.time import ZoneInfo, utc_now


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
        verbose: bool = False,
        logger: logging.Logger | None = None,
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
            verbose: Enable verbose logging (default: False)
            logger: Custom logger (uses default if None)

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
        self.verbose = verbose

        self._running = False
        self._job_registry: dict[str, Callable] = {}
        self._registry_lock = threading.RLock()

        # Initialize structured logger
        base_logger = logger or _default_logger
        self.logger = ContextLogger(base_logger, {"component": "PollingScheduler"})

        # Initialize job queue for backpressure control
        self._job_queue = JobQueue(max_queue_size=self.max_queue_size)

        # Initialize ThreadPoolExecutor for job execution
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="chronis-worker-"
        )

        # Initialize dedicated event loop for async jobs
        self._async_loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None

        # Initialize APScheduler (BackgroundScheduler - non-blocking)
        self._apscheduler = BackgroundScheduler(
            timezone="UTC",
            daemon=True,  # Run as daemon thread
        )
        self._last_poll_time: datetime | None = None

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

        # Register executor job to APScheduler (1 second interval)
        executor_trigger = IntervalTrigger(seconds=1, timezone="UTC")
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
            - running_jobs: Number of jobs currently executing
            - total_in_flight: Total jobs (pending + running)
            - available_slots: Available queue capacity
            - utilization: Queue utilization ratio (0.0 to 1.0)
        """
        return self._job_queue.get_status()

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
        if self._async_loop is not None:
            return

        # Create new event loop
        self._async_loop = asyncio.new_event_loop()

        # Start event loop in dedicated thread
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
            name="chronis-async-loop"
        )
        self._loop_thread.start()

    def _run_event_loop(self) -> None:
        """Run event loop forever (called in dedicated thread)."""
        if self._async_loop is None:
            return

        asyncio.set_event_loop(self._async_loop)
        try:
            self._async_loop.run_forever()
        finally:
            self._async_loop.close()

    def _stop_async_loop(self) -> None:
        """Stop dedicated event loop."""
        if self._async_loop is None:
            return

        # Stop the event loop
        self._async_loop.call_soon_threadsafe(self._async_loop.stop)

        # Wait for loop thread to finish
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)

        self._async_loop = None
        self._loop_thread = None

    # ------------------------------------------------------------------------
    # Internal Methods (APScheduler Polling Logic)
    # ------------------------------------------------------------------------

    def _poll_and_add_to_queue(self) -> None:
        """
        Poll ready jobs from storage and add to queue (called periodically by APScheduler).

        This method runs in APScheduler's background thread.
        """
        try:
            # Get available slots in queue
            available_slots = self._job_queue.get_available_slots()

            if available_slots <= 0:
                self.logger.warning(
                    "Job queue is full, skipping poll",
                    queue_status=self._job_queue.get_status()
                )
                return

            # Query ready jobs from storage (limit to available slots)
            current_time = utc_now()
            jobs = self._query_ready_jobs(current_time, limit=available_slots)

            if jobs:
                # Log only in verbose mode or when many jobs found
                if self.verbose or len(jobs) >= 10:
                    self.logger.info(
                        "Found ready jobs",
                        count=len(jobs),
                        queue_status=self._job_queue.get_status()
                    )

                # Add jobs to queue
                for job_data in jobs:
                    if not self._job_queue.add_job(job_data):
                        self.logger.warning(
                            "Failed to add job to queue",
                            job_id=job_data.get("job_id")
                        )

            self._last_poll_time = current_time

        except Exception as e:
            self.logger.error(f"Polling error: {e}", exc_info=True)

    def _execute_queued_jobs(self) -> None:
        """
        Execute jobs from queue (called every 1 second by APScheduler).

        This method runs in APScheduler's background thread.
        """
        try:
            # Execute jobs while queue is not empty and workers are available
            while not self._job_queue.is_empty():
                job_data = self._job_queue.get_next_job()
                if job_data is None:
                    break

                # Try to execute the job
                self._try_execute_job(job_data)

        except Exception as e:
            self.logger.error(f"Executor error: {e}", exc_info=True)

    def _query_ready_jobs(
        self,
        current_time: datetime,
        limit: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Query ready jobs from storage (using adapter).

        Args:
            current_time: Current time
            limit: Maximum number of jobs to return

        Returns:
            List of ready jobs
        """
        filters = {
            "status": "scheduled",
            "next_run_time_lte": current_time.isoformat()
        }
        return self.storage.query_jobs(filters=filters, limit=limit)

    @contextmanager
    def _acquire_lock_context(
        self, lock_key: str, job_logger: ContextLogger
    ) -> Generator[bool, None, None]:
        """
        Context manager for lock acquisition and release.

        Args:
            lock_key: Lock key
            job_logger: Context logger

        Yields:
            True if lock acquired, False otherwise
        """
        lock_acquired = False
        try:
            lock_acquired = self.lock.acquire(lock_key, self.lock_ttl_seconds, blocking=False)
            yield lock_acquired
        finally:
            if lock_acquired:
                try:
                    self.lock.release(lock_key)
                except Exception as e:
                    job_logger.error(f"Lock release failed: {e}", exc_info=True)

    def _try_execute_job(self, job_data: dict[str, Any]) -> None:
        """
        Try to execute job.

        Args:
            job_data: Job data from storage
        """
        job_id = job_data["job_id"]
        job_name = job_data.get("name", job_id)
        lock_key = f"{self.lock_prefix}{job_id}"

        job_logger = self.logger.with_context(job_id=job_id, job_name=job_name)

        # Check if job can be executed based on state
        job_status = JobStatus(job_data.get("status", "scheduled"))
        if job_status != JobStatus.SCHEDULED:
            self._job_queue.mark_completed(job_id)
            return

        with self._acquire_lock_context(lock_key, job_logger) as lock_acquired:
            if not lock_acquired:
                self._job_queue.mark_completed(job_id)
                return

            self._trigger_job(job_data, job_logger)

    def _trigger_job(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Trigger job execution using thread pool.

        The scheduler triggers the job and immediately updates the next run time.
        Job execution happens asynchronously in a worker thread from the pool.

        Args:
            job_data: Job data
            job_logger: Context logger
        """
        job_id = job_data["job_id"]

        try:
            # 1. Update state to RUNNING
            self._update_job_safely(
                job_id,
                {"status": JobStatus.RUNNING.value},
                "mark job as running"
            )

            # 2. Submit job to thread pool executor with completion callback
            future = self._executor.submit(
                self._execute_job_function_background,
                job_data,
                job_logger
            )

            # 3. Add done callback to remove from queue when complete
            future.add_done_callback(lambda f: self._on_job_complete(job_id))

            # Log only in verbose mode
            if self.verbose:
                job_logger.info("Job triggered")

        except Exception as e:
            job_logger.error(f"Trigger failed: {e}", exc_info=True)
            self._job_queue.mark_completed(job_id)
        finally:
            # 4. Immediately calculate and update next run time
            # This happens regardless of job execution result
            self._update_next_run_time(job_data)

    def _execute_job_function_background(
        self, job_data: dict[str, Any], job_logger: ContextLogger
    ) -> None:
        """
        Execute job function in background thread.

        This runs in a separate daemon thread. Success or failure doesn't affect
        the scheduler's ability to trigger the job at the next scheduled time.

        Args:
            job_data: Job data
            job_logger: Context logger
        """
        job_id = job_data["job_id"]
        trigger_type = job_data["trigger_type"]

        try:
            self._execute_job_function(job_data, job_logger)

            # For one-time jobs (DATE trigger), delete after execution
            if trigger_type == TriggerType.DATE.value:
                try:
                    self.storage.delete_job(job_id)
                    if self.verbose:
                        job_logger.info("One-time job deleted after execution")
                except Exception as e:
                    job_logger.error(f"Failed to delete one-time job: {e}")
            else:
                # Mark as SCHEDULED again after successful execution for recurring jobs
                self._update_job_safely(
                    job_id,
                    {"status": JobStatus.SCHEDULED.value},
                    "mark job as scheduled after execution"
                )

        except Exception as e:
            # Log the error but don't propagate it
            job_logger.error(f"Execution failed: {e}", exc_info=True)

            # Mark as FAILED
            self._update_job_safely(
                job_id,
                {"status": JobStatus.FAILED.value},
                "mark job as failed after execution error"
            )

    def _on_job_complete(self, job_id: str) -> None:
        """
        Callback invoked when job execution completes.

        This is called automatically by Future.add_done_callback()
        when the job function finishes (success or failure).

        Args:
            job_id: Job ID that completed
        """
        self._job_queue.mark_completed(job_id)

    def _execute_job_function(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute actual job function (supports both sync and async functions).

        Async functions are executed in the dedicated event loop for optimal performance.

        Args:
            job_data: Job data
            job_logger: Context logger

        Raises:
            ValueError: Unregistered function
            Exception: Exception raised during function execution
        """
        func_name = job_data.get("func_name")
        args = job_data.get("args", ())
        kwargs = job_data.get("kwargs", {})

        # Lookup registered function (thread-safe)
        with self._registry_lock:
            func = self._job_registry.get(func_name)  # type: ignore

        if not func:
            raise ValueError(f"Function {func_name} not registered")

        # Execute function (handle both sync and async)
        start_time = time.time()
        is_async = inspect.iscoroutinefunction(func)

        try:
            # Check if function is async
            if is_async:
                # Run async function in dedicated event loop
                if self._async_loop is None:
                    raise RuntimeError("Async event loop not initialized")

                future = asyncio.run_coroutine_threadsafe(
                    func(*args, **kwargs),
                    self._async_loop
                )
                # Wait for result (blocks current thread, but doesn't block event loop)
                future.result()
            else:
                # Run sync function normally
                func(*args, **kwargs)

            execution_time = time.time() - start_time
            # Log only in verbose mode
            if self.verbose:
                job_logger.info(
                    "Job completed",
                    execution_time=round(execution_time, 3),
                )
        except Exception as e:
            execution_time = time.time() - start_time
            # Always log errors
            job_logger.error(
                f"Job failed: {e}",
                exc_info=True,
                execution_time=round(execution_time, 3),
            )
            raise

    def _update_job_safely(
        self,
        job_id: str,
        updates: dict[str, Any],
        operation_name: str,
        extra_log_context: dict[str, Any] | None = None,
    ) -> bool:
        """
        Safely update job with standardized error handling.

        Args:
            job_id: Job ID
            updates: Update dictionary
            operation_name: Name of operation for logging
            extra_log_context: Additional log context

        Returns:
            True if successful, False otherwise
        """
        try:
            updates["updated_at"] = utc_now().isoformat()
            self.storage.update_job(job_id, updates)
            return True

        except Exception as e:
            self.logger.error(f"{operation_name} failed: {e}", job_id=job_id)
            return False


    def _update_next_run_time(self, job_data: dict[str, Any]) -> None:
        """
        Calculate and update next_run_time (with timezone consideration).

        Args:
            job_data: Job data
        """
        job_id = job_data["job_id"]
        trigger_type = job_data["trigger_type"]
        trigger_args = job_data["trigger_args"]
        timezone = job_data.get("timezone", "UTC")

        # For one-time jobs (DATE trigger), skip next_run_time update
        # They will be deleted after execution in _execute_job_function_background
        if trigger_type == TriggerType.DATE.value:
            return

        # Current time (timezone aware)
        tz = ZoneInfo(timezone)
        current_time_local = datetime.now(tz)

        # Calculate next run time using trigger strategy
        strategy = TriggerFactory.get_strategy(trigger_type)
        next_run_time_utc = strategy.calculate_next_run_time(
            trigger_args, timezone, current_time_local
        )

        if next_run_time_utc:
            self._update_job_schedule(job_id, next_run_time_utc, timezone)

    def _update_job_schedule(
        self, job_id: str, next_run_time_utc: datetime, timezone: str
    ) -> None:
        """
        Update job with next run time.

        Args:
            job_id: Job ID
            next_run_time_utc: Next run time in UTC
            timezone: IANA timezone string
        """
        tz = ZoneInfo(timezone)
        next_run_time_local = next_run_time_utc.astimezone(tz)

        updates = {
            "next_run_time": next_run_time_utc.isoformat(),
            "next_run_time_local": next_run_time_local.isoformat(),
        }
        self._update_job_safely(job_id, updates, "update next_run_time")

    # ------------------------------------------------------------------------
    # Job CRUD Operations
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
            ValidationError: Invalid parameters

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
        job_data = job.to_dict()
        try:
            result = self.storage.create_job(job_data)
            # Log only in verbose mode
            if self.verbose:
                self.logger.info("Job created", job_id=job.job_id)
            return JobInfo(result)
        except ValueError as e:
            raise JobAlreadyExistsError(str(e)) from e

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
        job_data = self.storage.get_job(job_id)
        return JobInfo(job_data) if job_data else None

    def query_jobs(
        self,
        filters: dict[str, Any] | None = None,
        limit: int | None = None
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
        jobs_data = self.storage.query_jobs(filters=filters, limit=limit)
        return [JobInfo(job_data) for job_data in jobs_data]

    def get_all_schedules(self) -> list[JobInfo]:
        """
        Get all schedules with trigger details.

        Deprecated: Use query_jobs() instead. JobInfo already contains trigger information.
            >>> jobs = scheduler.query_jobs()
            >>> for job in jobs:
            ...     print(f"{job.job_id}: {job.trigger_type}")

        Returns:
            List of all jobs (same as query_jobs)
        """
        return self.query_jobs()

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
        # Build updates dictionary from non-None parameters
        updates = {
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

        if not updates:
            # No updates provided, just fetch and return current state
            current = self.get_job(job_id)
            if not current:
                raise JobNotFoundError(f"Job {job_id} not found")
            return current

        updates["updated_at"] = utc_now().isoformat()

        try:
            result = self.storage.update_job(job_id, updates)
            return JobInfo(result)
        except ValueError as e:
            raise JobNotFoundError(str(e)) from e

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
        try:
            success = self.storage.delete_job(job_id)
            if success:
                self.logger.info("Job deleted", job_id=job_id)
            return success
        except Exception as e:
            self.logger.error(f"Delete failed: {e}", job_id=job_id)
            raise


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

        trigger_args = {}
        if seconds is not None:
            trigger_args["seconds"] = seconds
        if minutes is not None:
            trigger_args["minutes"] = minutes
        if hours is not None:
            trigger_args["hours"] = hours
        if days is not None:
            trigger_args["days"] = days
        if weeks is not None:
            trigger_args["weeks"] = weeks

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
        second: int | str | None = None,
        timezone: str = "UTC",
        args: tuple | None = None,
        kwargs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
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
            second: Second (0-59)
            timezone: IANA timezone (e.g., "Asia/Seoul", "UTC")
            args: Positional arguments for func
            kwargs: Keyword arguments for func
            metadata: Additional metadata

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

        trigger_args = {}
        if year is not None:
            trigger_args["year"] = year
        if month is not None:
            trigger_args["month"] = month
        if day is not None:
            trigger_args["day"] = day
        if week is not None:
            trigger_args["week"] = week
        if day_of_week is not None:
            trigger_args["day_of_week"] = day_of_week
        if hour is not None:
            trigger_args["hour"] = hour
        if minute is not None:
            trigger_args["minute"] = minute
        if second is not None:
            trigger_args["second"] = second

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
        )
        return self.create_job(job)
