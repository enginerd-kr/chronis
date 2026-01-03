"""Polling-based scheduler implementation."""

import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.base.scheduler import BaseScheduler
from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.execution.job_queue import JobQueue
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.schedulers.polling_orchestrator import PollingOrchestrator
from chronis.core.state.enums import TriggerType
from chronis.utils.logging import ContextLogger
from chronis.utils.time import get_timezone


class PollingScheduler(BaseScheduler):
    """
    Polling-based scheduler with non-blocking APScheduler backend.

    Supports various storage and lock systems through adapter pattern.
    Uses periodic polling to discover and execute scheduled jobs.
    """

    MIN_POLLING_INTERVAL = 1
    MAX_POLLING_INTERVAL = 3600

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
        logger: ContextLogger | None = None,
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
        # Validate polling-specific parameters
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

        # Initialize base scheduler
        super().__init__(
            storage_adapter=storage_adapter,
            lock_adapter=lock_adapter,
            max_workers=max_workers,
            lock_prefix=lock_prefix,
            lock_ttl_seconds=lock_ttl_seconds,
            verbose=verbose,
            logger=logger,
            on_failure=on_failure,
            on_success=on_success,
        )

        # Polling-specific configuration
        self.polling_interval_seconds = polling_interval_seconds
        self.max_queue_size = max_queue_size or (self.max_workers * 5)
        self.executor_interval_seconds = executor_interval_seconds or max(
            1, polling_interval_seconds / 2
        )

        # Initialize job queue for backpressure control
        self._job_queue = JobQueue(max_queue_size=self.max_queue_size)

        # Initialize polling orchestrator
        self._orchestrator = PollingOrchestrator(
            storage=self.storage,
            job_queue=self._job_queue,
            logger=self.logger,
            verbose=self.verbose,
        )

        # Initialize APScheduler (BackgroundScheduler - non-blocking)
        from apscheduler.executors.pool import (  # type: ignore[import-untyped]
            ThreadPoolExecutor as APSThreadPoolExecutor,
        )

        executors = {
            "default": APSThreadPoolExecutor(max_workers=1)  # Single thread for internal jobs
        }

        self._apscheduler = BackgroundScheduler(
            timezone="UTC",
            daemon=True,
            executors=executors,
        )

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
            from chronis.core.common.exceptions import SchedulerError

            raise SchedulerError(
                "Scheduler is already running. Call scheduler.stop() first to restart."
            )

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
            max_instances=1,
        )

        # Register polling job to APScheduler
        polling_trigger = IntervalTrigger(seconds=self.polling_interval_seconds, timezone="UTC")
        self._apscheduler.add_job(
            func=self._poll_and_enqueue,
            trigger=polling_trigger,
            id="polling_job",
            name="Job Polling",
            replace_existing=True,
            max_instances=1,
        )

        # Start APScheduler (non-blocking)
        self._apscheduler.start()
        self._running = True

    def stop(self, timeout: float = 30.0) -> dict[str, Any]:
        """
        Stop scheduler gracefully.

        Waits for running jobs (both sync and async) to complete before shutting down.

        Args:
            timeout: Maximum seconds to wait for async jobs to complete (default: 30).

        Returns:
            Dictionary with shutdown status:
            - sync_jobs_completed: Always True (sync jobs always wait)
            - async_jobs_completed: True if all async jobs finished, False if timeout
            - was_running: Whether scheduler was running before stop

        Example:
            >>> result = scheduler.stop(timeout=60.0)
            >>> if not result['async_jobs_completed']:
            ...     print("Warning: Some async jobs were interrupted")
        """
        if not self._running:
            return {
                "sync_jobs_completed": True,
                "async_jobs_completed": True,
                "was_running": False,
            }

        # Shutdown APScheduler (stops polling for new jobs)
        self._apscheduler.shutdown(wait=True)

        # Stop dedicated event loop (wait for async jobs with timeout)
        async_completed = self._stop_async_loop(timeout=timeout)

        # Shutdown thread pool executor (wait for sync jobs - always waits)
        self._executor.shutdown(wait=True, cancel_futures=False)

        self._running = False

        return {
            "sync_jobs_completed": True,
            "async_jobs_completed": async_completed,
            "was_running": True,
        }

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
        return self._orchestrator.get_queue_status()

    # ------------------------------------------------------------------------
    # Internal Methods (APScheduler Polling Logic)
    # ------------------------------------------------------------------------

    def _poll_and_enqueue(self) -> None:
        """
        Poll ready jobs from storage and add to queue.

        Called periodically by APScheduler.
        This method runs in APScheduler's background thread.
        """
        self._orchestrator.enqueue_jobs()

    def _execute_queued_jobs(self) -> None:
        """
        Execute jobs from queue.

        Called periodically by APScheduler.
        Fetches job data from storage for each queued job ID.

        This method runs in APScheduler's background thread.
        """
        try:
            # Execute jobs while queue is not empty
            while not self._orchestrator.is_queue_empty():
                job_id = self._orchestrator.get_next_job_from_queue()
                if job_id is None:
                    break

                # Fetch fresh job data from storage
                job_data = self.storage.get_job(job_id)
                if job_data is None:
                    # Job was deleted - mark as completed and continue
                    self._orchestrator.mark_job_completed(job_id)
                    continue

                # Try to execute with distributed lock
                executed = self._execution_coordinator.try_execute(
                    dict(job_data), on_complete=self._orchestrator.mark_job_completed
                )

                # If not executed (lock failed or wrong status), mark as completed in queue
                if not executed:
                    self._orchestrator.mark_job_completed(job_id)

        except Exception as e:
            self.logger.error(f"Executor error: {e}", exc_info=True)

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
        return func_name.replace("_", " ").title()

    def _generate_job_id(self, func: Callable | str) -> str:
        """
        Generate unique job ID.

        Format: {func_name}_{timestamp}_{random}
        Example: send_email_20251226_120530_a1b2c3d4

        Args:
            func: Function object or import path string

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
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

        # Random suffix (8 chars) for collision prevention
        random_suffix = secrets.token_hex(4)

        return f"{func_name}_{timestamp}_{random_suffix}"

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
        if_missed: Literal["skip", "run_once", "run_all"] | None = None,
        misfire_threshold_seconds: int = 60,
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
            on_failure: Failure handler for this specific job (optional)
            on_success: Success handler for this specific job (optional)
            max_retries: Maximum number of retry attempts (default: 0)
            retry_delay_seconds: Base delay between retries in seconds (default: 60)
            timeout_seconds: Job execution timeout in seconds (default: None)
            priority: Job priority 0-10 (default: 5, higher = more urgent)
            if_missed: Misfire policy (default: None, uses trigger default)
            misfire_threshold_seconds: Misfire threshold in seconds (default: 60)

        Returns:
            Created job info with generated or provided job_id
        """
        # Auto-generate name if not provided
        if name is None:
            name = self._generate_job_name(func)

        # Auto-generate job_id if not provided
        if job_id is None:
            job_id = self._generate_job_id(func)

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
            if_missed=if_missed,
            misfire_threshold_seconds=misfire_threshold_seconds,
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
        if_missed: Literal["skip", "run_once", "run_all"] | None = None,
        misfire_threshold_seconds: int = 60,
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
            max_retries: Maximum number of retry attempts (default: 0)
            retry_delay_seconds: Base delay between retries in seconds (default: 60)
            timeout_seconds: Job execution timeout in seconds (default: None)
            priority: Job priority 0-10 (default: 5, higher = more urgent)
            if_missed: Misfire policy (default: None, uses trigger default)
            misfire_threshold_seconds: Misfire threshold in seconds (default: 60)

        Returns:
            Created job info with generated or provided job_id
        """
        # Auto-generate name if not provided
        if name is None:
            name = self._generate_job_name(func)

        # Auto-generate job_id if not provided
        if job_id is None:
            job_id = self._generate_job_id(func)

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
            if_missed=if_missed,
            misfire_threshold_seconds=misfire_threshold_seconds,
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
        if_missed: Literal["skip", "run_once", "run_all"] | None = None,
        misfire_threshold_seconds: int = 60,
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
            max_retries: Maximum number of retry attempts (default: 0)
            retry_delay_seconds: Base delay between retries in seconds (default: 60)
            timeout_seconds: Job execution timeout in seconds (default: None)
            priority: Job priority 0-10 (default: 5, higher = more urgent)
            if_missed: Misfire policy (default: None, uses trigger default)
            misfire_threshold_seconds: Misfire threshold in seconds (default: 60)

        Returns:
            Created job info with generated or provided job_id
        """
        # Auto-generate name if not provided
        if name is None:
            name = self._generate_job_name(func)

        # Auto-generate job_id if not provided
        if job_id is None:
            job_id = self._generate_job_id(func)

        if isinstance(run_date, datetime):
            if run_date.tzinfo is None:
                # Naive datetime - convert to aware datetime in UTC
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
            if_missed=if_missed,
            misfire_threshold_seconds=misfire_threshold_seconds,
        )
        return self.create_job(job)
