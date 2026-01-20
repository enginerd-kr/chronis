"""Polling-based scheduler implementation."""

from collections.abc import Callable
from datetime import datetime
from typing import Any, Literal

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.base.scheduler import BaseScheduler
from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.execution.job_queue import JobQueue
from chronis.core.jobs.definition import JobInfo
from chronis.core.schedulers import job_builders
from chronis.core.schedulers.polling_orchestrator import PollingOrchestrator
from chronis.utils.logging import ContextLogger


class PollingScheduler(BaseScheduler):
    """
    Polling-based scheduler with non-blocking APScheduler backend.

    Supports various storage and lock systems through adapter pattern.
    Uses periodic polling to discover and execute scheduled jobs.
    """

    MIN_POLLING_INTERVAL = 0.1
    MAX_POLLING_INTERVAL = 3600

    def __init__(
        self,
        storage_adapter: JobStorageAdapter,
        lock_adapter: LockAdapter,
        polling_interval_seconds: int = 1,
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
        import logging

        # Suppress APScheduler's verbose INFO logs (Running job, executed successfully)
        logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
        logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)

        if self._running:
            from chronis.core.common.exceptions import SchedulerError

            raise SchedulerError(
                "Scheduler is already running. Call scheduler.stop() first to restart."
            )

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
        Execute jobs from queue in batches.

        Called periodically by APScheduler.
        Fetches job data from storage and submits jobs to thread pool in batches
        for concurrent execution, improving throughput in distributed environments.

        This method runs in APScheduler's background thread.
        """
        try:
            # Process jobs in batches for better concurrency
            batch_size = 100  # Process up to 100 jobs per iteration (increased for throughput)

            while not self._orchestrator.is_queue_empty():
                # Get batch of job IDs from queue
                job_ids = []
                for _ in range(batch_size):
                    if self._orchestrator.is_queue_empty():
                        break
                    job_id = self._orchestrator.get_next_job_from_queue()
                    if job_id:
                        job_ids.append(job_id)

                if not job_ids:
                    break

                # Fetch all job data in single batch operation (optimized!)
                jobs_dict = self.storage.get_jobs_batch(job_ids)

                # Process batch: submit for execution
                # Each try_execute() is non-blocking and submits to thread pool
                for job_id in job_ids:
                    job_data = jobs_dict.get(job_id)
                    if job_data is None:
                        # Job was deleted - mark as completed and continue
                        self._orchestrator.mark_job_completed(job_id)
                        continue

                    # Try to execute with distributed lock (non-blocking)
                    executed = self._execution_coordinator.try_execute(
                        dict(job_data), on_complete=self._orchestrator.mark_job_completed
                    )

                    # If not executed (lock failed or wrong status), mark as completed in queue
                    if not executed:
                        self._orchestrator.mark_job_completed(job_id)

        except Exception as e:
            self.logger.error("Executor error", error=str(e))

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
        """Create interval job (runs repeatedly at fixed intervals)."""
        job_def = job_builders.create_interval_job(
            func=func,
            job_id=job_id,
            name=name,
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            days=days,
            weeks=weeks,
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
        return self.create_job(job_def)

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
        """Create cron job (runs on specific date/time patterns)."""
        job_def = job_builders.create_cron_job(
            func=func,
            job_id=job_id,
            name=name,
            year=year,
            month=month,
            day=day,
            week=week,
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
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
        return self.create_job(job_def)

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
        """Create one-time job (runs once at specific date/time)."""
        job_def = job_builders.create_date_job(
            func=func,
            run_date=run_date,
            job_id=job_id,
            name=name,
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
        return self.create_job(job_def)
