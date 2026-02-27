"""Polling-based scheduler implementation."""

import hashlib
import secrets
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from chronis.core.schedulers.fluent_builders import FluentJobBuilder

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.base.scheduler import BaseScheduler
from chronis.core.common.exceptions import SchedulerError
from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.execution.job_queue import JobQueue
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.misfire import MisfireDetector
from chronis.core.schedulers.next_run_calculator import NextRunTimeCalculator
from chronis.core.state.enums import TriggerType
from chronis.type_defs import JobStorageData
from chronis.utils.logging import ContextLogger
from chronis.utils.time import get_timezone, utc_now


class _PeriodicRunner:
    """Lightweight periodic task runner using daemon threads.

    Replaces APScheduler BackgroundScheduler for simple interval-based
    periodic tasks. Each task runs in its own daemon thread with
    non-overlapping execution (waits for completion before next interval).
    """

    def __init__(self) -> None:
        self._tasks: list[tuple[Callable[[], Any], float, str]] = []
        self._threads: list[threading.Thread] = []
        self._stop_event = threading.Event()

    def add_task(self, func: Callable[[], Any], interval: float, name: str) -> None:
        """Register a periodic task (must be called before start)."""
        self._tasks.append((func, interval, name))

    def start(self) -> None:
        """Start all registered periodic tasks in daemon threads."""
        self._stop_event.clear()
        for func, interval, name in self._tasks:
            thread = threading.Thread(
                target=self._run_loop,
                args=(func, interval),
                daemon=True,
                name=f"chronis-{name}",
            )
            thread.start()
            self._threads.append(thread)

    def _run_loop(self, func: Callable[[], Any], interval: float) -> None:
        """Execute func periodically until stop is signaled."""
        while not self._stop_event.wait(timeout=interval):
            try:
                func()
            except Exception:
                pass  # Errors handled by func itself

    def shutdown(self, wait: bool = True) -> None:
        """Stop all periodic tasks."""
        self._stop_event.set()
        if wait:
            for thread in self._threads:
                thread.join(timeout=5)
        self._threads.clear()
        self._tasks.clear()


def _generate_job_name(func: Callable | str) -> str:
    """Generate human-readable job name from function (snake_case → Title Case)."""
    func_name = func.split(".")[-1] if isinstance(func, str) else func.__name__
    return func_name.replace("_", " ").title()


def _generate_job_id(func: Callable | str) -> str:
    """Generate unique job ID: {func_name}_{timestamp}_{random}."""
    func_name = func.split(".")[-1] if isinstance(func, str) else func.__name__
    func_name = "".join(c if c.isalnum() or c == "_" else "_" for c in func_name)[:30]
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    return f"{func_name}_{timestamp}_{secrets.token_hex(4)}"


class PollingScheduler(BaseScheduler):
    """
    Polling-based scheduler with lightweight periodic runner.

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

        # Random node ID for distributed job ordering
        # Each scheduler instance gets a unique ID to reduce lock contention
        self._node_id = str(uuid.uuid4())

        # Initialize periodic runner for polling and execution tasks
        self._runner = _PeriodicRunner()

    def start(self) -> None:
        """
        Start scheduler (non-blocking).

        Launches daemon threads for periodic polling and job execution.
        This method returns immediately.

        Example:
            >>> scheduler.start()
            >>> # Returns immediately - continues running in background
            >>> print("Main thread continues...")
            >>> time.sleep(60)
            >>> scheduler.stop()
        """
        if self._running:
            raise SchedulerError(
                "Scheduler is already running. Call scheduler.stop() first to restart."
            )

        # Register periodic tasks
        self._runner = _PeriodicRunner()
        self._runner.add_task(self._execute_queued_jobs, self.executor_interval_seconds, "executor")
        self._runner.add_task(self._enqueue_jobs, self.polling_interval_seconds, "polling")

        # Start periodic runner (non-blocking, daemon threads)
        self._runner.start()
        self._running = True

    def stop(self) -> dict[str, Any]:
        """
        Stop scheduler gracefully.

        Waits for all running jobs to complete before shutting down.
        Both sync and async jobs run in ThreadPoolExecutor, so shutdown
        blocks until all workers finish.

        Returns:
            Dictionary with shutdown status:
            - sync_jobs_completed: Always True (ThreadPool waits for completion)
            - async_jobs_completed: Always True (async jobs run in ThreadPool via asyncio.run)
            - was_running: Whether scheduler was running before stop

        Example:
            >>> result = scheduler.stop()
            >>> assert result['was_running'] is True
        """
        if not self._running:
            return {
                "sync_jobs_completed": True,
                "async_jobs_completed": True,
                "was_running": False,
            }

        # Shutdown periodic runner (stops polling for new jobs)
        self._runner.shutdown(wait=True)

        # Wait for async jobs on shared event loop
        self._execution_coordinator.shutdown_async(wait=True)

        # Shutdown thread pool executor (waits for sync jobs)
        self._executor.shutdown(wait=True, cancel_futures=False)

        self._running = False

        return {
            "sync_jobs_completed": True,
            "async_jobs_completed": True,
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
        return self._job_queue.get_status()

    # ------------------------------------------------------------------------
    # Internal Methods (Polling Logic)
    # ------------------------------------------------------------------------

    def _recover_stuck_jobs(self) -> None:
        """Detect and recover jobs stuck in RUNNING state after process crash."""
        try:
            cutoff = (utc_now() - timedelta(seconds=self.lock_ttl_seconds * 2)).isoformat()
            stuck_jobs = self.storage.query_jobs(
                filters={"status": "running", "updated_at_lte": cutoff},
                limit=10,
            )

            for job in stuck_jobs:
                job_id = job.get("job_id", "")
                self.logger.warning("Recovering stuck job", job_id=job_id)
                try:
                    self.storage.compare_and_swap_job(
                        job_id,
                        expected_values={"status": "running"},
                        updates={
                            "status": "scheduled",
                            "updated_at": utc_now().isoformat(),
                        },
                    )
                except Exception as e:
                    self.logger.warning("Failed to recover stuck job", job_id=job_id, error=str(e))
        except Exception as e:
            self.logger.debug("Stuck job detection skipped", error=str(e))

    def _enqueue_jobs(self) -> int:
        """
        Get ready jobs and enqueue them.

        Returns:
            Number of jobs added to queue
        """
        try:
            self._recover_stuck_jobs()

            available_slots = self._job_queue.get_available_slots()

            if available_slots <= 0:
                self.logger.warning("Job queue full, skipping enqueue")
                return 0

            jobs = self._get_ready_jobs(limit=available_slots)

            if jobs:
                added_count = 0
                for job_data in jobs:
                    job_id = job_data.get("job_id")
                    if not job_id:
                        continue
                    priority = job_data.get("priority", 5)
                    if self._job_queue.add_job(job_id, priority):
                        added_count += 1
                    else:
                        self.logger.warning("Failed to add job to queue", job_id=job_id)

                return added_count

            return 0

        except Exception as e:
            self.logger.error("Enqueue error", error=str(e))
            return 0

    def _get_ready_jobs(self, limit: int | None = None) -> list[Any]:
        """
        Query ready jobs from storage and classify them.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of ready job data sorted by priority (high to low) then time (early to late)
        """
        current_time = utc_now()

        # Query jobs that are ready (next_run_time <= current_time)
        # Over-fetch by 1.5x to account for node-specific hash reordering
        # which may push some jobs beyond the limit boundary after re-sorting
        query_limit = int(limit * 1.5 + 1) if limit else None
        filters = {"status": "scheduled", "next_run_time_lte": current_time.isoformat()}
        jobs = self.storage.query_jobs(filters=filters, limit=query_limit)

        # Classify into normal and misfired jobs
        normal_jobs, misfired_jobs = MisfireDetector.classify_due_jobs(
            jobs, current_time.isoformat()
        )

        if misfired_jobs:
            self.logger.warning(
                "Misfired jobs detected",
                count=len(misfired_jobs),
                job_ids=[j.get("job_id") for j in misfired_jobs],
            )

        # Apply misfire policy
        ready_misfired = []
        for job in misfired_jobs:
            policy = job.get("if_missed", "run_once")
            if policy == "skip":
                self._skip_misfired_job(job)
            else:
                # run_once / run_all: include in ready jobs
                ready_misfired.append(job)

        # Combine all jobs
        all_jobs = normal_jobs + ready_misfired

        # Sort by priority (descending), then by node-specific hash, then by next_run_time
        # The hash ensures each scheduler instance processes jobs in a different order,
        # reducing lock contention in distributed environments
        def sort_key(job: Any) -> tuple[int, int, str]:
            priority = job.get("priority", 5)
            job_id = job.get("job_id", "")
            next_run = job.get("next_run_time", "")

            # Create node-specific hash to distribute jobs across schedulers
            combined = f"{self._node_id}:{job_id}"
            hash_val = int(hashlib.md5(combined.encode()).hexdigest()[:8], 16)

            return (-int(priority), hash_val, str(next_run))

        all_jobs.sort(key=sort_key)  # type: ignore[arg-type]

        # Apply limit if specified
        if limit is not None:
            all_jobs = all_jobs[:limit]

        return all_jobs

    def _skip_misfired_job(self, job_data: JobStorageData) -> None:
        """Skip misfired job by advancing next_run_time to next future time."""
        job_id = job_data.get("job_id", "")
        trigger_type: str = job_data.get("trigger_type", "")

        if trigger_type == "date":
            self.storage.delete_job(job_id)
            self.logger.info("Skipped misfired one-time job (deleted)", job_id=job_id)
            return

        utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
            trigger_type, job_data.get("trigger_args", {}), job_data.get("timezone", "UTC")
        )

        if utc_time:
            self.storage.update_job(
                job_id,
                {
                    "next_run_time": utc_time.isoformat(),
                    "next_run_time_local": local_time.isoformat() if local_time else None,
                    "updated_at": utc_now().isoformat(),
                },
            )
            self.logger.info(
                "Skipped misfired job, advanced to next run",
                job_id=job_id,
                next_run_time=utc_time.isoformat(),
            )
        else:
            self.storage.update_job(
                job_id,
                {"status": "failed", "updated_at": utc_now().isoformat()},
            )
            self.logger.warning(
                "Cannot calculate next run time for misfired job, marking as failed",
                job_id=job_id,
                trigger_type=trigger_type,
            )

    def _execute_queued_jobs(self) -> None:
        """
        Execute jobs from queue in batches.

        Called periodically by the periodic runner.
        Fetches job data from storage and submits jobs to thread pool in batches
        for concurrent execution, improving throughput in distributed environments.
        """
        try:
            # Process jobs in batches for better concurrency
            batch_size = 100  # Process up to 100 jobs per iteration (increased for throughput)

            while not self._job_queue.is_empty():
                # Get batch of job IDs from queue
                job_ids = []
                for _ in range(batch_size):
                    if self._job_queue.is_empty():
                        break
                    job_id = self._job_queue.get_next_job()
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
                        self._job_queue.mark_completed(job_id)
                        continue

                    # Try to execute with distributed lock (non-blocking)
                    executed = self._execution_coordinator.try_execute(
                        dict(job_data), on_complete=self._job_queue.mark_completed
                    )

                    # If not executed (lock failed or wrong status), mark as completed in queue
                    if not executed:
                        self._job_queue.mark_completed(job_id)

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

        return self.create_job(
            JobDefinition(
                job_id=job_id or _generate_job_id(func),
                name=name or _generate_job_name(func),
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
        )

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

        return self.create_job(
            JobDefinition(
                job_id=job_id or _generate_job_id(func),
                name=name or _generate_job_name(func),
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
        )

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
        if isinstance(run_date, datetime):
            if run_date.tzinfo is None:
                run_date = run_date.astimezone(get_timezone("UTC"))
            run_date = run_date.isoformat()

        return self.create_job(
            JobDefinition(
                job_id=job_id or _generate_job_id(func),
                name=name or _generate_job_name(func),
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
        )

    # ========================================
    # Fluent Builder API (Simplified Interface)
    # ========================================

    def every(
        self,
        seconds: int | None = None,
        minutes: int | None = None,
        hours: int | None = None,
        days: int | None = None,
        weeks: int | None = None,
    ) -> "FluentJobBuilder":
        """
        Create an interval job using fluent API.

        Args:
            seconds: Run every N seconds
            minutes: Run every N minutes
            hours: Run every N hours
            days: Run every N days
            weeks: Run every N weeks

        Returns:
            FluentJobBuilder for method chaining

        Example:
            scheduler.every(minutes=5).run("sync_data")
            scheduler.every(hours=1).config(retry=3).run("backup")
        """
        from chronis.core.schedulers.fluent_builders import FluentJobBuilder

        return FluentJobBuilder(self).every(
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            days=days,
            weeks=weeks,
        )

    def on(
        self,
        *,
        minute: int | None = None,
        hour: int | None = None,
        day: int | None = None,
        day_of_week: int | str | None = None,
        month: int | str | None = None,
        year: int | None = None,
        week: int | None = None,
    ) -> "FluentJobBuilder":
        """
        Create a cron job - run at specific times.

        Args:
            minute: Minute (0-59)
            hour: Hour (0-23)
            day: Day of month (1-31)
            day_of_week: Day of week (0-6 for Mon-Sun, or "mon", "tue", etc.)
            month: Month (1-12)
            year: 4-digit year
            week: ISO week (1-53)

        Returns:
            FluentJobBuilder for method chaining

        Examples:
            # Every hour at minute 5
            scheduler.on(minute=5).run("task")

            # Daily at 9:30
            scheduler.on(hour=9, minute=30).run("report")

            # Every Monday at 9:00
            scheduler.on(day_of_week="mon", hour=9, minute=0).run("weekly")

            # Every month on the 1st at midnight
            scheduler.on(day=1, hour=0, minute=0).run("monthly")
        """
        from chronis.core.schedulers.fluent_builders import FluentJobBuilder

        return FluentJobBuilder(self).on(
            minute=minute,
            hour=hour,
            day=day,
            day_of_week=day_of_week,
            month=month,
            year=year,
            week=week,
        )

    def once(self, when: str | datetime) -> "FluentJobBuilder":
        """
        Create a one-time job at specified datetime.

        Args:
            when: ISO format datetime string or datetime object

        Returns:
            FluentJobBuilder for method chaining

        Example:
            scheduler.once(when="2025-01-20T10:00:00").run("notify")
        """
        from chronis.core.schedulers.fluent_builders import FluentJobBuilder

        return FluentJobBuilder(self).once(when=when)

    def config(
        self,
        *,
        job_id: str | None = None,
        name: str | None = None,
        timezone: str | None = None,
        retry: int | None = None,
        retry_delay: int | None = None,
        timeout: int | None = None,
        priority: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "FluentJobBuilder":
        """
        Start building a job with configuration options.

        Can be called before or after trigger methods (every, daily_at, etc.).

        Args:
            job_id: Custom job ID
            name: Custom job name
            timezone: Timezone for scheduling
            retry: Maximum retry attempts
            retry_delay: Delay between retries in seconds
            timeout: Execution timeout in seconds
            priority: Job priority (0-10, higher = more urgent)
            metadata: Custom metadata dict

        Returns:
            FluentJobBuilder for method chaining

        Example:
            scheduler.config(retry=3, timeout=300).every(minutes=5).run("sync")
            scheduler.every(minutes=5).config(retry=3).run("sync")
        """
        from chronis.core.schedulers.fluent_builders import FluentJobBuilder

        return FluentJobBuilder(self).config(
            job_id=job_id,
            name=name,
            timezone=timezone,
            retry=retry,
            retry_delay=retry_delay,
            timeout=timeout,
            priority=priority,
            metadata=metadata,
        )
