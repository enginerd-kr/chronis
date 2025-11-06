"""Polling-based scheduler implementation."""

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.enums import TriggerType
from chronis.core.exceptions import JobAlreadyExistsError, JobNotFoundError
from chronis.core.job import JobDefinition, JobInfo
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
        >>> from chronis.adapters.locks import RedisLockAdapter
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

    def __init__(
        self,
        storage_adapter: JobStorageAdapter,
        lock_adapter: LockAdapter,
        polling_interval_seconds: int = 10,
        lock_ttl_seconds: int = 300,
        lock_prefix: str = "scheduler:lock:",
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
            logger: Custom logger (uses default if None)
        """
        self.storage = storage_adapter
        self.lock = lock_adapter
        self.polling_interval_seconds = polling_interval_seconds
        self.lock_ttl_seconds = lock_ttl_seconds
        self.lock_prefix = lock_prefix

        self._running = False
        self._job_registry: dict[str, Callable] = {}

        # Initialize structured logger
        base_logger = logger or _default_logger
        self.logger = ContextLogger(base_logger, {"component": "PollingScheduler"})

        # Initialize APScheduler (BackgroundScheduler - non-blocking)
        self._apscheduler = BackgroundScheduler(
            timezone="UTC",
            daemon=True,  # Run as daemon thread
        )
        self._last_poll_time: datetime | None = None

    def register_job_function(self, name: str, func: Callable) -> None:
        """
        Register job function (for lookup by function name).

        Args:
            name: Function name (e.g., "my_module.my_job")
            func: Function object

        Example:
            >>> def send_email():
            ...     print("Sending email...")
            >>> scheduler.register_job_function("send_email", send_email)
        """
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
            self.logger.warning("Scheduler start called but already running")
            raise RuntimeError("Scheduler is already running")

        self.logger.info(
            "Starting scheduler",
            polling_interval=self.polling_interval_seconds,
            lock_ttl=self.lock_ttl_seconds,
        )

        # Register polling job to APScheduler
        trigger = IntervalTrigger(seconds=self.polling_interval_seconds, timezone="UTC")

        self._apscheduler.add_job(
            func=self._poll_and_execute_jobs,
            trigger=trigger,
            id="polling_job",
            name="Job Polling",
            replace_existing=True,
        )

        # Start APScheduler (non-blocking)
        self._apscheduler.start()
        self._running = True
        self.logger.info("Scheduler started successfully")

    def stop(self) -> None:
        """
        Stop scheduler.

        Terminates APScheduler and cleans up all resources.

        Example:
            >>> scheduler.stop()
        """
        if not self._running:
            self.logger.debug("Stop called but scheduler not running")
            return

        self.logger.info("Stopping scheduler")

        # Shutdown APScheduler
        self._apscheduler.shutdown(wait=True)

        self._running = False
        self.logger.info("Scheduler stopped successfully")

    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running

    # ------------------------------------------------------------------------
    # Internal Methods (APScheduler Polling Logic)
    # ------------------------------------------------------------------------

    def _poll_and_execute_jobs(self) -> None:
        """
        Poll and execute jobs (called periodically by APScheduler).

        This method runs in APScheduler's background thread.
        """
        try:
            # UTC aware time
            current_time = utc_now()

            self.logger.debug("Polling for ready jobs", current_time=current_time.isoformat())

            # 1. Query ready jobs from storage
            jobs = self._query_ready_jobs(current_time)

            if not jobs:
                self.logger.debug("No ready jobs found")
                return

            self.logger.info(
                "Found ready jobs",
                job_count=len(jobs),
                job_ids=[j.get("job_id") for j in jobs],
            )

            # 2. Try to execute each job
            for job_data in jobs:
                self._try_execute_job(job_data)

            self._last_poll_time = current_time

        except Exception as e:
            self.logger.error(
                f"Critical error in polling loop: {e}",
                exc_info=True,
                error_type=type(e).__name__,
            )

    def _query_ready_jobs(self, current_time: datetime) -> list[dict[str, Any]]:
        """
        Query ready jobs from storage (using adapter).

        Args:
            current_time: Current time

        Returns:
            List of ready jobs
        """
        return self.storage.query_ready_jobs(current_time)

    def _try_execute_job(self, job_data: dict[str, Any]) -> None:
        """
        Try to execute job (using lock adapter and error handling).

        Args:
            job_data: Job data from storage
        """
        job_id = job_data["job_id"]
        job_name = job_data.get("name", job_id)
        lock_key = f"{self.lock_prefix}{job_id}"

        job_logger = self.logger.with_context(job_id=job_id, job_name=job_name)

        # Try to acquire distributed lock (using adapter)
        lock_acquired = False
        try:
            lock_acquired = self.lock.acquire(lock_key, self.lock_ttl_seconds, blocking=False)

            if not lock_acquired:
                # Another pod is executing
                job_logger.debug("Job already locked by another pod, skipping")
                return

            job_logger.info("Lock acquired, starting job execution")

            # Execute job function (with retry logic)
            self._execute_job_with_retry(job_data, job_logger)

            # Update next_run_time on success
            self._update_next_run_time(job_data)

            job_logger.info("Job executed successfully")

        except Exception as e:
            job_logger.error(
                f"Unexpected error executing job: {e}",
                exc_info=True,
                error_type=type(e).__name__,
            )
            # Record error to storage
            self._record_job_error(job_id, str(e))

        finally:
            # Release lock (using adapter)
            if lock_acquired:
                try:
                    self.lock.release(lock_key)
                    job_logger.debug("Lock released")
                except Exception as e:
                    job_logger.error(f"Failed to release lock: {e}", exc_info=True)

    def _execute_job_with_retry(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute job with retry logic.

        Args:
            job_data: Job data
            job_logger: Context logger

        Raises:
            Exception: If all retries fail
        """
        max_retries = job_data.get("max_retries", 3)
        retry_delay = job_data.get("retry_delay_seconds", 60)
        use_backoff = job_data.get("retry_exponential_backoff", True)
        current_retry = job_data.get("retry_count", 0)

        job_logger.debug(
            "Starting job execution with retry settings",
            max_retries=max_retries,
            retry_delay=retry_delay,
            use_backoff=use_backoff,
            current_retry=current_retry,
        )

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    # Calculate retry delay
                    if use_backoff:
                        # Exponential backoff: delay * (2 ^ (attempt - 1))
                        delay = retry_delay * (2 ** (attempt - 1))
                    else:
                        delay = retry_delay

                    job_logger.warning(
                        f"Retrying job execution (attempt {attempt}/{max_retries})",
                        retry_attempt=attempt,
                        delay_seconds=delay,
                    )
                    time.sleep(delay)

                # Execute job function
                self._execute_job_function(job_data, job_logger)

                # Reset retry count on success
                if current_retry > 0:
                    self._reset_retry_count(job_data["job_id"])
                    job_logger.info("Job succeeded after retry", retry_attempt=attempt)

                return  # Success

            except Exception as e:
                last_exception = e
                job_logger.error(
                    f"Job execution failed (attempt {attempt + 1}/{max_retries + 1}): {e}",
                    exc_info=True,
                    retry_attempt=attempt,
                    error_type=type(e).__name__,
                )

                # Increment retry count on last attempt
                if attempt == max_retries:
                    self._increment_retry_count(job_data["job_id"], str(e))
                    break

        # All retries failed
        job_logger.error(
            f"Job failed after {max_retries + 1} attempts",
            max_retries=max_retries,
            last_error=str(last_exception),
        )
        raise last_exception  # type: ignore

    def _execute_job_function(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute actual job function.

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

        job_logger.debug(
            "Executing job function",
            func_name=func_name,
            args_count=len(args),
            kwargs_keys=list(kwargs.keys()),
        )

        # Lookup registered function
        func = self._job_registry.get(func_name)  # type: ignore
        if not func:
            job_logger.error(f"Function not registered: {func_name}")
            raise ValueError(f"Function {func_name} not registered")

        # Execute function
        start_time = time.time()
        try:
            func(*args, **kwargs)
            execution_time = time.time() - start_time
            job_logger.info(
                "Job function completed",
                func_name=func_name,
                execution_time_seconds=round(execution_time, 3),
            )
        except Exception as e:
            execution_time = time.time() - start_time
            job_logger.error(
                f"Job function raised exception: {e}",
                exc_info=True,
                func_name=func_name,
                execution_time_seconds=round(execution_time, 3),
                error_type=type(e).__name__,
            )
            raise

    def _increment_retry_count(self, job_id: str, error_message: str) -> None:
        """
        Increment job retry count and record error.

        Args:
            job_id: Job ID
            error_message: Error message
        """
        try:
            job_data = self.storage.get_job(job_id)
            if job_data:
                retry_count = job_data.get("retry_count", 0) + 1
                updates = {
                    "retry_count": retry_count,
                    "last_error": error_message[:500],  # Limit error message length
                    "updated_at": utc_now().isoformat(),
                }
                self.storage.update_job(job_id, updates)
                self.logger.debug("Incremented retry count", job_id=job_id, retry_count=retry_count)
        except Exception as e:
            self.logger.error(f"Failed to increment retry count: {e}", exc_info=True, job_id=job_id)

    def _reset_retry_count(self, job_id: str) -> None:
        """
        Reset job retry count.

        Args:
            job_id: Job ID
        """
        try:
            updates = {
                "retry_count": 0,
                "last_error": None,
                "updated_at": utc_now().isoformat(),
            }
            self.storage.update_job(job_id, updates)
            self.logger.debug("Reset retry count", job_id=job_id)
        except Exception as e:
            self.logger.error(f"Failed to reset retry count: {e}", exc_info=True, job_id=job_id)

    def _record_job_error(self, job_id: str, error_message: str) -> None:
        """
        Record job execution error.

        Args:
            job_id: Job ID
            error_message: Error message
        """
        try:
            updates = {
                "last_error": error_message[:500],
                "updated_at": utc_now().isoformat(),
            }
            self.storage.update_job(job_id, updates)
            self.logger.debug("Recorded job error", job_id=job_id)
        except Exception as e:
            self.logger.error(f"Failed to record job error: {e}", exc_info=True, job_id=job_id)

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

        # Timezone object
        tz = ZoneInfo(timezone)
        current_time_local = datetime.now(tz)

        next_run_time_local = None

        if trigger_type == "interval":
            seconds = trigger_args.get("seconds", 0)
            minutes = trigger_args.get("minutes", 0)
            hours = trigger_args.get("hours", 0)
            next_run_time_local = current_time_local + timedelta(
                seconds=seconds, minutes=minutes, hours=hours
            )

        elif trigger_type == "cron":
            from croniter import croniter

            cron_expr = (
                f"{trigger_args.get('minute', '*')} "
                f"{trigger_args.get('hour', '*')} "
                f"{trigger_args.get('day', '*')} "
                f"{trigger_args.get('month', '*')} "
                f"{trigger_args.get('day_of_week', '*')}"
            )

            # Pass timezone to croniter
            iter_obj = croniter(cron_expr, current_time_local)
            next_run_time_local = iter_obj.get_next(datetime)

        elif trigger_type == "date":
            # One-time - next_run_time = None
            next_run_time_local = None

        # Convert to UTC and save
        if next_run_time_local:
            next_run_time_utc = next_run_time_local.astimezone(ZoneInfo("UTC"))

            updates = {
                "next_run_time": next_run_time_utc.isoformat(),
                "next_run_time_local": next_run_time_local.isoformat(),
                "updated_at": utc_now().isoformat(),
            }
            self.storage.update_job(job_id, updates)

            self.logger.info(
                "Updated next_run_time",
                job_id=job_id,
                next_run_time_utc=next_run_time_utc.isoformat(),
                next_run_time_local=next_run_time_local.isoformat(),
                timezone=timezone,
            )
        else:
            # Deactivate one-time job
            updates = {
                "is_active": False,
                "updated_at": utc_now().isoformat(),
            }
            self.storage.update_job(job_id, updates)

            self.logger.info("Deactivated one-time job", job_id=job_id, trigger_type=trigger_type)

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
        self.logger.info(
            "Creating job",
            job_id=job.job_id,
            job_name=job.name,
            trigger_type=job.trigger_type.value,
            timezone=job.timezone,
        )

        job_data = job.to_dict()
        try:
            result = self.storage.create_job(job_data)
            self.logger.info(
                "Job created successfully",
                job_id=job.job_id,
                next_run_time=result.get("next_run_time"),
            )
            return JobInfo(result)
        except ValueError as e:
            self.logger.error(
                f"Failed to create job: {e}",
                job_id=job.job_id,
                error_type="JobAlreadyExists",
            )
            raise JobAlreadyExistsError(str(e)) from e
        except Exception as e:
            self.logger.error(
                f"Unexpected error creating job: {e}",
                exc_info=True,
                job_id=job.job_id,
                error_type=type(e).__name__,
            )
            raise

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

    def update_job(
        self,
        job_id: str,
        name: str | None = None,
        trigger_type: TriggerType | None = None,
        trigger_args: dict[str, Any] | None = None,
        is_active: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> JobInfo:
        """
        Update job (using adapter).

        Args:
            job_id: Job ID
            name: New name (optional)
            trigger_type: New trigger type (optional)
            trigger_args: New trigger parameters (optional)
            is_active: Active status (optional)
            metadata: Metadata (optional)

        Returns:
            Updated job info

        Raises:
            JobNotFoundError: Job not found

        Example:
            >>> scheduler.update_job(
            ...     "email-001",
            ...     trigger_args={"hour": "10", "minute": "0"},
            ...     is_active=False
            ... )
        """
        updates = {}
        if name is not None:
            updates["name"] = name
        if trigger_type is not None:
            updates["trigger_type"] = trigger_type.value
        if trigger_args is not None:
            updates["trigger_args"] = trigger_args
        if is_active is not None:
            updates["is_active"] = is_active
        if metadata is not None:
            updates["metadata"] = metadata

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
        self.logger.info("Deleting job", job_id=job_id)

        try:
            success = self.storage.delete_job(job_id)
            if success:
                self.logger.info("Job deleted successfully", job_id=job_id)
            else:
                self.logger.warning("Job not found for deletion", job_id=job_id)
            return success
        except Exception as e:
            self.logger.error(
                f"Error deleting job: {e}",
                exc_info=True,
                job_id=job_id,
                error_type=type(e).__name__,
            )
            raise

    def list_jobs(
        self,
        is_active: bool | None = None,
        limit: int = 100,
    ) -> list[JobInfo]:
        """
        List jobs (using adapter).

        Args:
            is_active: Active filter (None=all)
            limit: Maximum count

        Returns:
            Job list

        Example:
            >>> active_jobs = scheduler.list_jobs(is_active=True)
            >>> for job in active_jobs:
            ...     print(f"{job.name}: {job.next_run_time}")
        """
        jobs_data = self.storage.list_jobs(is_active=is_active, limit=limit)
        return [JobInfo(job_data) for job_data in jobs_data]
