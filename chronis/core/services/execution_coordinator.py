"""Job execution coordination service."""

from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.common.types import TriggerType
from chronis.core.execution.async_loop import AsyncExecutor
from chronis.core.lifecycle import JobLifecycleManager
from chronis.core.scheduling import NextRunTimeCalculator
from chronis.core.state import JobStatus
from chronis.utils.logging import ContextLogger
from chronis.utils.time import utc_now


class ExecutionCoordinator:
    """
    Coordinates job execution with concurrency control and distributed locking.

    This application service is responsible for:
    - Acquiring distributed locks to prevent duplicate execution
    - Executing job functions (sync and async)
    - Managing state transitions during execution
    - Updating next run times after execution
    - Handling execution failures

    This service ensures jobs are executed exactly once across distributed instances.
    """

    def __init__(
        self,
        storage: JobStorageAdapter,
        lock: LockAdapter,
        executor: ThreadPoolExecutor,
        async_executor: AsyncExecutor,
        function_registry: dict[str, Callable],
        logger: ContextLogger,
        lock_prefix: str = "chronis:lock:",
        lock_ttl_seconds: int = 300,
        verbose: bool = False,
    ) -> None:
        """
        Initialize execution coordinator.

        Args:
            storage: Storage adapter for job updates
            lock: Lock adapter for distributed locking
            executor: Thread pool executor for job execution
            async_executor: Async executor for coroutines
            function_registry: Registry of job functions by name
            logger: Context logger
            lock_prefix: Prefix for lock keys
            lock_ttl_seconds: Lock TTL in seconds
            verbose: Enable verbose logging
        """
        self.storage = storage
        self.lock = lock
        self.executor = executor
        self.async_executor = async_executor
        self.function_registry = function_registry
        self.logger = logger
        self.lock_prefix = lock_prefix
        self.lock_ttl_seconds = lock_ttl_seconds
        self.verbose = verbose

    def try_execute(self, job_data: dict[str, Any], on_complete: Callable[[str], None]) -> bool:
        """
        Try to execute a job with distributed lock.

        Args:
            job_data: Job data from storage
            on_complete: Callback when execution completes (receives job_id)

        Returns:
            True if execution started, False if skipped
        """
        job_id = job_data["job_id"]
        job_name = job_data.get("name", job_id)
        lock_key = f"{self.lock_prefix}{job_id}"

        job_logger = self.logger.with_context(job_id=job_id, job_name=job_name)

        # Check if job can be executed based on state
        job_status = JobStatus(job_data.get("status", "scheduled"))
        if job_status != JobStatus.SCHEDULED:
            return False

        # Try to acquire distributed lock
        with self._acquire_lock_context(lock_key, job_logger) as lock_acquired:
            if not lock_acquired:
                return False

            self._trigger_execution(job_data, job_logger, on_complete)
            return True

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

    def _trigger_execution(
        self,
        job_data: dict[str, Any],
        job_logger: ContextLogger,
        on_complete: Callable[[str], None],
    ) -> None:
        """
        Trigger job execution in thread pool.

        Args:
            job_data: Job data
            job_logger: Context logger
            on_complete: Completion callback
        """
        job_id = job_data["job_id"]

        try:
            # 1. Update state to RUNNING
            self._update_job_status(job_id, JobStatus.RUNNING)

            # 2. Submit to thread pool with completion callback
            future = self.executor.submit(self._execute_in_background, job_data, job_logger)

            # 3. Add done callback
            future.add_done_callback(lambda f: on_complete(job_id))

            # 4. Log only in verbose mode
            if self.verbose:
                job_logger.info("Job triggered")

        except Exception as e:
            job_logger.error(f"Trigger failed: {e}", exc_info=True)
        finally:
            # 5. Immediately calculate and update next run time
            self._update_next_run_time(job_data)

    def _execute_in_background(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute job function in background thread.

        Args:
            job_data: Job data
            job_logger: Context logger
        """
        job_id = job_data["job_id"]

        try:
            # Execute the actual job function
            self._execute_job_function(job_data, job_logger)

            # Determine next status after successful execution
            from chronis.core.jobs.definition import JobInfo

            job_info = JobInfo(job_data)
            next_status = JobLifecycleManager.determine_next_status_after_execution(
                job_info, execution_succeeded=True
            )

            if next_status is None:
                # One-time job - delete after execution
                self.storage.delete_job(job_id)
                if self.verbose:
                    job_logger.info("One-time job deleted after execution")
            else:
                # Recurring job - mark as SCHEDULED
                self._update_job_status(job_id, next_status)

        except Exception as e:
            job_logger.error(f"Execution failed: {e}", exc_info=True)
            self._update_job_status(job_id, JobStatus.FAILED)

    def _execute_job_function(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute the actual job function (sync or async).

        Args:
            job_data: Job data
            job_logger: Context logger
        """
        import inspect

        func_name = job_data["func_name"]
        args = tuple(job_data.get("args", []))
        kwargs = job_data.get("kwargs", {})

        # Look up function from registry
        func = self.function_registry.get(func_name)
        if not func:
            raise ValueError(f"Job function not found: {func_name}")

        # Execute based on function type
        if inspect.iscoroutinefunction(func):
            # Async function
            coro = func(*args, **kwargs)
            self.async_executor.execute_coroutine(coro)
        else:
            # Sync function
            func(*args, **kwargs)

    def _update_job_status(self, job_id: str, status: JobStatus) -> None:
        """
        Update job status in storage.

        Args:
            job_id: Job ID
            status: New status
        """
        try:
            self.storage.update_job(
                job_id,
                {
                    "status": status.value,
                    "updated_at": utc_now().isoformat(),
                },
            )
        except Exception:
            self.logger.error(
                f"Failed to update job status to {status.value}", job_id=job_id, exc_info=True
            )

    def _update_next_run_time(self, job_data: dict[str, Any]) -> None:
        """
        Calculate and update next run time for recurring jobs.

        Args:
            job_data: Job data
        """
        job_id = job_data["job_id"]
        trigger_type = job_data["trigger_type"]
        trigger_args = job_data["trigger_args"]
        timezone = job_data.get("timezone", "UTC")

        # Skip for one-time jobs (DATE trigger)
        if trigger_type == TriggerType.DATE.value:
            return

        # Calculate next run time
        utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
            trigger_type, trigger_args, timezone
        )

        if utc_time:
            try:
                self.storage.update_job(
                    job_id,
                    {
                        "next_run_time": utc_time.isoformat(),
                        "next_run_time_local": local_time.isoformat() if local_time else None,
                        "updated_at": utc_now().isoformat(),
                    },
                )
            except Exception:
                self.logger.error("Failed to update next run time", job_id=job_id, exc_info=True)
