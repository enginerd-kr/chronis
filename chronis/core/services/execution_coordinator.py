"""Job execution coordination service."""

from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any

from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core import lifecycle
from chronis.core.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.common.types import TriggerType
from chronis.core.execution.async_loop import AsyncExecutor
from chronis.core.jobs.definition import JobInfo
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
        failure_handler_registry: dict[str, OnFailureCallback],
        success_handler_registry: dict[str, OnSuccessCallback],
        global_on_failure: OnFailureCallback | None,
        global_on_success: OnSuccessCallback | None,
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
            failure_handler_registry: Registry of job-specific failure handlers by job_id
            success_handler_registry: Registry of job-specific success handlers by job_id
            global_on_failure: Global failure handler for all jobs
            global_on_success: Global success handler for all jobs
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
        self.failure_handler_registry = failure_handler_registry
        self.success_handler_registry = success_handler_registry
        self.global_on_failure = global_on_failure
        self.global_on_success = global_on_success
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
        Context manager for lock acquisition and release with retry.

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
                    self._release_lock_with_retry(lock_key)
                except RetryError as e:
                    job_logger.error(
                        f"Lock release failed after retries: {e}", lock_key=lock_key, exc_info=True
                    )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.1, min=0.1, max=1),
        reraise=True,
    )
    def _release_lock_with_retry(self, lock_key: str):
        """Release lock with automatic retry (0.1s, 0.2s, 0.4s backoff)."""
        self.lock.release(lock_key)

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
            # Update state to RUNNING
            self._update_job_status(job_id, JobStatus.RUNNING)

            # Submit to thread pool with completion callback
            future = self.executor.submit(self._execute_in_background, job_data, job_logger)

            # Add done callback
            future.add_done_callback(lambda f: on_complete(job_id))

            # Log only in verbose mode
            if self.verbose:
                job_logger.info("Job triggered")

        except Exception as e:
            job_logger.error(f"Trigger failed: {e}", exc_info=True)

    def _execute_in_background(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute job function in background thread.

        Args:
            job_data: Job data
            job_logger: Context logger
        """
        import inspect

        job_id = job_data["job_id"]
        func_name = job_data["func_name"]
        func = self.function_registry.get(func_name)

        # Check if function is async (will be handled in callback)
        is_async = func and inspect.iscoroutinefunction(func)

        try:
            # Execute the actual job function
            self._execute_job_function(job_data, job_logger)

            # Success - reset retry count if it was retried before
            if job_data.get("retry_count", 0) > 0:
                self.storage.update_job(
                    job_id, {"retry_count": 0, "updated_at": utc_now().isoformat()}
                )

            # Determine next status after successful execution
            next_status = lifecycle.determine_next_status_after_execution(
                trigger_type=job_data["trigger_type"], execution_succeeded=True
            )

            if next_status is None:
                # One-time job - delete after execution
                self.storage.delete_job(job_id)
                if self.verbose:
                    job_logger.info("One-time job deleted after execution")
            else:
                # Recurring job - update next_run_time and mark as SCHEDULED
                self._update_next_run_time(job_data)
                self._update_job_status(job_id, next_status)

            # Invoke success handler (skip for async - handled in callback)
            if not is_async:
                self._invoke_success_handler(job_id, job_data)

        except Exception as e:
            job_logger.error(f"Execution failed: {e}", exc_info=True)

            # Check if retry is possible
            retry_count = job_data.get("retry_count", 0)
            max_retries = job_data.get("max_retries", 0)

            if retry_count < max_retries:
                # Schedule retry with exponential backoff
                self._schedule_retry(job_data, retry_count + 1, job_logger)
            else:
                # No more retries - mark as FAILED
                self._update_job_status(job_id, JobStatus.FAILED)
                self._invoke_failure_handler(job_id, e, job_data)

    def _execute_job_function(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute the actual job function (sync or async) with timeout support.

        Args:
            job_data: Job data
            job_logger: Context logger

        Raises:
            TimeoutError: If job execution exceeds timeout_seconds
        """
        import asyncio
        import inspect

        func_name = job_data["func_name"]
        args = tuple(job_data.get("args", []))
        kwargs = job_data.get("kwargs", {})
        timeout_seconds = job_data.get("timeout_seconds")

        # Look up function from registry
        func = self.function_registry.get(func_name)
        if not func:
            from chronis.core.common.exceptions import FunctionNotRegisteredError

            raise FunctionNotRegisteredError(
                f"Function '{func_name}' is not registered. "
                "Call scheduler.register_job_function(name, func) before creating jobs."
            )

        # Execute based on function type
        if inspect.iscoroutinefunction(func):
            # Async function - execute with timeout
            coro = func(*args, **kwargs)

            if timeout_seconds:
                # Wrap with timeout
                coro = asyncio.wait_for(coro, timeout=timeout_seconds)

            future = self.async_executor.execute_coroutine(coro)

            # Add error callback to log async failures and update job status
            job_id = job_data["job_id"]

            def _handle_async_completion(fut):
                try:
                    fut.result()  # This will raise if the coroutine raised or timed out
                    # Async job succeeded - invoke success handler
                    self._invoke_success_handler(job_id, job_data)
                except TimeoutError:
                    timeout_msg = f"Job exceeded timeout of {timeout_seconds}s"
                    job_logger.error(timeout_msg)
                    self._update_job_status(job_id, JobStatus.FAILED)
                    self._invoke_failure_handler(job_id, TimeoutError(timeout_msg), job_data)
                except Exception as e:
                    job_logger.error(f"Async job execution failed: {e}", exc_info=True)
                    # Update job status to FAILED if async execution fails
                    self._update_job_status(job_id, JobStatus.FAILED)
                    self._invoke_failure_handler(job_id, e, job_data)

            future.add_done_callback(_handle_async_completion)
        else:
            # Sync function - execute with timeout
            if timeout_seconds:
                # Use threading to implement timeout for sync functions
                import threading

                result: dict[str, Any] = {"completed": False, "error": None}

                def _run_with_result():
                    try:
                        func(*args, **kwargs)
                        result["completed"] = True
                    except Exception as e:
                        result["error"] = e

                # Run function in a separate thread
                thread = threading.Thread(target=_run_with_result, daemon=True)
                thread.start()
                thread.join(timeout=timeout_seconds)

                # Check if thread completed
                if thread.is_alive():
                    # Timeout occurred - thread is still running
                    timeout_msg = f"Job exceeded timeout of {timeout_seconds}s"
                    job_logger.error(timeout_msg)
                    # Note: We cannot forcefully stop the thread, but we report timeout
                    raise TimeoutError(timeout_msg)

                # Check if function raised an error
                error = result["error"]
                if error:
                    raise error  # type: ignore[misc]

                # Check if function completed successfully
                if not result["completed"]:
                    raise RuntimeError("Job did not complete")
            else:
                # No timeout - execute directly
                func(*args, **kwargs)

    def _schedule_retry(
        self, job_data: dict[str, Any], next_retry_count: int, job_logger: ContextLogger
    ) -> None:
        """
        Schedule job retry with exponential backoff.

        Args:
            job_data: Job data
            next_retry_count: Next retry attempt number (1-indexed)
            job_logger: Context logger
        """
        from datetime import timedelta

        from chronis.utils.time import get_timezone

        job_id = job_data["job_id"]
        base_delay = job_data.get("retry_delay_seconds", 60)
        timezone = job_data.get("timezone", "UTC")

        # Exponential backoff: base_delay * (2 ^ (retry_count - 1))
        # Examples: 60s, 120s, 240s, 480s, ...
        delay_seconds = base_delay * (2 ** (next_retry_count - 1))

        # Cap at 1 hour to prevent excessive delays
        delay_seconds = min(delay_seconds, 3600)

        next_run_time = utc_now() + timedelta(seconds=delay_seconds)

        # Calculate local time for user display
        tz = get_timezone(timezone)
        next_run_time_local = next_run_time.astimezone(tz)

        try:
            self.storage.update_job(
                job_id,
                {
                    "retry_count": next_retry_count,
                    "next_run_time": next_run_time.isoformat(),
                    "next_run_time_local": next_run_time_local.isoformat(),
                    "status": JobStatus.SCHEDULED.value,
                    "updated_at": utc_now().isoformat(),
                },
            )

            max_retries = job_data.get("max_retries", 0)
            job_logger.info(
                f"Retry scheduled: attempt {next_retry_count}/{max_retries} in {delay_seconds}s"
            )

        except Exception as e:
            job_logger.error(f"Failed to schedule retry: {e}", exc_info=True)

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
        Calculate and update next run time and execution times for recurring jobs.

        Uses update_job_run_times() to track execution history for misfire detection.

        Args:
            job_data: Job data
        """
        job_id = job_data["job_id"]
        trigger_type = job_data["trigger_type"]
        trigger_args = job_data["trigger_args"]
        timezone = job_data.get("timezone", "UTC")

        # Get scheduled and actual times
        scheduled_time = job_data.get("next_run_time")  # What was scheduled
        actual_time = utc_now().isoformat()  # When it actually ran

        # Skip for one-time jobs (DATE trigger)
        if trigger_type == TriggerType.DATE.value:
            # Still record execution for one-time jobs, but next_run_time = None
            if scheduled_time:
                try:
                    self.storage.update_job_run_times(
                        job_id=job_id,
                        scheduled_time=scheduled_time,
                        actual_time=actual_time,
                        next_run_time=None,
                    )
                except Exception:
                    self.logger.error(
                        "Failed to update job run times", job_id=job_id, exc_info=True
                    )
            return

        # Calculate next run time for recurring jobs
        utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
            trigger_type, trigger_args, timezone
        )

        if utc_time and scheduled_time:
            try:
                # Use new update_job_run_times method
                self.storage.update_job_run_times(
                    job_id=job_id,
                    scheduled_time=scheduled_time,
                    actual_time=actual_time,
                    next_run_time=utc_time.isoformat(),
                )

                # Also update local time (for display)
                self.storage.update_job(
                    job_id,
                    {
                        "next_run_time_local": local_time.isoformat() if local_time else None,
                    },
                )
            except Exception:
                self.logger.error("Failed to update job run times", job_id=job_id, exc_info=True)

    def _invoke_failure_handler(
        self, job_id: str, error: Exception, job_data: dict[str, Any]
    ) -> None:
        """
        Invoke failure handlers. Both job-specific and global handlers are called.

        Args:
            job_id: Job ID that failed
            error: Exception that occurred
            job_data: Job data from storage
        """
        job_info = JobInfo.from_dict(job_data)

        # Invoke job-specific handler first
        job_handler = self.failure_handler_registry.get(job_id)
        if job_handler:
            try:
                job_handler(job_id, error, job_info)
            except Exception as handler_error:
                self.logger.error(
                    f"Job-specific failure handler raised exception: {handler_error}",
                    job_id=job_id,
                    exc_info=True,
                )

        # Also invoke global handler
        if self.global_on_failure:
            try:
                self.global_on_failure(job_id, error, job_info)
            except Exception as handler_error:
                self.logger.error(
                    f"Global failure handler raised exception: {handler_error}",
                    job_id=job_id,
                    exc_info=True,
                )

    def _invoke_success_handler(self, job_id: str, job_data: dict[str, Any]) -> None:
        """
        Invoke success handlers. Both job-specific and global handlers are called.

        Args:
            job_id: Job ID that succeeded
            job_data: Job data from storage
        """
        job_info = JobInfo.from_dict(job_data)

        # Invoke job-specific handler first
        job_handler = self.success_handler_registry.get(job_id)
        if job_handler:
            try:
                job_handler(job_id, job_info)
            except Exception as handler_error:
                self.logger.error(
                    f"Job-specific success handler raised exception: {handler_error}",
                    job_id=job_id,
                    exc_info=True,
                )

        # Also invoke global handler
        if self.global_on_success:
            try:
                self.global_on_success(job_id, job_info)
            except Exception as handler_error:
                self.logger.error(
                    f"Global success handler raised exception: {handler_error}",
                    job_id=job_id,
                    exc_info=True,
                )
