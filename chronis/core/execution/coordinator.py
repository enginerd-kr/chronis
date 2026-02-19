"""Job execution coordination service."""

from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.execution.callback_invoker import CallbackInvoker
from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.execution.retry_handler import RetryHandler
from chronis.core.jobs.definition import JobInfo
from chronis.core.schedulers.next_run_calculator import NextRunTimeCalculator
from chronis.core.state import JobStatus
from chronis.core.state.enums import TriggerType
from chronis.utils.logging import ContextLogger
from chronis.utils.time import utc_now


class ExecutionCoordinator:
    """
    Coordinates job execution with concurrency control and distributed locking.

    Ensures jobs are executed exactly once across distributed instances.
    """

    def __init__(
        self,
        storage: JobStorageAdapter,
        lock: LockAdapter,
        executor: ThreadPoolExecutor,
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
        self.function_registry = function_registry
        self.failure_handler_registry = failure_handler_registry
        self.success_handler_registry = success_handler_registry
        self.global_on_failure = global_on_failure
        self.global_on_success = global_on_success
        self.logger = logger
        self.lock_prefix = lock_prefix
        self.lock_ttl_seconds = lock_ttl_seconds
        self.verbose = verbose

        # Initialize retry handler
        self.retry_handler = RetryHandler(storage=storage, lock=lock, logger=logger)

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
        lock_key = job_id  # LockAdapter adds its own prefix

        job_logger = self.logger.with_context(job_id=job_id, job_name=job_name)

        # Pre-check: Skip if not SCHEDULED
        job_status = JobStatus(job_data.get("status", "scheduled"))
        if job_status != JobStatus.SCHEDULED:
            return False

        with self._acquire_lock_context(lock_key) as lock_acquired:
            if not lock_acquired:
                return False

            try:
                # OPTIMIZED: Compare-and-Swap (CAS) pattern
                # Atomically updates job only if it's still SCHEDULED
                # This eliminates TOCTOU vulnerability and reduces lock holding time
                success, updated_job = self._try_claim_job_with_cas(job_id, job_data)

                if not success or updated_job is None:
                    # Job was already claimed by another instance or no longer ready
                    return False

                # Job successfully claimed - trigger execution
                self._trigger_execution(updated_job, job_logger, on_complete)
                return True

            except Exception as e:
                job_logger.error("Job execution failed", error=str(e), exc_info=True)
                return False

    @contextmanager
    def _acquire_lock_context(self, lock_key: str) -> Generator[bool, None, None]:
        """
        Context manager for lock acquisition and release with retry.

        Args:
            lock_key: Lock key

        Yields:
            True if lock acquired, False otherwise
        """
        lock_acquired = False
        try:
            lock_acquired = self.lock.acquire(lock_key, self.lock_ttl_seconds, blocking=False)
            yield lock_acquired
        finally:
            if lock_acquired:
                self.retry_handler.try_release_lock(lock_key)

    def _trigger_execution(
        self,
        job_data: dict[str, Any],
        job_logger: ContextLogger,
        on_complete: Callable[[str], None],
    ) -> None:
        """
        Trigger job execution in thread pool with automatic rollback on failure.

        NOTE: Status is already updated to RUNNING by caller (try_execute) while holding lock.
        This method only submits the job to the thread pool.

        If ThreadPool submission fails (e.g., executor shutdown, resource exhaustion),
        the job status is rolled back to SCHEDULED to allow retry in the next polling cycle.

        Args:
            job_data: Job data
            job_logger: Context logger
            on_complete: Completion callback

        Raises:
            Exception: Re-raises any exception after rollback
        """
        job_id = job_data["job_id"]

        try:
            # Submit to thread pool with completion callback
            future = self.executor.submit(self._execute_in_background, job_data, job_logger)

            # Add done callback
            future.add_done_callback(lambda f: on_complete(job_id))

        except Exception as submit_error:
            # Rollback to SCHEDULED on submission failure
            job_logger.warning(
                "Executor submit failed, rolling back to SCHEDULED",
                error=str(submit_error),
                error_type=type(submit_error).__name__,
            )
            self._update_job_status(job_id, JobStatus.SCHEDULED)
            raise

    def _execute_in_background(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute job function in background thread.

        Args:
            job_data: Job data
            job_logger: Context logger
        """
        job_id = job_data["job_id"]

        try:
            # Execute the actual job function (sync or async via asyncio.run)
            self._execute_job_function(job_data, job_logger)

            # Success - reset retry count if it was retried before
            if job_data.get("retry_count", 0) > 0:
                self.storage.update_job(
                    job_id, {"retry_count": 0, "updated_at": utc_now().isoformat()}
                )

            # Determine next status after successful execution
            next_status = JobInfo.determine_next_status_after_execution(
                trigger_type=job_data["trigger_type"], execution_succeeded=True
            )

            if next_status is None:
                # One-time job - delete after execution
                self.storage.delete_job(job_id)
            else:
                # Recurring job - update next_run_time and mark as SCHEDULED
                self._update_next_run_time(job_data)
                self._update_job_status(job_id, next_status)

            # Invoke success handler
            CallbackInvoker(
                self.failure_handler_registry,
                self.success_handler_registry,
                self.global_on_failure,
                self.global_on_success,
                self.logger,
            ).invoke_success_callback(job_id, job_data)

        except Exception as e:
            job_logger.error("Execution failed", error=str(e), exc_info=True)

            # Check if retry is possible
            retry_count = job_data.get("retry_count", 0)
            max_retries = job_data.get("max_retries", 0)

            if retry_count < max_retries:
                # Schedule retry with exponential backoff
                self.retry_handler.schedule_retry(job_data, retry_count + 1, job_logger)
            else:
                # No more retries - mark as FAILED
                self._update_job_status(job_id, JobStatus.FAILED)
                CallbackInvoker(
                    self.failure_handler_registry,
                    self.success_handler_registry,
                    self.global_on_failure,
                    self.global_on_success,
                    self.logger,
                ).invoke_failure_callback(job_id, e, job_data)

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
            # Async function - execute via asyncio.run() in current worker thread
            coro = func(*args, **kwargs)

            if timeout_seconds:
                # Wrap with timeout
                coro = asyncio.wait_for(coro, timeout=timeout_seconds)

            asyncio.run(coro)
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
                    from chronis.core.common.exceptions import JobTimeoutError

                    # Timeout occurred - thread is still running
                    timeout_msg = (
                        f"Job exceeded timeout of {timeout_seconds}s. "
                        "Note: Thread cannot be forcefully stopped and may continue running."
                    )
                    job_logger.warning(
                        "Job timeout (thread still running)",
                        timeout_seconds=timeout_seconds,
                        job_type="sync",
                    )
                    # Note: We cannot forcefully stop the thread, but we report timeout
                    raise JobTimeoutError(timeout_msg)

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

    def _try_claim_job_with_cas(
        self, job_id: str, job_data: dict[str, Any]
    ) -> tuple[bool, dict[str, Any] | None]:
        """
        Try to claim a job for execution using Compare-and-Swap (CAS) pattern.

        This method atomically transitions a job from SCHEDULED to RUNNING only if:
        1. The job status is still SCHEDULED
        2. The next_run_time hasn't been updated by another instance

        This eliminates the Time-of-Check to Time-of-Use (TOCTOU) vulnerability
        by combining verification and update into a single atomic operation.

        Args:
            job_id: Job ID to claim
            job_data: Job data from queue (may be stale)

        Returns:
            Tuple of (success, updated_job_data):
                - success: True if job was successfully claimed
                - updated_job_data: Updated job data if success=True, None otherwise
        """
        trigger_type = job_data["trigger_type"]
        current_time_str = utc_now().isoformat()

        # Build expected values - job must match these to be claimed
        expected_values = {
            "status": JobStatus.SCHEDULED.value,
        }

        # Also verify next_run_time hasn't changed (prevents stale queue entries)
        queue_next_run = job_data.get("next_run_time")
        if queue_next_run:
            # Only claim if next_run_time is still in the past
            if queue_next_run > current_time_str:
                return (False, None)
            expected_values["next_run_time"] = queue_next_run

        # Build updates - what to change if expectations match
        from chronis.type_defs import JobUpdateData

        updates: JobUpdateData = {
            "status": JobStatus.RUNNING.value,
            "updated_at": current_time_str,
        }

        # For recurring jobs, optimistically update next_run_time
        if trigger_type != TriggerType.DATE.value:
            trigger_args = job_data["trigger_args"]
            timezone = job_data.get("timezone", "UTC")

            # Use scheduled time as base to prevent drift accumulation
            scheduled_time = job_data.get("next_run_time")
            base_time = None
            if scheduled_time:
                base_time = datetime.fromisoformat(scheduled_time.replace("Z", "+00:00"))

            utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
                trigger_type, trigger_args, timezone, current_time=base_time
            )

            # Handle misfire: if next_run_time is in the past, recalculate from now
            if utc_time and utc_time <= utc_now():
                utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
                    trigger_type, trigger_args, timezone
                )

            if utc_time:
                updates["next_run_time"] = utc_time.isoformat()
                if local_time:
                    updates["next_run_time_local"] = local_time.isoformat()

        # Atomic compare-and-swap operation
        try:
            success, updated_job = self.storage.compare_and_swap_job(
                job_id, expected_values, updates
            )
            if success and updated_job:
                result = dict(updated_job)
                # Preserve original scheduled time for _update_next_run_time()
                result["_original_scheduled_time"] = queue_next_run
                return (True, result)
            return (False, None)
        except ValueError:
            # Job not found (deleted)
            return (False, None)
        except Exception:
            # Other errors - don't claim job (expected in distributed environments)
            return (False, None)

    def _update_job_status(self, job_id: str, status: JobStatus) -> None:
        """
        Update job status in storage.

        Args:
            job_id: Job ID
            status: New status

        Raises:
            Exception: If storage update fails (caller must handle)
        """
        self.storage.update_job(
            job_id,
            {
                "status": status.value,
                "updated_at": utc_now().isoformat(),
            },
        )

    def _update_next_run_time(self, job_data: dict[str, Any]) -> None:
        """
        Update execution times for recurring jobs.

        Note: next_run_time is already calculated in _try_claim_job_with_cas().
        This method only updates last_scheduled_time and last_run_time.

        Args:
            job_data: Job data (with _original_scheduled_time from CAS)
        """
        job_id = job_data["job_id"]
        trigger_type = job_data["trigger_type"]

        # Use original scheduled time (before CAS update) for last_scheduled_time
        original_scheduled_time = job_data.get("_original_scheduled_time")
        actual_time = utc_now().isoformat()

        if trigger_type == TriggerType.DATE.value:
            if original_scheduled_time:
                try:
                    self.storage.update_job(
                        job_id,
                        {
                            "last_scheduled_time": original_scheduled_time,
                            "last_run_time": actual_time,
                            "next_run_time": None,
                            "updated_at": utc_now().isoformat(),
                        },
                    )
                except Exception:
                    pass  # Non-critical
            return

        # For recurring jobs, next_run_time was already updated in _try_claim_job_with_cas()
        # Only update last_scheduled_time and last_run_time here
        if original_scheduled_time:
            try:
                self.storage.update_job(
                    job_id,
                    {
                        "last_scheduled_time": original_scheduled_time,
                        "last_run_time": actual_time,
                        "updated_at": utc_now().isoformat(),
                    },
                )
            except Exception:
                pass  # Non-critical
