"""Job execution coordination service."""

import asyncio
import threading
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

from tenacity import RetryError, retry, stop_after_attempt, wait_random_exponential

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.common.exceptions import JobTimeoutError
from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.execution.job_executor import JobExecutor
from chronis.core.jobs.definition import JobInfo
from chronis.core.schedulers.next_run_calculator import NextRunTimeCalculator
from chronis.core.state import JobStatus
from chronis.core.state.enums import TriggerType
from chronis.type_defs import JobUpdateData
from chronis.utils.logging import ContextLogger
from chronis.utils.time import get_timezone, parse_iso_datetime, utc_now


class ExecutionCoordinator:
    """
    Coordinates job execution with concurrency control and distributed locking.

    Manages the job lifecycle: CAS claiming, lock management, status transitions,
    retry scheduling, and callback invocation. Delegates actual function execution
    to JobExecutor.
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
        lock_ttl_seconds: int = 300,
        verbose: bool = False,
    ) -> None:
        self.storage = storage
        self.lock = lock
        self.executor = executor
        self.function_registry = function_registry
        self.failure_handler_registry = failure_handler_registry
        self.success_handler_registry = success_handler_registry
        self.global_on_failure = global_on_failure
        self.global_on_success = global_on_success
        self.logger = logger
        self.lock_ttl_seconds = lock_ttl_seconds
        self.verbose = verbose

        self._job_executor = JobExecutor(function_registry, logger)

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
        lock_key = job_id

        job_logger = self.logger.with_context(job_id=job_id, job_name=job_name)

        # CAS is the authoritative check; no pre-check needed (avoids TOCTOU)

        with self._acquire_lock_context(lock_key) as lock_acquired:
            if not lock_acquired:
                return False

            try:
                success, updated_job = self._try_claim_job_with_cas(job_id, job_data)

                if not success or updated_job is None:
                    return False

                self._trigger_execution(updated_job, job_logger, on_complete)
                return True

            except Exception as e:
                job_logger.error("Job execution failed", error=str(e), exc_info=True)
                return False

    @contextmanager
    def _acquire_lock_context(self, lock_key: str) -> Generator[bool, None, None]:
        """Context manager for lock acquisition and release with retry."""
        lock_acquired = False
        try:
            lock_acquired = self.lock.acquire(lock_key, self.lock_ttl_seconds, blocking=False)
            yield lock_acquired
        finally:
            if lock_acquired:
                self._try_release_lock(lock_key)

    def _trigger_execution(
        self,
        job_data: dict[str, Any],
        job_logger: ContextLogger,
        on_complete: Callable[[str], None],
    ) -> None:
        """
        Trigger job execution with automatic rollback on failure.

        Routes async functions to the shared event loop and sync functions
        to the thread pool executor. For sync jobs with timeout_seconds,
        a threading.Timer marks the job as failed on timeout without creating
        zombie threads.
        """
        job_id = job_data["job_id"]
        func_name = job_data.get("func_name", "")
        timeout_seconds = job_data.get("timeout_seconds")

        try:
            if self._job_executor.is_async(func_name):
                loop = self._job_executor.ensure_async_loop()
                future = asyncio.run_coroutine_threadsafe(
                    self._execute_async(job_data, job_logger), loop
                )
                future.add_done_callback(lambda f: on_complete(job_id))
            else:
                # For sync jobs with timeout, use threading.Event to coordinate
                # between the worker thread and the Timer thread.
                timed_out = threading.Event() if timeout_seconds else None

                future = self.executor.submit(
                    self._execute_in_background, job_data, job_logger, timed_out
                )
                future.add_done_callback(lambda f: on_complete(job_id))

                if timeout_seconds and timed_out is not None:

                    def _on_timeout() -> None:
                        timed_out.set()
                        job_logger.error(
                            "Job timed out",
                            timeout_seconds=timeout_seconds,
                        )
                        self._handle_job_failure(
                            job_data,
                            JobTimeoutError(f"Job '{job_id}' timed out after {timeout_seconds}s"),
                            job_logger,
                        )

                    timer = threading.Timer(timeout_seconds, _on_timeout)
                    timer.daemon = True
                    timer.start()

                    # Cancel timer when the future completes normally
                    future.add_done_callback(lambda f: timer.cancel())

        except Exception as submit_error:
            job_logger.warning(
                "Executor submit failed, rolling back to SCHEDULED",
                error=str(submit_error),
                error_type=type(submit_error).__name__,
            )
            try:
                self._update_job_status(job_id, JobStatus.SCHEDULED)
            except Exception as rollback_error:
                job_logger.error(
                    "Failed to rollback job status after submit failure",
                    error=str(rollback_error),
                )
            raise submit_error

    # ------------------------------------------------------------------
    # Execution dispatch (delegates to JobExecutor)
    # ------------------------------------------------------------------

    def _execute_in_background(
        self,
        job_data: dict[str, Any],
        job_logger: ContextLogger,
        timed_out: threading.Event | None = None,
    ) -> None:
        """Execute sync job in background thread.

        Args:
            timed_out: If set, the Timer already fired and marked the job as failed.
                       This prevents double success/failure handling.
        """
        try:
            self._job_executor.execute_sync(job_data, job_logger)
            if timed_out and timed_out.is_set():
                return  # Timer already handled failure
            self._handle_job_success(job_data, job_logger)
        except Exception as e:
            if timed_out and timed_out.is_set():
                return  # Timer already handled failure
            self._handle_job_failure(job_data, e, job_logger)

    async def _execute_async(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """Execute async job on the shared event loop."""
        loop = asyncio.get_running_loop()
        try:
            await self._job_executor.execute_async(job_data, job_logger)
            # Run sync I/O (storage updates, callbacks) off the event loop
            await loop.run_in_executor(None, self._handle_job_success, job_data, job_logger)
        except Exception as e:
            await loop.run_in_executor(None, self._handle_job_failure, job_data, e, job_logger)

    def shutdown_async(self, wait: bool = True) -> None:
        """Shut down the shared async event loop."""
        self._job_executor.shutdown_async(wait)

    # ------------------------------------------------------------------
    # Success/failure handlers
    # ------------------------------------------------------------------

    def _handle_job_success(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """Handle successful job execution (shared by sync and async paths)."""
        job_id = job_data["job_id"]
        now = utc_now()
        try:
            next_status = JobInfo.determine_next_status_after_execution(
                trigger_type=job_data["trigger_type"], execution_succeeded=True
            )

            if next_status is None:
                # DATE job: delete directly
                self.storage.delete_job(job_id)
            else:
                # Recurring job: combine ALL updates into single storage call
                updates: JobUpdateData = {
                    "status": next_status.value,
                    "updated_at": now.isoformat(),
                }

                if job_data.get("retry_count", 0) > 0:
                    updates["retry_count"] = 0

                original_scheduled_time = job_data.get("_original_scheduled_time")
                if original_scheduled_time:
                    updates["last_scheduled_time"] = original_scheduled_time
                    updates["last_run_time"] = now.isoformat()

                self.storage.update_job(job_id, updates)

            self._invoke_success_callback(job_id, job_data)
        except Exception as e:
            job_logger.error(
                "Failed during post-execution success handling",
                error=str(e),
                job_id=job_id,
                exc_info=True,
            )

    def _handle_job_failure(
        self,
        job_data: dict[str, Any],
        error: Exception,
        job_logger: ContextLogger,
    ) -> None:
        """Handle failed job execution (shared by sync and async paths)."""
        job_id = job_data["job_id"]
        job_logger.error("Execution failed", error=str(error), exc_info=True)

        try:
            retry_count = job_data.get("retry_count", 0)
            max_retries = job_data.get("max_retries", 0)

            if retry_count < max_retries:
                self._schedule_retry(job_data, retry_count + 1, job_logger)
            else:
                self._update_job_status(job_id, JobStatus.FAILED)
                self._invoke_failure_callback(job_id, error, job_data)
        except Exception as e:
            job_logger.error(
                "Failed during post-execution failure handling",
                error=str(e),
                job_id=job_id,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # CAS and state management
    # ------------------------------------------------------------------

    def _try_claim_job_with_cas(
        self, job_id: str, job_data: dict[str, Any]
    ) -> tuple[bool, dict[str, Any] | None]:
        """
        Try to claim a job for execution using Compare-and-Swap (CAS) pattern.

        This method atomically transitions a job from SCHEDULED to RUNNING only if:
        1. The job status is still SCHEDULED
        2. The next_run_time hasn't been updated by another instance

        Args:
            job_id: Job ID to claim
            job_data: Job data from queue (may be stale)

        Returns:
            Tuple of (success, updated_job_data)
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
                base_time = parse_iso_datetime(scheduled_time)

            utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
                trigger_type, trigger_args, timezone, current_time=base_time
            )

            # Handle misfire: if next_run_time is in the past, recalculate from now
            # Exception: run_all policy keeps incremental time for catch-up execution
            if utc_time and utc_time <= utc_now() and job_data.get("if_missed") != "run_all":
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
        """Update job status in storage."""
        self.storage.update_job(
            job_id,
            {
                "status": status.value,
                "updated_at": utc_now().isoformat(),
            },
        )

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def _invoke_success_callback(self, job_id: str, job_data: dict[str, Any]) -> None:
        """Invoke job-specific and global success handlers."""
        handler = self.success_handler_registry.get(job_id)
        if not handler and not self.global_on_success:
            return

        job_info = JobInfo.from_dict(job_data)
        self._safe_invoke(handler, "Job-specific success", job_id, job_id, job_info)
        self._safe_invoke(self.global_on_success, "Global success", job_id, job_id, job_info)

    def _invoke_failure_callback(
        self, job_id: str, error: Exception, job_data: dict[str, Any]
    ) -> None:
        """Invoke job-specific and global failure handlers."""
        handler = self.failure_handler_registry.get(job_id)
        if not handler and not self.global_on_failure:
            return

        job_info = JobInfo.from_dict(job_data)
        self._safe_invoke(handler, "Job-specific failure", job_id, job_id, error, job_info)
        self._safe_invoke(self.global_on_failure, "Global failure", job_id, job_id, error, job_info)

    def _safe_invoke(self, handler: Any, label: str, job_id: str, *args: Any) -> None:
        """Invoke a callback handler with error logging."""
        if not handler:
            return
        try:
            handler(*args)
        except Exception as e:
            self.logger.error(
                f"{label} handler raised exception",
                error=str(e),
                job_id=job_id,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Retry and lock management
    # ------------------------------------------------------------------

    def _schedule_retry(
        self, job_data: dict[str, Any], next_retry_count: int, job_logger: ContextLogger
    ) -> None:
        """
        Schedule job retry with exponential backoff.

        Backoff formula: delay = base_delay * (2 ^ (retry_count - 1)), capped at 3600s.
        """
        job_id = job_data["job_id"]
        base_delay = job_data.get("retry_delay_seconds", 60)
        timezone = job_data.get("timezone", "UTC")
        now = utc_now()

        # Exponential backoff: 60s, 120s, 240s, 480s, 960s, 1800s, 3600s (cap)
        delay_seconds = min(base_delay * (2 ** (next_retry_count - 1)), 3600)

        next_run_time = now + timedelta(seconds=delay_seconds)
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
                    "updated_at": now.isoformat(),
                },
            )

            max_retries = job_data.get("max_retries", 0)
            job_logger.warning(
                "Retry scheduled",
                attempt=next_retry_count,
                max_retries=max_retries,
                delay_seconds=delay_seconds,
            )
        except Exception as e:
            job_logger.error("Failed to schedule retry", error=str(e))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=0.1, min=0.1, max=1),
        reraise=True,
    )
    def _release_lock_with_retry(self, lock_key: str) -> None:
        """Release lock with automatic retry and jitter."""
        self.lock.release(lock_key)

    def _try_release_lock(self, lock_key: str) -> None:
        """Try to release lock, catching retry errors."""
        try:
            self._release_lock_with_retry(lock_key)
        except RetryError:
            pass  # Non-critical, lock will expire via TTL
