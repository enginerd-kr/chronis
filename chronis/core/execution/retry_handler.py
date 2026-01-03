"""Job retry logic with exponential backoff."""

from datetime import timedelta
from typing import Any

from tenacity import RetryError, retry, stop_after_attempt, wait_random_exponential

from chronis.adapters.base import JobStorageAdapter, LockAdapter
from chronis.core.state import JobStatus
from chronis.utils.logging import ContextLogger
from chronis.utils.time import get_timezone, utc_now


class RetryHandler:
    """Handles job retry scheduling with exponential backoff."""

    def __init__(
        self,
        storage: JobStorageAdapter,
        lock: LockAdapter,
        logger: ContextLogger,
    ) -> None:
        """
        Initialize retry handler.

        Args:
            storage: Job storage adapter
            lock: Lock adapter for releasing locks with retry
            logger: Context logger
        """
        self.storage = storage
        self.lock = lock
        self.logger = logger

    def should_retry(self, retry_count: int, max_retries: int) -> bool:
        """
        Check if job should be retried.

        Args:
            retry_count: Current retry count
            max_retries: Maximum allowed retries

        Returns:
            True if job should be retried
        """
        return retry_count < max_retries

    def schedule_retry(
        self, job_data: dict[str, Any], next_retry_count: int, job_logger: ContextLogger
    ) -> None:
        """
        Schedule job retry with exponential backoff.

        Exponential backoff formula: delay = base_delay * (2 ^ (retry_count - 1))
        Max delay capped at 3600 seconds (1 hour).

        Args:
            job_data: Job data from storage
            next_retry_count: Next retry attempt number (1-indexed)
            job_logger: Context logger for this job
        """
        job_id = job_data["job_id"]
        base_delay = job_data.get("retry_delay_seconds", 60)
        timezone = job_data.get("timezone", "UTC")

        # Exponential backoff: 60s, 120s, 240s, 480s, 960s, 1800s, 3600s (cap)
        delay_seconds = base_delay * (2 ** (next_retry_count - 1))
        delay_seconds = min(delay_seconds, 3600)  # Cap at 1 hour

        next_run_time = utc_now() + timedelta(seconds=delay_seconds)

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=0.1, min=0.1, max=1),
        reraise=True,
    )
    def release_lock_with_retry(self, lock_key: str) -> None:
        """
        Release lock with automatic retry and jitter.

        Uses random exponential backoff (0.1-1s) to prevent thundering herd.

        Args:
            lock_key: Lock key to release

        Raises:
            RetryError: If all retry attempts fail
        """
        self.lock.release(lock_key)

    def try_release_lock(self, lock_key: str, job_logger: ContextLogger) -> None:
        """
        Try to release lock, catching and logging retry errors.

        Args:
            lock_key: Lock key to release
            job_logger: Context logger for this job
        """
        try:
            self.release_lock_with_retry(lock_key)
        except RetryError as e:
            job_logger.error(
                f"Lock release failed after retries: {e}", lock_key=lock_key, exc_info=True
            )
