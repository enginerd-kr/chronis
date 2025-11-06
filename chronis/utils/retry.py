"""Retry strategy for job execution."""

import time

from chronis.utils.logging import ContextLogger


class RetryStrategy:
    """Encapsulates retry logic with configurable backoff."""

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay_seconds: int = 60,
        use_exponential_backoff: bool = True,
    ) -> None:
        """
        Initialize retry strategy.

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay_seconds: Base delay between retries in seconds
            use_exponential_backoff: Enable exponential backoff
        """
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.use_exponential_backoff = use_exponential_backoff

    def calculate_delay(self, attempt: int) -> int:
        """
        Calculate delay for given attempt.

        Args:
            attempt: Attempt number (0-indexed, first retry is attempt 1)

        Returns:
            Delay in seconds
        """
        if self.use_exponential_backoff:
            # Exponential backoff: delay * (2 ^ (attempt - 1))
            return self.retry_delay_seconds * (2 ** (attempt - 1))
        return self.retry_delay_seconds

    def should_retry(self, attempt: int) -> bool:
        """
        Check if should retry after given attempt.

        Args:
            attempt: Current attempt number

        Returns:
            True if should retry
        """
        return attempt < self.max_retries

    def wait_before_retry(self, attempt: int, logger: ContextLogger | None = None) -> None:
        """
        Wait before retry with optional logging.

        Args:
            attempt: Current attempt number
            logger: Optional logger for logging wait time
        """
        delay = self.calculate_delay(attempt)

        if logger:
            logger.warning(
                f"Retrying (attempt {attempt}/{self.max_retries})",
                retry_attempt=attempt,
                delay_seconds=delay,
            )

        time.sleep(delay)
