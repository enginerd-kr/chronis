"""Base class for trigger strategies."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class TriggerStrategy(ABC):
    """Abstract base class for trigger calculation strategies."""

    @abstractmethod
    def calculate_next_run_time(
        self,
        trigger_args: dict[str, Any],
        timezone: str,
        current_time: datetime | None = None,
    ) -> datetime | None:
        """
        Calculate next run time for the job.

        Args:
            trigger_args: Trigger-specific arguments
            timezone: IANA timezone string (e.g., "Asia/Seoul", "UTC")
            current_time: Current time (timezone-aware), defaults to now in specified timezone

        Returns:
            Next run time in UTC, or None if no next run time
        """
        pass
