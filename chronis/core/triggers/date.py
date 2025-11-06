"""Date trigger strategy."""

from datetime import datetime
from typing import Any

from chronis.core.triggers.base import TriggerStrategy
from chronis.utils.time import ZoneInfo


class DateTrigger(TriggerStrategy):
    """Trigger for one-time execution at a specific date/time."""

    def calculate_next_run_time(
        self,
        trigger_args: dict[str, Any],
        timezone: str,
        current_time: datetime | None = None,
    ) -> datetime | None:
        """
        Calculate next run time for date trigger.

        Args:
            trigger_args: Must contain "run_date" in ISO 8601 format
            timezone: IANA timezone string
            current_time: Not used for date trigger

        Returns:
            Specified run time in UTC, or None if no run_date specified
        """
        run_date_str = trigger_args.get("run_date")
        if not run_date_str:
            return None

        # Parse ISO 8601 date string (with timezone)
        # Replace 'Z' suffix with '+00:00' for proper parsing
        next_time = datetime.fromisoformat(run_date_str.replace("Z", "+00:00"))

        # Convert to UTC
        return next_time.astimezone(ZoneInfo("UTC"))
