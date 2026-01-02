"""Interval trigger strategy."""

from datetime import datetime, timedelta
from typing import Any

from chronis.core.triggers.base import TriggerStrategy
from chronis.utils.time import ZoneInfo, get_timezone


class IntervalTrigger(TriggerStrategy):
    """Trigger for periodic/interval-based execution."""

    def calculate_next_run_time(
        self,
        trigger_args: dict[str, Any],
        timezone: str,
        current_time: datetime | None = None,
    ) -> datetime | None:
        """
        Calculate next run time for interval trigger.

        Args:
            trigger_args: Must contain "seconds", "minutes", "hours", "days", and/or "weeks"
            timezone: IANA timezone string
            current_time: Current time (timezone-aware)

        Returns:
            Next run time in UTC
        """
        tz = get_timezone(timezone)
        current = current_time if current_time else datetime.now(tz)

        seconds = trigger_args.get("seconds", 0)
        minutes = trigger_args.get("minutes", 0)
        hours = trigger_args.get("hours", 0)
        days = trigger_args.get("days", 0)
        weeks = trigger_args.get("weeks", 0)

        next_time = current + timedelta(
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            days=days,
            weeks=weeks,
        )

        return next_time.astimezone(ZoneInfo("UTC"))
