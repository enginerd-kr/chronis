"""Cron trigger strategy."""

from datetime import datetime
from typing import Any

from apscheduler.triggers.cron import CronTrigger as APSCronTrigger  # type: ignore[import-untyped]

from chronis.core.triggers.base import TriggerStrategy
from chronis.utils.time import ZoneInfo, get_timezone


class CronTrigger(TriggerStrategy):
    """Trigger for cron-based execution."""

    def calculate_next_run_time(
        self,
        trigger_args: dict[str, Any],
        timezone: str,
        current_time: datetime | None = None,
    ) -> datetime | None:
        """
        Calculate next run time for cron trigger.

        Args:
            trigger_args: Must contain cron fields (year, month, day, week, day_of_week, hour, minute)
            timezone: IANA timezone string
            current_time: Current time (timezone-aware)

        Returns:
            Next run time in UTC
        """
        tz = get_timezone(timezone)
        current = current_time if current_time else datetime.now(tz)

        if current.tzinfo is None:
            current = current.replace(tzinfo=tz)

        trigger = APSCronTrigger(
            year=trigger_args.get("year"),
            month=trigger_args.get("month"),
            day=trigger_args.get("day"),
            week=trigger_args.get("week"),
            day_of_week=trigger_args.get("day_of_week"),
            hour=trigger_args.get("hour"),
            minute=trigger_args.get("minute"),
            timezone=tz,
        )
        next_time = trigger.get_next_fire_time(None, current)

        if next_time is None:
            return None

        return next_time.astimezone(ZoneInfo("UTC"))
