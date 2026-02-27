"""Trigger strategies for job scheduling."""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any

from apscheduler.triggers.cron import CronTrigger as APSCronTrigger  # type: ignore[import-untyped]

from chronis.utils.time import ZoneInfo, get_timezone, parse_iso_datetime


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
            timezone: IANA timezone string
            current_time: Current time (timezone-aware), defaults to now in specified timezone

        Returns:
            Next run time in UTC, or None if no next run time
        """
        pass


class IntervalTrigger(TriggerStrategy):
    """Trigger for periodic/interval-based execution."""

    def calculate_next_run_time(
        self,
        trigger_args: dict[str, Any],
        timezone: str,
        current_time: datetime | None = None,
    ) -> datetime | None:
        tz = get_timezone(timezone)
        current = current_time if current_time else datetime.now(tz)

        interval = timedelta(
            seconds=trigger_args.get("seconds", 0),
            minutes=trigger_args.get("minutes", 0),
            hours=trigger_args.get("hours", 0),
            days=trigger_args.get("days", 0),
            weeks=trigger_args.get("weeks", 0),
        )

        if interval <= timedelta(0):
            raise ValueError(
                "Interval must be positive. "
                "Provide at least one of: seconds, minutes, hours, days, weeks with a value > 0."
            )

        next_time = current + interval
        return next_time.astimezone(ZoneInfo("UTC"))


class CronTrigger(TriggerStrategy):
    """Trigger for cron-based execution."""

    def calculate_next_run_time(
        self,
        trigger_args: dict[str, Any],
        timezone: str,
        current_time: datetime | None = None,
    ) -> datetime | None:
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


class DateTrigger(TriggerStrategy):
    """Trigger for one-time execution at a specific date/time."""

    def calculate_next_run_time(
        self,
        trigger_args: dict[str, Any],
        timezone: str,
        current_time: datetime | None = None,
    ) -> datetime | None:
        run_date_str = trigger_args.get("run_date")
        if not run_date_str:
            return None

        next_time = parse_iso_datetime(run_date_str)
        next_time_utc = next_time.astimezone(ZoneInfo("UTC"))

        if current_time and next_time_utc <= current_time.astimezone(ZoneInfo("UTC")):
            return None

        return next_time_utc


_TRIGGERS: dict[str, TriggerStrategy] = {
    "interval": IntervalTrigger(),
    "cron": CronTrigger(),
    "date": DateTrigger(),
}


def get_trigger_strategy(trigger_type: str) -> TriggerStrategy:
    """Get trigger strategy for the specified trigger type."""
    strategy = _TRIGGERS.get(trigger_type)
    if strategy is None:
        raise ValueError(f"Unknown trigger type: {trigger_type}")
    return strategy


__all__ = [
    "TriggerStrategy",
    "IntervalTrigger",
    "CronTrigger",
    "DateTrigger",
    "get_trigger_strategy",
]
