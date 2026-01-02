"""Scheduling domain services."""

from datetime import datetime
from typing import Any

from chronis.core.common.types import TriggerType
from chronis.core.triggers import get_trigger_strategy
from chronis.utils.time import get_timezone


class NextRunTimeCalculator:
    """Domain service for calculating next run times based on trigger configuration."""

    @staticmethod
    def calculate(
        trigger_type: TriggerType | str,
        trigger_args: dict[str, Any],
        timezone: str = "UTC",
        current_time: datetime | None = None,
    ) -> datetime | None:
        """
        Calculate next run time for a job.

        Args:
            trigger_type: Type of trigger (INTERVAL, CRON, DATE)
            trigger_args: Trigger-specific arguments
            timezone: IANA timezone string
            current_time: Current time (defaults to now in given timezone)

        Returns:
            Next run time in UTC, or None if no next run
        """
        if isinstance(trigger_type, TriggerType):
            trigger_type = trigger_type.value

        if current_time is None:
            tz = get_timezone(timezone)
            current_time = datetime.now(tz)

        strategy = get_trigger_strategy(trigger_type)
        return strategy.calculate_next_run_time(trigger_args, timezone, current_time)

    @staticmethod
    def calculate_with_local_time(
        trigger_type: TriggerType | str,
        trigger_args: dict[str, Any],
        timezone: str = "UTC",
        current_time: datetime | None = None,
    ) -> tuple[datetime | None, datetime | None]:
        """
        Calculate next run time and return both UTC and local times.

        Args:
            trigger_type: Type of trigger (INTERVAL, CRON, DATE)
            trigger_args: Trigger-specific arguments
            timezone: IANA timezone string
            current_time: Current time (defaults to now in given timezone)

        Returns:
            Tuple of (next_run_time_utc, next_run_time_local)
        """
        next_run_time_utc = NextRunTimeCalculator.calculate(
            trigger_type, trigger_args, timezone, current_time
        )

        if next_run_time_utc is None:
            return None, None

        tz = get_timezone(timezone)
        next_run_time_local = next_run_time_utc.astimezone(tz)

        return next_run_time_utc, next_run_time_local
