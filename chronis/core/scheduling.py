"""Scheduling domain services."""

from datetime import datetime
from typing import Any

from chronis.core.common.types import TriggerType
from chronis.core.triggers import TriggerFactory
from chronis.utils.time import get_timezone


class NextRunTimeCalculator:
    """
    Domain service for calculating next run times.

    This service encapsulates the logic for determining when a job should
    run next based on its trigger configuration. It acts as a facade over
    the trigger strategy pattern.
    """

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

        Example:
            >>> calculator = NextRunTimeCalculator()
            >>> next_time = calculator.calculate(
            ...     TriggerType.INTERVAL,
            ...     {"seconds": 30},
            ...     "UTC"
            ... )
        """
        # Convert trigger_type to string if enum
        if isinstance(trigger_type, TriggerType):
            trigger_type = trigger_type.value

        # Get current time in the job's timezone
        if current_time is None:
            tz = get_timezone(timezone)
            current_time = datetime.now(tz)

        # Get appropriate strategy and calculate
        strategy = TriggerFactory.get_strategy(trigger_type)
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

        This is useful when you need to store both representations.

        Args:
            trigger_type: Type of trigger (INTERVAL, CRON, DATE)
            trigger_args: Trigger-specific arguments
            timezone: IANA timezone string
            current_time: Current time (defaults to now in given timezone)

        Returns:
            Tuple of (next_run_time_utc, next_run_time_local)

        Example:
            >>> utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
            ...     TriggerType.CRON,
            ...     {"hour": "9", "minute": "0"},
            ...     "America/New_York"
            ... )
        """
        next_run_time_utc = NextRunTimeCalculator.calculate(
            trigger_type, trigger_args, timezone, current_time
        )

        if next_run_time_utc is None:
            return None, None

        # Convert to local timezone
        tz = get_timezone(timezone)
        next_run_time_local = next_run_time_utc.astimezone(tz)

        return next_run_time_utc, next_run_time_local

    @staticmethod
    def should_skip_calculation(trigger_type: TriggerType | str) -> bool:
        """
        Check if next run time calculation should be skipped.

        DATE triggers are one-time only, so we skip calculation after execution.

        Args:
            trigger_type: Type of trigger

        Returns:
            True if calculation should be skipped
        """
        if isinstance(trigger_type, str):
            trigger_type = TriggerType(trigger_type)

        return trigger_type == TriggerType.DATE

    @staticmethod
    def is_recurring(trigger_type: TriggerType | str) -> bool:
        """
        Check if a trigger type represents a recurring job.

        Args:
            trigger_type: Type of trigger

        Returns:
            True if the trigger is recurring (INTERVAL or CRON)
        """
        if isinstance(trigger_type, str):
            trigger_type = TriggerType(trigger_type)

        return trigger_type in (TriggerType.INTERVAL, TriggerType.CRON)

    @staticmethod
    def is_one_time(trigger_type: TriggerType | str) -> bool:
        """
        Check if a trigger type represents a one-time job.

        Args:
            trigger_type: Type of trigger

        Returns:
            True if the trigger is one-time (DATE)
        """
        if isinstance(trigger_type, str):
            trigger_type = TriggerType(trigger_type)

        return trigger_type == TriggerType.DATE
