"""Cron trigger strategy."""

from datetime import datetime
from typing import Any

from croniter import croniter

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
            trigger_args: Must contain cron fields (minute, hour, day, month, day_of_week)
            timezone: IANA timezone string
            current_time: Current time (timezone-aware)

        Returns:
            Next run time in UTC
        """
        tz = get_timezone(timezone)
        current = current_time if current_time else datetime.now(tz)

        # Build cron expression from trigger_args
        cron_expr = (
            f"{trigger_args.get('minute', '*')} "
            f"{trigger_args.get('hour', '*')} "
            f"{trigger_args.get('day', '*')} "
            f"{trigger_args.get('month', '*')} "
            f"{trigger_args.get('day_of_week', '*')}"
        )

        # Calculate next run time with timezone awareness
        iter_obj = croniter(cron_expr, current)
        next_time = iter_obj.get_next(datetime)

        # Convert to UTC
        return next_time.astimezone(ZoneInfo("UTC"))
