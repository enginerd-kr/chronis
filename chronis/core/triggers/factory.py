"""Trigger strategy getter."""

from chronis.core.triggers.base import TriggerStrategy
from chronis.core.triggers.cron import CronTrigger
from chronis.core.triggers.date import DateTrigger
from chronis.core.triggers.interval import IntervalTrigger

# Singleton instances for each trigger type
_INTERVAL_TRIGGER = IntervalTrigger()
_CRON_TRIGGER = CronTrigger()
_DATE_TRIGGER = DateTrigger()


def get_trigger_strategy(trigger_type: str) -> TriggerStrategy:
    """
    Get trigger strategy for the specified trigger type.

    Args:
        trigger_type: Type of trigger ("interval", "cron", "date")

    Returns:
        TriggerStrategy instance

    Raises:
        ValueError: If trigger type is unknown
    """
    if trigger_type == "interval":
        return _INTERVAL_TRIGGER
    elif trigger_type == "cron":
        return _CRON_TRIGGER
    elif trigger_type == "date":
        return _DATE_TRIGGER
    else:
        raise ValueError(f"Unknown trigger type: {trigger_type}")
