"""Trigger strategy getter."""

from chronis.core.triggers.base import TriggerStrategy
from chronis.core.triggers.cron import CronTrigger
from chronis.core.triggers.date import DateTrigger
from chronis.core.triggers.interval import IntervalTrigger

_TRIGGERS: dict[str, TriggerStrategy] = {
    "interval": IntervalTrigger(),
    "cron": CronTrigger(),
    "date": DateTrigger(),
}


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
    strategy = _TRIGGERS.get(trigger_type)
    if strategy is None:
        raise ValueError(f"Unknown trigger type: {trigger_type}")
    return strategy
