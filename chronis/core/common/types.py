"""Common type definitions for Chronis."""

from enum import Enum


class TriggerType(Enum):
    """Trigger type for job scheduling."""

    INTERVAL = "interval"  # Periodic execution
    CRON = "cron"  # Cron expression
    DATE = "date"  # One-time execution
