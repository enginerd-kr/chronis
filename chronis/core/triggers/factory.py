"""Factory for creating trigger strategies."""

from chronis.core.triggers.base import TriggerStrategy
from chronis.core.triggers.cron import CronTrigger
from chronis.core.triggers.date import DateTrigger
from chronis.core.triggers.interval import IntervalTrigger


class TriggerFactory:
    """Factory for creating trigger strategy instances."""

    # Singleton instances for each trigger type
    _strategies: dict[str, TriggerStrategy] = {
        "interval": IntervalTrigger(),
        "cron": CronTrigger(),
        "date": DateTrigger(),
    }

    @classmethod
    def get_strategy(cls, trigger_type: str) -> TriggerStrategy:
        """
        Get trigger strategy for the specified trigger type.

        Args:
            trigger_type: Type of trigger ("interval", "cron", "date")

        Returns:
            TriggerStrategy instance

        Raises:
            ValueError: If trigger type is unknown
        """
        strategy = cls._strategies.get(trigger_type)
        if not strategy:
            raise ValueError(f"Unknown trigger type: {trigger_type}")
        return strategy

    @classmethod
    def register_strategy(cls, trigger_type: str, strategy: TriggerStrategy) -> None:
        """
        Register a custom trigger strategy.

        Args:
            trigger_type: Type identifier for the trigger
            strategy: TriggerStrategy instance
        """
        cls._strategies[trigger_type] = strategy
