"""Misfire handling module."""

from enum import Enum

from chronis.core.common.types import TriggerType


class SimpleMisfirePolicy(str, Enum):
    """
    User-friendly misfire policies.

    These are the public-facing policies that users specify when creating jobs.
    """

    SKIP = "skip"
    RUN_ONCE = "run_once"
    RUN_ALL = "run_all"


class InternalMisfirePolicy(Enum):
    """
    Internal implementation policies (not exposed to users).

    These provide more granular control for future enhancements.
    """

    IGNORE = "ignore"
    FIRE_ONCE_NOW = "fire_once_now"
    FIRE_ALL_MISSED = "fire_all_missed"
    FIRE_AND_RESCHEDULE = "fire_and_reschedule"


# Policy mapping from simple to internal
POLICY_MAPPING = {
    SimpleMisfirePolicy.SKIP: InternalMisfirePolicy.IGNORE,
    SimpleMisfirePolicy.RUN_ONCE: InternalMisfirePolicy.FIRE_ONCE_NOW,
    SimpleMisfirePolicy.RUN_ALL: InternalMisfirePolicy.FIRE_ALL_MISSED,
}

# Default policies by trigger type
DEFAULT_POLICIES = {
    TriggerType.DATE: SimpleMisfirePolicy.RUN_ONCE,
    TriggerType.INTERVAL: SimpleMisfirePolicy.RUN_ONCE,
    TriggerType.CRON: SimpleMisfirePolicy.SKIP,
}


def get_default_policy(trigger_type: TriggerType) -> SimpleMisfirePolicy:
    """
    Get default misfire policy for a trigger type.

    Args:
        trigger_type: The trigger type

    Returns:
        Default misfire policy for that trigger type
    """
    return DEFAULT_POLICIES.get(trigger_type, SimpleMisfirePolicy.RUN_ONCE)


__all__ = [
    "SimpleMisfirePolicy",
    "InternalMisfirePolicy",
    "POLICY_MAPPING",
    "DEFAULT_POLICIES",
    "get_default_policy",
]
