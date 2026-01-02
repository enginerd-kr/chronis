"""Time utilities for Chronis."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def utc_now() -> datetime:
    """Get current UTC time (timezone-aware)."""
    return datetime.now(ZoneInfo("UTC"))


def get_timezone(tz_name: str) -> timezone:
    """
    Get timezone object from IANA timezone name.

    Args:
        tz_name: IANA timezone name (e.g., "Asia/Seoul", "America/New_York")

    Returns:
        Timezone object

    Raises:
        ValueError: Invalid timezone name
    """
    try:
        return ZoneInfo(tz_name)  # type: ignore
    except Exception as e:
        raise ValueError(f"Invalid timezone '{tz_name}': {e}") from e
