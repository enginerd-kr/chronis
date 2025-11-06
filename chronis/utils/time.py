"""Time utilities for Chronis."""

from datetime import datetime, timezone

# Python 3.9+ zoneinfo, fallback to pytz for Python 3.8
try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz  # type: ignore

    class ZoneInfo:  # type: ignore
        """Compatibility wrapper for pytz."""

        def __new__(cls, key: str) -> timezone:  # type: ignore
            if key == "UTC":
                return timezone.utc
            return pytz.timezone(key)  # type: ignore


def utc_now() -> datetime:
    """
    Get current UTC time (timezone-aware).

    Returns:
        Current UTC datetime
    """
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
