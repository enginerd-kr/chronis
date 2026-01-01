"""Pure unit tests for time utilities."""

import pytest

from chronis.utils.time import get_timezone, utc_now


class TestGetTimezoneErrorHandling:
    """Test get_timezone error handling."""

    def test_invalid_timezone_raises_valueerror(self):
        """Test that invalid timezone name raises ValueError with proper message."""
        with pytest.raises(ValueError) as exc_info:
            get_timezone("Invalid/Nonexistent/Timezone")

        error_msg = str(exc_info.value)
        assert "Invalid timezone" in error_msg
        assert "Invalid/Nonexistent/Timezone" in error_msg

    def test_empty_timezone_raises_valueerror(self):
        """Test that empty timezone name raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_timezone("")

        assert "Invalid timezone" in str(exc_info.value)

    def test_none_like_timezone_raises_valueerror(self):
        """Test that various invalid inputs raise ValueError."""
        invalid_names = ["", " ", "123", "UTC/Invalid"]

        for invalid_name in invalid_names:
            with pytest.raises(ValueError):
                get_timezone(invalid_name)


class TestUtcNow:
    """Test utc_now function."""

    def test_utc_now_returns_timezone_aware_datetime(self):
        """Test that utc_now returns timezone-aware datetime."""
        now = utc_now()

        assert now.tzinfo is not None

    def test_utc_now_is_utc_timezone(self):
        """Test that utc_now returns UTC timezone."""
        now = utc_now()

        # UTC offset should be 0
        assert now.utcoffset().total_seconds() == 0
