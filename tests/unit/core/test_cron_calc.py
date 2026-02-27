"""Tests for the custom cron calculator (replacement for APScheduler CronTrigger)."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from chronis.core.triggers.cron_calc import calculate_next_cron_time

UTC = ZoneInfo("UTC")


def dt(year, month, day, hour=0, minute=0, second=0, tz=UTC):
    return datetime(year, month, day, hour, minute, second, tzinfo=tz)


class TestBasicCronCalculation:
    """Basic cron calculation tests."""

    def test_next_minute(self):
        """No fields specified -> next minute."""
        current = dt(2025, 6, 15, 10, 30)
        result = calculate_next_cron_time(current)
        assert result == dt(2025, 6, 15, 10, 30)

    def test_next_minute_with_seconds(self):
        """If current has seconds, ceil to next minute."""
        current = dt(2025, 6, 15, 10, 30, 15)
        result = calculate_next_cron_time(current)
        assert result == dt(2025, 6, 15, 10, 31)

    def test_daily_at_specific_time(self):
        """hour=9, minute=0 from 10:30 -> next day 09:00."""
        current = dt(2025, 6, 15, 10, 30)
        result = calculate_next_cron_time(current, hour=9, minute=0)
        assert result == dt(2025, 6, 16, 9, 0)

    def test_daily_at_specific_time_before(self):
        """hour=14, minute=0 from 10:30 -> same day 14:00."""
        current = dt(2025, 6, 15, 10, 30)
        result = calculate_next_cron_time(current, hour=14, minute=0)
        assert result == dt(2025, 6, 15, 14, 0)

    def test_hourly_at_specific_minute(self):
        """minute=5 from 10:30 -> 11:05."""
        current = dt(2025, 6, 15, 10, 30)
        result = calculate_next_cron_time(current, minute=5)
        assert result == dt(2025, 6, 15, 11, 5)

    def test_int_values(self):
        """Ensure int values (not strings) work."""
        current = dt(2025, 6, 15, 10, 30)
        result = calculate_next_cron_time(current, hour=14, minute=0)
        assert result == dt(2025, 6, 15, 14, 0)

    def test_string_values(self):
        """Ensure string values work."""
        current = dt(2025, 6, 15, 10, 30)
        result = calculate_next_cron_time(current, hour="14", minute="0")
        assert result == dt(2025, 6, 15, 14, 0)


class TestCronExpressions:
    """Test cron expression parsing."""

    def test_step_every_5_minutes(self):
        """minute='*/5' from 10:32 -> 10:35."""
        current = dt(2025, 6, 15, 10, 32)
        result = calculate_next_cron_time(current, minute="*/5")
        assert result == dt(2025, 6, 15, 10, 35)

    def test_step_every_2_hours(self):
        """hour='*/2' from 11:00 -> 12:00."""
        current = dt(2025, 6, 15, 11, 0)
        result = calculate_next_cron_time(current, hour="*/2", minute=0)
        assert result == dt(2025, 6, 15, 12, 0)

    def test_range(self):
        """hour='9-17' from 18:00 -> next day 09:00."""
        current = dt(2025, 6, 15, 18, 0)
        result = calculate_next_cron_time(current, hour="9-17", minute=0)
        assert result == dt(2025, 6, 16, 9, 0)

    def test_range_within(self):
        """hour='9-17' from 10:00 -> 10:00 same day."""
        current = dt(2025, 6, 15, 10, 0)
        result = calculate_next_cron_time(current, hour="9-17", minute=0)
        assert result == dt(2025, 6, 15, 10, 0)

    def test_list(self):
        """minute='0,15,30,45' from 10:32 -> 10:45."""
        current = dt(2025, 6, 15, 10, 32)
        result = calculate_next_cron_time(current, minute="0,15,30,45")
        assert result == dt(2025, 6, 15, 10, 45)

    def test_list_rollover(self):
        """minute='0,15,30,45' from 10:50 -> 11:00."""
        current = dt(2025, 6, 15, 10, 50)
        result = calculate_next_cron_time(current, minute="0,15,30,45")
        assert result == dt(2025, 6, 15, 11, 0)

    def test_weekday_name(self):
        """day_of_week='mon' -> next Monday."""
        # 2025-06-15 is a Sunday
        current = dt(2025, 6, 15, 10, 0)
        result = calculate_next_cron_time(current, day_of_week="mon", hour=9, minute=0)
        assert result == dt(2025, 6, 16, 9, 0)
        assert result.weekday() == 0  # Monday

    def test_weekday_name_range(self):
        """day_of_week='mon-fri' from Saturday -> next Monday."""
        # 2025-06-14 is a Saturday
        current = dt(2025, 6, 14, 10, 0)
        result = calculate_next_cron_time(current, day_of_week="mon-fri", hour=9, minute=0)
        assert result == dt(2025, 6, 16, 9, 0)
        assert result.weekday() == 0  # Monday

    def test_weekday_numeric(self):
        """day_of_week='0' (Monday)."""
        # 2025-06-15 is a Sunday
        current = dt(2025, 6, 15, 10, 0)
        result = calculate_next_cron_time(current, day_of_week="0", hour=9, minute=0)
        assert result == dt(2025, 6, 16, 9, 0)
        assert result.weekday() == 0

    def test_weekday_numeric_range(self):
        """day_of_week='0-4' (Mon-Fri)."""
        # 2025-06-14 is Saturday
        current = dt(2025, 6, 14, 10, 0)
        result = calculate_next_cron_time(current, day_of_week="0-4", hour=9, minute=0)
        assert result == dt(2025, 6, 16, 9, 0)

    def test_month_name(self):
        """month='jan' from February -> next January."""
        current = dt(2025, 2, 15, 10, 0)
        result = calculate_next_cron_time(current, month="jan", day=1, hour=0, minute=0)
        assert result == dt(2026, 1, 1, 0, 0)

    def test_month_name_range(self):
        """month='mar-jun' from July -> next March."""
        current = dt(2025, 7, 15, 10, 0)
        result = calculate_next_cron_time(current, month="mar-jun", day=1, hour=0, minute=0)
        assert result == dt(2026, 3, 1, 0, 0)

    def test_last_day_of_month(self):
        """day='last' -> last day of month."""
        current = dt(2025, 6, 15, 10, 0)
        result = calculate_next_cron_time(current, day="last", hour=0, minute=0)
        assert result == dt(2025, 6, 30, 0, 0)

    def test_last_day_of_month_february(self):
        """day='last' in February (non-leap)."""
        current = dt(2025, 2, 1, 0, 0)
        result = calculate_next_cron_time(current, day="last", hour=0, minute=0)
        assert result == dt(2025, 2, 28, 0, 0)

    def test_weekday_position_first_monday(self):
        """day='1st mon' -> first Monday of month."""
        current = dt(2025, 6, 15, 10, 0)
        result = calculate_next_cron_time(current, day="1st mon", hour=9, minute=0)
        # First Monday of July 2025 is July 7
        assert result == dt(2025, 7, 7, 9, 0)
        assert result.weekday() == 0

    def test_weekday_position_last_friday(self):
        """day='last fri' -> last Friday of month."""
        current = dt(2025, 6, 1, 0, 0)
        result = calculate_next_cron_time(current, day="last fri", hour=9, minute=0)
        # Last Friday of June 2025 is June 27
        assert result == dt(2025, 6, 27, 9, 0)
        assert result.weekday() == 4

    def test_week_field(self):
        """week='1' -> ISO week 1."""
        current = dt(2025, 6, 15, 10, 0)
        result = calculate_next_cron_time(current, week="1", hour=0, minute=0)
        assert result is not None
        assert result.isocalendar()[1] == 1


class TestEdgeCases:
    """Edge case tests."""

    def test_month_boundary(self):
        """Dec 31 -> Jan 1 next year."""
        current = dt(2025, 12, 31, 23, 59)
        result = calculate_next_cron_time(current, month=1, day=1, hour=0, minute=0)
        assert result == dt(2026, 1, 1, 0, 0)

    def test_year_rollover(self):
        """Last minute of year rolls over."""
        current = dt(2025, 12, 31, 23, 59, 30)
        result = calculate_next_cron_time(current)
        assert result == dt(2026, 1, 1, 0, 0)

    def test_leap_year_feb_29(self):
        """day=29, month=2 on leap year."""
        current = dt(2024, 1, 1, 0, 0)
        result = calculate_next_cron_time(current, month=2, day=29, hour=0, minute=0)
        assert result == dt(2024, 2, 29, 0, 0)

    def test_leap_year_feb_29_from_non_leap(self):
        """day=29, month=2 from non-leap year -> next leap year."""
        current = dt(2025, 3, 1, 0, 0)
        result = calculate_next_cron_time(current, month=2, day=29, hour=0, minute=0)
        assert result == dt(2028, 2, 29, 0, 0)

    def test_day_31_in_short_month(self):
        """day=31 in a 30-day month -> skip to next 31-day month."""
        current = dt(2025, 6, 1, 0, 0)
        result = calculate_next_cron_time(current, day=31, hour=0, minute=0)
        # June has 30 days, so should go to July 31
        assert result == dt(2025, 7, 31, 0, 0)

    def test_range_step(self):
        """minute='0-30/10' -> 0, 10, 20, 30."""
        current = dt(2025, 6, 15, 10, 15)
        result = calculate_next_cron_time(current, minute="0-30/10")
        assert result == dt(2025, 6, 15, 10, 20)

    def test_invalid_expression_raises(self):
        """Invalid expression should raise ValueError."""
        current = dt(2025, 6, 15, 10, 0)
        with pytest.raises(ValueError):
            calculate_next_cron_time(current, hour="invalid")

    def test_zero_step_raises(self):
        """Step of 0 should raise ValueError."""
        current = dt(2025, 6, 15, 10, 0)
        with pytest.raises(ValueError):
            calculate_next_cron_time(current, minute="*/0")


class TestTimezones:
    """Timezone-specific tests."""

    def test_utc(self):
        current = dt(2025, 6, 15, 10, 30, tz=UTC)
        result = calculate_next_cron_time(current, hour=14, minute=0)
        assert result == dt(2025, 6, 15, 14, 0, tz=UTC)
        assert result.tzinfo == UTC

    def test_new_york(self):
        ny = ZoneInfo("America/New_York")
        current = dt(2025, 6, 15, 10, 30, tz=ny)
        result = calculate_next_cron_time(current, hour=14, minute=0)
        assert result.hour == 14
        assert result.minute == 0
        assert result.tzinfo is not None

    def test_seoul(self):
        seoul = ZoneInfo("Asia/Seoul")
        current = dt(2025, 6, 15, 10, 30, tz=seoul)
        result = calculate_next_cron_time(current, hour=14, minute=0)
        assert result.hour == 14
        assert result.minute == 0

    def test_dst_spring_forward(self):
        """hour=2 during spring-forward gap in US/Eastern.
        2025-03-09: clocks jump from 2:00 AM to 3:00 AM.
        Asking for hour=2:30 should skip to valid time."""
        ny = ZoneInfo("America/New_York")
        current = datetime(2025, 3, 9, 1, 0, tzinfo=ny)
        result = calculate_next_cron_time(current, hour=2, minute=30)
        assert result is not None
        # The result should be a valid time (not in the gap)

    def test_dst_fall_back(self):
        """hour=1 during fall-back in US/Eastern.
        2025-11-02: clocks fall back from 2:00 AM to 1:00 AM."""
        ny = ZoneInfo("America/New_York")
        current = datetime(2025, 11, 2, 0, 0, tzinfo=ny)
        result = calculate_next_cron_time(current, hour=1, minute=30)
        assert result is not None
        assert result.hour == 1
        assert result.minute == 30
