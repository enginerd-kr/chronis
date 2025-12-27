"""Unit tests for Trigger classes."""

from datetime import datetime, timedelta

from chronis.core.triggers import CronTrigger, DateTrigger, IntervalTrigger
from chronis.utils.time import utc_now


class TestIntervalTrigger:
    """Unit tests for IntervalTrigger."""

    def test_calculate_next_run_time_seconds(self):
        """Test interval trigger with seconds."""
        trigger = IntervalTrigger()
        current_time = utc_now()

        next_run = trigger.calculate_next_run_time(
            trigger_args={"seconds": 30}, timezone="UTC", current_time=current_time
        )

        assert next_run is not None
        assert next_run > current_time
        # Should be approximately 30 seconds from now
        delta = (next_run - current_time).total_seconds()
        assert 29 <= delta <= 31

    def test_calculate_next_run_time_minutes(self):
        """Test interval trigger with minutes."""
        trigger = IntervalTrigger()
        current_time = utc_now()

        next_run = trigger.calculate_next_run_time(
            trigger_args={"minutes": 5}, timezone="UTC", current_time=current_time
        )

        assert next_run is not None
        delta = (next_run - current_time).total_seconds()
        assert 299 <= delta <= 301  # ~5 minutes

    def test_calculate_next_run_time_combined(self):
        """Test interval trigger with combined units."""
        trigger = IntervalTrigger()
        current_time = utc_now()

        next_run = trigger.calculate_next_run_time(
            trigger_args={"minutes": 1, "seconds": 30}, timezone="UTC", current_time=current_time
        )

        assert next_run is not None
        delta = (next_run - current_time).total_seconds()
        assert 89 <= delta <= 91  # 90 seconds


class TestCronTrigger:
    """Unit tests for CronTrigger."""

    def test_calculate_next_run_time_daily(self):
        """Test cron trigger for daily execution."""
        trigger = CronTrigger()
        current_time = utc_now()

        next_run = trigger.calculate_next_run_time(
            trigger_args={"hour": "9", "minute": "0"}, timezone="UTC", current_time=current_time
        )

        assert next_run is not None
        assert next_run > current_time
        # Should be at 9:00
        assert next_run.hour == 9
        assert next_run.minute == 0

    def test_calculate_next_run_time_with_timezone(self):
        """Test cron trigger respects timezone."""
        trigger = CronTrigger()
        current_time = utc_now()

        next_run = trigger.calculate_next_run_time(
            trigger_args={"hour": "14", "minute": "30"},
            timezone="America/New_York",
            current_time=current_time,
        )

        assert next_run is not None
        # Convert to New York time to verify
        from chronis.utils.time import ZoneInfo

        ny_time = next_run.astimezone(ZoneInfo("America/New_York"))
        assert ny_time.hour == 14
        assert ny_time.minute == 30

    def test_calculate_next_run_time_specific_weekday(self):
        """Test cron trigger for specific day of week."""
        trigger = CronTrigger()
        current_time = utc_now()

        # Every Monday at 10:00
        next_run = trigger.calculate_next_run_time(
            trigger_args={"hour": "10", "minute": "0", "day_of_week": "mon"},
            timezone="UTC",
            current_time=current_time,
        )

        assert next_run is not None
        assert next_run > current_time
        assert next_run.weekday() == 0  # Monday


class TestDateTrigger:
    """Unit tests for DateTrigger."""

    def test_calculate_next_run_time_future(self):
        """Test date trigger with future date."""
        trigger = DateTrigger()
        future_date = utc_now() + timedelta(hours=2)

        next_run = trigger.calculate_next_run_time(
            trigger_args={"run_date": future_date.isoformat()},
            timezone="UTC",
            current_time=utc_now(),
        )

        assert next_run is not None
        assert next_run == future_date

    def test_calculate_next_run_time_past(self):
        """Test date trigger with past date returns None."""
        trigger = DateTrigger()
        past_date = utc_now() - timedelta(hours=2)

        next_run = trigger.calculate_next_run_time(
            trigger_args={"run_date": past_date.isoformat()}, timezone="UTC", current_time=utc_now()
        )

        assert next_run is None

    def test_calculate_next_run_time_no_date(self):
        """Test date trigger with no run_date returns None."""
        trigger = DateTrigger()

        next_run = trigger.calculate_next_run_time(
            trigger_args={}, timezone="UTC", current_time=utc_now()
        )

        assert next_run is None

    def test_calculate_next_run_time_with_timezone(self):
        """Test date trigger respects timezone."""
        trigger = DateTrigger()

        # Future date in New York timezone
        from chronis.utils.time import ZoneInfo

        ny_tz = ZoneInfo("America/New_York")
        future_date = datetime.now(ny_tz) + timedelta(hours=2)

        next_run = trigger.calculate_next_run_time(
            trigger_args={"run_date": future_date.isoformat()},
            timezone="America/New_York",
            current_time=utc_now(),
        )

        assert next_run is not None
        assert next_run.tzinfo is not None
