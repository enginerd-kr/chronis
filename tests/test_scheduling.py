"""Tests for NextRunTimeCalculator."""

from datetime import datetime, timezone

from chronis.core.common.types import TriggerType
from chronis.core.scheduling import NextRunTimeCalculator


class TestNextRunTimeCalculator:
    """Tests for NextRunTimeCalculator."""

    def test_calculate_interval_trigger(self):
        """Test calculating next run time for interval trigger."""
        next_run = NextRunTimeCalculator.calculate(TriggerType.INTERVAL, {"seconds": 30}, "UTC")

        assert next_run is not None
        assert next_run > datetime.now(timezone.utc)

    def test_calculate_with_string_trigger_type(self):
        """Test calculating with trigger type as string."""
        next_run = NextRunTimeCalculator.calculate("interval", {"minutes": 5}, "UTC")

        assert next_run is not None

    def test_calculate_cron_trigger(self):
        """Test calculating next run time for cron trigger."""
        next_run = NextRunTimeCalculator.calculate(
            TriggerType.CRON, {"hour": "9", "minute": "0"}, "UTC"
        )

        assert next_run is not None
        # Should be in the future
        assert next_run > datetime.now(timezone.utc)

    def test_calculate_date_trigger(self):
        """Test calculating next run time for date trigger."""
        future_time = datetime(2030, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        next_run = NextRunTimeCalculator.calculate(
            TriggerType.DATE, {"run_date": future_time.isoformat()}, "UTC"
        )

        assert next_run is not None
        assert next_run == future_time

    def test_calculate_with_timezone(self):
        """Test calculating with non-UTC timezone."""
        next_run = NextRunTimeCalculator.calculate(
            TriggerType.INTERVAL, {"hours": 1}, "America/New_York"
        )

        assert next_run is not None
        # Result should be timezone-aware
        assert next_run.tzinfo is not None

    def test_calculate_with_local_time(self):
        """Test calculating with both UTC and local times."""
        utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
            TriggerType.INTERVAL, {"minutes": 30}, "America/New_York"
        )

        assert utc_time is not None
        assert local_time is not None
        assert utc_time.tzinfo is not None
        assert local_time.tzinfo is not None
        # Local time should be different timezone
        assert str(local_time.tzinfo) == "America/New_York"

    def test_calculate_with_local_time_returns_none_for_past_date(self):
        """Test that past DATE triggers return None."""
        past_time = datetime(2020, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        utc_time, local_time = NextRunTimeCalculator.calculate_with_local_time(
            TriggerType.DATE, {"run_date": past_time.isoformat()}, "UTC"
        )

        assert utc_time is None
        assert local_time is None

    def test_should_skip_calculation_for_date_trigger(self):
        """Test that DATE triggers should skip calculation."""
        assert NextRunTimeCalculator.should_skip_calculation(TriggerType.DATE) is True
        assert NextRunTimeCalculator.should_skip_calculation("date") is True

    def test_should_not_skip_calculation_for_recurring_triggers(self):
        """Test that recurring triggers should not skip calculation."""
        assert NextRunTimeCalculator.should_skip_calculation(TriggerType.INTERVAL) is False
        assert NextRunTimeCalculator.should_skip_calculation(TriggerType.CRON) is False
        assert NextRunTimeCalculator.should_skip_calculation("interval") is False
        assert NextRunTimeCalculator.should_skip_calculation("cron") is False

    def test_is_recurring_for_interval_and_cron(self):
        """Test identifying recurring triggers."""
        assert NextRunTimeCalculator.is_recurring(TriggerType.INTERVAL) is True
        assert NextRunTimeCalculator.is_recurring(TriggerType.CRON) is True
        assert NextRunTimeCalculator.is_recurring("interval") is True
        assert NextRunTimeCalculator.is_recurring("cron") is True

    def test_is_not_recurring_for_date(self):
        """Test that DATE is not recurring."""
        assert NextRunTimeCalculator.is_recurring(TriggerType.DATE) is False
        assert NextRunTimeCalculator.is_recurring("date") is False

    def test_is_one_time_for_date(self):
        """Test identifying one-time triggers."""
        assert NextRunTimeCalculator.is_one_time(TriggerType.DATE) is True
        assert NextRunTimeCalculator.is_one_time("date") is True

    def test_is_not_one_time_for_recurring(self):
        """Test that recurring triggers are not one-time."""
        assert NextRunTimeCalculator.is_one_time(TriggerType.INTERVAL) is False
        assert NextRunTimeCalculator.is_one_time(TriggerType.CRON) is False
        assert NextRunTimeCalculator.is_one_time("interval") is False
        assert NextRunTimeCalculator.is_one_time("cron") is False


class TestNextRunTimeCalculatorIntegration:
    """Integration tests with actual trigger scenarios."""

    def test_interval_trigger_progression(self):
        """Test that interval triggers progress correctly."""
        # First calculation
        first_run = NextRunTimeCalculator.calculate(TriggerType.INTERVAL, {"seconds": 10}, "UTC")

        # Second calculation from first run time
        second_run = NextRunTimeCalculator.calculate(
            TriggerType.INTERVAL, {"seconds": 10}, "UTC", current_time=first_run
        )

        assert second_run is not None
        assert second_run > first_run
        # Should be approximately 10 seconds apart
        delta = (second_run - first_run).total_seconds()
        assert 9 <= delta <= 11  # Allow small margin

    def test_cron_trigger_with_timezone(self):
        """Test cron trigger respects timezone."""
        # 9 AM in New York
        next_run = NextRunTimeCalculator.calculate(
            TriggerType.CRON, {"hour": "9", "minute": "0"}, "America/New_York"
        )

        assert next_run is not None

        # Convert to New York time to verify
        from zoneinfo import ZoneInfo

        ny_time = next_run.astimezone(ZoneInfo("America/New_York"))
        assert ny_time.hour == 9
        assert ny_time.minute == 0

    def test_consistency_between_methods(self):
        """Test that calculate and calculate_with_local_time are consistent."""
        trigger_args = {"minutes": 15}
        timezone_str = "Europe/London"

        # Using calculate
        utc_only = NextRunTimeCalculator.calculate(TriggerType.INTERVAL, trigger_args, timezone_str)

        # Using calculate_with_local_time
        utc_with_local, _ = NextRunTimeCalculator.calculate_with_local_time(
            TriggerType.INTERVAL, trigger_args, timezone_str
        )

        # Should return same UTC time (within a small window due to time progression)
        assert utc_only is not None
        assert utc_with_local is not None
        delta = abs((utc_with_local - utc_only).total_seconds())
        assert delta < 1  # Should be less than 1 second apart
