"""Unit tests for MisfireHandler (pure logic, no mocks needed)."""

from datetime import UTC, datetime, timedelta

from chronis.core.misfire import MisfireHandler, MisfirePolicy


class TestMisfireHandlerSkipPolicy:
    """Test SKIP policy returns empty list."""

    def test_skip_policy_returns_empty_list(self):
        """SKIP policy should return empty list for any delay."""
        handler = MisfireHandler()

        job_data = {
            "job_id": "test-1",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 60},
            "timezone": "UTC",
            "if_missed": MisfirePolicy.SKIP.value,
        }

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)

        result = handler.handle(job_data, scheduled, current)
        assert result == []


class TestMisfireHandlerRunOncePolicy:
    """Test RUN_ONCE policy returns current time."""

    def test_run_once_returns_current_time(self):
        """RUN_ONCE should return list with current_time."""
        handler = MisfireHandler()

        job_data = {
            "job_id": "test-1",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 60},
            "timezone": "UTC",
            "if_missed": MisfirePolicy.RUN_ONCE.value,
        }

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)

        result = handler.handle(job_data, scheduled, current)

        assert len(result) == 1
        assert result[0] == current


class TestMisfireHandlerRunAllPolicy:
    """Test RUN_ALL policy returns all missed executions."""

    def test_run_all_with_interval_trigger(self):
        """RUN_ALL should return all missed interval executions."""
        handler = MisfireHandler()

        job_data = {
            "job_id": "test-1",
            "trigger_type": "interval",
            "trigger_args": {"minutes": 1},
            "timezone": "UTC",
            "if_missed": MisfirePolicy.RUN_ALL.value,
        }

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)

        result = handler.handle(job_data, scheduled, current)

        assert len(result) == 5

    def test_run_all_respects_max_limit(self):
        """RUN_ALL should cap at MAX_MISSED_RUNS."""
        handler = MisfireHandler()

        job_data = {
            "job_id": "test-1",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 1},
            "timezone": "UTC",
            "if_missed": MisfirePolicy.RUN_ALL.value,
        }

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = scheduled + timedelta(seconds=200)

        result = handler.handle(job_data, scheduled, current)

        assert len(result) == MisfireHandler.MAX_MISSED_RUNS
