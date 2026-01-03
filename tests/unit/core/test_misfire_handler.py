"""Unit tests for MisfireHandler (pure logic, no mocks needed)."""

from datetime import UTC, datetime, timedelta

from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobInfo
from chronis.core.misfire import MisfireHandler, MisfirePolicy
from chronis.core.state import JobStatus


class TestMisfireHandlerSkipPolicy:
    """Test SKIP policy returns empty list."""

    def test_skip_policy_returns_empty_list(self):
        """SKIP policy should return empty list for any delay."""
        handler = MisfireHandler()

        job = JobInfo(
            job_id="test-1",
            name="Test",
            trigger_type=TriggerType.INTERVAL.value,
            trigger_args={"seconds": 60},
            timezone="UTC",
            status=JobStatus.SCHEDULED,
            next_run_time=None,
            next_run_time_local=None,
            metadata={"if_missed": MisfirePolicy.SKIP.value},
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)

        result = handler.handle(job, scheduled, current)
        assert result == []


class TestMisfireHandlerRunOncePolicy:
    """Test RUN_ONCE policy returns current time."""

    def test_run_once_returns_current_time(self):
        """RUN_ONCE should return list with current_time."""
        handler = MisfireHandler()

        job = JobInfo(
            job_id="test-1",
            name="Test",
            trigger_type=TriggerType.INTERVAL.value,
            trigger_args={"seconds": 60},
            timezone="UTC",
            status=JobStatus.SCHEDULED,
            next_run_time=None,
            next_run_time_local=None,
            metadata={"if_missed": MisfirePolicy.RUN_ONCE.value},
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)

        result = handler.handle(job, scheduled, current)

        assert len(result) == 1
        assert result[0] == current


class TestMisfireHandlerRunAllPolicy:
    """Test RUN_ALL policy returns all missed executions."""

    def test_run_all_with_interval_trigger(self):
        """RUN_ALL should return all missed interval executions."""
        handler = MisfireHandler()

        job = JobInfo(
            job_id="test-1",
            name="Test",
            trigger_type=TriggerType.INTERVAL.value,
            trigger_args={"minutes": 1},
            timezone="UTC",
            status=JobStatus.SCHEDULED,
            next_run_time=None,
            next_run_time_local=None,
            metadata={"if_missed": MisfirePolicy.RUN_ALL.value},
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = datetime(2024, 1, 1, 12, 5, tzinfo=UTC)

        result = handler.handle(job, scheduled, current)

        assert len(result) == 5

    def test_run_all_respects_max_limit(self):
        """RUN_ALL should cap at MAX_MISSED_RUNS."""
        handler = MisfireHandler()

        job = JobInfo(
            job_id="test-1",
            name="Test",
            trigger_type=TriggerType.INTERVAL.value,
            trigger_args={"seconds": 1},
            timezone="UTC",
            status=JobStatus.SCHEDULED,
            next_run_time=None,
            next_run_time_local=None,
            metadata={"if_missed": MisfirePolicy.RUN_ALL.value},
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

        scheduled = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        current = scheduled + timedelta(seconds=200)

        result = handler.handle(job, scheduled, current)

        assert len(result) == MisfireHandler.MAX_MISSED_RUNS
