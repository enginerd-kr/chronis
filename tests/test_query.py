"""Tests for job query helper functions."""

from chronis.core.common.types import TriggerType
from chronis.core.query import (
    combine_filters,
    jobs_after_time,
    jobs_before_time,
    jobs_by_metadata,
    jobs_by_status,
    jobs_by_trigger_type,
    jobs_ready_before,
    scheduled_jobs,
)
from chronis.core.state import JobStatus
from chronis.utils.time import utc_now


class TestScheduledJobs:
    """Tests for scheduled_jobs helper."""

    def test_scheduled_jobs(self):
        """Test scheduled jobs filter."""
        filters = scheduled_jobs()

        assert filters == {"status": JobStatus.SCHEDULED.value}


class TestJobsByStatus:
    """Tests for jobs_by_status helper."""

    def test_jobs_by_status(self):
        """Test jobs_by_status for different statuses."""
        for status in [JobStatus.RUNNING, JobStatus.FAILED, JobStatus.PENDING, JobStatus.SCHEDULED]:
            filters = jobs_by_status(status)
            assert filters == {"status": status.value}


class TestTimeFilters:
    """Tests for time-based filters."""

    def test_jobs_before_time(self):
        """Test jobs_before_time filter."""
        time = utc_now()
        filters = jobs_before_time(time)

        assert filters == {"next_run_time_lte": time.isoformat()}

    def test_jobs_after_time(self):
        """Test jobs_after_time filter."""
        time = utc_now()
        filters = jobs_after_time(time)

        assert filters == {"next_run_time_gte": time.isoformat()}


class TestMetadataFilter:
    """Tests for metadata filter."""

    def test_jobs_by_metadata_simple(self):
        """Test simple metadata filter."""
        filters = jobs_by_metadata("environment", "production")

        assert filters == {"metadata.environment": "production"}

    def test_jobs_by_metadata_nested(self):
        """Test nested metadata filter."""
        filters = jobs_by_metadata("config.timeout", 30)

        assert filters == {"metadata.config.timeout": 30}

    def test_jobs_by_metadata_number(self):
        """Test metadata filter with number value."""
        filters = jobs_by_metadata("retry_count", 3)

        assert filters == {"metadata.retry_count": 3}


class TestTriggerTypeFilter:
    """Tests for trigger type filter."""

    def test_jobs_by_trigger_type(self):
        """Test trigger type filter."""
        filters = jobs_by_trigger_type(TriggerType.INTERVAL.value)

        assert filters == {"trigger_type": "interval"}


class TestCombineFilters:
    """Tests for combine_filters helper."""

    def test_combine_two_filters(self):
        """Test combining two filters."""
        filter1 = {"status": "scheduled"}
        filter2 = {"trigger_type": "interval"}

        combined = combine_filters(filter1, filter2)

        assert combined == {"status": "scheduled", "trigger_type": "interval"}

    def test_combine_multiple_filters(self):
        """Test combining multiple filters."""
        filter1 = {"status": "scheduled"}
        filter2 = {"trigger_type": "interval"}
        filter3 = {"metadata.env": "prod"}

        combined = combine_filters(filter1, filter2, filter3)

        assert combined == {
            "status": "scheduled",
            "trigger_type": "interval",
            "metadata.env": "prod",
        }


class TestJobsReadyBefore:
    """Tests for jobs_ready_before convenience function."""

    def test_jobs_ready_before(self):
        """Test jobs_ready_before combines status and time."""
        time = utc_now()
        filters = jobs_ready_before(time)

        assert filters == {
            "status": JobStatus.SCHEDULED.value,
            "next_run_time_lte": time.isoformat(),
        }


class TestRealWorldScenarios:
    """Real-world usage scenarios."""

    def test_poll_ready_jobs(self):
        """Test querying ready jobs (scheduler polling use case)."""
        current_time = utc_now()
        filters = jobs_ready_before(current_time)

        assert "status" in filters
        assert "next_run_time_lte" in filters
        assert filters["status"] == "scheduled"

    def test_failed_jobs_by_trigger_type(self):
        """Test querying failed jobs of a specific trigger type."""
        filters = combine_filters(jobs_by_status(JobStatus.FAILED), jobs_by_trigger_type("cron"))

        assert filters == {"status": "failed", "trigger_type": "cron"}

    def test_metadata_filtered_jobs(self):
        """Test querying jobs with metadata filters."""
        filters = combine_filters(scheduled_jobs(), jobs_by_metadata("tenant", "customer-123"))

        assert filters == {"status": "scheduled", "metadata.tenant": "customer-123"}
