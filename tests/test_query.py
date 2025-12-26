"""Tests for JobQuerySpec (Specification pattern)."""

from chronis.core.common.types import TriggerType
from chronis.core.query import (
    JobQuery,
    MetadataSpec,
    NextRunBeforeSpec,
    StatusSpec,
    TriggerTypeSpec,
    jobs_ready_before,
    scheduled_jobs,
)
from chronis.core.state import JobStatus
from chronis.utils.time import utc_now


class TestStatusSpec:
    """Tests for StatusSpec."""

    def test_scheduled_jobs(self):
        """Test scheduled jobs spec."""
        spec = scheduled_jobs()
        filters = spec.to_filters()

        assert filters == {"status": JobStatus.SCHEDULED.value}

    def test_status_spec_direct_usage(self):
        """Test StatusSpec can be used directly for any status."""
        # Test different statuses
        for status in [JobStatus.RUNNING, JobStatus.FAILED, JobStatus.PENDING]:
            spec = StatusSpec(status)
            filters = spec.to_filters()
            assert filters == {"status": status.value}


class TestNextRunTimeSpec:
    """Tests for NextRunBeforeSpec."""

    def test_next_run_before(self):
        """Test next_run_time <= spec."""
        time = utc_now()
        spec = NextRunBeforeSpec(time)
        filters = spec.to_filters()

        assert filters == {"next_run_time_lte": time.isoformat()}


class TestMetadataSpec:
    """Tests for MetadataSpec."""

    def test_metadata_filter(self):
        """Test metadata field filtering."""
        spec = MetadataSpec("tenant_id", "acme")
        filters = spec.to_filters()

        assert filters == {"metadata.tenant_id": "acme"}

    def test_metadata_filter_nested(self):
        """Test nested metadata field filtering."""
        spec = MetadataSpec("config.region", "us-west-2")
        filters = spec.to_filters()

        assert filters == {"metadata.config.region": "us-west-2"}

    def test_metadata_filter_number(self):
        """Test metadata with numeric value."""
        spec = MetadataSpec("priority", 10)
        filters = spec.to_filters()

        assert filters == {"metadata.priority": 10}


class TestTriggerTypeSpec:
    """Tests for TriggerTypeSpec."""

    def test_trigger_type_spec_direct_usage(self):
        """Test TriggerTypeSpec can be used directly for any trigger type."""
        # Test different trigger types
        for trigger_type in [TriggerType.INTERVAL, TriggerType.CRON, TriggerType.DATE]:
            spec = TriggerTypeSpec(trigger_type.value)
            filters = spec.to_filters()
            assert filters == {"trigger_type": trigger_type.value}


class TestSpecCombination:
    """Tests for combining specs with AND."""

    def test_and_combination(self):
        """Test combining specs with AND."""
        spec = scheduled_jobs().and_(TriggerTypeSpec(TriggerType.INTERVAL.value))
        filters = spec.to_filters()

        assert filters == {
            "status": JobStatus.SCHEDULED.value,
            "trigger_type": TriggerType.INTERVAL.value,
        }

    def test_multiple_and_combination(self):
        """Test chaining multiple AND combinations."""
        time = utc_now()
        spec = (
            scheduled_jobs()
            .and_(TriggerTypeSpec(TriggerType.INTERVAL.value))
            .and_(NextRunBeforeSpec(time))
        )
        filters = spec.to_filters()

        assert filters == {
            "status": JobStatus.SCHEDULED.value,
            "trigger_type": TriggerType.INTERVAL.value,
            "next_run_time_lte": time.isoformat(),
        }


class TestConvenienceFunctions:
    """Tests for convenience query functions."""

    def test_jobs_ready_before(self):
        """Test jobs_ready_before convenience function."""
        time = utc_now()
        spec = jobs_ready_before(time)
        filters = spec.to_filters()

        assert filters == {
            "status": JobStatus.SCHEDULED.value,
            "next_run_time_lte": time.isoformat(),
        }


class TestJobQuery:
    """Tests for JobQuery builder."""

    def test_query_with_spec(self):
        """Test query with specification."""
        query = JobQuery(spec=scheduled_jobs())
        filters = query.to_filters()

        assert filters == {"status": JobStatus.SCHEDULED.value}

    def test_query_with_limit(self):
        """Test query with limit."""
        query = JobQuery(spec=scheduled_jobs(), limit=10)

        assert query.limit == 10
        assert query.to_filters() == {"status": JobStatus.SCHEDULED.value}

    def test_query_with_offset(self):
        """Test query with offset."""
        query = JobQuery(spec=scheduled_jobs(), offset=5)

        assert query.offset == 5
        assert query.to_filters() == {"status": JobStatus.SCHEDULED.value}

    def test_query_builder_pattern(self):
        """Test query builder pattern."""
        query = JobQuery(spec=scheduled_jobs()).with_limit(10).with_offset(5)

        assert query.limit == 10
        assert query.offset == 5
        assert query.to_filters() == {"status": JobStatus.SCHEDULED.value}

    def test_query_default_all_jobs(self):
        """Test query with no spec returns all jobs."""
        query = JobQuery()
        filters = query.to_filters()

        assert filters == {}

    def test_query_immutability(self):
        """Test that JobQuery is immutable."""
        original = JobQuery(spec=scheduled_jobs())

        # Builder methods should return new instances
        with_limit = original.with_limit(10)
        with_offset = original.with_offset(5)

        # Original should be unchanged
        assert original.limit is None
        assert original.offset is None

        # New instances should have updates
        assert with_limit.limit == 10
        assert with_limit.offset is None

        assert with_offset.offset == 5
        assert with_offset.limit is None

    def test_query_chained_immutability(self):
        """Test that chained builder calls create new instances."""
        original = JobQuery(spec=scheduled_jobs())
        chained = original.with_limit(10).with_offset(5)

        # Original unchanged
        assert original.limit is None
        assert original.offset is None

        # Chained result has both updates
        assert chained.limit == 10
        assert chained.offset == 5


class TestRealWorldScenarios:
    """Tests for real-world query scenarios."""

    def test_poll_ready_jobs(self):
        """Test query for polling ready jobs."""
        now = utc_now()
        spec = jobs_ready_before(now)
        filters = spec.to_filters()

        assert filters == {
            "status": JobStatus.SCHEDULED.value,
            "next_run_time_lte": now.isoformat(),
        }

    def test_failed_cron_jobs(self):
        """Test query for failed cron jobs."""
        spec = StatusSpec(JobStatus.FAILED).and_(TriggerTypeSpec(TriggerType.CRON.value))
        filters = spec.to_filters()

        assert filters == {
            "status": JobStatus.FAILED.value,
            "trigger_type": TriggerType.CRON.value,
        }

    def test_metadata_filtered_jobs(self):
        """Test query with metadata filtering."""
        spec = (
            scheduled_jobs()
            .and_(MetadataSpec("environment", "production"))
            .and_(MetadataSpec("priority", "high"))
        )
        filters = spec.to_filters()

        assert filters == {
            "status": JobStatus.SCHEDULED.value,
            "metadata.environment": "production",
            "metadata.priority": "high",
        }
