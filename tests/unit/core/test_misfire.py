"""Unit tests for misfire handling."""

from datetime import UTC, datetime

from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobDefinition
from chronis.core.misfire import SimpleMisfirePolicy, get_default_policy
from chronis.core.misfire.utils import MisfireClassifier


class TestMisfireClassifier:
    """Test MisfireClassifier utility."""

    def test_is_misfired_true(self):
        """Test that job is detected as misfired when beyond threshold."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }

        # 2 minutes late = misfired
        current = datetime(2025, 1, 1, 9, 2, 0, tzinfo=UTC)
        assert MisfireClassifier.is_misfired(job_data, current) is True

    def test_is_misfired_false_within_threshold(self):
        """Test that job is not misfired when within threshold."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }

        # 30 seconds late = not misfired (within threshold)
        current = datetime(2025, 1, 1, 9, 0, 30, tzinfo=UTC)
        assert MisfireClassifier.is_misfired(job_data, current) is False

    def test_is_misfired_false_no_next_run_time(self):
        """Test that job without next_run_time is not misfired."""
        job_data = {
            "job_id": "test",
            "next_run_time": None,
            "misfire_threshold_seconds": 60,
        }

        current = datetime(2025, 1, 1, 9, 2, 0, tzinfo=UTC)
        assert MisfireClassifier.is_misfired(job_data, current) is False

    def test_classify_due_jobs(self):
        """Test classify_due_jobs separates normal and misfired."""
        jobs = [
            {
                "job_id": "normal",
                "next_run_time": "2025-01-01T09:00:00+00:00",
                "misfire_threshold_seconds": 60,
            },
            {
                "job_id": "misfired",
                "next_run_time": "2025-01-01T08:00:00+00:00",
                "misfire_threshold_seconds": 60,
            },
        ]

        current = "2025-01-01T09:00:30+00:00"
        normal, misfired = MisfireClassifier.classify_due_jobs(jobs, current)

        assert len(normal) == 1
        assert normal[0]["job_id"] == "normal"
        assert len(misfired) == 1
        assert misfired[0]["job_id"] == "misfired"

    def test_get_misfire_delay(self):
        """Test get_misfire_delay returns correct delay."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }

        # 2 minutes late
        current = datetime(2025, 1, 1, 9, 2, 0, tzinfo=UTC)
        delay = MisfireClassifier.get_misfire_delay(job_data, current)

        assert delay is not None
        assert delay.total_seconds() == 120  # 2 minutes

    def test_get_misfire_delay_not_misfired(self):
        """Test get_misfire_delay returns None when not misfired."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }

        # 30 seconds late (within threshold)
        current = datetime(2025, 1, 1, 9, 0, 30, tzinfo=UTC)
        delay = MisfireClassifier.get_misfire_delay(job_data, current)

        assert delay is None


class TestDefaultPolicies:
    """Test default misfire policies by trigger type."""

    def test_date_trigger_default(self):
        """Test DateTrigger defaults to run_once."""
        date_job = JobDefinition(
            job_id="test-date",
            name="test",
            trigger_type=TriggerType.DATE,
            trigger_args={"run_date": "2025-01-01 09:00:00"},
            func=lambda: None,
        )
        assert date_job.if_missed == "run_once"

    def test_cron_trigger_default(self):
        """Test CronTrigger defaults to skip."""
        cron_job = JobDefinition(
            job_id="test-cron",
            name="test",
            trigger_type=TriggerType.CRON,
            trigger_args={"hour": 9},
            func=lambda: None,
        )
        assert cron_job.if_missed == "skip"

    def test_interval_trigger_default(self):
        """Test IntervalTrigger defaults to run_once."""
        interval_job = JobDefinition(
            job_id="test-interval",
            name="test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"minutes": 5},
            func=lambda: None,
        )
        assert interval_job.if_missed == "run_once"

    def test_get_default_policy_function(self):
        """Test get_default_policy helper function."""
        assert get_default_policy(TriggerType.DATE) == SimpleMisfirePolicy.RUN_ONCE
        assert get_default_policy(TriggerType.CRON) == SimpleMisfirePolicy.SKIP
        assert get_default_policy(TriggerType.INTERVAL) == SimpleMisfirePolicy.RUN_ONCE


class TestMisfirePolicyOverride:
    """Test that misfire policy can be overridden."""

    def test_cron_override_to_run_once(self):
        """Test overriding cron default (skip) to run_once."""
        cron_job = JobDefinition(
            job_id="test-cron",
            name="test",
            trigger_type=TriggerType.CRON,
            trigger_args={"hour": 9},
            func=lambda: None,
            if_missed="run_once",  # Override
        )
        assert cron_job.if_missed == "run_once"

    def test_interval_override_to_skip(self):
        """Test overriding interval default (run_once) to skip."""
        interval_job = JobDefinition(
            job_id="test-interval",
            name="test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"minutes": 5},
            func=lambda: None,
            if_missed="skip",  # Override
        )
        assert interval_job.if_missed == "skip"

    def test_custom_threshold(self):
        """Test custom misfire threshold."""
        job = JobDefinition(
            job_id="test",
            name="test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"minutes": 10},
            func=lambda: None,
            misfire_threshold_seconds=300,  # 5 minutes
        )
        assert job.misfire_threshold_seconds == 300


class TestJobDefinitionToDict:
    """Test that JobDefinition.to_dict() includes misfire fields."""

    def test_to_dict_includes_misfire_fields(self):
        """Test that to_dict includes all misfire fields."""
        job = JobDefinition(
            job_id="test",
            name="test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"minutes": 5},
            func=lambda: None,
        )

        job_dict = job.to_dict()

        assert "if_missed" in job_dict
        assert "misfire_threshold_seconds" in job_dict
        assert "last_run_time" in job_dict
        assert "last_scheduled_time" in job_dict

        # Check values
        assert job_dict["if_missed"] == "run_once"  # Default for interval
        assert job_dict["misfire_threshold_seconds"] == 60
        assert job_dict["last_run_time"] is None
        assert job_dict["last_scheduled_time"] is None
