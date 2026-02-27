"""Unit tests for misfire handling."""

from datetime import UTC, datetime

from chronis.core.jobs.definition import JobDefinition
from chronis.core.misfire import MisfireDetector, MisfirePolicy
from chronis.core.state.enums import TriggerType


class TestMisfireDetector:
    """Test MisfireDetector utility."""

    def test_is_misfired_true(self):
        """Test that job is detected as misfired when beyond threshold."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }

        # 2 minutes late = misfired
        current = datetime(2025, 1, 1, 9, 2, 0, tzinfo=UTC)
        assert MisfireDetector.is_misfired(job_data, current) is True

    def test_is_misfired_false_within_threshold(self):
        """Test that job is not misfired when within threshold."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }

        # 30 seconds late = not misfired (within threshold)
        current = datetime(2025, 1, 1, 9, 0, 30, tzinfo=UTC)
        assert MisfireDetector.is_misfired(job_data, current) is False

    def test_is_misfired_false_no_next_run_time(self):
        """Test that job without next_run_time is not misfired."""
        job_data = {
            "job_id": "test",
            "next_run_time": None,
            "misfire_threshold_seconds": 60,
        }

        current = datetime(2025, 1, 1, 9, 2, 0, tzinfo=UTC)
        assert MisfireDetector.is_misfired(job_data, current) is False

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
        normal, misfired = MisfireDetector.classify_due_jobs(jobs, current)

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
        delay = MisfireDetector.get_misfire_delay(job_data, current)

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
        delay = MisfireDetector.get_misfire_delay(job_data, current)

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

    def test_get_default_for_trigger(self):
        """Test MisfirePolicy.get_default_for_trigger."""
        assert MisfirePolicy.get_default_for_trigger("date") == MisfirePolicy.RUN_ONCE
        assert MisfirePolicy.get_default_for_trigger("cron") == MisfirePolicy.SKIP
        assert MisfirePolicy.get_default_for_trigger("interval") == MisfirePolicy.RUN_ONCE


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


class TestMisfireBoundaryValues:
    """Test misfire detection boundary conditions."""

    def test_exactly_at_threshold_not_misfired(self):
        """next_run_time + threshold == current_time → NOT misfired (exclusive <)."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }
        # Exactly 60 seconds late = threshold boundary
        current = datetime(2025, 1, 1, 9, 1, 0, tzinfo=UTC)
        assert MisfireDetector.is_misfired(job_data, current) is False

    def test_one_second_past_threshold_is_misfired(self):
        """next_run_time + threshold + 1s → misfired."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "misfire_threshold_seconds": 60,
        }
        # 61 seconds late = past threshold
        current = datetime(2025, 1, 1, 9, 1, 1, tzinfo=UTC)
        assert MisfireDetector.is_misfired(job_data, current) is True

    def test_default_threshold_is_60_when_key_missing(self):
        """Missing misfire_threshold_seconds defaults to 60."""
        job_data = {
            "job_id": "test",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            # No misfire_threshold_seconds key
        }
        # 59 seconds → not misfired with default 60
        current_59 = datetime(2025, 1, 1, 9, 0, 59, tzinfo=UTC)
        assert MisfireDetector.is_misfired(job_data, current_59) is False

        # 61 seconds → misfired with default 60
        current_61 = datetime(2025, 1, 1, 9, 1, 1, tzinfo=UTC)
        assert MisfireDetector.is_misfired(job_data, current_61) is True

    def test_unknown_trigger_type_fallback_to_run_once(self):
        """Unknown trigger type defaults to RUN_ONCE policy."""
        assert MisfirePolicy.get_default_for_trigger("unknown") == MisfirePolicy.RUN_ONCE


class TestMisfireSkipPolicy:
    """Test _skip_misfired_job integration with polling scheduler."""

    def test_skip_advances_next_run_time(self):
        """skip 정책: misfired interval job의 next_run_time을 미래로 이동."""
        from datetime import timedelta

        from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
        from chronis.utils.time import utc_now

        storage = InMemoryStorageAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=InMemoryLockAdapter(),
            polling_interval_seconds=1,
            verbose=False,
        )

        now = utc_now()
        past = (now - timedelta(minutes=5)).isoformat()

        storage.create_job(
            {
                "job_id": "skip-job",
                "name": "Skip Job",
                "trigger_type": "interval",
                "trigger_args": {"seconds": 30},
                "timezone": "UTC",
                "status": "scheduled",
                "next_run_time": past,
                "next_run_time_local": past,
                "metadata": {},
                "if_missed": "skip",
                "misfire_threshold_seconds": 60,
                "created_at": past,
                "updated_at": past,
            }
        )

        scheduler._skip_misfired_job(storage.get_job("skip-job"))

        updated = storage.get_job("skip-job")
        # next_run_time should now be in the future
        assert updated["next_run_time"] > now.isoformat()

    def test_skip_deletes_date_job(self):
        """skip 정책: misfired date job은 삭제."""
        from datetime import timedelta

        from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
        from chronis.utils.time import utc_now

        storage = InMemoryStorageAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=InMemoryLockAdapter(),
            polling_interval_seconds=1,
            verbose=False,
        )

        past = (utc_now() - timedelta(hours=1)).isoformat()
        storage.create_job(
            {
                "job_id": "date-job",
                "name": "Date Job",
                "trigger_type": "date",
                "trigger_args": {"run_date": past},
                "timezone": "UTC",
                "status": "scheduled",
                "next_run_time": past,
                "next_run_time_local": past,
                "metadata": {},
                "if_missed": "skip",
                "misfire_threshold_seconds": 60,
                "created_at": past,
                "updated_at": past,
            }
        )

        scheduler._skip_misfired_job(storage.get_job("date-job"))
        assert storage.get_job("date-job") is None


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
