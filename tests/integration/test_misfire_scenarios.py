"""Integration tests for misfire handling scenarios."""

from datetime import timedelta

import pytest
from conftest import register_dummy_job

from chronis import InMemoryStorageAdapter
from chronis.core.misfire import MisfireDetector
from chronis.utils.time import utc_now


class TestMisfireIntegration:
    """Integration tests for misfire handling with PollingScheduler."""

    def test_job_creation_with_misfire_fields(self, basic_scheduler):
        """Test that jobs are created with misfire fields."""
        scheduler = basic_scheduler
        register_dummy_job(scheduler, "test_func")

        # Create job with custom misfire config
        job_info = scheduler.create_interval_job(
            func="test_func",
            job_id="test-job",
            name="Test Job",
            seconds=30,
            if_missed="run_all",
            misfire_threshold_seconds=120,
        )

        # Verify job has misfire fields
        job_data = scheduler.storage.get_job(job_info.job_id)
        assert job_data is not None
        assert job_data["if_missed"] == "run_all"
        assert job_data["misfire_threshold_seconds"] == 120
        assert job_data["last_run_time"] is None
        assert job_data["last_scheduled_time"] is None

    def test_job_creation_with_default_misfire_policy(self, basic_scheduler):
        """Test that jobs use default misfire policy based on trigger type."""
        scheduler = basic_scheduler
        register_dummy_job(scheduler, "test_func")

        # Cron job - should default to "skip"
        cron_job = scheduler.create_cron_job(
            func="test_func",
            job_id="cron-job",
            name="Cron Job",
            hour=9,
            minute=0,
        )

        job_data = scheduler.storage.get_job(cron_job.job_id)
        assert job_data is not None
        assert job_data["if_missed"] == "skip"

        # Interval job - should default to "run_once"
        interval_job = scheduler.create_interval_job(
            func="test_func",
            job_id="interval-job",
            name="Interval Job",
            minutes=5,
        )

        job_data = scheduler.storage.get_job(interval_job.job_id)
        assert job_data is not None
        assert job_data["if_missed"] == "run_once"

    def test_misfire_classification_in_query(self):
        """Test that misfire classification works with query_jobs."""
        storage = InMemoryStorageAdapter()

        # Create a job that should be misfired
        past_time = (utc_now() - timedelta(minutes=5)).isoformat()
        job_data = {
            "job_id": "misfired-job",
            "name": "Misfired Job",
            "trigger_type": "interval",
            "trigger_args": {"minutes": 30},
            "timezone": "UTC",
            "func_name": "test_func",
            "args": (),
            "kwargs": {},
            "status": "scheduled",
            "next_run_time": past_time,
            "next_run_time_local": past_time,
            "metadata": {},
            "created_at": past_time,
            "updated_at": past_time,
            "max_retries": 0,
            "retry_delay_seconds": 60,
            "retry_count": 0,
            "timeout_seconds": None,
            "priority": 5,
            "if_missed": "run_once",
            "misfire_threshold_seconds": 60,
            "last_run_time": None,
            "last_scheduled_time": None,
        }

        storage.create_job(job_data)

        # Query due jobs
        current_time = utc_now()
        due_jobs = storage.query_jobs(
            filters={"status": "scheduled", "next_run_time_lte": current_time.isoformat()}
        )

        # Classify
        normal, misfired = MisfireDetector.classify_due_jobs(due_jobs, current_time.isoformat())

        # Should be misfired (5 minutes late, threshold 60s)
        assert len(misfired) == 1
        assert len(normal) == 0
        assert misfired[0]["job_id"] == "misfired-job"

    def test_update_job_with_run_times(self):
        """Test that update_job works correctly with run time fields."""
        storage = InMemoryStorageAdapter()

        # Create a job
        now = utc_now()
        job_data = {
            "job_id": "test-job",
            "name": "Test Job",
            "trigger_type": "interval",
            "trigger_args": {"minutes": 5},
            "timezone": "UTC",
            "func_name": "test_func",
            "args": (),
            "kwargs": {},
            "status": "scheduled",
            "next_run_time": now.isoformat(),
            "next_run_time_local": now.isoformat(),
            "metadata": {},
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "max_retries": 0,
            "retry_delay_seconds": 60,
            "retry_count": 0,
            "timeout_seconds": None,
            "priority": 5,
            "if_missed": "run_once",
            "misfire_threshold_seconds": 60,
            "last_run_time": None,
            "last_scheduled_time": None,
        }

        storage.create_job(job_data)

        # Update run times using update_job
        scheduled = now.isoformat()
        actual = (now + timedelta(seconds=5)).isoformat()
        next_run = (now + timedelta(minutes=5)).isoformat()

        updated = storage.update_job(
            job_id="test-job",
            updates={
                "last_scheduled_time": scheduled,
                "last_run_time": actual,
                "next_run_time": next_run,
            },
        )

        # Verify updates
        assert updated["last_scheduled_time"] == scheduled
        assert updated["last_run_time"] == actual
        assert updated["next_run_time"] == next_run

    def test_update_job_run_times_nonexistent_job(self):
        """Test that update_job raises error for nonexistent job."""
        storage = InMemoryStorageAdapter()

        with pytest.raises(ValueError, match="Job nonexistent not found"):
            storage.update_job(
                job_id="nonexistent",
                updates={
                    "last_scheduled_time": "2025-01-01T09:00:00Z",
                    "last_run_time": "2025-01-01T09:00:05Z",
                    "next_run_time": "2025-01-01T10:00:00Z",
                },
            )


class TestMisfirePolicyBehavior:
    """Test actual behavior of different misfire policies."""

    def test_run_once_policy_executes_once(self, basic_scheduler, execution_tracker):
        """Test that run_once policy executes only once when multiple runs missed."""
        scheduler = basic_scheduler

        scheduler.register_job_function("test_func", lambda: execution_tracker.record("test_func"))

        # Create interval job with run_once policy, scheduled in the past (3 intervals missed)
        past_time = utc_now() - timedelta(minutes=15)
        job = scheduler.create_interval_job(
            func="test_func",
            job_id="run-once-job",
            minutes=5,
            if_missed="run_once",
            misfire_threshold_seconds=60,
        )

        # Manually set next_run_time to past (simulating misfire)
        scheduler.storage.update_job(job.job_id, {"next_run_time": past_time.isoformat()})

        # Poll and execute
        added = scheduler._enqueue_jobs()
        assert added == 1

        job_id = scheduler._job_queue.get_next_job()
        if job_id:
            job_data = scheduler.storage.get_job(job_id)
            assert job_data is not None
            scheduler._execution_coordinator.try_execute(job_data, lambda job_id: None)

        import time

        time.sleep(0.5)

        # Should execute only once despite missing 3 intervals
        assert execution_tracker.count() == 1

    def test_skip_policy_advances_next_run_time(self, basic_scheduler):
        """Test that skip policy advances next_run_time without executing."""
        scheduler = basic_scheduler
        register_dummy_job(scheduler, "test_func")

        # Create job with skip policy
        past_time = utc_now() - timedelta(minutes=10)
        job = scheduler.create_interval_job(
            func="test_func",
            job_id="skip-job",
            minutes=5,
            if_missed="skip",
            misfire_threshold_seconds=60,
        )

        # Set to past time (misfired)
        scheduler.storage.update_job(job.job_id, {"next_run_time": past_time.isoformat()})

        # Poll - skip policy should NOT enqueue the job
        added = scheduler._enqueue_jobs()
        assert added == 0

        # next_run_time should be advanced to the future
        updated = scheduler.storage.get_job("skip-job")
        assert updated is not None
        assert updated["next_run_time"] is not None
        assert updated["next_run_time"] > utc_now().isoformat()
        assert updated["status"] == "scheduled"

    def test_run_all_policy_keeps_incremental_next_run(self, basic_scheduler, execution_tracker):
        """Test that run_all policy advances next_run_time incrementally, not to future."""
        scheduler = basic_scheduler
        scheduler.register_job_function("test_func", lambda: execution_tracker.record("test_func"))

        # Create job with run_all policy, 5-minute interval, 15 minutes late (3 missed)
        past_time = utc_now() - timedelta(minutes=15)
        job = scheduler.create_interval_job(
            func="test_func",
            job_id="run-all-job",
            minutes=5,
            if_missed="run_all",
            misfire_threshold_seconds=60,
        )

        # Set to past (3 intervals missed)
        scheduler.storage.update_job(job.job_id, {"next_run_time": past_time.isoformat()})

        # First poll + execute
        added = scheduler._enqueue_jobs()
        assert added == 1

        job_id = scheduler._job_queue.get_next_job()
        assert job_id is not None
        job_data = scheduler.storage.get_job(job_id)
        assert job_data is not None
        scheduler._execution_coordinator.try_execute(dict(job_data), lambda jid: None)

        import time

        time.sleep(0.5)

        # After first execution, next_run_time should still be in the past (incremental)
        updated = scheduler.storage.get_job("run-all-job")
        assert updated is not None
        assert updated["status"] == "scheduled"
        assert updated["next_run_time"] is not None
        assert updated["next_run_time"] < utc_now().isoformat()
