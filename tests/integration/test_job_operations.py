"""Integration tests for job operations (no background scheduler)."""

from datetime import UTC, datetime, timedelta

import pytest
from conftest import register_dummy_job

from chronis.core.common.exceptions import (
    InvalidJobStateError,
    JobAlreadyExistsError,
    JobNotFoundError,
)


@pytest.fixture
def scheduler(fast_scheduler):
    """Use fast_scheduler from conftest."""
    return fast_scheduler


class TestJobCRUD:
    """Test job CRUD operations integration."""

    def test_create_job_stores_in_storage(self, scheduler):
        """Test that creating job stores in storage."""
        register_dummy_job(scheduler)

        # Create interval job
        job = scheduler.create_interval_job(func="dummy", seconds=30)

        # Verify in storage
        stored = scheduler.storage.get_job(job.job_id)
        assert stored is not None
        assert stored["job_id"] == job.job_id
        assert stored["status"] == "scheduled"

    def test_update_job_modifies_storage(self, scheduler):
        """Test that updating job modifies storage."""
        register_dummy_job(scheduler)

        job = scheduler.create_interval_job(func="dummy", seconds=30)

        # Update job
        scheduler.update_job(job.job_id, name="Updated Name")

        # Verify in storage
        stored = scheduler.storage.get_job(job.job_id)
        assert stored["name"] == "Updated Name"

    def test_delete_job_removes_from_storage(self, scheduler):
        """Test that deleting job removes from storage."""
        register_dummy_job(scheduler)

        job = scheduler.create_interval_job(func="dummy", seconds=30)

        # Delete job
        result = scheduler.delete_job(job.job_id)
        assert result is True

        # Verify removed from storage
        stored = scheduler.storage.get_job(job.job_id)
        assert stored is None

    def test_query_jobs_returns_from_storage(self, scheduler):
        """Test that query_jobs returns from storage."""
        register_dummy_job(scheduler)

        # Create multiple jobs
        scheduler.create_interval_job(job_id="job-1", func="dummy", seconds=30)
        scheduler.create_interval_job(job_id="job-2", func="dummy", seconds=60)

        # Query all jobs
        jobs = scheduler.query_jobs()

        assert len(jobs) == 2
        job_ids = {j.job_id for j in jobs}
        assert "job-1" in job_ids
        assert "job-2" in job_ids


class TestPauseResume:
    """Test pause and resume integration."""

    def test_pause_updates_storage_status(self, scheduler):
        """Test that pause updates status in storage."""
        register_dummy_job(scheduler)

        job = scheduler.create_interval_job(func="dummy", seconds=30)

        # Pause
        result = scheduler.pause_job(job.job_id)
        assert result is True

        # Verify in storage
        stored = scheduler.storage.get_job(job.job_id)
        assert stored["status"] == "paused"

    def test_resume_updates_storage_status(self, scheduler):
        """Test that resume updates status in storage."""
        register_dummy_job(scheduler)

        job = scheduler.create_interval_job(func="dummy", seconds=30)
        scheduler.pause_job(job.job_id)

        # Resume
        result = scheduler.resume_job(job.job_id)
        assert result is True

        # Verify in storage
        stored = scheduler.storage.get_job(job.job_id)
        assert stored["status"] == "scheduled"

    def test_paused_jobs_not_polled(self, scheduler):
        """Test that paused jobs are not picked up by polling."""
        register_dummy_job(scheduler)

        # Create job and make it ready
        job = scheduler.create_interval_job(func="dummy", seconds=30)
        scheduler.storage.update_job(
            job.job_id,
            {"next_run_time": (datetime.now(UTC) - timedelta(seconds=1)).isoformat()},
        )

        # Pause job
        scheduler.pause_job(job.job_id)

        # Poll
        added = scheduler._enqueue_jobs()

        # Should not add paused jobs
        assert added == 0


class TestResumeNextRunTime:
    """Test resume behavior with next_run_time in the past."""

    def test_resume_preserves_original_next_run_time(self, scheduler):
        """pause → resume: next_run_time은 원래 값 그대로 유지."""
        register_dummy_job(scheduler)

        job = scheduler.create_interval_job(func="dummy", seconds=30)
        original_next_run = scheduler.storage.get_job(job.job_id)["next_run_time"]

        scheduler.pause_job(job.job_id)
        scheduler.resume_job(job.job_id)

        stored = scheduler.storage.get_job(job.job_id)
        assert stored["next_run_time"] == original_next_run

    def test_resume_with_past_next_run_time_gets_enqueued(self, scheduler):
        """resume 후 next_run_time이 과거이면 다음 poll에서 enqueue됨."""
        register_dummy_job(scheduler)

        job = scheduler.create_interval_job(func="dummy", seconds=30)

        # Set next_run_time to past, then pause, then resume
        past_time = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        scheduler.storage.update_job(job.job_id, {"next_run_time": past_time})
        scheduler.pause_job(job.job_id)
        scheduler.resume_job(job.job_id)

        # next_run_time should still be in the past
        stored = scheduler.storage.get_job(job.job_id)
        assert stored["next_run_time"] == past_time
        assert stored["status"] == "scheduled"

        # Should be picked up by enqueue
        added = scheduler._enqueue_jobs()
        assert added >= 1


class TestSchedulerCRUDErrorHandling:
    """Test scheduler-level CRUD error handling."""

    def test_create_duplicate_raises_job_already_exists(self, scheduler):
        """중복 create → JobAlreadyExistsError."""
        register_dummy_job(scheduler)

        scheduler.create_interval_job(job_id="dup-1", func="dummy", seconds=30)

        with pytest.raises(JobAlreadyExistsError):
            scheduler.create_interval_job(job_id="dup-1", func="dummy", seconds=60)

    def test_update_empty_params_returns_current(self, scheduler):
        """빈 파라미터 update → 현재 값 그대로 반환."""
        register_dummy_job(scheduler)

        job = scheduler.create_interval_job(job_id="upd-1", func="dummy", seconds=30)

        result = scheduler.update_job("upd-1")
        assert result.job_id == "upd-1"
        assert result.name == job.name

    def test_update_nonexistent_raises_not_found(self, scheduler):
        """존재하지 않는 job update → JobNotFoundError."""
        with pytest.raises(JobNotFoundError):
            scheduler.update_job("nonexistent", name="New Name")

    def test_get_nonexistent_returns_none(self, scheduler):
        """존재하지 않는 job get → None."""
        result = scheduler.get_job("nonexistent")
        assert result is None

    def test_pause_nonexistent_raises_not_found(self, scheduler):
        """존재하지 않는 job pause → JobNotFoundError."""
        with pytest.raises(JobNotFoundError):
            scheduler.pause_job("nonexistent")

    def test_resume_scheduled_raises_invalid_state(self, scheduler):
        """SCHEDULED 상태에서 resume → InvalidJobStateError."""
        register_dummy_job(scheduler)

        scheduler.create_interval_job(job_id="sched-1", func="dummy", seconds=30)

        with pytest.raises(InvalidJobStateError):
            scheduler.resume_job("sched-1")

    def test_create_job_registers_on_failure_handler(self, scheduler):
        """create_job의 on_failure 핸들러가 레지스트리에 등록됨."""
        register_dummy_job(scheduler)

        failures = []

        def on_fail(job_id, error, job_info):
            failures.append(job_id)

        job = scheduler.create_interval_job(
            func="dummy", seconds=30, on_failure=on_fail
        )

        assert job.job_id in scheduler._failure_handler_registry

    def test_create_job_registers_on_success_handler(self, scheduler):
        """create_job의 on_success 핸들러가 레지스트리에 등록됨."""
        register_dummy_job(scheduler)

        successes = []

        def on_success(job_id, job_info):
            successes.append(job_id)

        job = scheduler.create_interval_job(
            func="dummy", seconds=30, on_success=on_success
        )

        assert job.job_id in scheduler._success_handler_registry
