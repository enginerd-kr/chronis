"""Integration tests for job operations (no background scheduler)."""

from datetime import UTC, datetime, timedelta

import pytest
from conftest import register_dummy_job


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
        added = scheduler._orchestrator.enqueue_jobs()

        # Should not add paused jobs
        assert added == 0
