"""Integration tests for priority-based job execution."""

import time
from datetime import timedelta

import pytest

from chronis.adapters.lock import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.scheduler import PollingScheduler
from chronis.utils.time import utc_now


@pytest.fixture
def scheduler():
    """Create scheduler with in-memory adapters."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        max_workers=5,
        polling_interval_seconds=1,
        executor_interval_seconds=1,
    )
    yield scheduler
    if scheduler.is_running():
        scheduler.stop()


class TestPriorityExecution:
    """Test that jobs are executed in priority order."""

    def test_jobs_executed_in_priority_order(self, scheduler):
        """Test that higher priority jobs are executed before lower priority jobs."""
        execution_order = []

        def track_execution(job_id):
            execution_order.append(job_id)

        scheduler.register_job_function("track_execution", track_execution)

        # Create interval jobs with different priorities
        # Set next_run_time to past to make them all due simultaneously
        past_time = utc_now() - timedelta(seconds=5)

        scheduler.create_interval_job(
            func="track_execution",
            job_id="low",
            args=("low",),
            seconds=60,
            priority=2,
        )

        scheduler.create_interval_job(
            func="track_execution",
            job_id="high",
            args=("high",),
            seconds=60,
            priority=9,
        )

        scheduler.create_interval_job(
            func="track_execution",
            job_id="medium",
            args=("medium",),
            seconds=60,
            priority=5,
        )

        # Manually set all jobs to be due at the same past time
        for job_id in ["low", "high", "medium"]:
            scheduler.storage.update_job(job_id, {"next_run_time": past_time.isoformat()})

        # Poll and enqueue all jobs
        added = scheduler._scheduling_orchestrator.poll_and_enqueue()
        assert added == 3, f"Expected 3 jobs to be enqueued, got {added}"

        # Execute jobs sequentially from priority queue
        for _ in range(3):
            job_data = scheduler._scheduling_orchestrator.get_next_job_from_queue()
            if job_data:
                success = scheduler._execution_coordinator.try_execute(
                    job_data, lambda job_id: None
                )
                assert success, f"Failed to execute job {job_data['job_id']}"

        # Wait for all executions to complete
        time.sleep(1.0)

        # Verify execution order: high -> medium -> low
        assert len(execution_order) == 3, (
            f"Expected 3 executions, got {len(execution_order)}: {execution_order}"
        )
        assert execution_order[0] == "high", f"First should be 'high', got {execution_order[0]}"
        assert execution_order[1] == "medium", (
            f"Second should be 'medium', got {execution_order[1]}"
        )
        assert execution_order[2] == "low", f"Third should be 'low', got {execution_order[2]}"

    def test_default_priority_is_5(self, scheduler):
        """Test that jobs without explicit priority default to 5."""
        scheduler.create_interval_job(
            func=lambda: None,
            job_id="default",
            seconds=60,
        )

        job = scheduler.get_job("default")
        assert job.priority == 5
