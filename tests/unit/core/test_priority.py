"""Unit tests for job priority functionality."""

import pytest

from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobDefinition, JobInfo


class TestPriorityValidation:
    """Test priority field validation in JobDefinition."""

    def test_priority_default_value(self):
        """Test that priority defaults to 5."""
        job = JobDefinition(
            job_id="test",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 60},
            func="test.func",
        )
        assert job.priority == 5

    def test_priority_valid_range(self):
        """Test that valid priority values (0-10) are accepted."""
        for priority in range(0, 11):
            job = JobDefinition(
                job_id=f"test_{priority}",
                name="Test Job",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 60},
                func="test.func",
                priority=priority,
            )
            assert job.priority == priority

    def test_priority_below_minimum_rejected(self):
        """Test that priority < 0 is rejected."""
        with pytest.raises(ValueError, match="Priority must be between 0 and 10"):
            JobDefinition(
                job_id="test",
                name="Test Job",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 60},
                func="test.func",
                priority=-1,
            )

    def test_priority_above_maximum_rejected(self):
        """Test that priority > 10 is rejected."""
        with pytest.raises(ValueError, match="Priority must be between 0 and 10"):
            JobDefinition(
                job_id="test",
                name="Test Job",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 60},
                func="test.func",
                priority=11,
            )

    def test_priority_in_to_dict(self):
        """Test that priority is included in to_dict output."""
        job = JobDefinition(
            job_id="test",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 60},
            func="test.func",
            priority=8,
        )
        job_dict = job.to_dict()
        assert "priority" in job_dict
        assert job_dict["priority"] == 8

    def test_priority_default_in_to_dict(self):
        """Test that default priority (5) is included in to_dict."""
        job = JobDefinition(
            job_id="test",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 60},
            func="test.func",
        )
        job_dict = job.to_dict()
        assert job_dict["priority"] == 5


class TestJobInfoPriority:
    """Test priority field in JobInfo."""

    def test_job_info_from_dict_with_priority(self):
        """Test JobInfo.from_dict includes priority."""
        job_data = {
            "job_id": "test",
            "name": "Test Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 60},
            "timezone": "UTC",
            "status": "scheduled",
            "next_run_time": "2025-01-01T00:00:00+00:00",
            "next_run_time_local": "2025-01-01T00:00:00+00:00",
            "metadata": {},
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
            "priority": 7,
        }
        job_info = JobInfo.from_dict(job_data)
        assert job_info.priority == 7

    def test_job_info_from_dict_default_priority(self):
        """Test JobInfo.from_dict defaults priority to 5 if missing."""
        job_data = {
            "job_id": "test",
            "name": "Test Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 60},
            "timezone": "UTC",
            "status": "scheduled",
            "next_run_time": "2025-01-01T00:00:00+00:00",
            "next_run_time_local": "2025-01-01T00:00:00+00:00",
            "metadata": {},
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
        }
        job_info = JobInfo.from_dict(job_data)
        assert job_info.priority == 5


class TestJobQueuePriority:
    """Test priority queue behavior."""

    def test_priority_queue_ordering(self):
        """Test that jobs are dequeued in priority order."""
        from chronis.core.job_queue import JobQueue

        queue = JobQueue(max_queue_size=10)

        # Add jobs with different priorities
        queue.add_job({"job_id": "low", "priority": 2, "name": "Low Priority"})
        queue.add_job({"job_id": "high", "priority": 9, "name": "High Priority"})
        queue.add_job({"job_id": "medium", "priority": 5, "name": "Medium Priority"})

        # Jobs should be dequeued in priority order (high to low)
        job1 = queue.get_next_job()
        assert job1["job_id"] == "high"

        job2 = queue.get_next_job()
        assert job2["job_id"] == "medium"

        job3 = queue.get_next_job()
        assert job3["job_id"] == "low"

    def test_priority_queue_fifo_within_same_priority(self):
        """Test that FIFO order is maintained for same priority jobs."""
        from chronis.core.job_queue import JobQueue

        queue = JobQueue(max_queue_size=10)

        # Add multiple jobs with same priority
        queue.add_job({"job_id": "first", "priority": 5, "name": "First"})
        queue.add_job({"job_id": "second", "priority": 5, "name": "Second"})
        queue.add_job({"job_id": "third", "priority": 5, "name": "Third"})

        # Should maintain FIFO order
        assert queue.get_next_job()["job_id"] == "first"
        assert queue.get_next_job()["job_id"] == "second"
        assert queue.get_next_job()["job_id"] == "third"

    def test_priority_queue_default_priority(self):
        """Test that jobs without priority field default to 5."""
        from chronis.core.job_queue import JobQueue

        queue = JobQueue(max_queue_size=10)

        # Add job without priority field
        queue.add_job({"job_id": "no_priority", "name": "No Priority"})
        queue.add_job({"job_id": "high_priority", "priority": 8, "name": "High"})

        # High priority job should come first
        assert queue.get_next_job()["job_id"] == "high_priority"
        assert queue.get_next_job()["job_id"] == "no_priority"
