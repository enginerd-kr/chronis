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

    def test_priority_serialization(self):
        """Test that priority is serialized in to_dict."""
        job = JobDefinition(
            job_id="test",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 60},
            func="test.func",
            priority=8,
        )
        job_dict = job.to_dict()
        assert job_dict["priority"] == 8


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
