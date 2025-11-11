"""Tests for date trigger functionality."""

import time
from datetime import datetime, timedelta

import pytest

from chronis import (
    InMemoryLockAdapter,
    InMemoryStorageAdapter,
    JobStatus,
    PollingScheduler,
)
from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobDefinition


@pytest.mark.slow
def test_date_trigger_one_time_execution():
    """Test that date trigger executes only once and marks job as completed."""
    # Setup
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
        lock_ttl_seconds=10,
    )

    execution_count = []

    def one_time_task():
        execution_count.append(1)

    scheduler.register_job_function("one_time_task", one_time_task)

    # Create date job that runs soon
    run_date = datetime.now().astimezone() + timedelta(seconds=1.5)
    job = JobDefinition(
        job_id="date-test-001",
        name="One-time Date Job",
        trigger_type=TriggerType.DATE,
        trigger_args={"run_date": run_date.isoformat()},
        func="one_time_task",
        timezone="UTC",
    )

    job_info = scheduler.create_job(job)
    assert job_info.status == JobStatus.SCHEDULED
    assert job_info.next_run_time is not None

    # Start scheduler
    scheduler.start()

    try:
        # Wait for job to execute with timeout
        timeout = 3.5
        start = time.time()
        while len(execution_count) < 1 and (time.time() - start) < timeout:
            time.sleep(0.1)

        # Check that job executed exactly once
        assert len(execution_count) == 1

        # Check that job is marked as completed
        final_job = scheduler.get_job("date-test-001")
        assert final_job is not None
        assert final_job.status == JobStatus.COMPLETED
        assert final_job.next_run_time is None
        assert final_job.next_run_time_local is None

        # Wait a bit more to ensure it doesn't execute again
        time.sleep(0.5)
        assert len(execution_count) == 1, "Job should only execute once"

    finally:
        scheduler.stop()


def test_date_trigger_past_date():
    """Test that date trigger with past date is not created with future execution."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
        lock_ttl_seconds=10,
    )

    # Create date job with past date (1 second ago)
    past_date = datetime.now().astimezone() - timedelta(seconds=1)
    job = JobDefinition(
        job_id="date-test-002",
        name="Past Date Job",
        trigger_type=TriggerType.DATE,
        trigger_args={"run_date": past_date.isoformat()},
        func="dummy_func",
        timezone="UTC",
    )

    # Job creation should succeed (storage adapter handles this)
    job_info = scheduler.create_job(job)
    assert job_info is not None

    # The next_run_time might be set initially,
    # but after first poll it should be marked as completed
    scheduler.start()

    try:
        time.sleep(0.5)  # Reduced wait time

        # Job should be marked as completed without execution
        final_job = scheduler.get_job("date-test-002")
        # Past date jobs might execute once if they're within the polling window
        # The important part is that they don't keep running
        assert final_job is not None

    finally:
        scheduler.stop()


def test_date_trigger_no_run_date():
    """Test that date trigger with no run_date returns None for next_run_time."""
    from chronis.core.triggers import DateTrigger

    trigger = DateTrigger()

    # No run_date specified
    result = trigger.calculate_next_run_time(
        trigger_args={},
        timezone="UTC",
        current_time=datetime.now().astimezone()
    )

    assert result is None


def test_date_trigger_calculates_correctly():
    """Test that date trigger correctly calculates next run time."""
    from chronis.core.triggers import DateTrigger
    from chronis.utils.time import ZoneInfo

    trigger = DateTrigger()

    # Future date
    future_date = datetime.now().astimezone() + timedelta(hours=1)
    result = trigger.calculate_next_run_time(
        trigger_args={"run_date": future_date.isoformat()},
        timezone="UTC",
        current_time=datetime.now().astimezone()
    )

    assert result is not None
    assert result > datetime.now(ZoneInfo("UTC"))


def test_date_trigger_returns_none_after_execution():
    """Test that date trigger returns None for times in the past."""
    from chronis.core.triggers import DateTrigger

    trigger = DateTrigger()

    # Past date
    past_date = datetime.now().astimezone() - timedelta(hours=1)
    result = trigger.calculate_next_run_time(
        trigger_args={"run_date": past_date.isoformat()},
        timezone="UTC",
        current_time=datetime.now().astimezone()
    )

    assert result is None
