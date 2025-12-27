"""Tests for State Pattern implementation and fire-and-forget execution."""

import time

import pytest

from chronis import (
    InMemoryLockAdapter,
    InMemoryStorageAdapter,
    JobStatus,
    PollingScheduler,
)
from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobDefinition


def test_job_state_transitions():
    """Test job state transitions using State Pattern."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def dummy_func():
        print("Job executed")

    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create job with PENDING status
    job = JobDefinition(
        job_id="state-test",
        name="State Test Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 60},
        func=dummy_func,
        status=JobStatus.PENDING,
    )

    job_info = scheduler.create_job(job)
    # Should be SCHEDULED after creation (has next_run_time)
    assert job_info.status == JobStatus.SCHEDULED


@pytest.mark.slow
def test_fire_and_forget_execution():
    """Test fire-and-forget job execution."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
    )

    execution_count = {"count": 0}

    def counting_func():
        execution_count["count"] += 1

    scheduler.register_job_function(
        f"{counting_func.__module__}.{counting_func.__name__}", counting_func
    )

    # Create job with short interval
    job = JobDefinition(
        job_id="fire-test",
        name="Fire and Forget Test",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 2},  # 2 second interval
        func=counting_func,
    )

    scheduler.create_job(job)
    scheduler.start()

    # Wait for job to execute at least once with timeout
    timeout = 3.5
    start = time.time()
    while execution_count["count"] < 1 and (time.time() - start) < timeout:
        time.sleep(0.1)

    scheduler.stop()

    # Job should have executed at least once
    # Note: In fire-and-forget mode, we don't track success/failure
    # We just verify the job was triggered
    assert execution_count["count"] >= 1


def test_query_jobs():
    """Test query_jobs method."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def dummy_func():
        print("Job executed")

    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create multiple jobs with different trigger types
    interval_job = JobDefinition(
        job_id="interval-job",
        name="Interval Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 10},
        func=dummy_func,
    )

    cron_job = JobDefinition(
        job_id="cron-job",
        name="Cron Job",
        trigger_type=TriggerType.CRON,
        trigger_args={"hour": 9, "minute": 0},
        func=dummy_func,
        timezone="Asia/Seoul",
    )

    date_job = JobDefinition(
        job_id="date-job",
        name="Date Job",
        trigger_type=TriggerType.DATE,
        trigger_args={"run_date": "2025-12-31T23:59:59Z"},
        func=dummy_func,
    )

    scheduler.create_job(interval_job)
    scheduler.create_job(cron_job)
    scheduler.create_job(date_job)

    # Get all schedules
    schedules = scheduler.query_jobs()

    # Verify we got all schedules
    assert len(schedules) == 3

    # Find each schedule by job_id
    schedule_dict = {s.job_id: s for s in schedules}

    # Verify interval job schedule
    interval_schedule = schedule_dict["interval-job"]
    assert (
        interval_schedule.trigger_type == TriggerType.INTERVAL.value
        or interval_schedule.trigger_type == TriggerType.INTERVAL
    )
    assert interval_schedule.trigger_args["seconds"] == 10
    assert interval_schedule.next_run_time is not None

    # Verify cron job schedule
    cron_schedule = schedule_dict["cron-job"]
    assert (
        cron_schedule.trigger_type == TriggerType.CRON.value
        or cron_schedule.trigger_type == TriggerType.CRON
    )
    assert cron_schedule.trigger_args["hour"] == 9
    assert cron_schedule.trigger_args["minute"] == 0
    assert cron_schedule.timezone == "Asia/Seoul"
    assert cron_schedule.next_run_time is not None

    # Verify date job schedule
    date_schedule = schedule_dict["date-job"]
    assert (
        date_schedule.trigger_type == TriggerType.DATE.value
        or date_schedule.trigger_type == TriggerType.DATE
    )
    assert date_schedule.trigger_args["run_date"] == "2025-12-31T23:59:59Z"
    assert date_schedule.next_run_time is not None


def test_pause_scheduled_job():
    """Test pausing a scheduled job."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def dummy_func():
        pass

    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create scheduled job
    job_info = scheduler.create_interval_job(job_id="test-pause", func=dummy_func, seconds=60)
    assert job_info.status == JobStatus.SCHEDULED

    # Pause job
    result = scheduler.pause_job("test-pause")
    assert result is True

    job = scheduler.get_job("test-pause")
    assert job.status == JobStatus.PAUSED


def test_resume_paused_job():
    """Test resuming a paused job."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def dummy_func():
        pass

    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create and pause job
    scheduler.create_interval_job(job_id="test-resume", func=dummy_func, seconds=60)
    scheduler.pause_job("test-resume")

    # Resume job
    result = scheduler.resume_job("test-resume")
    assert result is True

    job = scheduler.get_job("test-resume")
    assert job.status == JobStatus.SCHEDULED


def test_pause_running_job_returns_false():
    """Test pausing running job returns False."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def dummy_func():
        pass

    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create job and manually set to RUNNING
    scheduler.create_interval_job(job_id="test-running", func=dummy_func, seconds=60)
    scheduler.update_job("test-running", status=JobStatus.RUNNING)

    # Try to pause - should return False
    result = scheduler.pause_job("test-running")
    assert result is False

    job = scheduler.get_job("test-running")
    assert job.status == JobStatus.RUNNING


def test_resume_non_paused_job_returns_false():
    """Test resuming non-paused job returns False."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def dummy_func():
        pass

    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create scheduled job (not paused)
    scheduler.create_interval_job(job_id="test-not-paused", func=dummy_func, seconds=60)

    # Try to resume - should return False
    result = scheduler.resume_job("test-not-paused")
    assert result is False

    job = scheduler.get_job("test-not-paused")
    assert job.status == JobStatus.SCHEDULED


def test_pause_nonexistent_job():
    """Test pausing non-existent job raises error."""
    from chronis.core.common.exceptions import JobNotFoundError

    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    with pytest.raises(JobNotFoundError):
        scheduler.pause_job("nonexistent")


@pytest.mark.slow
def test_paused_jobs_not_executed():
    """Test that paused jobs are not executed during polling."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage, lock_adapter=lock, polling_interval_seconds=1
    )

    execution_count = {"count": 0}

    def counting_func():
        execution_count["count"] += 1

    scheduler.register_job_function(
        f"{counting_func.__module__}.{counting_func.__name__}", counting_func
    )

    # Create job with short interval
    scheduler.create_interval_job(job_id="test-paused-exec", func=counting_func, seconds=1)

    # Pause immediately
    scheduler.pause_job("test-paused-exec")

    # Start and wait
    scheduler.start()
    time.sleep(3)
    scheduler.stop()

    # Should not have executed
    assert execution_count["count"] == 0

    # Verify still paused
    job = scheduler.get_job("test-paused-exec")
    assert job.status == JobStatus.PAUSED
