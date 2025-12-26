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


def test_get_all_schedules():
    """Test get_all_schedules method."""
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
    schedules = scheduler.get_all_schedules()

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
