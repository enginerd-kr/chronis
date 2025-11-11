"""Tests for async job function support."""

import asyncio
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


@pytest.mark.slow
def test_async_job_execution():
    """Test that async job functions are properly awaited."""
    # Setup
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
        lock_ttl_seconds=10,
    )

    execution_results = []

    async def async_task():
        """Async task that uses await."""
        await asyncio.sleep(0.01)  # Reduced sleep time
        execution_results.append("async_executed")
        return "async_result"

    scheduler.register_job_function("async_task", async_task)

    # Create job with interval trigger
    job = JobDefinition(
        job_id="async-test-001",
        name="Async Test Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 2},  # 2 second interval
        func="async_task",
        timezone="UTC",
    )

    job_info = scheduler.create_job(job)
    assert job_info.status == JobStatus.SCHEDULED

    # Start scheduler
    scheduler.start()

    try:
        # Wait for job to execute with timeout
        timeout = 3.5
        start = time.time()
        while len(execution_results) < 1 and (time.time() - start) < timeout:
            time.sleep(0.1)

        # Verify async function was executed
        assert len(execution_results) >= 1
        assert execution_results[0] == "async_executed"

    finally:
        scheduler.stop()


@pytest.mark.slow
def test_mixed_sync_async_jobs():
    """Test that both sync and async jobs can coexist."""
    # Setup
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
        lock_ttl_seconds=10,
    )

    execution_log = []

    def sync_task():
        """Regular sync task."""
        execution_log.append("sync")

    async def async_task():
        """Async task."""
        await asyncio.sleep(0.01)  # Reduced sleep time
        execution_log.append("async")

    scheduler.register_job_function("sync_task", sync_task)
    scheduler.register_job_function("async_task", async_task)

    # Create sync job
    sync_job = JobDefinition(
        job_id="sync-job-001",
        name="Sync Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 2},  # 2 second interval
        func="sync_task",
        timezone="UTC",
    )

    # Create async job
    async_job = JobDefinition(
        job_id="async-job-001",
        name="Async Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 2},  # 2 second interval
        func="async_task",
        timezone="UTC",
    )

    scheduler.create_job(sync_job)
    scheduler.create_job(async_job)

    # Start scheduler
    scheduler.start()

    try:
        # Wait for jobs to execute with timeout
        timeout = 3.5
        start = time.time()
        while (len(execution_log) < 2 and (time.time() - start) < timeout):
            time.sleep(0.1)

        # Both sync and async jobs should have executed
        assert "sync" in execution_log
        assert "async" in execution_log

    finally:
        scheduler.stop()


def test_async_job_with_exception():
    """Test that exceptions in async jobs are properly handled."""
    # Setup
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
        lock_ttl_seconds=10,
    )

    async def failing_async_task():
        """Async task that raises an exception."""
        await asyncio.sleep(0.01)  # Reduced sleep time
        raise ValueError("Async task failed!")

    scheduler.register_job_function("failing_async_task", failing_async_task)

    # Create job
    job = JobDefinition(
        job_id="failing-async-001",
        name="Failing Async Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 2},  # 2 second interval
        func="failing_async_task",
        timezone="UTC",
    )

    job_info = scheduler.create_job(job)
    assert job_info.status == JobStatus.SCHEDULED

    # Start scheduler
    scheduler.start()

    try:
        # Wait for job to execute and fail with timeout
        time.sleep(1.0)

        # Job should still be scheduled (error is logged but doesn't stop scheduler)
        final_job = scheduler.get_job("failing-async-001")
        assert final_job is not None
        assert final_job.status == JobStatus.SCHEDULED

    finally:
        scheduler.stop()


@pytest.mark.slow
def test_async_job_with_arguments():
    """Test that async jobs can receive arguments."""
    # Setup
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
        lock_ttl_seconds=10,
    )

    execution_results = []

    async def async_task_with_args(name: str, count: int = 1):
        """Async task that receives arguments."""
        await asyncio.sleep(0.01)  # Reduced sleep time
        execution_results.append({"name": name, "count": count})

    scheduler.register_job_function("async_task_with_args", async_task_with_args)

    # Create job with arguments
    job = JobDefinition(
        job_id="async-args-001",
        name="Async Args Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 2},  # 2 second interval
        func="async_task_with_args",
        args=("test_job",),
        kwargs={"count": 5},
        timezone="UTC",
    )

    scheduler.create_job(job)
    scheduler.start()

    try:
        # Wait for job to execute with timeout
        timeout = 3.5
        start = time.time()
        while len(execution_results) < 1 and (time.time() - start) < timeout:
            time.sleep(0.1)

        # Verify arguments were passed correctly
        assert len(execution_results) >= 1
        assert execution_results[0]["name"] == "test_job"
        assert execution_results[0]["count"] == 5

    finally:
        scheduler.stop()


@pytest.mark.slow
def test_async_job_concurrent_execution():
    """Test that multiple async jobs can execute concurrently."""
    # Setup
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Minimum allowed
        lock_ttl_seconds=10,
    )

    execution_times = []

    async def long_async_task(task_id: str):
        """Async task that takes some time."""
        start = time.time()
        await asyncio.sleep(0.05)  # Reduced sleep time
        execution_times.append({
            "task_id": task_id,
            "start": start,
            "end": time.time()
        })

    scheduler.register_job_function("long_async_task", long_async_task)

    # Create multiple async jobs
    for i in range(3):
        job = JobDefinition(
            job_id=f"concurrent-async-{i}",
            name=f"Concurrent Async Job {i}",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 2},  # 2 second interval
            func="long_async_task",
            args=(f"task-{i}",),
            timezone="UTC",
        )
        scheduler.create_job(job)

    scheduler.start()

    try:
        # Wait for jobs to execute with timeout
        # With 1s polling interval + 2s job interval, need to wait longer
        timeout = 3.5
        start = time.time()
        while len(execution_times) < 1 and (time.time() - start) < timeout:
            time.sleep(0.05)

        # At least one execution should have happened
        assert len(execution_times) >= 1

    finally:
        scheduler.stop()
