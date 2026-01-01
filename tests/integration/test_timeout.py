"""Integration tests for job timeout functionality."""

import asyncio
import time

import pytest

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
from chronis.core.state import JobStatus


@pytest.mark.integration
class TestJobTimeout:
    """Test job timeout functionality for both sync and async jobs."""

    def test_sync_job_timeout_fails_job(self):
        """Test that sync job times out and marks job as failed."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
        )

        # Create a slow function that takes 5 seconds
        def slow_job():
            time.sleep(5)

        scheduler.register_job_function("slow_job", slow_job)

        # Create job with 1 second timeout (runs every 1 second for quick testing)
        job = scheduler.create_interval_job(
            func="slow_job", seconds=1, timeout_seconds=1
        )

        scheduler.start()
        time.sleep(4)  # Wait for job to execute and timeout
        scheduler.stop()

        # Job should have failed due to timeout
        result = scheduler.get_job(job.job_id)
        assert result is not None
        assert result.status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_async_job_timeout_fails_job(self):
        """Test that async job times out and marks job as failed."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
        )

        # Create a slow async function
        async def slow_async_job():
            await asyncio.sleep(5)

        scheduler.register_job_function("slow_async_job", slow_async_job)

        # Create job with 1 second timeout (runs every 1 second for quick testing)
        job = scheduler.create_interval_job(
            func="slow_async_job", seconds=1, timeout_seconds=1
        )

        scheduler.start()
        await asyncio.sleep(4)  # Wait for job to execute and timeout
        scheduler.stop()

        # Job should have failed due to timeout
        result = scheduler.get_job(job.job_id)
        assert result is not None
        assert result.status == JobStatus.FAILED

    def test_sync_job_within_timeout_succeeds(self):
        """Test that sync job completes successfully within timeout."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
        )

        execution_count = {"count": 0}

        def fast_job():
            execution_count["count"] += 1
            time.sleep(0.1)  # Fast job

        scheduler.register_job_function("fast_job", fast_job)

        # Create job with 5 second timeout (plenty of time, runs every 1 second)
        job = scheduler.create_interval_job(
            func="fast_job", seconds=1, timeout_seconds=5
        )

        scheduler.start()
        time.sleep(4)  # Wait for execution
        scheduler.stop()

        # Job should have succeeded
        assert execution_count["count"] > 0
        result = scheduler.get_job(job.job_id)
        assert result is not None
        assert result.status == JobStatus.SCHEDULED  # Recurring job stays scheduled

    @pytest.mark.asyncio
    async def test_async_job_within_timeout_succeeds(self):
        """Test that async job completes successfully within timeout."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
        )

        execution_count = {"count": 0}

        async def fast_async_job():
            execution_count["count"] += 1
            await asyncio.sleep(0.1)  # Fast job

        scheduler.register_job_function("fast_async_job", fast_async_job)

        # Create job with 5 second timeout (plenty of time, runs every 1 second)
        job = scheduler.create_interval_job(
            func="fast_async_job", seconds=1, timeout_seconds=5
        )

        scheduler.start()
        await asyncio.sleep(4)  # Wait for execution
        scheduler.stop()

        # Job should have succeeded
        assert execution_count["count"] > 0
        result = scheduler.get_job(job.job_id)
        assert result is not None
        assert result.status == JobStatus.SCHEDULED  # Recurring job stays scheduled

    def test_job_without_timeout_runs_indefinitely(self):
        """Test that job without timeout can run for extended period."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
        )

        execution_count = {"count": 0}

        def long_job():
            execution_count["count"] += 1
            time.sleep(2)  # Takes 2 seconds

        scheduler.register_job_function("long_job", long_job)

        # Create job WITHOUT timeout (runs every 1 second)
        job = scheduler.create_interval_job(func="long_job", seconds=1, timeout_seconds=None)

        scheduler.start()
        time.sleep(5)  # Wait for execution
        scheduler.stop()

        # Job should have succeeded even though it took 2 seconds
        assert execution_count["count"] > 0
        result = scheduler.get_job(job.job_id)
        assert result is not None
        assert result.status == JobStatus.SCHEDULED
