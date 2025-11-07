"""Tests for distributed lock functionality with multiple schedulers."""

import threading
import time
from datetime import datetime, timedelta

from chronis import PollingScheduler
from chronis.adapters.locks.memory import InMemoryLockAdapter
from chronis.adapters.storage.memory import InMemoryStorageAdapter


def test_prevent_duplicate_execution():
    """Test that same job doesn't execute multiple times with multiple schedulers."""
    # Shared storage and lock
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    # Thread-safe execution counter
    execution_count = {"count": 0}
    count_lock = threading.Lock()

    def test_func():
        with count_lock:
            execution_count["count"] += 1
        time.sleep(0.1)  # Simulate some work

    # Create 3 schedulers sharing same storage/lock
    # IMPORTANT: Use same lock_prefix for all schedulers to share the same lock
    schedulers = []
    for i in range(3):
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            lock_ttl_seconds=5,
            lock_prefix="shared:lock:",  # Same prefix for distributed locking
            verbose=False,
        )
        # Register function on each scheduler
        scheduler.register_job_function("test_func", test_func)
        schedulers.append(scheduler)

    # Schedule a job that runs every 2 seconds
    job_id = schedulers[0].create_interval_job(
        job_id="test_job",
        name="Test Job",
        func="test_func",  # Use string reference
        seconds=2,
    )

    # Start all schedulers
    for scheduler in schedulers:
        scheduler.start()

    try:
        # Wait for ~7 seconds (should trigger 3 times with 2-second interval)
        time.sleep(7)

        # Stop all schedulers
        for scheduler in schedulers:
            scheduler.stop()

        # Verify job executed 2-3 times (not 6-9 times if lock wasn't working)
        # Timing can vary slightly, so we allow 2-3 executions
        assert 2 <= execution_count["count"] <= 3, (
            f"Expected 2-3 executions (with lock working), got {execution_count['count']}"
        )

    finally:
        for scheduler in schedulers:
            if scheduler.is_running():
                scheduler.stop()


def test_lock_contention_with_many_jobs():
    """Test 100 jobs with 5 schedulers, each job executes exactly once."""
    # Shared storage and lock
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    # Thread-safe execution counters per job
    execution_counts = {}
    count_lock = threading.Lock()

    def test_func(job_id: str):
        with count_lock:
            execution_counts[job_id] = execution_counts.get(job_id, 0) + 1
        time.sleep(0.01)  # Simulate light work

    # Create 5 schedulers
    # IMPORTANT: Use same lock_prefix for all schedulers to share the same lock
    schedulers = []
    for i in range(5):
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            lock_ttl_seconds=5,
            lock_prefix="shared:lock:",  # Same prefix for distributed locking
            verbose=False,
            max_queue_size=50,  # Allow more jobs in queue
        )
        # Register function on each scheduler
        scheduler.register_job_function("test_func", test_func)
        schedulers.append(scheduler)

    # Schedule 100 jobs that run once (using date trigger)
    run_time = datetime.now() + timedelta(seconds=2)
    for i in range(100):
        job_id = f"job_{i}"
        schedulers[i % 5].create_date_job(
            job_id=job_id,
            name=f"Job {i}",
            func="test_func",
            run_date=run_time,
            kwargs={"job_id": job_id},
        )

    # Start all schedulers
    for scheduler in schedulers:
        scheduler.start()

    try:
        # Wait for all jobs to execute
        time.sleep(5)

        # Stop all schedulers
        for scheduler in schedulers:
            scheduler.stop()

        # Verify each job executed exactly once
        assert len(execution_counts) == 100, (
            f"Expected 100 jobs executed, got {len(execution_counts)}"
        )

        for job_id, count in execution_counts.items():
            assert count == 1, f"Job {job_id} executed {count} times, expected 1"

    finally:
        for scheduler in schedulers:
            if scheduler.is_running():
                scheduler.stop()


def test_job_distribution_across_schedulers():
    """Test that jobs are executed across multiple schedulers without duplication."""
    # Shared storage and lock
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    # Track unique executions
    execution_times = {}
    exec_lock = threading.Lock()

    def test_func(job_id: str):
        with exec_lock:
            if job_id not in execution_times:
                execution_times[job_id] = []
            execution_times[job_id].append(time.time())
        time.sleep(0.01)

    # Create 3 schedulers
    # IMPORTANT: Use same lock_prefix for all schedulers to share the same lock
    schedulers = []
    for i in range(3):
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            lock_ttl_seconds=5,
            lock_prefix="shared:lock:",  # Same prefix for distributed locking
            verbose=False,
        )
        # Register function on each scheduler
        scheduler.register_job_function("test_func", test_func)
        schedulers.append(scheduler)

    # Schedule 10 jobs at same time
    run_time = datetime.now() + timedelta(seconds=2)
    for i in range(10):
        job_id = f"job_{i}"
        # Each scheduler adds jobs, but they should be distributed at execution
        schedulers[i % 3].create_date_job(
            job_id=job_id,
            name=f"Job {i}",
            func="test_func",
            run_date=run_time,
            kwargs={"job_id": job_id},
        )

    # Start all schedulers
    for scheduler in schedulers:
        scheduler.start()

    try:
        # Wait for all jobs to execute
        time.sleep(5)

        # Stop all schedulers
        for scheduler in schedulers:
            scheduler.stop()

        # Verify all jobs executed at least once
        assert len(execution_times) >= 10, (
            f"Expected at least 10 jobs executed, got {len(execution_times)}"
        )

        # Verify no job executed more than once (lock should prevent duplicates)
        for job_id, times in execution_times.items():
            assert len(times) == 1, (
                f"Job {job_id} executed {len(times)} times (lock failed to prevent duplicate execution)"
            )

    finally:
        for scheduler in schedulers:
            if scheduler.is_running():
                scheduler.stop()


def test_lock_timeout_recovery():
    """Test that lock timeout allows job to be picked up by another scheduler."""
    # Shared storage and lock with short timeout
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    execution_count = {"count": 0}
    count_lock = threading.Lock()

    def test_func():
        with count_lock:
            execution_count["count"] += 1
        time.sleep(0.1)

    # Create 2 schedulers with short lock TTL
    # IMPORTANT: Use same lock_prefix for all schedulers to share the same lock
    scheduler1 = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,
        lock_ttl_seconds=2,
        lock_prefix="shared:lock:",  # Same prefix for distributed locking
        verbose=False,
    )
    scheduler2 = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,
        lock_ttl_seconds=2,
        lock_prefix="shared:lock:",  # Same prefix for distributed locking
        verbose=False,
    )

    # Register function on both schedulers
    scheduler1.register_job_function("test_func", test_func)
    scheduler2.register_job_function("test_func", test_func)

    # Schedule a job
    job_id = scheduler1.create_date_job(
        job_id="test_job",
        name="Test Job",
        func="test_func",
        run_date=datetime.now() + timedelta(seconds=1),
    )

    # Start both schedulers
    scheduler1.start()
    scheduler2.start()

    try:
        # Wait for job execution
        time.sleep(4)

        # Stop schedulers
        scheduler1.stop()
        scheduler2.stop()

        # Verify job executed exactly once (despite lock timeout)
        assert execution_count["count"] == 1, (
            f"Expected 1 execution, got {execution_count['count']}"
        )

    finally:
        if scheduler1.is_running():
            scheduler1.stop()
        if scheduler2.is_running():
            scheduler2.stop()


def test_concurrent_polling_with_queue_limit():
    """Test that queue size limit works correctly with multiple schedulers."""
    # Shared storage and lock
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    execution_count = {"count": 0}
    count_lock = threading.Lock()

    def slow_func():
        with count_lock:
            execution_count["count"] += 1
        time.sleep(1)  # Slow job to test queue limits

    # Create 2 schedulers with small queue size
    # IMPORTANT: Use same lock_prefix for all schedulers to share the same lock
    max_queue_size = 5
    schedulers = []
    for i in range(2):
        scheduler = PollingScheduler(
            storage_adapter=storage,
            lock_adapter=lock,
            polling_interval_seconds=1,
            lock_ttl_seconds=5,
            lock_prefix="shared:lock:",  # Same prefix for distributed locking
            verbose=False,
            max_queue_size=max_queue_size,
        )
        # Register function on each scheduler
        scheduler.register_job_function("slow_func", slow_func)
        schedulers.append(scheduler)

    # Schedule 20 jobs at once
    run_time = datetime.now() + timedelta(seconds=1)
    for i in range(20):
        schedulers[0].create_date_job(
            job_id=f"job_{i}",
            name=f"Job {i}",
            func="slow_func",
            run_date=run_time,
        )

    # Start schedulers
    for scheduler in schedulers:
        scheduler.start()

    try:
        # Wait for some jobs to execute
        time.sleep(8)

        # Check queue status
        status = schedulers[0].get_queue_status()

        # Queue should respect limits
        total_in_flight = status["pending_jobs"] + status["running_jobs"]
        assert total_in_flight <= max_queue_size, (
            f"Expected total in-flight <= {max_queue_size}, "
            f"got {total_in_flight} (pending={status['pending_jobs']}, running={status['running_jobs']})"
        )

    finally:
        for scheduler in schedulers:
            if scheduler.is_running():
                scheduler.stop()
