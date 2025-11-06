"""Basic tests to verify framework setup."""


from chronis import (
    InMemoryLockAdapter,
    InMemoryStorageAdapter,
    JobDefinition,
    PollingScheduler,
    TriggerType,
)


def test_imports():
    """Test that all core imports work."""
    assert InMemoryStorageAdapter is not None
    assert InMemoryLockAdapter is not None
    assert PollingScheduler is not None
    assert JobDefinition is not None
    assert TriggerType is not None


def test_create_scheduler():
    """Test scheduler creation."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=10,
    )
    assert scheduler is not None
    assert not scheduler.is_running()


def test_create_job():
    """Test job creation."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
    )

    def dummy_func():
        print("Hello from dummy job")

    job = JobDefinition(
        job_id="test-001",
        name="Test Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 5},
        func=dummy_func,
    )

    # Register function
    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create job
    job_info = scheduler.create_job(job)
    assert job_info.job_id == "test-001"
    assert job_info.name == "Test Job"
    assert job_info.is_active is True


def test_list_jobs():
    """Test listing jobs."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
    )

    def dummy_func():
        print("Hello")

    scheduler.register_job_function(f"{dummy_func.__module__}.{dummy_func.__name__}", dummy_func)

    # Create multiple jobs
    for i in range(3):
        job = JobDefinition(
            job_id=f"test-{i}",
            name=f"Test Job {i}",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 5},
            func=dummy_func,
        )
        scheduler.create_job(job)

    # List all jobs
    jobs = scheduler.list_jobs()
    assert len(jobs) == 3
