"""Shared fixtures for unit tests."""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest


@pytest.fixture
def mock_storage():
    """Create a mock storage adapter."""
    storage = Mock()
    storage.create_job = Mock(return_value={})
    storage.get_job = Mock(return_value=None)
    storage.update_job = Mock(return_value={})
    storage.delete_job = Mock(return_value=True)
    storage.query_jobs = Mock(return_value=[])
    storage.count_jobs = Mock(return_value=0)
    storage.update_job_run_times = Mock(return_value={})
    return storage


@pytest.fixture
def mock_lock():
    """Create a mock lock adapter."""
    lock = Mock()
    lock.acquire = Mock(return_value=True)
    lock.release = Mock(return_value=True)
    lock.extend = Mock(return_value=True)
    lock.reset = Mock()
    return lock


@pytest.fixture
def mock_executor():
    """Create a mock thread pool executor."""
    executor = Mock()
    executor.submit = Mock(return_value=Mock(add_done_callback=Mock()))
    return executor


@pytest.fixture
def mock_async_executor():
    """Create a mock async executor."""
    executor = Mock()
    executor.execute_coroutine = Mock(return_value=Mock())
    executor.start = Mock()
    executor.stop = Mock()
    executor.is_running = Mock(return_value=False)
    return executor


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    logger.debug = Mock()
    logger.with_context = Mock(return_value=logger)
    return logger


@pytest.fixture
def mock_job_queue():
    """Create a mock job queue."""
    queue = Mock()
    queue.add_job = Mock(return_value=True)
    queue.get_next_job = Mock(return_value=None)
    queue.get_available_slots = Mock(return_value=10)
    queue.get_status = Mock(return_value={"size": 0, "max_size": 10})
    queue.size = Mock(return_value=0)
    queue.is_empty = Mock(return_value=True)
    queue.mark_completed = Mock()
    return queue


@pytest.fixture
def sample_job_data():
    """Create sample job data dict for testing."""
    return {
        "job_id": "test-job-1",
        "name": "Test Job",
        "func_name": "test_func",
        "trigger_type": "interval",
        "trigger_args": {"seconds": 60},
        "timezone": "UTC",
        "status": "scheduled",
        "args": [],
        "kwargs": {},
        "metadata": {},
        "created_at": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "updated_at": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "next_run_time": datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc).isoformat(),
        "next_run_time_local": datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc).isoformat(),
        "max_retries": 0,
        "retry_count": 0,
        "retry_delay_seconds": 60,
        "priority": 5,
    }
