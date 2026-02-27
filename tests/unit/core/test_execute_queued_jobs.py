"""Unit tests for _execute_queued_jobs() error paths."""

from unittest.mock import MagicMock, Mock, patch

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
from chronis.utils.time import utc_now


def _make_scheduler(**overrides):
    defaults = dict(
        storage_adapter=InMemoryStorageAdapter(),
        lock_adapter=InMemoryLockAdapter(),
        polling_interval_seconds=1,
        verbose=False,
    )
    defaults.update(overrides)
    return PollingScheduler(**defaults)


class TestExecuteQueuedJobsErrorPaths:
    """Test _execute_queued_jobs() error handling."""

    def test_deleted_job_marked_completed(self):
        """Queue에 있지만 storage에서 삭제된 job → mark_completed 호출."""
        scheduler = _make_scheduler()

        # Add job to queue
        scheduler._job_queue.add_job("deleted-job")

        # Storage has no such job (simulates deletion between enqueue and execute)
        # InMemoryStorage returns None for get_job("deleted-job")

        scheduler._execute_queued_jobs()

        # Job should be removed from queue tracking
        assert "deleted-job" not in scheduler._job_queue._known_job_ids

    def test_try_execute_failure_marks_completed(self):
        """try_execute 실패 (lock 획득 실패) → mark_completed 호출."""
        scheduler = _make_scheduler()
        storage = scheduler.storage

        now = utc_now()
        storage.create_job({
            "job_id": "lock-fail",
            "name": "Lock Fail Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": "scheduled",
            "next_run_time": now.isoformat(),
            "next_run_time_local": now.isoformat(),
            "metadata": {},
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        })

        scheduler._job_queue.add_job("lock-fail")

        # Mock try_execute to return False (lock failure)
        scheduler._execution_coordinator.try_execute = Mock(return_value=False)

        scheduler._execute_queued_jobs()

        # Job should be removed from queue tracking
        assert "lock-fail" not in scheduler._job_queue._known_job_ids
        scheduler._execution_coordinator.try_execute.assert_called_once()

    def test_backpressure_full_queue_returns_zero(self):
        """Queue가 가득 찬 상태에서 _enqueue_jobs() → 0 반환."""
        scheduler = _make_scheduler(max_queue_size=2)

        # Fill the queue
        scheduler._job_queue.add_job("job-1")
        scheduler._job_queue.add_job("job-2")

        assert scheduler._job_queue.get_available_slots() == 0

        added = scheduler._enqueue_jobs()
        assert added == 0

    def test_outer_exception_caught_and_logged(self):
        """_execute_queued_jobs 내부 예외 → 로깅 후 정상 반환."""
        scheduler = _make_scheduler()

        # Add a job to make the while loop enter
        scheduler._job_queue.add_job("crash-job")

        # Mock get_jobs_batch to raise
        scheduler.storage.get_jobs_batch = Mock(side_effect=RuntimeError("DB down"))

        # Should not raise
        scheduler._execute_queued_jobs()

        # Logger should have been called with error
        # (We can't easily check logger mock since it's a real logger,
        # but the key test is that no exception propagates)

    def test_empty_queue_returns_immediately(self):
        """빈 큐에서 _execute_queued_jobs() 즉시 반환."""
        scheduler = _make_scheduler()

        assert scheduler._job_queue.is_empty()

        # Should complete without error and without touching storage
        scheduler._execute_queued_jobs()
