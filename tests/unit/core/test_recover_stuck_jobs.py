"""Unit tests for PollingScheduler._recover_stuck_jobs()."""

from datetime import timedelta
from unittest.mock import MagicMock

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


def _make_stuck_job(storage, job_id="stuck-1", stuck_seconds=600, lock_ttl=300):
    """Create a job in RUNNING state with updated_at far in the past."""
    stuck_time = (utc_now() - timedelta(seconds=stuck_seconds)).isoformat()
    storage.create_job({
        "job_id": job_id,
        "name": "Stuck Job",
        "trigger_type": "interval",
        "trigger_args": {"seconds": 30},
        "timezone": "UTC",
        "status": "running",
        "next_run_time": utc_now().isoformat(),
        "next_run_time_local": utc_now().isoformat(),
        "metadata": {},
        "created_at": stuck_time,
        "updated_at": stuck_time,
    })


class TestRecoverStuckJobs:
    """Test _recover_stuck_jobs() detection and recovery logic."""

    def test_recovers_stuck_running_job(self):
        """RUNNING 상태 + updated_at이 lock_ttl*2 초과 → SCHEDULED로 복구."""
        scheduler = _make_scheduler()
        storage = scheduler.storage
        _make_stuck_job(storage, stuck_seconds=700, lock_ttl=300)

        scheduler._recover_stuck_jobs()

        job = storage.get_job("stuck-1")
        assert job["status"] == "scheduled"

    def test_does_not_recover_recent_running_job(self):
        """RUNNING 상태지만 updated_at이 범위 내 → 복구 안 함."""
        scheduler = _make_scheduler()
        storage = scheduler.storage

        # updated_at이 최근 (1초 전)
        recent_time = (utc_now() - timedelta(seconds=1)).isoformat()
        storage.create_job({
            "job_id": "recent-1",
            "name": "Recent Running Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": "running",
            "next_run_time": utc_now().isoformat(),
            "next_run_time_local": utc_now().isoformat(),
            "metadata": {},
            "created_at": recent_time,
            "updated_at": recent_time,
        })

        scheduler._recover_stuck_jobs()

        job = storage.get_job("recent-1")
        assert job["status"] == "running"

    def test_does_not_recover_scheduled_job(self):
        """SCHEDULED 상태 job은 복구 대상이 아님."""
        scheduler = _make_scheduler()
        storage = scheduler.storage

        old_time = (utc_now() - timedelta(seconds=700)).isoformat()
        storage.create_job({
            "job_id": "sched-1",
            "name": "Old Scheduled Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": "scheduled",
            "next_run_time": utc_now().isoformat(),
            "next_run_time_local": utc_now().isoformat(),
            "metadata": {},
            "created_at": old_time,
            "updated_at": old_time,
        })

        scheduler._recover_stuck_jobs()

        job = storage.get_job("sched-1")
        assert job["status"] == "scheduled"

    def test_cas_failure_logs_warning(self):
        """CAS 실패 시 경고 로깅만 하고 예외 미전파."""
        storage = MagicMock()
        # query returns a stuck job
        storage.query_jobs.return_value = [{"job_id": "stuck-1", "status": "running"}]
        # CAS fails
        storage.compare_and_swap_job.side_effect = Exception("CAS conflict")

        scheduler = _make_scheduler()
        scheduler.storage = storage  # override storage

        # Should not raise - exception is caught and logged
        scheduler._recover_stuck_jobs()

        storage.compare_and_swap_job.assert_called_once()

    def test_no_stuck_jobs_returns_normally(self):
        """복구 대상이 없을 때 정상 반환."""
        scheduler = _make_scheduler()
        # No jobs at all
        scheduler._recover_stuck_jobs()
        # Should complete without error
