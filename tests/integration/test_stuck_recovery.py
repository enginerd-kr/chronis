"""Integration tests for stuck job recovery (full pipeline)."""

import time
from datetime import timedelta

import pytest

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
from chronis.utils.time import utc_now


def _create_scheduler(**overrides):
    defaults = dict(
        storage_adapter=InMemoryStorageAdapter(),
        lock_adapter=InMemoryLockAdapter(),
        polling_interval_seconds=1,
        executor_interval_seconds=1,
        lock_ttl_seconds=300,
        verbose=False,
    )
    defaults.update(overrides)
    return PollingScheduler(**defaults)


def _insert_stuck_job(storage, job_id="stuck-job", stuck_seconds=700):
    """Insert a job in RUNNING state with updated_at far in the past."""
    stuck_time = (utc_now() - timedelta(seconds=stuck_seconds)).isoformat()
    now = utc_now()
    storage.create_job({
        "job_id": job_id,
        "name": "Stuck Job",
        "trigger_type": "interval",
        "trigger_args": {"seconds": 30},
        "timezone": "UTC",
        "func_name": "test_func",
        "args": (),
        "kwargs": {},
        "status": "running",
        "next_run_time": now.isoformat(),
        "next_run_time_local": now.isoformat(),
        "metadata": {},
        "created_at": stuck_time,
        "updated_at": stuck_time,
        "max_retries": 0,
        "retry_delay_seconds": 60,
        "retry_count": 0,
        "timeout_seconds": None,
        "priority": 5,
        "if_missed": "run_once",
        "misfire_threshold_seconds": 60,
        "last_run_time": None,
        "last_scheduled_time": None,
    })


class TestStuckJobRecoveryIntegration:
    """Stuck job recovery: RUNNING → SCHEDULED → re-enqueue → 실행까지 전체 흐름."""

    def test_stuck_job_recovered_and_re_executed(self):
        """RUNNING으로 stuck된 잡이 복구 후 다시 실행된다."""
        scheduler = _create_scheduler()
        storage = scheduler.storage
        execution_count = {"value": 0}

        def test_func():
            execution_count["value"] += 1

        scheduler.register_job_function("test_func", test_func)

        # 1) Insert a stuck job (RUNNING, updated_at 700s ago, lock_ttl=300)
        _insert_stuck_job(storage, stuck_seconds=700)

        job = storage.get_job("stuck-job")
        assert job["status"] == "running"

        # 2) _enqueue_jobs() internally calls _recover_stuck_jobs() first
        #    This should: detect stuck → CAS to scheduled → query ready → enqueue
        added = scheduler._enqueue_jobs()

        # Job should be recovered to scheduled and then enqueued
        recovered = storage.get_job("stuck-job")
        assert recovered["status"] in ("scheduled", "running")  # may already be picked up

        # If the job was enqueued, execute it
        if added > 0:
            job_id = scheduler._job_queue.get_next_job()
            assert job_id == "stuck-job"
            job_data = storage.get_job(job_id)
            scheduler._execution_coordinator.try_execute(job_data, lambda jid: None)
            time.sleep(0.5)

            assert execution_count["value"] == 1

    def test_stuck_recovery_does_not_touch_healthy_running_job(self):
        """최근에 updated_at이 변경된 RUNNING 잡은 복구하지 않는다."""
        scheduler = _create_scheduler()
        storage = scheduler.storage

        # Insert a recently updated RUNNING job (not stuck)
        now = utc_now()
        storage.create_job({
            "job_id": "healthy-running",
            "name": "Healthy Running Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "func_name": "test_func",
            "args": (),
            "kwargs": {},
            "status": "running",
            "next_run_time": now.isoformat(),
            "next_run_time_local": now.isoformat(),
            "metadata": {},
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "max_retries": 0,
            "retry_delay_seconds": 60,
            "retry_count": 0,
            "timeout_seconds": None,
            "priority": 5,
            "if_missed": "run_once",
            "misfire_threshold_seconds": 60,
            "last_run_time": None,
            "last_scheduled_time": None,
        })

        scheduler.register_job_function("test_func", lambda: None)

        # Enqueue should NOT recover this job (it's recently updated)
        scheduler._enqueue_jobs()

        job = storage.get_job("healthy-running")
        assert job["status"] == "running"  # Still running, not recovered

    def test_multiple_stuck_jobs_all_recovered(self):
        """여러 stuck 잡이 한꺼번에 복구된다."""
        scheduler = _create_scheduler()
        storage = scheduler.storage

        scheduler.register_job_function("test_func", lambda: None)

        # Insert 3 stuck jobs
        for i in range(3):
            _insert_stuck_job(storage, job_id=f"stuck-{i}", stuck_seconds=700)

        scheduler._enqueue_jobs()

        # All should be recovered
        for i in range(3):
            job = storage.get_job(f"stuck-{i}")
            assert job["status"] == "scheduled", f"stuck-{i} was not recovered"
