"""Integration tests for backpressure: queue full → reject → drain → re-enqueue."""

import time
from datetime import timedelta

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
from chronis.utils.time import utc_now


def _create_scheduler(max_queue_size=3, max_workers=2, **overrides):
    defaults = {
        "storage_adapter": InMemoryStorageAdapter(),
        "lock_adapter": InMemoryLockAdapter(),
        "polling_interval_seconds": 1,
        "executor_interval_seconds": 1,
        "max_workers": max_workers,
        "max_queue_size": max_queue_size,
        "verbose": False,
    }
    defaults.update(overrides)
    return PollingScheduler(**defaults)


def _insert_ready_job(storage, job_id, priority=5):
    """Insert a SCHEDULED job with next_run_time in the past (ready)."""
    past = (utc_now() - timedelta(seconds=5)).isoformat()
    now = utc_now().isoformat()
    storage.create_job(
        {
            "job_id": job_id,
            "name": f"Job {job_id}",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 300},
            "timezone": "UTC",
            "func_name": "test_func",
            "args": (),
            "kwargs": {},
            "status": "scheduled",
            "next_run_time": past,
            "next_run_time_local": past,
            "metadata": {},
            "created_at": now,
            "updated_at": now,
            "max_retries": 0,
            "retry_delay_seconds": 60,
            "retry_count": 0,
            "timeout_seconds": None,
            "priority": priority,
            "if_missed": "run_once",
            "misfire_threshold_seconds": 60,
            "last_run_time": None,
            "last_scheduled_time": None,
        }
    )


class TestBackpressureIntegration:
    """큐 용량 한계 → 거부 → 슬롯 해제 → 재enqueue 전체 흐름."""

    def test_full_queue_rejects_then_accepts_after_drain(self):
        """큐가 꽉 차면 enqueue 0, 전부 drain 후 다시 enqueue."""
        scheduler = _create_scheduler(max_queue_size=3)
        storage = scheduler.storage
        scheduler.register_job_function("test_func", lambda: None)

        # 5개 잡 등록 (큐 용량 3)
        for i in range(5):
            _insert_ready_job(storage, f"job-{i}")

        # 1차 poll: 3개만 enqueue (큐 용량 한계)
        added1 = scheduler._enqueue_jobs()
        assert added1 == 3

        status = scheduler._job_queue.get_status()
        assert status["pending_jobs"] == 3
        assert status["available_slots"] == 0

        # 2차 poll: 큐 꽉 찬 상태 → 0개 enqueue
        added2 = scheduler._enqueue_jobs()
        assert added2 == 0

        # 3개 전부 dequeue + complete → known_job_ids 클리어
        for _ in range(3):
            job_id = scheduler._job_queue.get_next_job()
            scheduler._job_queue.mark_completed(job_id)

        assert scheduler._job_queue.get_available_slots() == 3

        # 3차 poll: 슬롯 3 → ready 잡 5개 중 3개 enqueue
        # (완료된 3개도 다시 ready이므로 5개 전부 ready, limit=3)
        added3 = scheduler._enqueue_jobs()
        assert added3 == 3

    def test_backpressure_with_real_execution(self):
        """실제 실행 포함: 큐 꽉참 → 실행 완료 → 남은 잡 enqueue → 실행."""
        scheduler = _create_scheduler(max_queue_size=2, max_workers=2)
        storage = scheduler.storage
        executed = {"ids": []}

        def test_func():
            executed["ids"].append("done")

        scheduler.register_job_function("test_func", test_func)

        # 4개 잡 (큐 용량 2)
        for i in range(4):
            _insert_ready_job(storage, f"bp-{i}")

        # 1차: 2개만 enqueue
        added1 = scheduler._enqueue_jobs()
        assert added1 == 2

        # 1차 실행
        for _ in range(2):
            job_id = scheduler._job_queue.get_next_job()
            if job_id:
                job_data = storage.get_job(job_id)
                scheduler._execution_coordinator.try_execute(
                    job_data, scheduler._job_queue.mark_completed
                )
        time.sleep(0.5)

        first_batch = len(executed["ids"])
        assert first_batch == 2

        # 2차: 나머지 2개 enqueue
        added2 = scheduler._enqueue_jobs()
        assert added2 == 2

        # 2차 실행
        for _ in range(2):
            job_id = scheduler._job_queue.get_next_job()
            if job_id:
                job_data = storage.get_job(job_id)
                scheduler._execution_coordinator.try_execute(
                    job_data, scheduler._job_queue.mark_completed
                )
        time.sleep(0.5)

        # 전체 4개 실행 완료
        assert len(executed["ids"]) == 4

    def test_queue_status_reflects_backpressure(self):
        """get_queue_status()가 backpressure 상태를 정확히 반영."""
        scheduler = _create_scheduler(max_queue_size=2)
        storage = scheduler.storage
        scheduler.register_job_function("test_func", lambda: None)

        _insert_ready_job(storage, "s-1")
        _insert_ready_job(storage, "s-2")

        scheduler._enqueue_jobs()

        status = scheduler.get_queue_status()
        assert status["pending_jobs"] == 2
        assert status["available_slots"] == 0
        assert status["utilization"] == 1.0

        # Dequeue one
        job_id = scheduler._job_queue.get_next_job()
        scheduler._job_queue.mark_completed(job_id)

        status2 = scheduler.get_queue_status()
        assert status2["pending_jobs"] == 1
        assert status2["available_slots"] == 1
        assert 0.4 < status2["utilization"] < 0.6
