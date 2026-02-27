"""Integration tests for misfire → actual execution pipeline."""

import time
from datetime import timedelta

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler
from chronis.utils.time import utc_now


def _create_scheduler(**overrides):
    defaults = dict(
        storage_adapter=InMemoryStorageAdapter(),
        lock_adapter=InMemoryLockAdapter(),
        polling_interval_seconds=1,
        executor_interval_seconds=1,
        verbose=False,
    )
    defaults.update(overrides)
    return PollingScheduler(**defaults)


def _insert_misfired_job(storage, job_id, policy, trigger_type="interval",
                          trigger_args=None, minutes_late=5):
    """Insert a job with next_run_time in the past (misfired)."""
    past = (utc_now() - timedelta(minutes=minutes_late)).isoformat()
    now = utc_now().isoformat()
    storage.create_job({
        "job_id": job_id,
        "name": f"Misfired {job_id}",
        "trigger_type": trigger_type,
        "trigger_args": trigger_args or {"seconds": 30},
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
        "priority": 5,
        "if_missed": policy,
        "misfire_threshold_seconds": 60,
        "last_run_time": None,
        "last_scheduled_time": None,
    })


class TestMisfireRunOnceExecution:
    """run_once: 놓친 잡 → enqueue → 실행 → scheduled 복귀까지 전체 흐름."""

    def test_run_once_misfired_job_executes_and_reschedules(self):
        """run_once 정책: 놓친 잡이 1회 실행되고 next_run_time이 미래로 갱신된다."""
        scheduler = _create_scheduler()
        storage = scheduler.storage
        executed = {"count": 0}

        def test_func():
            executed["count"] += 1

        scheduler.register_job_function("test_func", test_func)

        # 5분 늦은 interval 잡 (threshold 60s → misfired)
        _insert_misfired_job(storage, "misfired-1", policy="run_once",
                            trigger_args={"minutes": 5})

        # 1) enqueue - run_once이므로 enqueue됨
        added = scheduler._enqueue_jobs()
        assert added == 1

        # 2) dequeue + execute
        job_id = scheduler._job_queue.get_next_job()
        assert job_id == "misfired-1"

        job_data = storage.get_job(job_id)
        scheduler._execution_coordinator.try_execute(
            job_data, scheduler._job_queue.mark_completed
        )
        time.sleep(0.5)

        # 3) 실행 확인
        assert executed["count"] == 1

        # 4) interval 잡이므로 status=scheduled, next_run_time 미래
        final = storage.get_job("misfired-1")
        assert final["status"] == "scheduled"
        assert final["next_run_time"] > utc_now().isoformat()


class TestMisfireSkipExecution:
    """skip: 놓친 잡 → 실행 없이 next_run_time만 점프."""

    def test_skip_misfired_job_not_executed_but_rescheduled(self):
        """skip 정책: 함수 실행 없이 next_run_time이 미래로 갱신된다."""
        scheduler = _create_scheduler()
        storage = scheduler.storage
        executed = {"count": 0}

        def test_func():
            executed["count"] += 1

        scheduler.register_job_function("test_func", test_func)

        _insert_misfired_job(storage, "skip-1", policy="skip",
                            trigger_args={"minutes": 5})

        original = storage.get_job("skip-1")
        original_next_run = original["next_run_time"]

        # enqueue - skip이므로 0개 enqueue, 대신 next_run_time 갱신
        added = scheduler._enqueue_jobs()
        assert added == 0

        # 실행 안 됨
        assert executed["count"] == 0

        # next_run_time 미래로 점프
        updated = storage.get_job("skip-1")
        assert updated["status"] == "scheduled"
        assert updated["next_run_time"] > original_next_run
        assert updated["next_run_time"] > utc_now().isoformat()


class TestMisfireRunAllExecution:
    """run_all: 놓친 잡 → 증분 catch-up → 여러 번 실행."""

    def test_run_all_catches_up_incrementally(self):
        """run_all 정책: 15분 늦은 5분 interval → 1회 실행 후 next_run_time 증분."""
        scheduler = _create_scheduler()
        storage = scheduler.storage
        executed = {"count": 0}

        def test_func():
            executed["count"] += 1

        scheduler.register_job_function("test_func", test_func)

        # 15분 늦은 5분 interval (3번 놓침)
        _insert_misfired_job(storage, "runall-1", policy="run_all",
                            trigger_args={"minutes": 5}, minutes_late=15)

        # 1차 poll + execute
        added = scheduler._enqueue_jobs()
        assert added == 1

        job_id = scheduler._job_queue.get_next_job()
        job_data = storage.get_job(job_id)
        scheduler._execution_coordinator.try_execute(
            job_data, scheduler._job_queue.mark_completed
        )
        time.sleep(0.5)

        assert executed["count"] == 1

        # 1차 실행 후 next_run_time은 여전히 과거 (증분 전진)
        after_first = storage.get_job("runall-1")
        assert after_first["status"] == "scheduled"
        assert after_first["next_run_time"] < utc_now().isoformat()

        # 2차 poll + execute (아직 과거이므로 다시 enqueue)
        added2 = scheduler._enqueue_jobs()
        assert added2 == 1

        job_id2 = scheduler._job_queue.get_next_job()
        job_data2 = storage.get_job(job_id2)
        scheduler._execution_coordinator.try_execute(
            job_data2, scheduler._job_queue.mark_completed
        )
        time.sleep(0.5)

        assert executed["count"] == 2

        # 2차 후에도 여전히 과거 (원래 3번 놓침)
        after_second = storage.get_job("runall-1")
        assert after_second["status"] == "scheduled"


class TestMisfireDateJobExecution:
    """date 잡 misfire 처리."""

    def test_skip_deletes_misfired_date_job(self):
        """skip 정책 + date 잡 → 삭제."""
        scheduler = _create_scheduler()
        storage = scheduler.storage
        scheduler.register_job_function("test_func", lambda: None)

        _insert_misfired_job(storage, "date-skip", policy="skip",
                            trigger_type="date",
                            trigger_args={"run_date": "2020-01-01 00:00:00"})

        scheduler._enqueue_jobs()

        # date + skip → 삭제됨
        assert storage.get_job("date-skip") is None

    def test_run_once_misfired_date_job_executes_then_deletes(self):
        """run_once 정책 + date 잡 → 1회 실행 후 삭제."""
        scheduler = _create_scheduler()
        storage = scheduler.storage
        executed = {"count": 0}

        def test_func():
            executed["count"] += 1

        scheduler.register_job_function("test_func", test_func)

        _insert_misfired_job(storage, "date-once", policy="run_once",
                            trigger_type="date",
                            trigger_args={"run_date": "2020-01-01 00:00:00"})

        added = scheduler._enqueue_jobs()
        assert added == 1

        job_id = scheduler._job_queue.get_next_job()
        job_data = storage.get_job(job_id)
        scheduler._execution_coordinator.try_execute(
            job_data, scheduler._job_queue.mark_completed
        )
        time.sleep(0.5)

        assert executed["count"] == 1

        # date 잡은 실행 후 삭제됨
        assert storage.get_job("date-once") is None
