"""Unit tests for _PeriodicRunner."""

import threading
import time

from chronis.core.schedulers.polling_scheduler import _PeriodicRunner


class TestPeriodicRunnerBasic:
    """Test _PeriodicRunner basic operations."""

    def test_start_executes_task_periodically(self):
        """등록된 task가 주기적으로 실행된다."""
        counter = {"value": 0}
        lock = threading.Lock()

        def increment():
            with lock:
                counter["value"] += 1

        runner = _PeriodicRunner()
        runner.add_task(increment, 0.1, "test")
        runner.start()

        time.sleep(0.55)  # Should execute ~5 times
        runner.shutdown(wait=True)

        with lock:
            assert counter["value"] >= 3

    def test_shutdown_stops_execution(self):
        """shutdown 후 task 실행이 중단된다."""
        counter = {"value": 0}
        lock = threading.Lock()

        def increment():
            with lock:
                counter["value"] += 1

        runner = _PeriodicRunner()
        runner.add_task(increment, 0.1, "test")
        runner.start()

        time.sleep(0.3)
        runner.shutdown(wait=True)

        with lock:
            count_after_stop = counter["value"]

        time.sleep(0.3)

        with lock:
            assert counter["value"] == count_after_stop  # No more increments


class TestPeriodicRunnerExceptionHandling:
    """Test _run_loop exception handling."""

    def test_exception_does_not_stop_loop(self):
        """func() 예외 발생 시 pass → 루프 계속 실행."""
        call_count = {"value": 0}
        lock = threading.Lock()

        def failing_task():
            with lock:
                call_count["value"] += 1
            if call_count["value"] <= 2:
                raise RuntimeError("Intentional error")

        runner = _PeriodicRunner()
        runner.add_task(failing_task, 0.1, "failing")
        runner.start()

        time.sleep(0.55)
        runner.shutdown(wait=True)

        with lock:
            # Should have continued despite exceptions
            assert call_count["value"] >= 3


class TestPeriodicRunnerShutdown:
    """Test shutdown behavior."""

    def test_shutdown_wait_true_joins_threads(self):
        """shutdown(wait=True)는 스레드 종료까지 대기."""
        runner = _PeriodicRunner()
        runner.add_task(lambda: time.sleep(0.01), 0.1, "task1")
        runner.start()

        assert len(runner._threads) == 1

        runner.shutdown(wait=True)

        # After shutdown, threads and tasks should be cleared
        assert len(runner._threads) == 0
        assert len(runner._tasks) == 0

    def test_shutdown_wait_false_returns_immediately(self):
        """shutdown(wait=False)는 즉시 반환."""
        runner = _PeriodicRunner()
        runner.add_task(lambda: time.sleep(0.01), 0.1, "task1")
        runner.start()

        start = time.time()
        runner.shutdown(wait=False)
        elapsed = time.time() - start

        assert elapsed < 0.5
        # Threads/tasks still cleared
        assert len(runner._threads) == 0

    def test_multiple_tasks_all_execute(self):
        """여러 task 등록 시 모두 실행."""
        results = {"a": 0, "b": 0}
        lock = threading.Lock()

        def task_a():
            with lock:
                results["a"] += 1

        def task_b():
            with lock:
                results["b"] += 1

        runner = _PeriodicRunner()
        runner.add_task(task_a, 0.1, "task-a")
        runner.add_task(task_b, 0.1, "task-b")
        runner.start()

        time.sleep(0.35)
        runner.shutdown(wait=True)

        with lock:
            assert results["a"] >= 2
            assert results["b"] >= 2
