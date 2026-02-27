"""Unit tests for JobQueue class."""

import threading

from chronis.core.execution.job_queue import JobQueue


class TestJobQueueBasic:
    """Test basic add/get operations."""

    def test_add_and_get_job(self):
        """Test basic add and get workflow."""
        queue = JobQueue()

        assert queue.add_job("job-1") is True

        result = queue.get_next_job()
        assert result == "job-1"

    def test_add_duplicate_rejected(self):
        """Test that adding the same job_id twice is rejected."""
        queue = JobQueue()

        assert queue.add_job("job-1") is True
        assert queue.add_job("job-1") is False

    def test_get_from_empty_queue(self):
        """Test that get from empty queue returns None."""
        queue = JobQueue()

        assert queue.get_next_job() is None

    def test_queue_full_rejected(self):
        """Test that adding beyond max_queue_size is rejected."""
        queue = JobQueue(max_queue_size=2)

        assert queue.add_job("job-1") is True
        assert queue.add_job("job-2") is True
        assert queue.add_job("job-3") is False


class TestJobQueuePriority:
    """Test priority ordering behavior."""

    def test_higher_priority_first(self):
        """Test that higher priority jobs are dequeued first."""
        queue = JobQueue()

        queue.add_job("low", priority=3)
        queue.add_job("high", priority=7)
        queue.add_job("mid", priority=5)

        assert queue.get_next_job() == "high"
        assert queue.get_next_job() == "mid"
        assert queue.get_next_job() == "low"

    def test_same_priority_fifo(self):
        """Test that jobs with same priority maintain FIFO order."""
        queue = JobQueue()

        queue.add_job("first", priority=5)
        queue.add_job("second", priority=5)
        queue.add_job("third", priority=5)

        assert queue.get_next_job() == "first"
        assert queue.get_next_job() == "second"
        assert queue.get_next_job() == "third"


class TestJobQueueLifecycle:
    """Test job lifecycle transitions (pending -> in-flight -> completed)."""

    def test_get_moves_to_in_flight(self):
        """Test that get_next_job moves job to in-flight set."""
        queue = JobQueue()
        queue.add_job("job-1")

        queue.get_next_job()

        assert "job-1" in queue._in_flight_jobs

    def test_mark_completed_allows_readd(self):
        """Test that mark_completed removes job from tracking, allowing re-add."""
        queue = JobQueue()

        queue.add_job("job-1")
        queue.get_next_job()
        queue.mark_completed("job-1")

        assert "job-1" not in queue._known_job_ids
        assert "job-1" not in queue._in_flight_jobs
        assert queue.add_job("job-1") is True

    def test_available_slots_accuracy(self):
        """Test that available_slots is accurate through add/get/mark lifecycle."""
        queue = JobQueue(max_queue_size=5)

        assert queue.get_available_slots() == 5

        queue.add_job("job-1")
        queue.add_job("job-2")
        assert queue.get_available_slots() == 3

        queue.get_next_job()  # job-1 moves to in-flight (still occupies slot)
        assert queue.get_available_slots() == 3

        queue.mark_completed("job-1")  # job-1 freed
        assert queue.get_available_slots() == 4

        queue.get_next_job()  # job-2 moves to in-flight
        queue.mark_completed("job-2")
        assert queue.get_available_slots() == 5


class TestJobQueueStatus:
    """Test status query methods."""

    def test_is_empty_and_is_full(self):
        """Test is_empty and is_full state transitions."""
        queue = JobQueue(max_queue_size=2)

        assert queue.is_empty() is True
        assert queue.is_full() is False

        queue.add_job("job-1")
        assert queue.is_empty() is False
        assert queue.is_full() is False

        queue.add_job("job-2")
        assert queue.is_empty() is False
        assert queue.is_full() is True

        queue.get_next_job()  # job-1: pending -> in-flight, still occupies a slot
        assert queue.is_empty() is False  # job-2 still in pending queue
        assert queue.is_full() is True  # total (1 pending + 1 in-flight) == max

        queue.get_next_job()  # job-2: pending -> in-flight
        assert queue.is_empty() is True  # pending queue is now empty
        assert queue.is_full() is True  # but total (0 pending + 2 in-flight) still full

        queue.mark_completed("job-1")
        assert queue.is_empty() is True  # still no pending jobs
        assert queue.is_full() is False  # freed one slot

        queue.mark_completed("job-2")
        assert queue.is_empty() is True
        assert queue.is_full() is False

    def test_get_status_counts(self):
        """Test get_status returns accurate pending, in_flight, and utilization."""
        queue = JobQueue(max_queue_size=10)

        queue.add_job("job-1")
        queue.add_job("job-2")
        queue.add_job("job-3")

        queue.get_next_job()  # job-1 -> in-flight

        status = queue.get_status()
        assert status["pending_jobs"] == 2
        assert status["in_flight_jobs"] == 1
        assert status["total_in_flight"] == 3
        assert status["max_queue_size"] == 10
        assert status["available_slots"] == 7
        assert status["utilization"] == 0.3
        assert "job-1" in status["in_flight_job_ids"]


class TestJobQueueThreadSafety:
    """Test thread safety of concurrent operations."""

    def test_concurrent_add_and_get(self):
        """Test concurrent add and get with 10 threads, no data loss or duplication."""
        queue = JobQueue(max_queue_size=1000)
        num_threads = 10
        jobs_per_thread = 50
        total_jobs = num_threads * jobs_per_thread

        add_results: list[bool] = []
        add_lock = threading.Lock()

        def add_worker(thread_id: int) -> None:
            for i in range(jobs_per_thread):
                result = queue.add_job(f"thread-{thread_id}-job-{i}")
                with add_lock:
                    add_results.append(result)

        # Phase 1: concurrent adds
        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=add_worker, args=(t,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All adds should succeed (unique job_ids, queue large enough)
        assert len(add_results) == total_jobs
        assert all(add_results), "All add operations should succeed"

        # Phase 2: concurrent gets
        got_jobs: list[str] = []
        got_lock = threading.Lock()

        def get_worker() -> None:
            while True:
                job_id = queue.get_next_job()
                if job_id is None:
                    break
                with got_lock:
                    got_jobs.append(job_id)

        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=get_worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify: no duplicates, no loss
        assert len(got_jobs) == total_jobs, f"Expected {total_jobs} jobs, got {len(got_jobs)}"
        assert len(set(got_jobs)) == total_jobs, "No duplicate job_ids should be returned"
