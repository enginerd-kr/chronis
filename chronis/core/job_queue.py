"""Job queue with backpressure control."""

import threading
from queue import Empty, Queue
from typing import Any


class JobQueue:
    """
    Internal job queue with backpressure control.

    Manages job execution flow:
    1. Pending queue: Jobs waiting to be executed (FIFO)
    2. Running set: Jobs currently being executed
    3. Automatic removal on completion via callback

    Note: Jobs are already sorted by next_run_time from storage,
    so a simple FIFO queue is sufficient.
    """

    def __init__(self, max_queue_size: int = 100) -> None:
        """
        Initialize job queue.

        Args:
            max_queue_size: Maximum number of jobs in queue (pending + running)
        """
        self.max_queue_size = max_queue_size

        # Pending queue: jobs waiting to be executed (FIFO queue)
        self._pending_queue: Queue[dict[str, Any]] = Queue(maxsize=max_queue_size)

        # Running jobs: job_id -> True (thread-safe set)
        self._running_jobs: set[str] = set()
        self._lock = threading.RLock()

    def get_available_slots(self) -> int:
        """
        Get number of available slots in queue.

        Returns:
            Number of available slots (0 if full)
        """
        with self._lock:
            total_in_flight = self._pending_queue.qsize() + len(self._running_jobs)
        return max(0, self.max_queue_size - total_in_flight)

    def add_job(self, job_data: dict[str, Any]) -> bool:
        """
        Add job to pending queue.

        Args:
            job_data: Job data dictionary

        Returns:
            True if added, False if queue is full
        """
        try:
            self._pending_queue.put_nowait(job_data)
            return True
        except Exception:
            return False

    def get_next_job(self) -> dict[str, Any] | None:
        """
        Get next job from pending queue and mark as running.

        Returns:
            Job data or None if queue is empty
        """
        try:
            job_data = self._pending_queue.get_nowait()
            job_id = job_data["job_id"]

            # Mark as running
            with self._lock:
                self._running_jobs.add(job_id)

            self._pending_queue.task_done()
            return job_data

        except Empty:
            return None

    def mark_completed(self, job_id: str) -> None:
        """
        Mark job as completed and remove from running set.

        Args:
            job_id: Job ID to remove
        """
        with self._lock:
            self._running_jobs.discard(job_id)

    def get_status(self) -> dict[str, Any]:
        """
        Get queue status.

        Returns:
            Dictionary with queue statistics
        """
        with self._lock:
            running_count = len(self._running_jobs)
            running_jobs = list(self._running_jobs)

        pending_count = self._pending_queue.qsize()
        total_in_flight = pending_count + running_count

        return {
            "pending_jobs": pending_count,
            "running_jobs": running_count,
            "total_in_flight": total_in_flight,
            "max_queue_size": self.max_queue_size,
            "available_slots": self.get_available_slots(),
            "utilization": total_in_flight / self.max_queue_size if self.max_queue_size > 0 else 0,
            "running_job_ids": running_jobs,
        }

    def is_empty(self) -> bool:
        """Check if pending queue is empty."""
        return self._pending_queue.empty()

    def is_full(self) -> bool:
        """Check if queue is full."""
        return self.get_available_slots() <= 0
