"""Job queue with backpressure control."""

import threading
from queue import Empty, Full, Queue
from typing import Any


class JobQueue:
    """
    Internal job queue with backpressure control.

    Manages job execution flow:
    1. Pending queue: Jobs waiting to be executed (FIFO)
    2. In-flight set: Jobs that have been dequeued for execution
    3. Automatic removal on completion via callback

    Note: Jobs are already sorted by next_run_time from storage,
    so a simple FIFO queue is sufficient.

    The in-flight set tracks jobs that have been dequeued but may not
    have completed execution yet. This is distinct from actual execution
    state, which is managed by ExecutionCoordinator with distributed locks.
    """

    def __init__(self, max_queue_size: int = 100) -> None:
        """
        Initialize job queue.

        Args:
            max_queue_size: Maximum number of jobs in queue (pending + in-flight)
        """
        self.max_queue_size = max_queue_size

        # Pending queue: jobs waiting to be executed (FIFO queue)
        self._pending_queue: Queue[dict[str, Any]] = Queue(maxsize=max_queue_size)

        # In-flight jobs: jobs dequeued and sent for execution (thread-safe set)
        self._in_flight_jobs: set[str] = set()
        self._lock = threading.RLock()

    def get_available_slots(self) -> int:
        """
        Get number of available slots in queue.

        Returns:
            Number of available slots (0 if full)
        """
        with self._lock:
            total_in_flight = self._pending_queue.qsize() + len(self._in_flight_jobs)
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
        except Full:
            return False

    def get_next_job(self) -> dict[str, Any] | None:
        """
        Get next job from pending queue and mark as in-flight.

        Returns:
            Job data or None if queue is empty
        """
        try:
            job_data = self._pending_queue.get_nowait()
            job_id = job_data["job_id"]

            # Mark as in-flight
            with self._lock:
                self._in_flight_jobs.add(job_id)

            return job_data

        except Empty:
            return None

    def mark_completed(self, job_id: str) -> None:
        """
        Mark job as completed and remove from in-flight set.

        Args:
            job_id: Job ID to remove
        """
        with self._lock:
            self._in_flight_jobs.discard(job_id)

    def get_status(self) -> dict[str, Any]:
        """
        Get queue status.

        Returns:
            Dictionary with queue statistics
        """
        with self._lock:
            in_flight_count = len(self._in_flight_jobs)
            in_flight_job_ids = list(self._in_flight_jobs)

        pending_count = self._pending_queue.qsize()
        total_in_flight = pending_count + in_flight_count

        return {
            "pending_jobs": pending_count,
            "in_flight_jobs": in_flight_count,
            "total_in_flight": total_in_flight,
            "max_queue_size": self.max_queue_size,
            "available_slots": self.get_available_slots(),
            "utilization": total_in_flight / self.max_queue_size if self.max_queue_size > 0 else 0,
            "in_flight_job_ids": in_flight_job_ids,
        }

    def is_empty(self) -> bool:
        """Check if pending queue is empty."""
        return self._pending_queue.empty()

    def is_full(self) -> bool:
        """Check if queue is full."""
        return self.get_available_slots() <= 0
