"""Job queue with backpressure control."""

import threading
from queue import Empty, Full, PriorityQueue
from typing import Any


class JobQueue:
    """
    Internal job queue with backpressure control and priority support.

    Manages job execution flow:
    1. Pending queue: Jobs waiting to be executed (Priority Queue)
    2. In-flight set: Jobs that have been dequeued for execution
    3. Automatic removal on completion via callback

    Priority Queue Behavior:
    - Jobs are sorted by priority (higher number = higher priority)
    - For same priority, FIFO order is maintained via sequence counter
    - Jobs are stored as tuples: (negative_priority, sequence, job_data)
      * negative_priority: So higher priority numbers come first
      * sequence: Auto-incrementing counter for FIFO within same priority

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

        # Pending queue: jobs waiting to be executed (Priority Queue)
        # Items are tuples: (negative_priority, sequence, job_data)
        self._pending_queue: PriorityQueue[tuple[int, int, dict[str, Any]]] = PriorityQueue(
            maxsize=max_queue_size
        )

        # Sequence counter for FIFO ordering within same priority
        self._sequence_counter = 0

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
        Add job to pending queue with priority.

        Jobs are enqueued with their priority value. Higher priority jobs
        will be dequeued first. For jobs with same priority, FIFO order
        is maintained.

        Args:
            job_data: Job data dictionary (must include 'priority' field)

        Returns:
            True if added, False if queue is full
        """
        try:
            # Get priority (default to 5 if not set)
            priority = job_data.get("priority", 5)

            # Increment sequence counter for FIFO within same priority
            with self._lock:
                sequence = self._sequence_counter
                self._sequence_counter += 1

            # Enqueue as tuple: (negative_priority, sequence, job_data)
            # Negative priority so higher numbers come first
            self._pending_queue.put_nowait((-priority, sequence, job_data))
            return True
        except Full:
            return False

    def get_next_job(self) -> dict[str, Any] | None:
        """
        Get next job from pending queue and mark as in-flight.

        Jobs are dequeued in priority order (highest priority first).
        For same priority, FIFO order is maintained.

        Returns:
            Job data or None if queue is empty
        """
        try:
            # Dequeue tuple: (negative_priority, sequence, job_data)
            _priority, _sequence, job_data = self._pending_queue.get_nowait()
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
