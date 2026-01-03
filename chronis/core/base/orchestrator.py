"""Base orchestrator for scheduling logic."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from chronis.core.job_queue import JobQueue
from chronis.utils.logging import ContextLogger


class BaseOrchestrator(ABC):
    """
    Base class for scheduling orchestrators.

    Defines the contract for:
    1. Retrieving ready jobs (implementation-specific)
    2. Managing job queue (common)
    3. Tracking queue status (common)

    Subclasses implement job retrieval strategy:
    - PollingOrchestrator: Queries storage periodically
    - EventOrchestrator: Listens to event streams
    """

    def __init__(
        self,
        job_queue: JobQueue,
        logger: ContextLogger,
        verbose: bool = False,
    ) -> None:
        """
        Initialize base orchestrator.

        Args:
            job_queue: Job queue for managing execution
            logger: Context logger
            verbose: Enable verbose logging
        """
        self.job_queue = job_queue
        self.logger = logger
        self.verbose = verbose
        self.last_poll_time: datetime | None = None

    @abstractmethod
    def _get_ready_jobs(self, limit: int | None = None) -> list[Any]:
        """
        Get ready jobs using implementation-specific strategy.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job data dictionaries sorted by priority

        Note:
            This method must be implemented by subclasses.
            - PollingOrchestrator: Queries storage
            - EventOrchestrator: Listens to events
        """
        pass

    def enqueue_jobs(self) -> int:
        """
        Get ready jobs and enqueue them.

        This is the main public method that orchestrates the flow:
        1. Check available queue slots
        2. Get ready jobs (via _get_ready_jobs)
        3. Add jobs to queue with priority

        Returns:
            Number of jobs added to queue
        """
        try:
            available_slots = self.job_queue.get_available_slots()

            if available_slots <= 0:
                self.logger.warning(
                    "Job queue is full, skipping enqueue", queue_status=self.job_queue.get_status()
                )
                return 0

            jobs = self._get_ready_jobs(limit=available_slots)

            if jobs:
                if self.verbose or len(jobs) >= 10:
                    self.logger.info(
                        "Found ready jobs",
                        count=len(jobs),
                        queue_status=self.job_queue.get_status(),
                    )

                added_count = 0
                for job_data in jobs:
                    job_id = job_data.get("job_id")
                    if not job_id:
                        continue
                    priority = job_data.get("priority", 5)
                    if self.job_queue.add_job(job_id, priority):
                        added_count += 1
                    else:
                        self.logger.warning("Failed to add job to queue", job_id=job_id)

                return added_count

            return 0

        except Exception as e:
            self.logger.error(f"Enqueue error: {e}", exc_info=True)
            return 0

    def get_next_job_from_queue(self) -> str | None:
        """
        Get next job ID from queue for execution.

        Returns:
            Job ID or None if queue is empty
        """
        if self.job_queue.is_empty():
            return None

        return self.job_queue.get_next_job()

    def mark_job_completed(self, job_id: str) -> None:
        """
        Mark job as completed in queue.

        Args:
            job_id: Job ID
        """
        self.job_queue.mark_completed(job_id)

    def get_queue_status(self) -> dict[str, Any]:
        """
        Get current queue status.

        Returns:
            Dictionary with queue metrics
        """
        return self.job_queue.get_status()

    def is_queue_empty(self) -> bool:
        """
        Check if queue is empty.

        Returns:
            True if queue has no pending jobs
        """
        return self.job_queue.is_empty()
