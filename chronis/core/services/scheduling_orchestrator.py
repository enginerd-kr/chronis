"""Scheduling orchestration service."""

from datetime import datetime
from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.core.job_queue import JobQueue
from chronis.core.misfire.utils import MisfireClassifier
from chronis.core.query import jobs_ready_before
from chronis.utils.logging import ContextLogger
from chronis.utils.time import utc_now


class SchedulingOrchestrator:
    """
    Orchestrates job polling and queue management.

    This application service is responsible for:
    - Polling storage for ready jobs
    - Adding jobs to the execution queue
    - Managing queue backpressure
    - Coordinating between storage and queue

    This service runs periodically (via APScheduler) and ensures jobs
    are discovered and queued for execution at the right time.
    """

    def __init__(
        self,
        storage: JobStorageAdapter,
        job_queue: JobQueue,
        logger: ContextLogger,
        verbose: bool = False,
    ) -> None:
        """
        Initialize scheduling orchestrator.

        Args:
            storage: Storage adapter for querying jobs
            job_queue: Job queue for managing execution
            logger: Context logger
            verbose: Enable verbose logging
        """
        self.storage = storage
        self.job_queue = job_queue
        self.logger = logger
        self.verbose = verbose
        self.last_poll_time: datetime | None = None

    def poll_and_enqueue(self) -> int:
        """
        Poll storage for ready jobs and add them to queue.

        This is the main polling loop that:
        1. Checks queue capacity
        2. Queries storage for ready jobs
        3. Adds jobs to queue
        4. Tracks last poll time

        Returns:
            Number of jobs added to queue

        Raises:
            No exceptions - all errors are logged
        """
        try:
            # Get available slots in queue
            available_slots = self.job_queue.get_available_slots()

            if available_slots <= 0:
                self.logger.warning(
                    "Job queue is full, skipping poll", queue_status=self.job_queue.get_status()
                )
                return 0

            # Query ready jobs from storage
            current_time = utc_now()
            jobs = self._query_ready_jobs(current_time, limit=available_slots)

            if jobs:
                # Log only in verbose mode or when many jobs found
                if self.verbose or len(jobs) >= 10:
                    self.logger.info(
                        "Found ready jobs",
                        count=len(jobs),
                        queue_status=self.job_queue.get_status(),
                    )

                # Add jobs to queue
                added_count = 0
                for job_data in jobs:
                    if self.job_queue.add_job(job_data):
                        added_count += 1
                    else:
                        self.logger.warning(
                            "Failed to add job to queue", job_id=job_data.get("job_id")
                        )

                self.last_poll_time = current_time
                return added_count

            self.last_poll_time = current_time
            return 0

        except Exception as e:
            self.logger.error(f"Polling error: {e}", exc_info=True)
            return 0

    def get_next_job_from_queue(self) -> dict[str, Any] | None:
        """
        Get next job from queue for execution.

        Returns:
            Job data or None if queue is empty
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

    def _query_ready_jobs(self, current_time: datetime, limit: int | None = None) -> list[Any]:
        """
        Query ready jobs from storage and classify them (normal vs misfired).

        Uses the jobs_ready_before helper to find jobs that
        are scheduled and have next_run_time <= current_time.

        Jobs are classified using MisfireClassifier, then both normal
        and misfired jobs are sorted by priority and returned together.

        Args:
            current_time: Current time
            limit: Maximum number of jobs to return

        Returns:
            List of ready job data sorted by priority (high to low) then time (early to late)
        """
        from typing import cast

        # Get filter for ready jobs
        filters = jobs_ready_before(current_time)

        jobs = self.storage.query_jobs(filters=cast(dict[str, Any], filters), limit=None)

        # Classify into normal and misfired
        normal_jobs, misfired_jobs = MisfireClassifier.classify_due_jobs(
            jobs, current_time.isoformat()
        )

        # Log misfired jobs
        if misfired_jobs:
            self.logger.warning(
                "Misfired jobs detected",
                count=len(misfired_jobs),
                job_ids=[j.get("job_id") for j in misfired_jobs],
            )

        # Combine normal and misfired jobs (both need to be processed)
        all_jobs = normal_jobs + misfired_jobs

        # Sort by priority (descending) then next_run_time (ascending)
        # Higher priority = higher number = executed first
        def sort_key(job: Any) -> tuple[int, str]:
            priority = job.get("priority", 5)
            next_run = job.get("next_run_time", "")
            return (-int(priority), str(next_run))

        all_jobs.sort(key=sort_key)

        # Apply limit after sorting
        if limit is not None:
            all_jobs = all_jobs[:limit]

        return all_jobs
