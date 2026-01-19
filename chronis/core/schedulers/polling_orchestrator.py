"""Polling-based scheduler orchestrator."""

import hashlib
import uuid
from typing import Any, cast

from chronis.adapters.base import JobStorageAdapter
from chronis.core.base.orchestrator import BaseOrchestrator
from chronis.core.execution.job_queue import JobQueue
from chronis.core.misfire import MisfireClassifier
from chronis.core.query import jobs_ready_before
from chronis.utils.logging import ContextLogger
from chronis.utils.time import utc_now


class PollingOrchestrator(BaseOrchestrator):
    """
    Polling-based orchestrator for traditional time-based scheduling.

    Retrieves ready jobs by querying storage periodically.
    """

    def __init__(
        self,
        storage: JobStorageAdapter,
        job_queue: JobQueue,
        logger: ContextLogger,
        verbose: bool = False,
    ) -> None:
        """
        Initialize polling orchestrator.

        Args:
            storage: Storage adapter for querying jobs
            job_queue: Job queue for managing execution
            logger: Context logger
            verbose: Enable verbose logging
        """
        super().__init__(job_queue=job_queue, logger=logger, verbose=verbose)
        self.storage = storage
        # Random node ID for distributed job ordering
        # Each scheduler instance gets a unique ID to reduce lock contention
        self._node_id = str(uuid.uuid4())
        logger.info(f"PollingOrchestrator initialized with node_id={self._node_id[:8]}...")

    def _get_ready_jobs(self, limit: int | None = None) -> list[Any]:
        """
        Query ready jobs from storage and classify them.

        Implementation of abstract method using polling strategy.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of ready job data sorted by priority (high to low) then time (early to late)
        """
        current_time = utc_now()
        self.last_poll_time = current_time

        # Query jobs that are ready (next_run_time <= current_time)
        # Fetch more than needed for re-sorting (priority + hash ordering)
        # Buffer: 3x to ensure we have enough after priority/hash re-sorting
        query_limit = (limit * 3) if limit else None
        filters = jobs_ready_before(current_time)
        jobs = self.storage.query_jobs(filters=cast(dict[str, Any], filters), limit=query_limit)

        # Classify into normal and misfired jobs
        normal_jobs, misfired_jobs = MisfireClassifier.classify_due_jobs(
            jobs, current_time.isoformat()
        )

        if misfired_jobs:
            self.logger.warning(
                "Misfired jobs detected",
                count=len(misfired_jobs),
                job_ids=[j.get("job_id") for j in misfired_jobs],
            )

        # Combine all jobs
        all_jobs = normal_jobs + misfired_jobs

        # Sort by priority (descending), then by node-specific hash, then by next_run_time
        # The hash ensures each scheduler instance processes jobs in a different order,
        # reducing lock contention in distributed environments
        def sort_key(job: Any) -> tuple[int, int, str]:
            priority = job.get("priority", 5)
            job_id = job.get("job_id", "")
            next_run = job.get("next_run_time", "")

            # Create node-specific hash to distribute jobs across schedulers
            combined = f"{self._node_id}:{job_id}"
            hash_val = int(hashlib.md5(combined.encode()).hexdigest()[:8], 16)

            return (-int(priority), hash_val, str(next_run))

        all_jobs.sort(key=sort_key)  # type: ignore[arg-type]

        # Apply limit if specified
        if limit is not None:
            all_jobs = all_jobs[:limit]

        return all_jobs


__all__ = ["PollingOrchestrator"]
