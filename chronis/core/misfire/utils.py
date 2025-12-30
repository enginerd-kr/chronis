"""Misfire classification utilities (DB-independent)."""

from datetime import datetime, timedelta

from chronis.type_defs import JobStorageData


class MisfireClassifier:
    """
    DB-independent misfire classification logic.

    This is a pure utility class with no dependencies on storage adapters.
    Can be used anywhere for misfire detection (CLI, monitoring, etc.).
    """

    @staticmethod
    def classify_due_jobs(
        jobs: list[JobStorageData],
        current_time: str,
    ) -> tuple[list[JobStorageData], list[JobStorageData]]:
        """
        Classify jobs into normal and misfired.

        Args:
            jobs: List of due jobs (already filtered by next_run_time <= current_time)
            current_time: Current time in ISO format (UTC)

        Returns:
            (normal_jobs, misfired_jobs) tuple
        """
        current_dt = datetime.fromisoformat(current_time)
        normal = []
        misfired = []

        for job in jobs:
            if MisfireClassifier.is_misfired(job, current_dt):
                misfired.append(job)
            else:
                normal.append(job)

        return normal, misfired

    @staticmethod
    def is_misfired(
        job: JobStorageData,
        current_time: datetime,
    ) -> bool:
        """
        Check if a single job is misfired.

        A job is misfired if:
        next_run_time + threshold < current_time

        Args:
            job: Job data from storage
            current_time: Current time (timezone-aware datetime)

        Returns:
            True if misfired, False otherwise
        """
        next_run = job.get("next_run_time")
        if not next_run:
            return False

        next_run_dt = datetime.fromisoformat(next_run)
        threshold_seconds = job.get("misfire_threshold_seconds", 60)
        threshold = timedelta(seconds=threshold_seconds)

        return next_run_dt + threshold < current_time

    @staticmethod
    def get_misfire_delay(
        job: JobStorageData,
        current_time: datetime,
    ) -> timedelta | None:
        """
        Get how much a job is delayed.

        Args:
            job: Job data from storage
            current_time: Current time (timezone-aware datetime)

        Returns:
            Delay duration if misfired, None otherwise
        """
        next_run = job.get("next_run_time")
        if not next_run:
            return None

        next_run_dt = datetime.fromisoformat(next_run)
        delay = current_time - next_run_dt

        threshold_seconds = job.get("misfire_threshold_seconds", 60)
        threshold = timedelta(seconds=threshold_seconds)

        return delay if delay > threshold else None


__all__ = ["MisfireClassifier"]
