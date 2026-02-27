"""Misfire detection and handling.

This module provides unified misfire detection and handling functionality.
All misfire-related logic is consolidated here for simplicity.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from chronis.type_defs import JobStorageData


class MisfirePolicy(str, Enum):
    """
    Misfire policy enumeration.

    Policies:
    - SKIP: Skip missed executions, move to next scheduled time
    - RUN_ONCE: Execute once immediately, then resume schedule
    - RUN_ALL: Execute all missed runs (up to limit)

    Default policies by trigger type:
    - DATE: RUN_ONCE (one-time jobs should run if missed)
    - CRON: SKIP (cron jobs skip missed executions)
    - INTERVAL: RUN_ONCE (interval jobs catch up once)
    """

    SKIP = "skip"
    RUN_ONCE = "run_once"
    RUN_ALL = "run_all"

    @staticmethod
    def get_default_for_trigger(trigger_type: str) -> MisfirePolicy:
        """
        Get default misfire policy for trigger type.

        Args:
            trigger_type: Trigger type (date, cron, interval)

        Returns:
            Default policy as MisfirePolicy enum
        """
        if trigger_type == "date":
            return MisfirePolicy.RUN_ONCE
        elif trigger_type == "cron":
            return MisfirePolicy.SKIP
        elif trigger_type == "interval":
            return MisfirePolicy.RUN_ONCE
        return MisfirePolicy.RUN_ONCE


class MisfireDetector:
    """
    Misfire detection utilities.

    Provides DB-independent misfire classification logic.
    Can be used anywhere for misfire detection (CLI, monitoring, etc.).
    """

    @staticmethod
    def is_misfired(job: JobStorageData, current_time: datetime) -> bool:
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
            if MisfireDetector.is_misfired(job, current_dt):
                misfired.append(job)
            else:
                normal.append(job)

        return normal, misfired

    @staticmethod
    def get_misfire_delay(job: JobStorageData, current_time: datetime) -> timedelta | None:
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


__all__ = [
    "MisfirePolicy",
    "MisfireDetector",
]
