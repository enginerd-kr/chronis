"""Misfire detection and handling.

This module provides unified misfire detection and handling functionality.
All misfire-related logic is consolidated here for simplicity.
"""

from datetime import datetime, timedelta
from enum import Enum

from chronis.core.jobs.definition import JobInfo
from chronis.core.scheduling import NextRunTimeCalculator
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
    def get_default_for_trigger(trigger_type: str) -> "MisfirePolicy":
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


class MisfireHandler:
    """Handle misfired jobs according to policy."""

    MAX_MISSED_RUNS = 100

    def handle(
        self,
        job: JobInfo,
        scheduled_time: datetime,
        current_time: datetime,
    ) -> list[datetime]:
        """
        Determine execution times for misfired job.

        Args:
            job: Job information
            scheduled_time: Original scheduled time that was missed
            current_time: Current time when misfire was detected

        Returns:
            List of times to execute the job (empty list = skip execution)
        """
        policy_str = job.metadata.get("if_missed", "run_once")

        if policy_str == MisfirePolicy.SKIP.value:
            return []

        elif policy_str == MisfirePolicy.RUN_ONCE.value:
            return [current_time]

        elif policy_str == MisfirePolicy.RUN_ALL.value:
            return self._get_all_missed_runs(job, scheduled_time, current_time)

        return []

    def _get_all_missed_runs(
        self,
        job: JobInfo,
        scheduled_time: datetime,
        current_time: datetime,
    ) -> list[datetime]:
        """
        Calculate all missed execution times.

        Args:
            job: Job information
            scheduled_time: Original scheduled time that was missed
            current_time: Current time

        Returns:
            List of all missed execution times (up to MAX_MISSED_RUNS)
        """
        missed_runs: list[datetime] = []
        check_time = scheduled_time

        while check_time < current_time and len(missed_runs) < self.MAX_MISSED_RUNS:
            missed_runs.append(check_time)

            next_time = NextRunTimeCalculator.calculate(
                job.trigger_type,
                job.trigger_args,
                job.timezone,
                check_time,
            )

            if next_time is None:
                break

            check_time = next_time

        return missed_runs


# Aliases for backward compatibility
MisfireClassifier = MisfireDetector
SimpleMisfirePolicy = MisfirePolicy


def get_default_policy(trigger_type):
    """
    Get default misfire policy for a trigger type.

    Args:
        trigger_type: Trigger type (TriggerType enum or string)

    Returns:
        MisfirePolicy enum
    """
    from chronis.core.common.types import TriggerType

    if isinstance(trigger_type, TriggerType):
        trigger_type_str = trigger_type.value
    else:
        trigger_type_str = str(trigger_type)

    return MisfirePolicy.get_default_for_trigger(trigger_type_str)


__all__ = [
    "MisfirePolicy",
    "SimpleMisfirePolicy",  # Backward compatibility
    "MisfireDetector",
    "MisfireClassifier",  # Backward compatibility
    "MisfireHandler",
    "get_default_policy",
]
