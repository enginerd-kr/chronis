"""Misfire handler for policy-based execution."""

from datetime import datetime

from chronis.core.jobs.definition import JobInfo
from chronis.core.misfire import SimpleMisfirePolicy
from chronis.core.scheduling import NextRunTimeCalculator


class MisfireHandler:
    """
    Handle misfired jobs according to policy.

    This class determines what execution times should be used
    when a job has misfired, based on its misfire policy.
    """

    # Safety limit to prevent infinite loops
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
        # Get policy from metadata (stored as string)
        policy_str = job.metadata.get("if_missed", "run_once")
        policy = SimpleMisfirePolicy(policy_str)

        if policy == SimpleMisfirePolicy.SKIP:
            return []  # Skip this execution

        elif policy == SimpleMisfirePolicy.RUN_ONCE:
            return [current_time]  # Execute once immediately

        elif policy == SimpleMisfirePolicy.RUN_ALL:
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
        missed_runs = []
        check_time = scheduled_time

        while check_time < current_time and len(missed_runs) < self.MAX_MISSED_RUNS:
            missed_runs.append(check_time)

            # Calculate next run time based on trigger
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


__all__ = ["MisfireHandler"]
