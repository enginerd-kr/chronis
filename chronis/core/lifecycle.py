"""Job lifecycle management."""

from datetime import datetime

from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobInfo
from chronis.core.state import JobStatus
from chronis.utils.time import utc_now


class JobLifecycleManager:
    """
    Manages job lifecycle including state transitions and execution eligibility.

    This is a pure domain service that contains business logic for:
    - Determining if a job can be executed
    - Determining state transitions based on execution results
    - Managing job lifecycle rules
    """

    @staticmethod
    def can_execute(job: JobInfo) -> bool:
        """
        Check if a job can be executed.

        A job can be executed if:
        1. Its state allows execution (via State pattern)
        2. Its next_run_time has passed (if applicable)

        Args:
            job: Job information

        Returns:
            True if job can be executed, False otherwise
        """
        # Check state-based execution eligibility
        if not job.can_execute():
            return False

        # Check time-based execution eligibility
        if job.next_run_time:
            return job.next_run_time <= utc_now()

        return True

    @staticmethod
    def is_ready_for_execution(job: JobInfo, current_time: datetime | None = None) -> bool:
        """
        Check if a job is ready for execution at a specific time.

        Args:
            job: Job information
            current_time: Time to check against (defaults to now)

        Returns:
            True if job is ready, False otherwise
        """
        if current_time is None:
            current_time = utc_now()

        # Must be in SCHEDULED or PENDING state
        if job.status not in (JobStatus.SCHEDULED, JobStatus.PENDING):
            return False

        # Must have next_run_time set and it must be in the past
        if not job.next_run_time:
            return False

        return job.next_run_time <= current_time

    @staticmethod
    def determine_next_status_after_execution(job: JobInfo, execution_succeeded: bool) -> JobStatus:
        """
        Determine what status a job should transition to after execution.

        Business rules:
        - One-time jobs (DATE trigger): Should be deleted (return None)
        - Recurring jobs + success: SCHEDULED
        - Any job + failure: FAILED

        Args:
            job: Job information
            execution_succeeded: Whether execution was successful

        Returns:
            Next status, or None if job should be deleted
        """
        if not execution_succeeded:
            return JobStatus.FAILED

        # One-time jobs should be deleted after successful execution
        # Returning None signals that the job should be removed
        trigger_type = TriggerType(job.trigger_type)
        if trigger_type == TriggerType.DATE:
            return None  # Signal for deletion

        # Recurring jobs go back to SCHEDULED
        return JobStatus.SCHEDULED

    @staticmethod
    def should_delete_after_execution(job: JobInfo) -> bool:
        """
        Check if a job should be deleted after execution.

        Args:
            job: Job information

        Returns:
            True if job should be deleted after execution
        """
        trigger_type = TriggerType(job.trigger_type)
        return trigger_type == TriggerType.DATE

    @staticmethod
    def can_retry(job: JobInfo) -> bool:
        """
        Check if a failed job can be retried.

        Currently all failed jobs can be retried. This can be extended
        with retry limits, backoff strategies, etc.

        Args:
            job: Job information

        Returns:
            True if job can be retried
        """
        return job.status == JobStatus.FAILED

    @staticmethod
    def validate_state_transition(current_status: JobStatus, next_status: JobStatus) -> bool:
        """
        Validate if a state transition is allowed.

        Valid transitions:
        - PENDING -> SCHEDULED, RUNNING
        - SCHEDULED -> RUNNING, FAILED
        - RUNNING -> SCHEDULED, FAILED
        - FAILED -> SCHEDULED, RUNNING

        Args:
            current_status: Current job status
            next_status: Desired next status

        Returns:
            True if transition is valid

        Raises:
            ValueError: If transition is invalid
        """
        valid_transitions = {
            JobStatus.PENDING: {JobStatus.SCHEDULED, JobStatus.RUNNING},
            JobStatus.SCHEDULED: {JobStatus.RUNNING, JobStatus.FAILED},
            JobStatus.RUNNING: {JobStatus.SCHEDULED, JobStatus.FAILED},
            JobStatus.FAILED: {JobStatus.SCHEDULED, JobStatus.RUNNING},
        }

        allowed = valid_transitions.get(current_status, set())
        if next_status not in allowed:
            raise ValueError(
                f"Invalid state transition: {current_status.value} -> {next_status.value}"
            )

        return True
