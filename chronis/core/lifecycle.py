"""Job lifecycle management."""

from datetime import datetime

from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobInfo
from chronis.core.state import JobStatus
from chronis.utils.time import utc_now


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
    if not job.can_execute():
        return False

    if job.next_run_time:
        return job.next_run_time <= utc_now()

    return True


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

    if job.status not in (JobStatus.SCHEDULED, JobStatus.PENDING):
        return False

    if not job.next_run_time:
        return False

    return job.next_run_time <= current_time


def determine_next_status_after_execution(
    trigger_type: str | TriggerType, execution_succeeded: bool
) -> JobStatus | None:
    """
    Determine what status a job should transition to after execution.

    Business rules:
    - One-time jobs (DATE trigger): Should be deleted (return None)
    - Recurring jobs + success: SCHEDULED
    - Any job + failure: FAILED

    Args:
        trigger_type: Job trigger type (string or enum)
        execution_succeeded: Whether execution was successful

    Returns:
        Next status, or None if job should be deleted
    """
    if not execution_succeeded:
        return JobStatus.FAILED

    if isinstance(trigger_type, str):
        trigger_type = TriggerType(trigger_type)

    if trigger_type == TriggerType.DATE:
        return None

    return JobStatus.SCHEDULED
