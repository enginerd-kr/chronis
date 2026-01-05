"""Job query helper functions."""

from datetime import datetime

from chronis.core.state import JobStatus
from chronis.type_defs import JobQueryFilter, TriggerTypeStr


def scheduled_jobs() -> JobQueryFilter:
    """
    Get filter for scheduled jobs.

    Returns:
        Filter dictionary for scheduled jobs
    """
    return {"status": JobStatus.SCHEDULED.value}


def jobs_ready_before(time: datetime) -> JobQueryFilter:
    """
    Get filter for jobs ready to run before specified time.

    This is the primary query used by the scheduler's polling mechanism.

    Args:
        time: Time threshold

    Returns:
        Filter for scheduled jobs with next_run_time <= time
    """
    return {
        "status": JobStatus.SCHEDULED.value,
        "next_run_time_lte": time.isoformat(),
    }


def jobs_by_status(status: JobStatus) -> JobQueryFilter:
    """
    Get filter for jobs by status.

    Args:
        status: Job status to filter by

    Returns:
        Filter dictionary
    """
    return {"status": status.value}


def jobs_by_trigger_type(trigger_type: TriggerTypeStr) -> JobQueryFilter:
    """
    Get filter for jobs by trigger type.

    Args:
        trigger_type: Trigger type (interval, cron, date)

    Returns:
        Filter dictionary
    """
    return {"trigger_type": trigger_type}


def jobs_by_metadata(key: str, value: str) -> dict[str, str]:
    """
    Get filter for jobs by metadata field.

    Args:
        key: Metadata key
        value: Metadata value

    Returns:
        Filter dictionary with metadata key
    """
    return {f"metadata.{key}": value}


def jobs_before_time(time: datetime) -> JobQueryFilter:
    """
    Get filter for jobs with next_run_time <= specified time.

    Args:
        time: Time threshold

    Returns:
        Filter dictionary
    """
    return {"next_run_time_lte": time.isoformat()}


def jobs_after_time(time: datetime) -> JobQueryFilter:
    """
    Get filter for jobs with next_run_time >= specified time.

    Args:
        time: Time threshold

    Returns:
        Filter dictionary
    """
    return {"next_run_time_gte": time.isoformat()}


def combine_filters(*filters: dict[str, str]) -> dict[str, str]:
    """
    Combine multiple filters into one.

    Args:
        *filters: Filter dictionaries to combine

    Returns:
        Combined filter dictionary
    """
    result = {}
    for f in filters:
        result.update(f)
    return result
