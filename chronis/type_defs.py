"""Type definitions using TypedDict for internal data structures."""

from typing import Any, NotRequired, TypedDict


# Storage adapter data format
class JobStorageData(TypedDict):
    """Type definition for job data stored in storage adapters."""

    job_id: str
    name: str
    trigger_type: str
    trigger_args: dict[str, Any]
    timezone: str
    func_name: str
    args: tuple
    kwargs: dict[str, Any]
    status: str
    next_run_time: str | None
    next_run_time_local: str | None
    metadata: dict[str, Any]
    created_at: str
    updated_at: str
    # Retry configuration
    max_retries: int
    retry_delay_seconds: int
    retry_count: int
    # Timeout configuration
    timeout_seconds: int | None
    # Priority configuration
    priority: int


# Trigger args by type
class IntervalTriggerArgs(TypedDict, total=False):
    """Type definition for interval trigger arguments."""

    seconds: int
    minutes: int
    hours: int
    days: int
    weeks: int


class CronTriggerArgs(TypedDict, total=False):
    """Type definition for cron trigger arguments."""

    year: str
    minute: str
    hour: str
    day: str
    month: str
    week: str
    day_of_week: str


class DateTriggerArgs(TypedDict):
    """Type definition for date trigger arguments."""

    run_date: str


# Query filter dictionaries
class JobQueryFilter(TypedDict, total=False):
    """Type definition for job query filters."""

    status: str
    trigger_type: str
    next_run_time_lte: str
    next_run_time_gte: str
    # Metadata filters are dynamic: "metadata.{key}": value
    # So we can't type them strictly here


class JobUpdateData(TypedDict, total=False):
    """Type definition for job update data."""

    name: NotRequired[str]
    trigger_type: NotRequired[str]
    trigger_args: NotRequired[dict[str, Any]]
    timezone: NotRequired[str]
    status: NotRequired[str]
    next_run_time: NotRequired[str | None]
    next_run_time_local: NotRequired[str | None]
    metadata: NotRequired[dict[str, Any]]
    updated_at: NotRequired[str]
    retry_count: NotRequired[int]
