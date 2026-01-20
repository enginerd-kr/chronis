"""Type definitions using TypedDict for internal data structures."""

from typing import Any, Literal, NotRequired, TypedDict

# Type aliases for string literal types
type TriggerTypeStr = Literal["interval", "cron", "date"]
type JobStatusStr = Literal["pending", "scheduled", "running", "paused", "failed"]
type MisfirePolicyStr = Literal["skip", "run_once", "run_all"]
type DayOfWeek = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
type Month = Literal[
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"
]


# Storage adapter data format
class JobStorageData(TypedDict):
    """Type definition for job data stored in storage adapters."""

    job_id: str
    name: str
    trigger_type: TriggerTypeStr
    trigger_args: dict[str, Any]
    timezone: str
    func_name: str
    args: tuple
    kwargs: dict[str, Any]
    status: JobStatusStr
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
    # Misfire configuration
    if_missed: MisfirePolicyStr
    misfire_threshold_seconds: int
    last_run_time: str | None
    last_scheduled_time: str | None


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

    status: JobStatusStr
    trigger_type: TriggerTypeStr
    next_run_time_lte: str
    next_run_time_gte: str
    # Metadata filters are dynamic: "metadata.{key}": value
    # So we can't type them strictly here


class JobUpdateData(TypedDict, total=False):
    """Type definition for job update data."""

    name: NotRequired[str]
    trigger_type: NotRequired[TriggerTypeStr]
    trigger_args: NotRequired[dict[str, Any]]
    timezone: NotRequired[str]
    status: NotRequired[JobStatusStr]
    next_run_time: NotRequired[str | None]
    next_run_time_local: NotRequired[str | None]
    last_scheduled_time: NotRequired[str | None]
    last_run_time: NotRequired[str | None]
    metadata: NotRequired[dict[str, Any]]
    updated_at: NotRequired[str]
    retry_count: NotRequired[int]
