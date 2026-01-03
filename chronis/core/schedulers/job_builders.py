"""Job creation helper functions and builders."""

import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal

from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.state.enums import TriggerType
from chronis.utils.time import get_timezone


def generate_job_name(func: Callable | str) -> str:
    """
    Generate human-readable job name from function.

    Args:
        func: Function object or import path string

    Returns:
        Human-readable name (e.g., "Send Email", "Generate Report")
    """
    if isinstance(func, str):
        func_name = func.split(".")[-1]
    else:
        func_name = func.__name__

    # Convert snake_case to Title Case
    return func_name.replace("_", " ").title()


def generate_job_id(func: Callable | str) -> str:
    """
    Generate unique job ID.

    Format: {func_name}_{timestamp}_{random}
    Example: send_email_20251226_120530_a1b2c3d4

    Args:
        func: Function object or import path string

    Returns:
        Unique job identifier
    """
    # Extract function name
    if isinstance(func, str):
        func_name = func.split(".")[-1]
    else:
        func_name = func.__name__

    # Sanitize: only alphanumeric and underscore
    func_name = "".join(c if c.isalnum() or c == "_" else "_" for c in func_name)

    # Truncate if too long
    if len(func_name) > 30:
        func_name = func_name[:30]

    # Timestamp (sortable, UTC)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    # Random suffix (8 chars) for collision prevention
    random_suffix = secrets.token_hex(4)

    return f"{func_name}_{timestamp}_{random_suffix}"


def create_interval_job(
    create_job_func: Callable[[JobDefinition], JobInfo],
    func: Callable | str,
    job_id: str | None = None,
    name: str | None = None,
    seconds: int | None = None,
    minutes: int | None = None,
    hours: int | None = None,
    days: int | None = None,
    weeks: int | None = None,
    timezone: str = "UTC",
    args: tuple | None = None,
    kwargs: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    on_failure: OnFailureCallback | None = None,
    on_success: OnSuccessCallback | None = None,
    max_retries: int = 0,
    retry_delay_seconds: int = 60,
    timeout_seconds: int | None = None,
    priority: int = 5,
    if_missed: Literal["skip", "run_once", "run_all"] | None = None,
    misfire_threshold_seconds: int = 60,
) -> JobInfo:
    """
    Create interval job (runs repeatedly at fixed intervals).

    Args:
        create_job_func: Function to call to create the job (scheduler.create_job)
        func: Function to execute (callable or import path string)
        job_id: Unique job identifier (auto-generated if None)
        name: Human-readable job name (auto-generated if None)
        seconds: Interval in seconds
        minutes: Interval in minutes
        hours: Interval in hours
        days: Interval in days
        weeks: Interval in weeks
        timezone: IANA timezone (e.g., "Asia/Seoul", "UTC")
        args: Positional arguments for func
        kwargs: Keyword arguments for func
        metadata: User-defined metadata (optional)
        on_failure: Failure handler for this specific job (optional)
        on_success: Success handler for this specific job (optional)
        max_retries: Maximum number of retry attempts (default: 0)
        retry_delay_seconds: Base delay between retries in seconds (default: 60)
        timeout_seconds: Job execution timeout in seconds (default: None)
        priority: Job priority 0-10 (default: 5, higher = more urgent)
        if_missed: Misfire policy (default: None, uses trigger default)
        misfire_threshold_seconds: Misfire threshold in seconds (default: 60)

    Returns:
        Created job info with generated or provided job_id
    """
    # Auto-generate name if not provided
    if name is None:
        name = generate_job_name(func)

    # Auto-generate job_id if not provided
    if job_id is None:
        job_id = generate_job_id(func)

    # Build trigger_args from non-None parameters
    trigger_args = {
        k: v
        for k, v in {
            "seconds": seconds,
            "minutes": minutes,
            "hours": hours,
            "days": days,
            "weeks": weeks,
        }.items()
        if v is not None
    }

    if not trigger_args:
        raise ValueError("At least one interval parameter must be specified")

    job = JobDefinition(
        job_id=job_id,
        name=name,
        trigger_type=TriggerType.INTERVAL,
        trigger_args=trigger_args,
        func=func,
        timezone=timezone,
        args=args,
        kwargs=kwargs,
        metadata=metadata,
        on_failure=on_failure,
        on_success=on_success,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
        priority=priority,
        if_missed=if_missed,
        misfire_threshold_seconds=misfire_threshold_seconds,
    )
    return create_job_func(job)


def create_cron_job(
    create_job_func: Callable[[JobDefinition], JobInfo],
    func: Callable | str,
    job_id: str | None = None,
    name: str | None = None,
    year: int | str | None = None,
    month: int | str | None = None,
    day: int | str | None = None,
    week: int | str | None = None,
    day_of_week: int | str | None = None,
    hour: int | str | None = None,
    minute: int | str | None = None,
    timezone: str = "UTC",
    args: tuple | None = None,
    kwargs: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    on_failure: OnFailureCallback | None = None,
    on_success: OnSuccessCallback | None = None,
    max_retries: int = 0,
    retry_delay_seconds: int = 60,
    timeout_seconds: int | None = None,
    priority: int = 5,
    if_missed: Literal["skip", "run_once", "run_all"] | None = None,
    misfire_threshold_seconds: int = 60,
) -> JobInfo:
    """
    Create cron job (runs on specific date/time patterns).

    Args:
        create_job_func: Function to call to create the job (scheduler.create_job)
        func: Function to execute (callable or import path string)
        job_id: Unique job identifier (auto-generated if None)
        name: Human-readable job name (auto-generated if None)
        year: 4-digit year
        month: Month (1-12)
        day: Day of month (1-31)
        week: ISO week (1-53)
        day_of_week: Day of week (0-6 or mon,tue,wed,thu,fri,sat,sun)
        hour: Hour (0-23)
        minute: Minute (0-59)
        timezone: IANA timezone (e.g., "Asia/Seoul", "UTC")
        args: Positional arguments for func
        kwargs: Keyword arguments for func
        metadata: Additional metadata
        on_failure: Failure handler for this specific job (optional)
        on_success: Success handler for this specific job (optional)
        max_retries: Maximum number of retry attempts (default: 0)
        retry_delay_seconds: Base delay between retries in seconds (default: 60)
        timeout_seconds: Job execution timeout in seconds (default: None)
        priority: Job priority 0-10 (default: 5, higher = more urgent)
        if_missed: Misfire policy (default: None, uses trigger default)
        misfire_threshold_seconds: Misfire threshold in seconds (default: 60)

    Returns:
        Created job info with generated or provided job_id
    """
    # Auto-generate name if not provided
    if name is None:
        name = generate_job_name(func)

    # Auto-generate job_id if not provided
    if job_id is None:
        job_id = generate_job_id(func)

    # Build trigger_args from non-None parameters
    trigger_args = {
        k: v
        for k, v in {
            "year": year,
            "month": month,
            "day": day,
            "week": week,
            "day_of_week": day_of_week,
            "hour": hour,
            "minute": minute,
        }.items()
        if v is not None
    }

    if not trigger_args:
        raise ValueError("At least one cron parameter must be specified")

    job = JobDefinition(
        job_id=job_id,
        name=name,
        trigger_type=TriggerType.CRON,
        trigger_args=trigger_args,
        func=func,
        timezone=timezone,
        args=args,
        kwargs=kwargs,
        metadata=metadata,
        on_failure=on_failure,
        on_success=on_success,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
        priority=priority,
        if_missed=if_missed,
        misfire_threshold_seconds=misfire_threshold_seconds,
    )
    return create_job_func(job)


def create_date_job(
    create_job_func: Callable[[JobDefinition], JobInfo],
    func: Callable | str,
    run_date: str | datetime,
    job_id: str | None = None,
    name: str | None = None,
    timezone: str = "UTC",
    args: tuple | None = None,
    kwargs: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    on_failure: OnFailureCallback | None = None,
    on_success: OnSuccessCallback | None = None,
    max_retries: int = 0,
    retry_delay_seconds: int = 60,
    timeout_seconds: int | None = None,
    priority: int = 5,
    if_missed: Literal["skip", "run_once", "run_all"] | None = None,
    misfire_threshold_seconds: int = 60,
) -> JobInfo:
    """
    Create one-time job (runs once at specific date/time).

    Args:
        create_job_func: Function to call to create the job (scheduler.create_job)
        func: Function to execute (callable or import path string)
        run_date: ISO format datetime string or datetime object
        job_id: Unique job identifier (auto-generated if None)
        name: Human-readable job name (auto-generated if None)
        timezone: IANA timezone (e.g., "Asia/Seoul", "UTC")
        args: Positional arguments for func
        kwargs: Keyword arguments for func
        metadata: Additional metadata
        on_failure: Failure handler for this specific job (optional)
        on_success: Success handler for this specific job (optional)
        max_retries: Maximum number of retry attempts (default: 0)
        retry_delay_seconds: Base delay between retries in seconds (default: 60)
        timeout_seconds: Job execution timeout in seconds (default: None)
        priority: Job priority 0-10 (default: 5, higher = more urgent)
        if_missed: Misfire policy (default: None, uses trigger default)
        misfire_threshold_seconds: Misfire threshold in seconds (default: 60)

    Returns:
        Created job info with generated or provided job_id
    """
    # Auto-generate name if not provided
    if name is None:
        name = generate_job_name(func)

    # Auto-generate job_id if not provided
    if job_id is None:
        job_id = generate_job_id(func)

    if isinstance(run_date, datetime):
        if run_date.tzinfo is None:
            # Naive datetime - convert to aware datetime in UTC
            run_date = run_date.astimezone(get_timezone("UTC"))
        run_date = run_date.isoformat()

    job = JobDefinition(
        job_id=job_id,
        name=name,
        trigger_type=TriggerType.DATE,
        trigger_args={"run_date": run_date},
        func=func,
        timezone=timezone,
        args=args,
        kwargs=kwargs,
        metadata=metadata,
        on_failure=on_failure,
        on_success=on_success,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
        priority=priority,
        if_missed=if_missed,
        misfire_threshold_seconds=misfire_threshold_seconds,
    )
    return create_job_func(job)
