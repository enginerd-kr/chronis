"""Fluent Builder API for simplified job creation."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
    from chronis.core.jobs.definition import JobInfo
    from chronis.core.schedulers.polling_scheduler import PollingScheduler


class FluentJobBuilder:
    """
    Unified fluent builder for all job types.

    Supports flexible ordering of trigger and config methods.
    The run() method must be called last to create the job.

    Three trigger methods:
        - every(): Interval-based scheduling (run repeatedly)
        - on(): Cron-based scheduling (run at specific times)
        - once(): One-time scheduling (run once)

    Examples:
        # Interval: every 5 minutes
        scheduler.every(minutes=5).run("sync")

        # Cron: every hour at minute 5
        scheduler.on(minute=5).run("task")

        # Cron: daily at 9:30
        scheduler.on(hour=9, minute=30).run("report")

        # Cron: every Monday at 9:00
        scheduler.on(day_of_week="mon", hour=9).run("weekly")

        # One-time
        scheduler.once(when="2025-01-20T10:00:00").run("notify")

        # With config
        scheduler.every(minutes=5).config(retry=3).run("sync")
        scheduler.config(retry=3).every(minutes=5).run("sync")
    """

    def __init__(self, scheduler: PollingScheduler) -> None:
        self._scheduler = scheduler

        # Trigger settings
        self._trigger_type: str | None = None
        self._trigger_params: dict[str, Any] = {}

        # Config settings
        self._job_id: str | None = None
        self._name: str | None = None
        self._timezone: str = "UTC"
        self._max_retries: int = 0
        self._retry_delay_seconds: int = 60
        self._timeout_seconds: int | None = None
        self._priority: int = 5
        self._metadata: dict[str, Any] | None = None
        self._on_failure: OnFailureCallback | None = None
        self._on_success: OnSuccessCallback | None = None
        self._if_missed: Literal["skip", "run_once", "run_all"] | None = None
        self._misfire_threshold_seconds: int = 60

    # ========================================
    # Trigger Methods (3 APIs)
    # ========================================

    def every(
        self,
        seconds: int | None = None,
        minutes: int | None = None,
        hours: int | None = None,
        days: int | None = None,
        weeks: int | None = None,
    ) -> FluentJobBuilder:
        """
        Set interval trigger - run repeatedly at fixed intervals.

        Args:
            seconds: Run every N seconds
            minutes: Run every N minutes
            hours: Run every N hours
            days: Run every N days
            weeks: Run every N weeks

        Returns:
            Self for method chaining

        Examples:
            scheduler.every(seconds=30).run("task")
            scheduler.every(minutes=5).run("sync")
            scheduler.every(hours=1, minutes=30).run("backup")
        """
        self._trigger_type = "interval"
        self._trigger_params = {
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

        if not self._trigger_params:
            raise ValueError("At least one interval parameter must be specified")

        return self

    def on(
        self,
        *,
        minute: int | None = None,
        hour: int | None = None,
        day: int | None = None,
        day_of_week: int | str | None = None,
        month: int | str | None = None,
        year: int | None = None,
        week: int | None = None,
    ) -> FluentJobBuilder:
        """
        Set cron trigger - run at specific times.

        Args:
            minute: Minute (0-59)
            hour: Hour (0-23)
            day: Day of month (1-31)
            day_of_week: Day of week (0-6 for Mon-Sun, or "mon", "tue", etc.)
            month: Month (1-12)
            year: 4-digit year
            week: ISO week (1-53)

        Returns:
            Self for method chaining

        Examples:
            # Every hour at minute 5
            scheduler.on(minute=5).run("task")

            # Daily at 9:30
            scheduler.on(hour=9, minute=30).run("report")

            # Every Monday at 9:00
            scheduler.on(day_of_week="mon", hour=9, minute=0).run("weekly")

            # Every month on the 1st at midnight
            scheduler.on(day=1, hour=0, minute=0).run("monthly")
        """
        self._trigger_type = "cron"
        self._trigger_params = {
            k: v
            for k, v in {
                "minute": minute,
                "hour": hour,
                "day": day,
                "day_of_week": day_of_week,
                "month": month,
                "year": year,
                "week": week,
            }.items()
            if v is not None
        }

        if not self._trigger_params:
            raise ValueError("At least one cron parameter must be specified")

        return self

    def once(self, when: str | datetime) -> FluentJobBuilder:
        """
        Set one-time date trigger - run once at specified time.

        Args:
            when: ISO format datetime string or datetime object

        Returns:
            Self for method chaining

        Examples:
            scheduler.once(when="2025-01-20T10:00:00").run("notify")
            scheduler.once(when=datetime(2025, 1, 20, 10, 0)).run("task")
        """
        self._trigger_type = "date"
        self._trigger_params = {"run_date": when}
        return self

    # ========================================
    # Config Method
    # ========================================

    def config(
        self,
        *,
        job_id: str | None = None,
        name: str | None = None,
        timezone: str | None = None,
        retry: int | None = None,
        retry_delay: int | None = None,
        timeout: int | None = None,
        priority: int | None = None,
        metadata: dict[str, Any] | None = None,
        on_success: OnSuccessCallback | None = None,
        on_failure: OnFailureCallback | None = None,
        if_missed: Literal["skip", "run_once", "run_all"] | None = None,
        misfire_threshold: int | None = None,
    ) -> FluentJobBuilder:
        """
        Configure job execution options.

        Can be called before or after trigger methods.

        Args:
            job_id: Custom job ID
            name: Custom job name
            timezone: Timezone for scheduling
            retry: Maximum retry attempts
            retry_delay: Delay between retries in seconds
            timeout: Execution timeout in seconds
            priority: Job priority (0-10, higher = more urgent)
            metadata: Custom metadata dict
            on_success: Success callback
            on_failure: Failure callback
            if_missed: Misfire policy ("skip", "run_once", "run_all")
            misfire_threshold: Misfire threshold in seconds

        Returns:
            Self for method chaining
        """
        if job_id is not None:
            self._job_id = job_id
        if name is not None:
            self._name = name
        if timezone is not None:
            self._timezone = timezone
        if retry is not None:
            self._max_retries = retry
        if retry_delay is not None:
            self._retry_delay_seconds = retry_delay
        if timeout is not None:
            self._timeout_seconds = timeout
        if priority is not None:
            self._priority = priority
        if metadata is not None:
            if self._metadata is None:
                self._metadata = {}
            self._metadata.update(metadata)
        if on_success is not None:
            self._on_success = on_success
        if on_failure is not None:
            self._on_failure = on_failure
        if if_missed is not None:
            self._if_missed = if_missed
        if misfire_threshold is not None:
            self._misfire_threshold_seconds = misfire_threshold

        return self

    # ========================================
    # Run Method
    # ========================================

    def run(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> JobInfo:
        """
        Create and register the job.

        This method must be called last.

        Args:
            func: Function to execute (callable or import path string)
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            JobInfo for the created job

        Raises:
            ValueError: If no trigger is configured
        """
        if self._trigger_type is None:
            raise ValueError("No trigger configured. Call every(), on(), or once() before run()")

        common = {
            "func": func,
            "job_id": self._job_id,
            "name": self._name,
            "timezone": self._timezone,
            "args": args if args else None,
            "kwargs": kwargs if kwargs else None,
            "metadata": self._metadata,
            "on_failure": self._on_failure,
            "on_success": self._on_success,
            "max_retries": self._max_retries,
            "retry_delay_seconds": self._retry_delay_seconds,
            "timeout_seconds": self._timeout_seconds,
            "priority": self._priority,
            "if_missed": self._if_missed,
            "misfire_threshold_seconds": self._misfire_threshold_seconds,
        }

        if self._trigger_type == "interval":
            return self._scheduler.create_interval_job(**common, **self._trigger_params)  # type: ignore[arg-type]
        elif self._trigger_type == "cron":
            return self._scheduler.create_cron_job(**common, **self._trigger_params)  # type: ignore[arg-type]
        elif self._trigger_type == "date":
            return self._scheduler.create_date_job(**common, **self._trigger_params)  # type: ignore[arg-type]
        else:
            raise ValueError(f"Unknown trigger type: {self._trigger_type}")
