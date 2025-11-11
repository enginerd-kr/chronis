"""Job definition and information classes."""

from collections.abc import Callable
from datetime import datetime
from typing import Any

from chronis.core.common.types import TriggerType
from chronis.core.state import JobStatus, StateFactory
from chronis.utils.time import get_timezone, utc_now


class JobDefinition:
    """Job definition class with timezone and retry support."""

    def __init__(
        self,
        job_id: str,
        name: str,
        trigger_type: TriggerType,
        trigger_args: dict[str, Any],
        func: Callable | str,
        timezone: str = "UTC",
        args: tuple | None = None,
        kwargs: dict[str, Any] | None = None,
        status: JobStatus = JobStatus.PENDING,
        next_run_time: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize job definition.

        Args:
            job_id: Unique job ID
            name: Job name
            trigger_type: Trigger type (interval/cron/date)
            trigger_args: Trigger parameters
                - interval: {"seconds": 5, "minutes": 0, "hours": 0}
                - cron: {"minute": "0", "hour": "9", "day_of_week": "0-4"}
                - date: {"run_date": "2025-11-05T14:30:00Z"}
            func: Function to execute or function name (string)
            timezone: IANA timezone (e.g., "Asia/Seoul", "America/New_York", "UTC")
            args: Function positional arguments
            kwargs: Function keyword arguments
            status: Job status (default: PENDING)
            next_run_time: Next execution time (auto-calculated if None)
            metadata: Additional metadata
        """
        self.job_id = job_id
        self.name = name
        self.trigger_type = trigger_type
        self.trigger_args = trigger_args
        self.func = func
        self.timezone = timezone
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.status = status
        self.next_run_time = next_run_time
        self.metadata = metadata or {}

        # Validate timezone
        self._validate_timezone()

    def _validate_timezone(self) -> None:
        """Validate timezone."""
        try:
            get_timezone(self.timezone)
        except Exception as e:
            raise ValueError(f"Invalid timezone '{self.timezone}': {e}") from e

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary for storage (with timezone and state).

        Returns:
            Dictionary representation
        """
        # Calculate next_run_time (UTC)
        if self.next_run_time is None:
            next_run_time_utc = self._calculate_initial_next_run_time()
        else:
            next_run_time_utc = self.next_run_time

        # Calculate local time (for user display)
        next_run_time_local = None
        if next_run_time_utc:
            tz = get_timezone(self.timezone)
            next_run_time_local = next_run_time_utc.astimezone(tz)

        # Convert func to string if callable
        if isinstance(self.func, str):
            func_name = self.func
        else:
            func_name = f"{self.func.__module__}.{self.func.__name__}"

        # Set initial status to SCHEDULED if next_run_time exists
        initial_status = JobStatus.SCHEDULED if next_run_time_utc else self.status

        return {
            "job_id": self.job_id,
            "name": self.name,
            "trigger_type": self.trigger_type.value,
            "trigger_args": self.trigger_args,
            "timezone": self.timezone,
            "func_name": func_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "status": initial_status.value,
            # UTC time (for internal processing)
            "next_run_time": (next_run_time_utc.isoformat() if next_run_time_utc else None),
            # Local time (for user display)
            "next_run_time_local": (
                next_run_time_local.isoformat() if next_run_time_local else None
            ),
            "metadata": self.metadata,
            "created_at": utc_now().isoformat(),
            "updated_at": utc_now().isoformat(),
        }

    def _calculate_initial_next_run_time(self) -> datetime | None:
        """
        Calculate initial next_run_time (with timezone consideration).

        Returns:
            Next run time in UTC, or None
        """
        from chronis.core.triggers import TriggerFactory

        # Current time (timezone aware)
        tz = get_timezone(self.timezone)
        current_time = datetime.now(tz)

        # Get appropriate strategy and calculate next run time
        strategy = TriggerFactory.get_strategy(self.trigger_type.value)
        return strategy.calculate_next_run_time(self.trigger_args, self.timezone, current_time)


class JobInfo:
    """Job information query result (with timezone and state support)."""

    def __init__(self, data: dict[str, Any]) -> None:
        self.job_id: str = data["job_id"]
        self.name: str = data["name"]
        self.trigger_type: str = data["trigger_type"]
        self.trigger_args: dict[str, Any] = data["trigger_args"]
        self.timezone: str = data.get("timezone", "UTC")

        # Status
        self.status: JobStatus = JobStatus(data["status"])

        # UTC time
        self.next_run_time: datetime | None = (
            datetime.fromisoformat(data["next_run_time"]) if data.get("next_run_time") else None
        )

        # Local time (if available)
        self.next_run_time_local: datetime | None = (
            datetime.fromisoformat(data["next_run_time_local"])
            if data.get("next_run_time_local")
            else None
        )

        self.metadata: dict[str, Any] = data.get("metadata", {})
        self.created_at: datetime = datetime.fromisoformat(data["created_at"])
        self.updated_at: datetime = datetime.fromisoformat(data["updated_at"])

    def get_state(self):
        """Get the state object for this job."""
        return StateFactory.get_state(self.status)

    def can_execute(self) -> bool:
        """Check if this job can be executed."""
        return self.get_state().can_execute()

    def get_next_run_time(self, timezone: str | None = None) -> datetime | None:
        """
        Get next_run_time in desired timezone.

        Args:
            timezone: Timezone to convert to (None = job's timezone)

        Returns:
            Next run time with timezone applied
        """
        if not self.next_run_time:
            return None

        target_tz = get_timezone(timezone or self.timezone)
        return self.next_run_time.astimezone(target_tz)
