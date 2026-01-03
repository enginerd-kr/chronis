"""Job definition and information classes."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from chronis.core.state import JobStatus
from chronis.core.state.enums import TriggerType
from chronis.utils.time import get_timezone, utc_now

if TYPE_CHECKING:
    from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
else:
    OnFailureCallback = Any
    OnSuccessCallback = Any


class JobDefinition(BaseModel):
    """Job definition class with runtime validation."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    job_id: str
    name: str
    trigger_type: TriggerType
    trigger_args: dict[str, Any]
    func: Callable | str
    timezone: str = "UTC"
    args: tuple | None = None
    kwargs: dict[str, Any] | None = None
    status: JobStatus = JobStatus.PENDING
    next_run_time: datetime | None = None
    metadata: dict[str, Any] | None = None
    on_failure: "OnFailureCallback | None" = None
    on_success: "OnSuccessCallback | None" = None
    max_retries: int = 0
    retry_delay_seconds: int = 60
    timeout_seconds: int | None = None
    priority: int = 5
    if_missed: Literal["skip", "run_once", "run_all"] | None = None
    misfire_threshold_seconds: int = 60

    def model_post_init(self, __context: Any) -> None:
        """Normalize None values to defaults after validation."""
        if self.args is None:
            object.__setattr__(self, "args", ())
        if self.kwargs is None:
            object.__setattr__(self, "kwargs", {})
        if self.metadata is None:
            object.__setattr__(self, "metadata", {})

        if self.if_missed is None:
            from chronis.core.misfire import get_default_policy

            default_policy = get_default_policy(self.trigger_type)
            object.__setattr__(self, "if_missed", default_policy.value)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate timezone."""
        try:
            get_timezone(v)
            return v
        except Exception as e:
            raise ValueError(f"Invalid timezone '{v}': {e}") from e

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: int) -> int:
        """Validate priority is in range 0-10."""
        if not 0 <= v <= 10:
            raise ValueError(f"Priority must be between 0 and 10, got {v}")
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        if self.next_run_time is None:
            next_run_time_utc = self._calculate_initial_next_run_time()
        else:
            next_run_time_utc = self.next_run_time

        next_run_time_local = None
        if next_run_time_utc:
            tz = get_timezone(self.timezone)
            next_run_time_local = next_run_time_utc.astimezone(tz)

        if isinstance(self.func, str):
            func_name = self.func
        else:
            func_name = f"{self.func.__module__}.{self.func.__name__}"

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
            "next_run_time": (next_run_time_utc.isoformat() if next_run_time_utc else None),
            "next_run_time_local": (
                next_run_time_local.isoformat() if next_run_time_local else None
            ),
            "metadata": self.metadata,
            "max_retries": self.max_retries,
            "retry_delay_seconds": self.retry_delay_seconds,
            "retry_count": 0,
            "timeout_seconds": self.timeout_seconds,
            "priority": self.priority,
            "if_missed": self.if_missed,
            "misfire_threshold_seconds": self.misfire_threshold_seconds,
            "last_run_time": None,
            "last_scheduled_time": None,
            "created_at": utc_now().isoformat(),
            "updated_at": utc_now().isoformat(),
        }

    def _calculate_initial_next_run_time(self) -> datetime | None:
        """Calculate initial next_run_time in UTC."""
        from chronis.core.schedulers.next_run_calculator import NextRunTimeCalculator

        tz = get_timezone(self.timezone)
        current_time = datetime.now(tz)

        return NextRunTimeCalculator.calculate(
            self.trigger_type, self.trigger_args, self.timezone, current_time
        )


@dataclass(frozen=True)
class JobInfo:
    """Immutable job information query result."""

    job_id: str
    name: str
    trigger_type: str
    trigger_args: dict[str, Any]
    timezone: str
    status: JobStatus
    next_run_time: datetime | None
    next_run_time_local: datetime | None
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    # Retry information
    max_retries: int = 0
    retry_delay_seconds: int = 60
    retry_count: int = 0
    # Timeout information
    timeout_seconds: int | None = None
    # Priority information
    priority: int = 5

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobInfo":
        """Create JobInfo from storage dictionary."""
        return cls(
            job_id=data["job_id"],
            name=data["name"],
            trigger_type=data["trigger_type"],
            trigger_args=data["trigger_args"],
            timezone=data.get("timezone", "UTC"),
            status=JobStatus(data["status"]),
            next_run_time=(
                datetime.fromisoformat(data["next_run_time"]) if data.get("next_run_time") else None
            ),
            next_run_time_local=(
                datetime.fromisoformat(data["next_run_time_local"])
                if data.get("next_run_time_local")
                else None
            ),
            metadata=data.get("metadata", {}),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            max_retries=data.get("max_retries", 0),
            retry_delay_seconds=data.get("retry_delay_seconds", 60),
            retry_count=data.get("retry_count", 0),
            timeout_seconds=data.get("timeout_seconds"),
            priority=data.get("priority", 5),
        )

    def can_execute(self, current_time: datetime | None = None) -> bool:
        """
        Check if a job can be executed.

        A job can be executed if:
        1. Its state allows execution (via State pattern)
        2. Its next_run_time has passed (if applicable)

        Args:
            current_time: Time to check against (defaults to now)

        Returns:
            True if job can be executed, False otherwise
        """
        if not self.status.can_execute():
            return False

        if self.next_run_time:
            if current_time is None:
                current_time = utc_now()
            return self.next_run_time <= current_time

        return True

    def is_ready_for_execution(self, current_time: datetime | None = None) -> bool:
        """
        Check if a job is ready for execution at a specific time.

        Args:
            current_time: Time to check against (defaults to now)

        Returns:
            True if job is ready, False otherwise
        """
        if current_time is None:
            current_time = utc_now()

        if self.status not in (JobStatus.SCHEDULED, JobStatus.PENDING):
            return False

        if not self.next_run_time:
            return False

        return self.next_run_time <= current_time

    @staticmethod
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
