"""Job query specifications (Specification pattern)."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from chronis.core.state import JobStatus


class JobQuerySpec(ABC):
    """
    Abstract base class for job query specifications.

    Implements the Specification pattern to encapsulate query criteria
    as domain objects rather than dictionaries.
    """

    @abstractmethod
    def to_filters(self) -> dict[str, Any]:
        """
        Convert specification to filter dictionary for adapter.

        Returns:
            Dictionary of filter conditions
        """
        pass

    def and_(self, other: "JobQuerySpec") -> "AndSpec":
        """Combine with another spec using AND logic."""
        return AndSpec(self, other)


@dataclass
class StatusSpec(JobQuerySpec):
    """Filter jobs by status."""

    status: JobStatus

    def to_filters(self) -> dict[str, Any]:
        return {"status": self.status.value}


@dataclass
class NextRunBeforeSpec(JobQuerySpec):
    """Filter jobs where next_run_time <= specified time."""

    time: datetime

    def to_filters(self) -> dict[str, Any]:
        return {"next_run_time_lte": self.time.isoformat()}


@dataclass
class NextRunAfterSpec(JobQuerySpec):
    """Filter jobs where next_run_time >= specified time."""

    time: datetime

    def to_filters(self) -> dict[str, Any]:
        return {"next_run_time_gte": self.time.isoformat()}


@dataclass
class MetadataSpec(JobQuerySpec):
    """Filter jobs by metadata field."""

    key: str
    value: Any

    def to_filters(self) -> dict[str, Any]:
        return {f"metadata.{self.key}": self.value}


@dataclass
class TriggerTypeSpec(JobQuerySpec):
    """Filter jobs by trigger type."""

    trigger_type: str

    def to_filters(self) -> dict[str, Any]:
        return {"trigger_type": self.trigger_type}


@dataclass
class AndSpec(JobQuerySpec):
    """Combine multiple specs with AND logic."""

    left: JobQuerySpec
    right: JobQuerySpec

    def to_filters(self) -> dict[str, Any]:
        """Merge filters from both specs."""
        filters = {}
        filters.update(self.left.to_filters())
        filters.update(self.right.to_filters())
        return filters


@dataclass
class AllJobsSpec(JobQuerySpec):
    """Match all jobs (no filters)."""

    def to_filters(self) -> dict[str, Any]:
        return {}


@dataclass(frozen=True)
class JobQuery:
    """
    Immutable job query with spec, pagination, and sorting.

    This is a builder-style query object that combines:
    - Specification (what to match)
    - Pagination (limit, offset)
    - Sorting (order by)

    All builder methods return new instances (immutable pattern).
    """

    spec: JobQuerySpec = field(default_factory=AllJobsSpec)
    limit: int | None = None
    offset: int | None = None

    def to_filters(self) -> dict[str, Any]:
        """Convert to filter dict for adapter."""
        return self.spec.to_filters()

    def with_limit(self, limit: int) -> "JobQuery":
        """
        Create new query with limit (immutable builder pattern).

        Args:
            limit: Maximum number of results

        Returns:
            New JobQuery instance with updated limit
        """
        return JobQuery(spec=self.spec, limit=limit, offset=self.offset)

    def with_offset(self, offset: int) -> "JobQuery":
        """
        Create new query with offset (immutable builder pattern).

        Args:
            offset: Number of results to skip

        Returns:
            New JobQuery instance with updated offset
        """
        return JobQuery(spec=self.spec, limit=self.limit, offset=offset)


# Convenience factory functions


def scheduled_jobs() -> StatusSpec:
    """Query for scheduled jobs."""
    return StatusSpec(JobStatus.SCHEDULED)


def jobs_ready_before(time: datetime) -> JobQuerySpec:
    """
    Query for jobs ready to run before specified time.

    This is the primary query used by the scheduler's polling mechanism.

    Args:
        time: Time threshold

    Returns:
        Spec for scheduled jobs with next_run_time <= time
    """
    return scheduled_jobs().and_(NextRunBeforeSpec(time))
