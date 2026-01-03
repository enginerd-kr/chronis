"""Callback protocols and types for job execution."""

from typing import Protocol

from chronis.core.jobs.definition import JobInfo


class OnFailureCallback(Protocol):
    """Protocol for job failure callbacks."""

    def __call__(self, job_id: str, error: Exception, job_info: JobInfo) -> None:
        """
        Handle job failure.

        Args:
            job_id: Job ID that failed
            error: Exception that occurred
            job_info: Job information snapshot
        """
        ...


class OnSuccessCallback(Protocol):
    """Protocol for job success callbacks."""

    def __call__(self, job_id: str, job_info: JobInfo) -> None:
        """
        Handle job success.

        Args:
            job_id: Job ID that succeeded
            job_info: Job information snapshot
        """
        ...
