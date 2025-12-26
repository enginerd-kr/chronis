"""Callback protocols and types for job execution."""

from typing import Protocol

from chronis.core.jobs.definition import JobInfo


class OnFailureCallback(Protocol):
    """
    Protocol for job failure callbacks.

    This callback is invoked when a job execution fails.

    Args:
        job_id: Unique identifier of the failed job
        error: Exception that caused the failure
        job_info: Job information at the time of failure
    """

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
    """
    Protocol for job success callbacks.

    This callback is invoked when a job execution succeeds.

    Args:
        job_id: Unique identifier of the successful job
        job_info: Job information at the time of success
    """

    def __call__(self, job_id: str, job_info: JobInfo) -> None:
        """
        Handle job success.

        Args:
            job_id: Job ID that succeeded
            job_info: Job information snapshot
        """
        ...
