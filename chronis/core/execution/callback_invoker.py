"""Job callback invocation logic."""

from typing import Any

from chronis.core.execution.callbacks import OnFailureCallback, OnSuccessCallback
from chronis.core.jobs.definition import JobInfo
from chronis.utils.logging import ContextLogger


class CallbackInvoker:
    """Invokes job success and failure callbacks."""

    def __init__(
        self,
        failure_handler_registry: dict[str, OnFailureCallback],
        success_handler_registry: dict[str, OnSuccessCallback],
        global_on_failure: OnFailureCallback | None,
        global_on_success: OnSuccessCallback | None,
        logger: ContextLogger,
    ) -> None:
        """
        Initialize callback invoker.

        Args:
            failure_handler_registry: Job-specific failure handlers
            success_handler_registry: Job-specific success handlers
            global_on_failure: Global failure handler (optional)
            global_on_success: Global success handler (optional)
            logger: Context logger
        """
        self.failure_handler_registry = failure_handler_registry
        self.success_handler_registry = success_handler_registry
        self.global_on_failure = global_on_failure
        self.global_on_success = global_on_success
        self.logger = logger

    def invoke_failure_callback(
        self, job_id: str, error: Exception, job_data: dict[str, Any]
    ) -> None:
        """
        Invoke failure handlers for a failed job.

        Both job-specific and global handlers are called.

        Args:
            job_id: Job ID that failed
            error: Exception that occurred
            job_data: Job data from storage
        """
        job_info = JobInfo.from_dict(job_data)

        # Invoke job-specific handler first
        job_handler = self.failure_handler_registry.get(job_id)
        if job_handler:
            try:
                job_handler(job_id, error, job_info)
            except Exception as handler_error:
                self.logger.error(
                    "Job-specific failure handler raised exception",
                    error=str(handler_error),
                    job_id=job_id,
                    exc_info=True,
                )

        # Also invoke global handler
        if self.global_on_failure:
            try:
                self.global_on_failure(job_id, error, job_info)
            except Exception as handler_error:
                self.logger.error(
                    "Global failure handler raised exception",
                    error=str(handler_error),
                    job_id=job_id,
                    exc_info=True,
                )

    def invoke_success_callback(self, job_id: str, job_data: dict[str, Any]) -> None:
        """
        Invoke success handlers for a successful job.

        Both job-specific and global handlers are called.

        Args:
            job_id: Job ID that succeeded
            job_data: Job data from storage
        """
        job_info = JobInfo.from_dict(job_data)

        # Invoke job-specific handler first
        job_handler = self.success_handler_registry.get(job_id)
        if job_handler:
            try:
                job_handler(job_id, job_info)
            except Exception as handler_error:
                self.logger.error(
                    "Job-specific success handler raised exception",
                    error=str(handler_error),
                    job_id=job_id,
                    exc_info=True,
                )

        # Also invoke global handler
        if self.global_on_success:
            try:
                self.global_on_success(job_id, job_info)
            except Exception as handler_error:
                self.logger.error(
                    "Global success handler raised exception",
                    error=str(handler_error),
                    job_id=job_id,
                    exc_info=True,
                )
