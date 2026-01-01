"""Custom exceptions for Chronis."""


# ============================================================================
# Base Exception Classes
# ============================================================================


class SchedulerError(Exception):
    """Base exception for all scheduler errors."""

    pass


class SchedulerConfigurationError(SchedulerError):
    """
    Configuration and setup errors.

    These errors indicate invalid configuration or improper setup.
    They should be caught during development and testing.
    If these occur in production, it indicates a deployment issue.
    """

    pass


class SchedulerLookupError(SchedulerError):
    """
    Lookup and query errors.

    These errors occur when jobs or resources cannot be found.
    They can happen during normal operation and should be handled gracefully.
    """

    pass


class SchedulerStateError(SchedulerError):
    """
    State transition errors.

    These errors occur when attempting invalid state transitions.
    They can happen during normal operation and should be handled gracefully.
    """

    pass


# ============================================================================
# Configuration Errors (Development-time)
# ============================================================================


class FunctionNotRegisteredError(SchedulerConfigurationError):
    """
    Function not registered with scheduler.

    Solution: Call scheduler.register_job_function(name, func) before creating jobs.
    """

    pass


# ============================================================================
# Lookup Errors (Runtime)
# ============================================================================


class JobNotFoundError(SchedulerLookupError):
    """
    Job not found.

    Common causes:
    - Job was deleted
    - One-time jobs auto-delete after execution
    - Incorrect job_id

    Solution: Use scheduler.query_jobs() to see available jobs.
    """

    pass


class JobAlreadyExistsError(SchedulerLookupError):
    """
    Job with this ID already exists.

    Solution:
    - Use a different job_id
    - Delete existing job with scheduler.delete_job()
    - Query existing jobs with scheduler.query_jobs()
    """

    pass


# ============================================================================
# State Errors (Runtime)
# ============================================================================


class InvalidJobStateError(SchedulerStateError):
    """
    Invalid job state transition.

    Examples:
    - Trying to pause a RUNNING job (only SCHEDULED/PENDING can be paused)
    - Trying to resume a non-PAUSED job

    Solution: Check job.status before performing operations.
    """

    pass


class JobTimeoutError(SchedulerStateError):
    """
    Job exceeded timeout duration.

    Solutions:
    - Increase timeout_seconds if the job needs more time
    - Optimize the job function to run faster
    - Check for infinite loops or blocking operations
    """

    pass
