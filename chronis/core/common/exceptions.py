"""Custom exceptions for Chronis."""


class SchedulerError(Exception):
    """Base exception for scheduler errors."""

    pass


class JobAlreadyExistsError(SchedulerError):
    """Job already exists."""

    pass


class JobNotFoundError(SchedulerError):
    """Job not found."""

    pass


class ValidationError(SchedulerError):
    """Invalid parameters."""

    pass


class ConnectionError(SchedulerError):
    """Storage or lock adapter connection failed."""

    pass
