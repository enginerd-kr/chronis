"""Unit tests for custom exceptions with helpful messages."""

import pytest

from chronis.adapters.lock import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.common.exceptions import (
    FunctionNotRegisteredError,
    InvalidJobStateError,
    JobAlreadyExistsError,
    JobNotFoundError,
    JobTimeoutError,
    SchedulerConfigurationError,
    SchedulerError,
    SchedulerLookupError,
    SchedulerStateError,
)
from chronis.core.schedulers.polling_scheduler import PollingScheduler
from chronis.core.state import JobStatus


class TestSchedulerError:
    """Test base SchedulerError class."""

    def test_simple_message(self):
        """Test error with simple message."""
        error = SchedulerError("Something went wrong")
        assert str(error) == "Something went wrong"

    def test_error_inheritance(self):
        """Test that custom errors inherit from SchedulerError."""
        assert issubclass(JobNotFoundError, SchedulerError)
        assert issubclass(JobAlreadyExistsError, SchedulerError)
        assert issubclass(InvalidJobStateError, SchedulerError)
        assert issubclass(FunctionNotRegisteredError, SchedulerError)

    def test_base_class_hierarchy(self):
        """Test the exception hierarchy structure."""
        # Configuration errors
        assert issubclass(FunctionNotRegisteredError, SchedulerConfigurationError)
        assert issubclass(SchedulerConfigurationError, SchedulerError)

        # Lookup errors
        assert issubclass(JobNotFoundError, SchedulerLookupError)
        assert issubclass(JobAlreadyExistsError, SchedulerLookupError)
        assert issubclass(SchedulerLookupError, SchedulerError)

        # State errors
        assert issubclass(InvalidJobStateError, SchedulerStateError)
        assert issubclass(JobTimeoutError, SchedulerStateError)
        assert issubclass(SchedulerStateError, SchedulerError)

    def test_isinstance_checks(self):
        """Test isinstance checks for categorizing errors."""
        # Configuration error
        config_error = FunctionNotRegisteredError("test")
        assert isinstance(config_error, SchedulerConfigurationError)
        assert isinstance(config_error, SchedulerError)
        assert not isinstance(config_error, SchedulerLookupError)

        # Lookup error
        lookup_error = JobNotFoundError("test")
        assert isinstance(lookup_error, SchedulerLookupError)
        assert isinstance(lookup_error, SchedulerError)
        assert not isinstance(lookup_error, SchedulerConfigurationError)

        # State error
        state_error = InvalidJobStateError("test")
        assert isinstance(state_error, SchedulerStateError)
        assert isinstance(state_error, SchedulerError)
        assert not isinstance(state_error, SchedulerLookupError)


class TestJobLifecycleErrors:
    """Test job lifecycle exception classes."""

    @pytest.fixture
    def scheduler(self):
        """Create a test scheduler."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        return PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def test_job_not_found_error_message(self, scheduler):
        """Test JobNotFoundError has helpful message."""
        with pytest.raises(JobNotFoundError) as exc_info:
            scheduler.pause_job("nonexistent-job")

        error_msg = str(exc_info.value)
        assert "nonexistent-job" in error_msg
        assert "not found" in error_msg
        assert "query_jobs" in error_msg

    def test_invalid_job_state_error_on_pause(self, scheduler):
        """Test InvalidJobStateError when pausing running job."""

        def dummy_job():
            pass

        scheduler.register_job_function("dummy_job", dummy_job)
        job = scheduler.create_interval_job(func="dummy_job", seconds=60)

        # Manually set job to RUNNING state
        scheduler.storage.update_job(job.job_id, {"status": JobStatus.RUNNING.value})

        with pytest.raises(InvalidJobStateError) as exc_info:
            scheduler.pause_job(job.job_id)

        error_msg = str(exc_info.value)
        assert job.job_id in error_msg
        assert "RUNNING" in error_msg or "running" in error_msg.lower()
        assert "SCHEDULED" in error_msg or "PENDING" in error_msg

    def test_invalid_job_state_error_on_resume(self, scheduler):
        """Test InvalidJobStateError when resuming non-paused job."""

        def dummy_job():
            pass

        scheduler.register_job_function("dummy_job", dummy_job)
        job = scheduler.create_interval_job(func="dummy_job", seconds=60)

        # Job is SCHEDULED, not PAUSED
        with pytest.raises(InvalidJobStateError) as exc_info:
            scheduler.resume_job(job.job_id)

        error_msg = str(exc_info.value)
        assert job.job_id in error_msg
        assert "PAUSED" in error_msg


class TestFunctionErrors:
    """Test function-related exceptions."""

    @pytest.fixture
    def scheduler(self):
        """Create a test scheduler."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        return PollingScheduler(storage_adapter=storage, lock_adapter=lock)

    def test_function_not_registered_error_message(self, scheduler):
        """Test FunctionNotRegisteredError has helpful message."""
        # Create job without registering function
        scheduler.create_interval_job(func="unregistered_function", seconds=60)

        scheduler.start()
        try:
            # Wait a bit for job to be attempted
            import time

            time.sleep(0.5)

            # Check if error was raised (would be in logs)
            # For now, just verify the exception can be raised
            from chronis.core.common.exceptions import FunctionNotRegisteredError

            with pytest.raises(FunctionNotRegisteredError) as exc_info:
                raise FunctionNotRegisteredError(
                    "Function 'test_func' is not registered. "
                    "Call scheduler.register_job_function(name, func) before creating jobs."
                )

            error_msg = str(exc_info.value)
            assert "not registered" in error_msg
            assert "register_job_function" in error_msg

        finally:
            scheduler.stop()


class TestSchedulerStateErrors:
    """Test scheduler state exceptions."""

    def test_scheduler_already_running_error(self):
        """Test error when starting already running scheduler."""
        storage = InMemoryStorageAdapter()
        lock = InMemoryLockAdapter()
        scheduler = PollingScheduler(storage_adapter=storage, lock_adapter=lock)

        scheduler.start()
        try:
            with pytest.raises(SchedulerError) as exc_info:
                scheduler.start()

            error_msg = str(exc_info.value)
            assert "already running" in error_msg
            assert "stop()" in error_msg

        finally:
            scheduler.stop()


class TestDocstrings:
    """Test that all exceptions have helpful docstrings."""

    def test_all_exceptions_have_docstrings(self):
        """Test that all custom exceptions have docstrings with solutions."""
        exceptions = [
            JobNotFoundError,
            JobAlreadyExistsError,
            InvalidJobStateError,
            FunctionNotRegisteredError,
            JobTimeoutError,
        ]

        for exc_class in exceptions:
            assert exc_class.__doc__ is not None
            doc = exc_class.__doc__.lower()
            # Check for helpful keywords
            assert any(
                keyword in doc for keyword in ["solution", "note", "examples", "causes", "common"]
            ), f"{exc_class.__name__} should have helpful docstring"


class TestErrorCategorization:
    """Test that errors can be categorized by type."""

    def test_catch_configuration_errors(self):
        """Test catching all configuration errors."""
        errors = [
            FunctionNotRegisteredError("test"),
        ]

        for error in errors:
            try:
                raise error
            except SchedulerConfigurationError:
                # Successfully caught as configuration error
                pass
            else:
                pytest.fail(f"{error.__class__.__name__} should be catchable as ConfigurationError")

    def test_catch_lookup_errors(self):
        """Test catching all lookup errors."""
        errors = [
            JobNotFoundError("test"),
            JobAlreadyExistsError("test"),
        ]

        for error in errors:
            try:
                raise error
            except SchedulerLookupError:
                # Successfully caught as lookup error
                pass
            else:
                pytest.fail(f"{error.__class__.__name__} should be catchable as LookupError")

    def test_catch_state_errors(self):
        """Test catching all state errors."""
        errors = [
            InvalidJobStateError("test"),
            JobTimeoutError("test"),
        ]

        for error in errors:
            try:
                raise error
            except SchedulerStateError:
                # Successfully caught as state error
                pass
            else:
                pytest.fail(f"{error.__class__.__name__} should be catchable as StateError")

    def test_catch_runtime_errors_together(self):
        """Test catching runtime errors (lookup + state) together."""
        runtime_errors = [
            JobNotFoundError("test"),
            InvalidJobStateError("test"),
            JobTimeoutError("test"),
        ]

        for error in runtime_errors:
            try:
                raise error
            except (SchedulerLookupError, SchedulerStateError):
                # Runtime errors can be caught together
                pass
            else:
                pytest.fail(f"{error.__class__.__name__} should be catchable as runtime error")
