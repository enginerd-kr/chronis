"""Pure unit tests for ExecutionCoordinator with mocks for all dependencies."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from chronis.core.execution.callback_invoker import CallbackInvoker
from chronis.core.execution.coordinator import ExecutionCoordinator


class TestTriggerExecutionLogging:
    """Test _trigger_execution logging behavior."""

    def test_trigger_execution_does_not_log_info(self):
        """Test that verbose=False doesn't log."""
        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=Mock(),
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
            verbose=False,
        )

        coordinator._update_job_status = Mock()
        coordinator.executor.submit = Mock(return_value=Mock(add_done_callback=Mock()))

        job_logger = Mock()
        coordinator._trigger_execution({"job_id": "test-1"}, job_logger, Mock())

        # Verify info was NOT called
        job_logger.info.assert_not_called()

    def test_exception_during_trigger_is_logged(self):
        """Test that exception during execution trigger is logged."""
        executor_mock = Mock()
        executor_mock.submit.side_effect = Exception("Submit failed")

        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=executor_mock,
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_logger = Mock()

        # Should raise exception and rollback
        with pytest.raises(Exception, match="Submit failed"):
            coordinator._trigger_execution({"job_id": "test-1"}, job_logger, Mock())

        # Verify warning was logged (rollback message)
        job_logger.warning.assert_called_once()
        warning_msg = job_logger.warning.call_args[0][0]
        assert "rolling back to SCHEDULED" in warning_msg


class TestExecuteInBackgroundRetryLogic:
    """Test retry logic in _execute_in_background."""

    @patch("chronis.utils.time.utc_now")
    def test_successful_execution_with_previous_retries_resets_count(self, mock_utc_now):
        """Test that successful execution resets retry_count to 0."""
        mock_utc_now.return_value = Mock(isoformat=Mock(return_value="2024-01-01T12:00:00"))

        storage_mock = Mock()
        coordinator = ExecutionCoordinator(
            storage=storage_mock,
            lock=Mock(),
            executor=Mock(),
            async_executor=Mock(),
            function_registry={"test_func": lambda: None},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {
            "job_id": "test-1",
            "name": "Test Job",
            "func_name": "test_func",
            "trigger_type": "date",
            "trigger_args": {},
            "args": [],
            "kwargs": {},
            "retry_count": 2,  # Had previous retries
            "status": "running",
            "timezone": "UTC",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
        }

        with patch(
            "chronis.core.jobs.definition.JobInfo.determine_next_status_after_execution"
        ) as status_mock:
            status_mock.return_value = None

            coordinator._execute_in_background(job_data, Mock())

        # Verify retry_count was reset to 0
        update_calls = [call for call in storage_mock.method_calls if call[0] == "update_job"]
        assert any("retry_count" in str(call) and "0" in str(call) for call in update_calls)


class TestScheduleRetryErrorHandling:
    """Test error handling in retry_handler.schedule_retry."""

    @patch("chronis.utils.time.get_timezone")
    @patch("chronis.utils.time.utc_now")
    def test_storage_error_during_retry_scheduling_is_logged(self, mock_utc_now, mock_get_tz):
        """Test that storage error is caught and logged."""
        from zoneinfo import ZoneInfo

        mock_utc_now.return_value = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
        mock_get_tz.return_value = ZoneInfo("UTC")

        storage_mock = Mock()
        storage_mock.update_job.side_effect = Exception("Storage failed")

        coordinator = ExecutionCoordinator(
            storage=storage_mock,
            lock=Mock(),
            executor=Mock(),
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {
            "job_id": "test-1",
            "retry_delay_seconds": 60,
            "max_retries": 3,
            "timezone": "UTC",
        }

        job_logger = Mock()

        # Should not raise
        coordinator.retry_handler.schedule_retry(job_data, 1, job_logger)

        # Verify error was logged
        job_logger.error.assert_called_once()
        assert "Failed to schedule retry" in job_logger.error.call_args[0][0]


class TestCallbackExceptionHandling:
    """Test that callback exceptions don't break execution."""

    def test_success_callback_exception_is_logged_job_specific(self):
        """Test job-specific success callback exception is caught."""

        def failing_callback(job_id, job_info):
            raise RuntimeError("Callback failed")

        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=Mock(),
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={"test-1": failing_callback},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {
            "job_id": "test-1",
            "name": "Test",
            "status": "running",
            "func_name": "test",
            "trigger_type": "date",
            "trigger_args": {},
            "timezone": "UTC",
            "metadata": {},
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }

        # Should not raise
        CallbackInvoker(
            coordinator.failure_handler_registry,
            coordinator.success_handler_registry,
            coordinator.global_on_failure,
            coordinator.global_on_success,
            coordinator.logger,
        ).invoke_success_callback("test-1", job_data)

        # Verify error was logged
        coordinator.logger.error.assert_called_once()
        assert (
            "Job-specific success handler raised exception"
            in coordinator.logger.error.call_args[0][0]
        )

    def test_success_callback_exception_is_logged_global(self):
        """Test global success callback exception is caught."""

        def failing_global(job_id, job_info):
            raise RuntimeError("Global failed")

        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=Mock(),
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=failing_global,
            logger=Mock(),
        )

        job_data = {
            "job_id": "test-1",
            "name": "Test",
            "status": "running",
            "func_name": "test",
            "trigger_type": "date",
            "trigger_args": {},
            "timezone": "UTC",
            "metadata": {},
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }

        CallbackInvoker(
            coordinator.failure_handler_registry,
            coordinator.success_handler_registry,
            coordinator.global_on_failure,
            coordinator.global_on_success,
            coordinator.logger,
        ).invoke_success_callback("test-1", job_data)

        coordinator.logger.error.assert_called_once()
        assert "Global success handler raised exception" in coordinator.logger.error.call_args[0][0]

    def test_failure_callback_exception_is_logged_job_specific(self):
        """Test job-specific failure callback exception is caught."""

        def failing_callback(job_id, error, job_info):
            raise RuntimeError("Callback failed")

        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=Mock(),
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={"test-1": failing_callback},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {
            "job_id": "test-1",
            "name": "Test",
            "status": "failed",
            "func_name": "test",
            "trigger_type": "date",
            "trigger_args": {},
            "timezone": "UTC",
            "metadata": {},
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }

        CallbackInvoker(
            coordinator.failure_handler_registry,
            coordinator.success_handler_registry,
            coordinator.global_on_failure,
            coordinator.global_on_success,
            coordinator.logger,
        ).invoke_failure_callback("test-1", ValueError("Test"), job_data)

        coordinator.logger.error.assert_called_once()
        assert (
            "Job-specific failure handler raised exception"
            in coordinator.logger.error.call_args[0][0]
        )

    def test_failure_callback_exception_is_logged_global(self):
        """Test global failure callback exception is caught."""

        def failing_global(job_id, error, job_info):
            raise RuntimeError("Global failed")

        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=Mock(),
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=failing_global,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {
            "job_id": "test-1",
            "name": "Test",
            "status": "failed",
            "func_name": "test",
            "trigger_type": "date",
            "trigger_args": {},
            "timezone": "UTC",
            "metadata": {},
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }

        CallbackInvoker(
            coordinator.failure_handler_registry,
            coordinator.success_handler_registry,
            coordinator.global_on_failure,
            coordinator.global_on_success,
            coordinator.logger,
        ).invoke_failure_callback("test-1", ValueError("Test"), job_data)

        coordinator.logger.error.assert_called_once()
        assert "Global failure handler raised exception" in coordinator.logger.error.call_args[0][0]


class TestExecutorSubmitRollback:
    """Test executor submit failure rollback behavior."""

    def test_trigger_execution_rolls_back_on_submit_failure(self):
        """ThreadPool submit 실패 시 SCHEDULED로 복구."""
        # Mock storage
        storage_mock = Mock()

        # Mock executor that raises on submit
        executor_mock = Mock()
        executor_mock.submit.side_effect = RuntimeError("ThreadPool is shutting down")

        coordinator = ExecutionCoordinator(
            storage=storage_mock,
            lock=Mock(),
            executor=executor_mock,
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {
            "job_id": "test-1",
            "status": "scheduled",
        }

        job_logger = Mock()

        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="ThreadPool is shutting down"):
            coordinator._trigger_execution(job_data, job_logger, Mock())

        # Verify status update - only one call for rollback
        # (RUNNING update now happens in try_execute before calling _trigger_execution)
        assert storage_mock.update_job.call_count == 1

        # First (and only) call: SCHEDULED (rollback)
        rollback_call = storage_mock.update_job.call_args_list[0]
        assert rollback_call[0][0] == "test-1"
        assert "status" in rollback_call[0][1]
        from chronis.core.state import JobStatus

        assert rollback_call[0][1]["status"] == JobStatus.SCHEDULED.value

        # Verify warning was logged
        job_logger.warning.assert_called_once()
        warning_msg = job_logger.warning.call_args[0][0]
        assert "rolling back to SCHEDULED" in warning_msg

    def test_trigger_execution_succeeds_normally(self):
        """정상적인 경우 rollback 발생 안 함."""
        storage_mock = Mock()
        executor_mock = Mock()
        future_mock = Mock()
        executor_mock.submit.return_value = future_mock

        coordinator = ExecutionCoordinator(
            storage=storage_mock,
            lock=Mock(),
            executor=executor_mock,
            async_executor=Mock(),
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
            verbose=False,
        )

        job_data = {"job_id": "test-1", "status": "scheduled"}
        job_logger = Mock()

        # Should not raise
        coordinator._trigger_execution(job_data, job_logger, Mock())

        # RUNNING update는 이제 try_execute에서 발생하므로, _trigger_execution에서는 없어야 함
        assert storage_mock.update_job.call_count == 0

        # Verify submit was called
        executor_mock.submit.assert_called_once()

        # Verify callback registered
        future_mock.add_done_callback.assert_called_once()

        # Verify no warning logged
        job_logger.warning.assert_not_called()
