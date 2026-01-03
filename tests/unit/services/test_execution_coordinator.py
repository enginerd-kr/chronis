"""Pure unit tests for ExecutionCoordinator with mocks for all dependencies."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from tenacity import RetryError

from chronis.core.execution.coordinator import ExecutionCoordinator


class TestLockReleaseRetryError:
    """Test lock release retry error handling."""

    def test_retry_error_during_lock_release_is_caught_and_logged(
        self, mock_storage, mock_lock, mock_executor, mock_async_executor, mock_logger
    ):
        """Test that RetryError from _release_lock_with_retry is caught and logged."""
        coordinator = ExecutionCoordinator(
            storage=mock_storage,
            lock=mock_lock,
            executor=mock_executor,
            async_executor=mock_async_executor,
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=mock_logger,
        )

        # Mock lock to return True (acquired)
        mock_lock.acquire.return_value = True

        # Mock _release_lock_with_retry to raise RetryError
        coordinator._release_lock_with_retry = Mock(side_effect=RetryError("Failed"))

        job_logger = Mock()

        # Execute context manager
        with coordinator._acquire_lock_context("test:lock", job_logger):
            pass

        # Verify error was logged
        job_logger.error.assert_called_once()
        error_msg = job_logger.error.call_args[0][0]
        assert "Lock release failed after retries" in error_msg


class TestTriggerExecutionLogging:
    """Test _trigger_execution logging behavior."""

    def test_verbose_mode_logs_job_triggered(self):
        """Test that verbose=True logs 'Job triggered' message."""
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
            verbose=True,
        )

        coordinator._update_job_status = Mock()
        coordinator.executor.submit = Mock(return_value=Mock(add_done_callback=Mock()))

        job_logger = Mock()
        coordinator._trigger_execution({"job_id": "test-1"}, job_logger, Mock())

        # Verify info log was called
        job_logger.info.assert_called_once_with("Job triggered")

    def test_non_verbose_mode_does_not_log(self):
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

    def test_one_time_job_deletion_logs_in_verbose_mode(self):
        """Test that one-time job deletion is logged in verbose mode."""
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
            verbose=True,
        )

        job_data = {
            "job_id": "test-1",
            "name": "Test Job",
            "func_name": "test_func",
            "trigger_type": "date",
            "trigger_args": {},
            "args": [],
            "kwargs": {},
            "status": "running",
            "timezone": "UTC",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
        }

        job_logger = Mock()

        with patch(
            "chronis.core.jobs.definition.JobInfo.determine_next_status_after_execution"
        ) as status_mock:
            status_mock.return_value = None

            coordinator._execute_in_background(job_data, job_logger)

        # Verify deletion and logging
        storage_mock.delete_job.assert_called_once_with("test-1")
        job_logger.info.assert_called_once_with("One-time job deleted after execution")


class TestScheduleRetryErrorHandling:
    """Test error handling in _schedule_retry."""

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
        coordinator._schedule_retry(job_data, 1, job_logger)

        # Verify error was logged
        job_logger.error.assert_called_once()
        assert "Failed to schedule retry" in job_logger.error.call_args[0][0]


class TestUpdateNextRunTimeErrorHandling:
    """Test error handling in _update_next_run_time."""

    @patch("chronis.core.execution.coordinator.NextRunTimeCalculator")
    @patch("chronis.utils.time.utc_now")
    def test_storage_error_during_update_is_logged(self, mock_utc_now, mock_calc):
        """Test that storage error during update_job is logged."""
        mock_utc_now.return_value = Mock(isoformat=Mock(return_value="2024-01-01T13:00:00"))
        mock_calc.calculate_with_local_time.return_value = (
            datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
            datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
        )

        storage_mock = Mock()
        storage_mock.update_job.side_effect = Exception("Storage error")
        logger_mock = Mock()

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
            logger=logger_mock,
        )

        job_data = {
            "job_id": "test-1",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 60},
            "timezone": "UTC",
            "next_run_time": "2024-01-01T12:00:00",
        }

        # Should not raise
        coordinator._update_next_run_time(job_data)

        # Verify error was logged
        logger_mock.error.assert_called()
        assert "Failed to update job run times" in logger_mock.error.call_args[0][0]

    @patch("chronis.core.execution.coordinator.NextRunTimeCalculator")
    @patch("chronis.utils.time.utc_now")
    def test_update_local_time_error_is_logged(self, mock_utc_now, mock_calc):
        """Test that error during second update_job call is logged."""
        mock_utc_now.return_value = Mock(isoformat=Mock(return_value="2024-01-01T13:00:00"))
        mock_calc.calculate_with_local_time.return_value = (
            datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
            datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
        )

        storage_mock = Mock()
        storage_mock.update_job.side_effect = Exception("Update error")
        logger_mock = Mock()

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
            logger=logger_mock,
        )

        job_data = {
            "job_id": "test-1",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 60},
            "timezone": "UTC",
            "next_run_time": "2024-01-01T12:00:00",
        }

        # Should not raise
        coordinator._update_next_run_time(job_data)

        # Error is logged
        logger_mock.error.assert_called()


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
        coordinator._invoke_success_handler("test-1", job_data)

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

        coordinator._invoke_success_handler("test-1", job_data)

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

        coordinator._invoke_failure_handler("test-1", ValueError("Test"), job_data)

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

        coordinator._invoke_failure_handler("test-1", ValueError("Test"), job_data)

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
