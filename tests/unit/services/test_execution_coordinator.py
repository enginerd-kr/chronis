"""Pure unit tests for ExecutionCoordinator with mocks for all dependencies."""

import asyncio
import threading
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from chronis.core.execution.coordinator import ExecutionCoordinator
from chronis.core.execution.job_executor import JobExecutor
from chronis.core.state import JobStatus


class TestTriggerExecutionLogging:
    """Test _trigger_execution logging behavior."""

    def test_trigger_execution_does_not_log_info(self):
        """Test that verbose=False doesn't log."""
        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=Mock(),

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
            # Test with recurring job (INTERVAL) so retry_count reset is included in update
            status_mock.return_value = JobStatus.SCHEDULED
            job_data["trigger_type"] = "interval"

            coordinator._execute_in_background(job_data, Mock())

        # Verify retry_count was reset to 0 in a single update_job call
        update_calls = [call for call in storage_mock.method_calls if call[0] == "update_job"]
        assert len(update_calls) == 1
        updates_dict = update_calls[0][1][1]  # second positional arg
        assert updates_dict["retry_count"] == 0
        assert updates_dict["status"] == "scheduled"


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
        coordinator._invoke_success_callback("test-1", job_data)

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

        coordinator._invoke_success_callback("test-1", job_data)

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

        coordinator._invoke_failure_callback("test-1", ValueError("Test"), job_data)

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

        coordinator._invoke_failure_callback("test-1", ValueError("Test"), job_data)

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


class TestAsyncJobDispatch:
    """Test async job routing to the shared event loop."""

    def test_async_job_dispatches_to_event_loop(self):
        """Async function should be routed to the shared event loop, not the thread pool."""
        async def async_func():
            pass

        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=Mock(),
            function_registry={"async_func": async_func},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {"job_id": "test-1", "func_name": "async_func"}
        on_complete = Mock()

        # Mock the event loop to avoid actually starting one
        mock_loop = Mock()
        mock_future = Mock()
        mock_loop.is_closed.return_value = False
        coordinator._job_executor._async_loop = mock_loop
        coordinator._job_executor._async_thread = Mock()

        with patch("asyncio.run_coroutine_threadsafe", return_value=mock_future) as mock_rcts:
            coordinator._trigger_execution(job_data, Mock(), on_complete)

        # Async path should be used, not executor.submit
        mock_rcts.assert_called_once()
        coordinator.executor.submit.assert_not_called()
        mock_future.add_done_callback.assert_called_once()

    def test_sync_job_dispatches_to_thread_pool(self):
        """Sync function should be routed to the thread pool, not the event loop."""
        def sync_func():
            pass

        executor_mock = Mock()
        future_mock = Mock()
        executor_mock.submit.return_value = future_mock

        coordinator = ExecutionCoordinator(
            storage=Mock(),
            lock=Mock(),
            executor=executor_mock,
            function_registry={"sync_func": sync_func},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_data = {"job_id": "test-1", "func_name": "sync_func"}

        with patch("asyncio.run_coroutine_threadsafe") as mock_rcts:
            coordinator._trigger_execution(job_data, Mock(), Mock())

        executor_mock.submit.assert_called_once()
        mock_rcts.assert_not_called()


class TestEnsureAsyncLoopThreadSafety:
    """Test that ensure_async_loop() is thread-safe."""

    def test_concurrent_calls_create_single_loop(self):
        """Multiple threads calling ensure_async_loop() should produce exactly one loop."""
        executor = JobExecutor(function_registry={}, logger=Mock())
        barrier = threading.Barrier(10)
        loops: list[asyncio.AbstractEventLoop] = []
        lock = threading.Lock()

        def get_loop():
            barrier.wait()
            loop = executor.ensure_async_loop()
            with lock:
                loops.append(loop)

        threads = [threading.Thread(target=get_loop) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # All threads should get the same loop instance
        assert len(loops) == 10
        assert all(loop is loops[0] for loop in loops)

        # Cleanup
        executor.shutdown_async(wait=False)


class TestSubmitFailureRollback:
    """Test rollback behavior when both submit and rollback fail."""

    def test_rollback_failure_does_not_mask_original_error(self):
        """If rollback also fails, the original submit error should still be raised."""
        storage_mock = Mock()
        storage_mock.update_job.side_effect = RuntimeError("Storage down")

        executor_mock = Mock()
        executor_mock.submit.side_effect = RuntimeError("ThreadPool is shutting down")

        coordinator = ExecutionCoordinator(
            storage=storage_mock,
            lock=Mock(),
            executor=executor_mock,
            function_registry={},
            failure_handler_registry={},
            success_handler_registry={},
            global_on_failure=None,
            global_on_success=None,
            logger=Mock(),
        )

        job_logger = Mock()

        # Original error should propagate, not the rollback error
        with pytest.raises(RuntimeError, match="ThreadPool is shutting down"):
            coordinator._trigger_execution({"job_id": "test-1"}, job_logger, Mock())

        # Rollback was attempted
        storage_mock.update_job.assert_called_once()
        # Rollback failure was logged
        job_logger.error.assert_called_once()
        assert "Failed to rollback" in job_logger.error.call_args[0][0]


class TestShutdownAsync:
    """Test async shutdown waits for running tasks."""

    def test_shutdown_waits_for_running_tasks(self):
        """shutdown_async(wait=True) should wait for running async tasks."""
        executor = JobExecutor(function_registry={}, logger=Mock())
        loop = executor.ensure_async_loop()

        completed = threading.Event()

        async def slow_task():
            await asyncio.sleep(0.3)
            completed.set()

        asyncio.run_coroutine_threadsafe(slow_task(), loop)

        executor.shutdown_async(wait=True)

        # Task should have completed before shutdown returned
        assert completed.is_set()
        assert executor._async_loop is None

    def test_shutdown_without_loop_is_noop(self):
        """shutdown_async() on uninitialized executor should not raise."""
        executor = JobExecutor(function_registry={}, logger=Mock())
        # Should not raise
        executor.shutdown_async(wait=True)
