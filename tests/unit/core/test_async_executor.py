"""Pure unit tests for AsyncExecutor."""

import logging
from unittest.mock import Mock

import pytest

from chronis.core.execution.async_loop import AsyncExecutor


class TestAsyncExecutorStartWithLogger:
    """Test AsyncExecutor.start() with logger."""

    def test_start_when_already_running_logs_warning_and_raises(self):
        """Test that starting when already running logs warning and raises RuntimeError."""
        logger_mock = Mock(spec=logging.Logger)
        executor = AsyncExecutor(logger=logger_mock)

        # Start first time
        executor.start()

        try:
            # Try to start again - should log warning and raise
            with pytest.raises(RuntimeError) as exc_info:
                executor.start()

            assert "already running" in str(exc_info.value).lower()
            logger_mock.warning.assert_called_once()
            assert "already running" in logger_mock.warning.call_args[0][0].lower()

        finally:
            executor.stop()

    def test_start_logs_debug_message(self):
        """Test that start logs debug message when logger is provided."""
        logger_mock = Mock(spec=logging.Logger)
        executor = AsyncExecutor(logger=logger_mock)

        executor.start()

        try:
            # Verify debug log was called
            logger_mock.debug.assert_called_once()
            assert "Started dedicated event loop" in logger_mock.debug.call_args[0][0]

        finally:
            executor.stop()


class TestAsyncExecutorStopWithLogger:
    """Test AsyncExecutor.stop() with logger."""

    def test_stop_logs_debug_message(self):
        """Test that stop logs debug message when logger is provided."""
        logger_mock = Mock(spec=logging.Logger)
        executor = AsyncExecutor(logger=logger_mock)

        executor.start()
        executor.stop()

        # Verify debug log was called for stop
        debug_calls = [call[0][0] for call in logger_mock.debug.call_args_list]
        assert any("Stopped dedicated event loop" in msg for msg in debug_calls)


class TestAsyncExecutorRunEventLoopEarlyReturn:
    """Test _run_event_loop early return when loop is None."""

    def test_run_event_loop_returns_early_if_loop_is_none(self):
        """Test that _run_event_loop returns early if self._loop is None."""
        executor = AsyncExecutor()

        # Manually call _run_event_loop when _loop is None
        # Should just return without error
        executor._run_event_loop()

        # If we get here, it returned successfully
        assert executor._loop is None


class TestAsyncExecutorExecuteCoroutineNotRunning:
    """Test execute_coroutine when not running."""

    def test_execute_coroutine_raises_when_not_running(self):
        """Test that execute_coroutine raises RuntimeError when not running."""
        executor = AsyncExecutor()

        async def test_coro():
            return "result"

        with pytest.raises(RuntimeError) as exc_info:
            executor.execute_coroutine(test_coro())

        assert "not running" in str(exc_info.value).lower()


class TestAsyncExecutorIsRunningWhenLoopClosed:
    """Test is_running when loop is closed."""

    def test_is_running_false_when_loop_exists_but_closed(self):
        """Test is_running returns False when loop exists but is closed."""
        executor = AsyncExecutor()
        executor.start()

        # Stop the executor
        executor.stop()

        # After stop, _loop should be None
        assert executor._loop is None
        assert executor.is_running() is False
