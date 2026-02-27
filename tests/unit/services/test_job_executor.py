"""Unit tests for JobExecutor sync/async execution and timeout handling."""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from chronis.core.common.exceptions import FunctionNotRegisteredError, JobTimeoutError
from chronis.core.execution.job_executor import JobExecutor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job_data(
    func_name: str = "my_func",
    job_id: str = "job-1",
    args: list | None = None,
    kwargs: dict | None = None,
    timeout_seconds: float | None = None,
) -> dict:
    data: dict = {"func_name": func_name, "job_id": job_id}
    if args is not None:
        data["args"] = args
    if kwargs is not None:
        data["kwargs"] = kwargs
    if timeout_seconds is not None:
        data["timeout_seconds"] = timeout_seconds
    return data


# ===========================================================================
# TestJobExecutorSync
# ===========================================================================


class TestJobExecutorSync:
    """Tests for JobExecutor.execute_sync."""

    def test_execute_sync_success(self):
        """Normal sync execution - function should be called once."""
        func = MagicMock()
        registry = {"my_func": func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data()
        executor.execute_sync(job_data, job_logger=MagicMock())

        func.assert_called_once_with()

    def test_execute_sync_with_args(self):
        """Args and kwargs should be forwarded to the registered function."""
        func = MagicMock()
        registry = {"my_func": func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data(args=[1, "hello"], kwargs={"key": "value"})
        executor.execute_sync(job_data, job_logger=MagicMock())

        func.assert_called_once_with(1, "hello", key="value")

    def test_execute_sync_unregistered_function(self):
        """Calling an unregistered function should raise FunctionNotRegisteredError."""
        executor = JobExecutor(function_registry={}, logger=MagicMock())

        job_data = _make_job_data(func_name="unknown_func")

        with pytest.raises(FunctionNotRegisteredError):
            executor.execute_sync(job_data, job_logger=MagicMock())

    def test_execute_sync_timeout(self):
        """A slow function exceeding timeout_seconds should raise JobTimeoutError."""

        def slow_func():
            time.sleep(2)

        registry = {"slow": slow_func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data(func_name="slow", timeout_seconds=0.5)

        with pytest.raises(JobTimeoutError):
            executor.execute_sync(job_data, job_logger=MagicMock())

    def test_execute_sync_within_timeout(self):
        """A fast function should complete normally even with a timeout set."""
        call_record = []

        def fast_func():
            call_record.append(True)

        registry = {"fast": fast_func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data(func_name="fast", timeout_seconds=5.0)
        executor.execute_sync(job_data, job_logger=MagicMock())

        assert len(call_record) == 1

    def test_execute_sync_exception_propagation(self):
        """Exceptions raised inside the job function should propagate to the caller."""

        def failing_func():
            raise ValueError("something went wrong")

        registry = {"fail": failing_func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data(func_name="fail")

        with pytest.raises(ValueError, match="something went wrong"):
            executor.execute_sync(job_data, job_logger=MagicMock())


# ===========================================================================
# TestJobExecutorAsync
# ===========================================================================


class TestJobExecutorAsync:
    """Tests for JobExecutor.execute_async."""

    @pytest.mark.asyncio
    async def test_execute_async_success(self):
        """Normal async execution - coroutine should run to completion."""
        call_record = []

        async def async_func():
            call_record.append(True)

        registry = {"afunc": async_func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data(func_name="afunc")
        await executor.execute_async(job_data, job_logger=MagicMock())

        assert len(call_record) == 1

    @pytest.mark.asyncio
    async def test_execute_async_unregistered_function(self):
        """Calling an unregistered async function should raise FunctionNotRegisteredError."""
        executor = JobExecutor(function_registry={}, logger=MagicMock())

        job_data = _make_job_data(func_name="missing")

        with pytest.raises(FunctionNotRegisteredError):
            await executor.execute_async(job_data, job_logger=MagicMock())

    @pytest.mark.asyncio
    async def test_execute_async_timeout(self):
        """A slow coroutine exceeding timeout should raise asyncio.TimeoutError."""

        async def slow_coro():
            await asyncio.sleep(2)

        registry = {"slow_async": slow_coro}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data(func_name="slow_async", timeout_seconds=0.5)

        with pytest.raises(asyncio.TimeoutError):
            await executor.execute_async(job_data, job_logger=MagicMock())

    @pytest.mark.asyncio
    async def test_execute_async_within_timeout(self):
        """A fast coroutine should complete normally even with a timeout set."""
        call_record = []

        async def fast_coro():
            call_record.append(True)

        registry = {"fast_async": fast_coro}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        job_data = _make_job_data(func_name="fast_async", timeout_seconds=5.0)
        await executor.execute_async(job_data, job_logger=MagicMock())

        assert len(call_record) == 1


# ===========================================================================
# TestIsAsync
# ===========================================================================


class TestIsAsync:
    """Tests for JobExecutor.is_async."""

    def test_is_async_true(self):
        """An async function should return True."""

        async def coro_func():
            pass

        registry = {"coro": coro_func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        assert executor.is_async("coro") is True

    def test_is_async_false(self):
        """A regular sync function should return False."""

        def sync_func():
            pass

        registry = {"sync": sync_func}
        executor = JobExecutor(function_registry=registry, logger=MagicMock())

        assert executor.is_async("sync") is False

    def test_is_async_unregistered(self):
        """An unregistered function name should return False."""
        executor = JobExecutor(function_registry={}, logger=MagicMock())

        assert executor.is_async("nonexistent") is False
