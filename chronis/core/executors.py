"""Job executors for handling sync and async job execution."""

import asyncio
import inspect
import logging
import threading
import time
from typing import Any, Callable

from chronis.core.registry import JobRegistry
from chronis.utils.logging import ContextLogger


class AsyncExecutor:
    """
    Manages dedicated event loop for async job execution.

    Runs a persistent event loop in a background thread, allowing
    efficient execution of async functions without the overhead of
    creating/destroying event loops for each execution.

    Usage:
        >>> executor = AsyncExecutor()
        >>> executor.start()
        >>> result = executor.execute_coroutine(my_async_func())
        >>> executor.stop()
    """

    def __init__(self, logger: logging.Logger | None = None):
        """
        Initialize AsyncExecutor.

        Args:
            logger: Optional logger for debugging
        """
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._logger = logger

    def start(self) -> None:
        """
        Start dedicated event loop in background thread.

        Raises:
            RuntimeError: If event loop is already running
        """
        if self._loop is not None:
            if self._logger:
                self._logger.warning("Async loop already running")
            raise RuntimeError("AsyncExecutor is already running")

        # Create new event loop
        self._loop = asyncio.new_event_loop()

        # Start event loop in dedicated thread
        self._thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
            name="chronis-async-loop"
        )
        self._thread.start()

        if self._logger:
            self._logger.debug("Started dedicated event loop for async jobs")

    def _run_event_loop(self) -> None:
        """Run event loop forever (called in dedicated thread)."""
        if self._loop is None:
            return

        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            self._loop.close()

    def execute_coroutine(self, coro) -> Any:
        """
        Execute coroutine in the dedicated event loop.

        Args:
            coro: Coroutine to execute

        Returns:
            Result of coroutine execution

        Raises:
            RuntimeError: If event loop is not running
        """
        if self._loop is None:
            raise RuntimeError("AsyncExecutor is not running. Call start() first.")

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def stop(self) -> None:
        """
        Stop dedicated event loop.

        Waits for the loop thread to finish (with timeout).
        """
        if self._loop is None:
            return

        # Stop the event loop
        self._loop.call_soon_threadsafe(self._loop.stop)

        # Wait for loop thread to finish
        if self._thread is not None:
            self._thread.join(timeout=5.0)

        self._loop = None
        self._thread = None

        if self._logger:
            self._logger.debug("Stopped dedicated event loop")

    def is_running(self) -> bool:
        """Check if event loop is running."""
        return self._loop is not None and not self._loop.is_closed()


class JobExecutor:
    """
    Handles execution of job functions (both sync and async).

    Coordinates with AsyncExecutor for async functions and executes
    sync functions directly. Provides timing and error handling.

    Usage:
        >>> registry = JobRegistry()
        >>> async_executor = AsyncExecutor()
        >>> executor = JobExecutor(registry, async_executor)
        >>> executor.execute(job_data, logger)
    """

    def __init__(
        self,
        registry: JobRegistry,
        async_executor: AsyncExecutor | None = None
    ):
        """
        Initialize JobExecutor.

        Args:
            registry: JobRegistry containing registered functions
            async_executor: AsyncExecutor for async function execution (optional)
        """
        self.registry = registry
        self.async_executor = async_executor

    def execute(self, job_data: dict[str, Any], logger: ContextLogger) -> None:
        """
        Execute job function (supports both sync and async).

        Args:
            job_data: Job data containing func_name, args, kwargs
            logger: Context logger for this job

        Raises:
            ValueError: If function is not registered
            RuntimeError: If async function but no AsyncExecutor
            Exception: Any exception raised by the job function
        """
        func_name = job_data.get("func_name")
        args = job_data.get("args", ())
        kwargs = job_data.get("kwargs", {})

        logger.debug(
            "Executing job function",
            func_name=func_name,
            args_count=len(args),
            kwargs_keys=list(kwargs.keys()),
        )

        # Get registered function
        func = self.registry.get(func_name)
        if not func:
            logger.error(f"Function not registered: {func_name}")
            raise ValueError(f"Function {func_name} not registered")

        # Execute with timing
        start_time = time.time()
        is_async = inspect.iscoroutinefunction(func)

        try:
            # Execute based on function type
            if is_async:
                result = self._execute_async(func, args, kwargs)
            else:
                result = self._execute_sync(func, args, kwargs)

            execution_time = time.time() - start_time
            logger.info(
                "Job function completed",
                func_name=func_name,
                execution_time_seconds=round(execution_time, 3),
                is_async=is_async,
            )

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Job function raised exception: {e}",
                exc_info=True,
                func_name=func_name,
                execution_time_seconds=round(execution_time, 3),
                error_type=type(e).__name__,
            )
            raise

    def _execute_sync(self, func: Callable, args: tuple, kwargs: dict) -> Any:
        """Execute synchronous function."""
        return func(*args, **kwargs)

    def _execute_async(self, func: Callable, args: tuple, kwargs: dict) -> Any:
        """
        Execute asynchronous function.

        Raises:
            RuntimeError: If AsyncExecutor is not configured
        """
        if self.async_executor is None:
            raise RuntimeError(
                "Cannot execute async function: AsyncExecutor not configured"
            )

        if not self.async_executor.is_running():
            raise RuntimeError(
                "Cannot execute async function: AsyncExecutor is not running"
            )

        coro = func(*args, **kwargs)
        return self.async_executor.execute_coroutine(coro)
