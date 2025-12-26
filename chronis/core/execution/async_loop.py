"""Async event loop management for job execution."""

import asyncio
import logging
import threading


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
            target=self._run_event_loop, daemon=True, name="chronis-async-loop"
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

    def execute_coroutine(self, coro) -> asyncio.Future:
        """
        Execute coroutine in the dedicated event loop (non-blocking).

        This method schedules the coroutine for execution without waiting
        for it to complete, allowing the calling thread to continue immediately.

        Args:
            coro: Coroutine to execute

        Returns:
            Future object representing the coroutine execution.
            Use future.result() to wait for completion if needed.

        Raises:
            RuntimeError: If event loop is not running

        Example:
            >>> async def my_task():
            ...     await asyncio.sleep(1)
            ...     return "done"
            >>> future = executor.execute_coroutine(my_task())
            >>> # Continue working while task runs
            >>> result = future.result()  # Wait only if you need the result
        """
        if self._loop is None:
            raise RuntimeError("AsyncExecutor is not running. Call start() first.")

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future

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
