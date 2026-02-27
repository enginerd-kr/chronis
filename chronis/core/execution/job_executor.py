"""Job function execution with sync/async dispatch."""

import asyncio
import inspect
import threading
from collections.abc import Callable
from typing import Any

from chronis.core.common.exceptions import FunctionNotRegisteredError
from chronis.utils.logging import ContextLogger


class JobExecutor:
    """
    Handles the mechanics of running job functions (sync and async).

    Separated from ExecutionCoordinator to isolate "how to invoke a function"
    from "how to manage job lifecycle" (CAS, locks, retries, callbacks).
    """

    def __init__(
        self,
        function_registry: dict[str, Callable],
        logger: ContextLogger,
    ) -> None:
        self.function_registry = function_registry
        self.logger = logger

        # Shared event loop for async job execution (lazy-initialized)
        self._async_loop: asyncio.AbstractEventLoop | None = None
        self._async_thread: threading.Thread | None = None
        self._async_lock = threading.Lock()

    def is_async(self, func_name: str) -> bool:
        """Check if a registered function is async."""
        func = self.function_registry.get(func_name)
        return func is not None and inspect.iscoroutinefunction(func)

    def execute_sync(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute sync job function.

        Timeout is handled at the coordinator level via threading.Timer,
        not here. This avoids creating zombie threads that can never be killed.

        Raises:
            FunctionNotRegisteredError: If function not found
        """
        func_name = job_data["func_name"]
        args = tuple(job_data.get("args", []))
        kwargs = job_data.get("kwargs", {})

        func = self.function_registry.get(func_name)
        if not func:
            raise FunctionNotRegisteredError(
                f"Function '{func_name}' is not registered. "
                "Call scheduler.register_job_function(name, func) before creating jobs."
            )

        func(*args, **kwargs)

    async def execute_async(self, job_data: dict[str, Any], job_logger: ContextLogger) -> None:
        """
        Execute async job function with optional timeout.

        Raises:
            FunctionNotRegisteredError: If function not found
            asyncio.TimeoutError: If execution exceeds timeout
        """
        func_name = job_data["func_name"]
        args = tuple(job_data.get("args", []))
        kwargs = job_data.get("kwargs", {})
        timeout_seconds = job_data.get("timeout_seconds")

        func = self.function_registry.get(func_name)
        if not func:
            raise FunctionNotRegisteredError(
                f"Function '{func_name}' is not registered. "
                "Call scheduler.register_job_function(name, func) before creating jobs."
            )

        coro = func(*args, **kwargs)
        if timeout_seconds:
            await asyncio.wait_for(coro, timeout=timeout_seconds)
        else:
            await coro

    def ensure_async_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create the shared event loop for async jobs (thread-safe)."""
        if self._async_loop is None or self._async_loop.is_closed():
            with self._async_lock:
                # Double-checked locking to avoid redundant loop creation
                if self._async_loop is None or self._async_loop.is_closed():
                    self._async_loop = asyncio.new_event_loop()
                    self._async_thread = threading.Thread(
                        target=self._async_loop.run_forever,
                        daemon=True,
                        name="chronis-async",
                    )
                    self._async_thread.start()
        return self._async_loop

    def shutdown_async(self, wait: bool = True) -> None:
        """
        Shut down the shared async event loop.

        Args:
            wait: If True, wait for running async jobs to complete.
        """
        if self._async_loop is None or self._async_loop.is_closed():
            return

        if wait:

            async def _drain() -> None:
                tasks = [
                    t
                    for t in asyncio.all_tasks(self._async_loop)
                    if t is not asyncio.current_task()
                ]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            try:
                future = asyncio.run_coroutine_threadsafe(_drain(), self._async_loop)
                future.result(timeout=60)
            except Exception as e:
                self.logger.warning("Async drain failed during shutdown", error=str(e))

        self._async_loop.call_soon_threadsafe(self._async_loop.stop)
        if self._async_thread:
            self._async_thread.join(timeout=5)
        self._async_loop.close()
        self._async_loop = None
        self._async_thread = None
