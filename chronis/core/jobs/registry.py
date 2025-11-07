"""Job function registry for managing registered job functions."""

import threading
from typing import Callable


class JobRegistry:
    """
    Thread-safe registry for job functions.

    Manages the mapping between function names and callable objects,
    providing thread-safe access to registered functions.

    Usage:
        >>> registry = JobRegistry()
        >>> registry.register("send_email", send_email_func)
        >>> func = registry.get("send_email")
    """

    def __init__(self):
        """Initialize empty registry with thread lock."""
        self._functions: dict[str, Callable] = {}
        self._lock = threading.RLock()

    def register(self, name: str, func: Callable) -> None:
        """
        Register a job function (thread-safe).

        Args:
            name: Function name (e.g., "my_module.my_job")
            func: Function object

        Example:
            >>> def send_email():
            ...     print("Sending email...")
            >>> registry.register("send_email", send_email)
        """
        with self._lock:
            self._functions[name] = func

    def get(self, name: str) -> Callable | None:
        """
        Get registered function (thread-safe).

        Args:
            name: Function name

        Returns:
            Function object or None if not found
        """
        with self._lock:
            return self._functions.get(name)

    def unregister(self, name: str) -> bool:
        """
        Unregister a function (thread-safe).

        Args:
            name: Function name

        Returns:
            True if function was removed, False if not found
        """
        with self._lock:
            if name in self._functions:
                del self._functions[name]
                return True
            return False

    def list_functions(self) -> list[str]:
        """
        List all registered function names (thread-safe).

        Returns:
            List of function names
        """
        with self._lock:
            return list(self._functions.keys())

    def clear(self) -> None:
        """Clear all registered functions (thread-safe)."""
        with self._lock:
            self._functions.clear()

    def __len__(self) -> int:
        """Return number of registered functions."""
        with self._lock:
            return len(self._functions)

    def __contains__(self, name: str) -> bool:
        """Check if function is registered."""
        with self._lock:
            return name in self._functions
