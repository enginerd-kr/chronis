"""Contributed lock adapters for Chronis."""

__all__ = []

# Redis lock adapter (optional)
try:
    from chronis.contrib.lock.redis import RedisLockAdapter

    __all__.append("RedisLockAdapter")
except ImportError:
    RedisLockAdapter = None  # type: ignore[assignment, misc]
