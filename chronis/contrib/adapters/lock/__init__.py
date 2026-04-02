"""Contributed lock adapters for Chronis."""

__all__ = []

# Redis lock adapter (optional)
try:
    from chronis.contrib.adapters.lock.redis import RedisLock

    __all__.append("RedisLock")
except ImportError:
    RedisLock = None  # type: ignore[assignment, misc]
