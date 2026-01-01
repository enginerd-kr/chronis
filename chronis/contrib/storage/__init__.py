"""Contributed storage adapters for Chronis."""

__all__ = []

# Redis storage adapter (optional)
try:
    from chronis.contrib.storage.redis import RedisStorageAdapter

    __all__.append("RedisStorageAdapter")
except ImportError:
    RedisStorageAdapter = None  # type: ignore[assignment, misc]
