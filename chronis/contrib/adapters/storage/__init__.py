"""Contributed storage adapters for Chronis."""

__all__ = []

# Redis storage adapter (optional)
try:
    from chronis.contrib.adapters.storage.redis import RedisStorageAdapter

    __all__.append("RedisStorageAdapter")
except ImportError:
    RedisStorageAdapter = None  # type: ignore[assignment, misc]

# PostgreSQL storage adapter (optional)
try:
    from chronis.contrib.adapters.storage.postgres import PostgreSQLStorageAdapter

    __all__.append("PostgreSQLStorageAdapter")
except ImportError:
    PostgreSQLStorageAdapter = None  # type: ignore[assignment, misc]
