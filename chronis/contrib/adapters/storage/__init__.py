"""Contributed storage adapters for Chronis."""

__all__ = []

# Redis storage adapter (optional)
try:
    from chronis.contrib.adapters.storage.redis import RedisStorage

    __all__.append("RedisStorage")
except ImportError:
    RedisStorage = None  # type: ignore[assignment, misc]

# PostgreSQL storage adapter (optional)
try:
    from chronis.contrib.adapters.storage.postgres import PostgreSQLStorage

    __all__.append("PostgreSQLStorage")
except ImportError:
    PostgreSQLStorage = None  # type: ignore[assignment, misc]
