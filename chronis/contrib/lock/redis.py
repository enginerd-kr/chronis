"""Redis-based distributed lock adapter."""

from typing import Any

from chronis.adapters.base import LockAdapter


class RedisLockAdapter(LockAdapter):
    """
    Redis-based distributed lock adapter.

    This implementation uses Redis SET with NX (set if not exists) and EX (expiry)
    for atomic lock acquisition with automatic expiration.

    Design Decisions:
    - Uses simple SET/DEL operations (no Lua scripts)
    - Lock value is a simple "1" (no lock ownership tracking)
    - Relies on TTL for automatic cleanup
    - No blocking mode implementation (blocking=True returns same as False)

    Advanced Features (Optional):
    - Consider using python-redis-lock for more features
    - Add lock ownership tracking if needed
    - Implement blocking mode with BLPOP pattern
    - Add monitoring for lock contention

    Example:
        >>> import redis
        >>> client = redis.Redis(host='localhost', port=6379, db=0)
        >>> lock = RedisLockAdapter(client)
        >>> acquired = lock.acquire("my-lock", ttl_seconds=300)
        >>> if acquired:
        ...     try:
        ...         # Do work
        ...         pass
        ...     finally:
        ...         lock.release("my-lock")
    """

    def __init__(self, redis_client: Any, key_prefix: str = "chronis:lock:") -> None:
        """
        Initialize Redis lock adapter.

        Args:
            redis_client: Redis client instance (redis.Redis or redis.asyncio.Redis)
            key_prefix: Prefix for lock keys (default: "chronis:lock:")

        Example:
            >>> import redis
            >>> client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            >>> lock = RedisLockAdapter(client)
        """
        self.redis = redis_client
        self.key_prefix = key_prefix

    def acquire(self, lock_key: str, ttl_seconds: int, blocking: bool = False) -> bool:
        """
        Acquire distributed lock using Redis SET NX EX.

        Uses Redis atomic SET operation with:
        - NX: Only set if key doesn't exist
        - EX: Set expiry time in seconds

        Args:
            lock_key: Lock identifier (will be prefixed)
            ttl_seconds: Lock expiry time in seconds
            blocking: Not implemented (always non-blocking)

        Returns:
            True if lock acquired, False otherwise

        Implementation:
            SET {prefix}{lock_key} "1" NX EX {ttl_seconds}
        """
        full_key = f"{self.key_prefix}{lock_key}"

        # SET with NX (not exists) and EX (expiry)
        # Returns True if key was set, False if key already existed
        result = self.redis.set(
            name=full_key,
            value="1",
            nx=True,  # Only set if not exists
            ex=ttl_seconds,  # Expiry in seconds
        )

        return bool(result)

    def release(self, lock_key: str) -> bool:
        """
        Release distributed lock.

        Args:
            lock_key: Lock identifier (will be prefixed)

        Returns:
            True if lock was released, False if lock didn't exist

        Implementation:
            DEL {prefix}{lock_key}
        """
        full_key = f"{self.key_prefix}{lock_key}"

        # DEL returns number of keys deleted (0 or 1)
        result = self.redis.delete(full_key)

        return result > 0
