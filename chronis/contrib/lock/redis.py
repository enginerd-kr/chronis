"""Redis-based distributed lock adapter."""

import time
import uuid
from typing import Any

from chronis.adapters.base import LockAdapter

# Lua script for atomic release with ownership verification and signaling
LUA_RELEASE_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    redis.call('del', KEYS[1])
    redis.call('lpush', KEYS[2], '1')
    redis.call('expire', KEYS[2], 1)
    return 1
"""

# Lua script for atomic extend with ownership verification
LUA_EXTEND_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    redis.call('expire', KEYS[1], ARGV[2])
    return 1
"""


class RedisLockAdapter(LockAdapter):
    """
    Redis-based distributed lock adapter with ownership tracking.

    Features:
    - Lock ownership tracking via UUID tokens
    - Atomic operations using Lua scripts
    - BLPOP-based efficient blocking mode
    - TTL extension for long-running jobs
    - Signal mechanism for waiting processes

    Security:
    - Prevents accidental release of other process's locks
    - Atomic check-and-delete operations
    - Owner verification on all operations

    Example:
        >>> import redis
        >>> client = redis.Redis(host='localhost', port=6379, db=0)
        >>> lock = RedisLockAdapter(client)
        >>>
        >>> # Non-blocking acquire
        >>> if lock.acquire("my-lock", ttl_seconds=60):
        ...     try:
        ...         # Do work
        ...         pass
        ...     finally:
        ...         lock.release("my-lock")
        >>>
        >>> # Blocking acquire with timeout
        >>> if lock.acquire("my-lock", ttl_seconds=60, blocking=True, timeout=10):
        ...     # Got the lock within 10 seconds
        ...     lock.extend("my-lock", ttl_seconds=60)  # Extend TTL
        ...     lock.release("my-lock")
    """

    def __init__(self, redis_client: Any, key_prefix: str = "chronis:lock:") -> None:
        """
        Initialize Redis lock adapter.

        Args:
            redis_client: Redis client instance (redis.Redis)
            key_prefix: Prefix for lock keys (default: "chronis:lock:")

        Example:
            >>> import redis
            >>> client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            >>> lock = RedisLockAdapter(client)
        """
        self.redis = redis_client
        self.key_prefix = key_prefix
        self.instance_token = str(uuid.uuid4())  # Unique instance identifier

    def acquire(
        self,
        lock_key: str,
        ttl_seconds: int,
        blocking: bool = False,
        timeout: float | None = None,
        owner_id: str | None = None,
    ) -> bool:
        """
        Acquire distributed lock using Redis SET NX EX with ownership token.

        Implementation:
        - Uses Redis SET with NX (not exists) and EX (expiry)
        - Stores owner_id as value for ownership verification
        - Blocking mode uses BLPOP on signal key (no spinloop!)

        Args:
            lock_key: Lock identifier (will be prefixed)
            ttl_seconds: Lock expiry time in seconds
            blocking: If True, wait for lock to become available
            timeout: Max wait time in seconds (None = wait forever)
            owner_id: Optional owner identifier (uses instance token if None)

        Returns:
            True if lock acquired, False otherwise
        """
        full_key = f"{self.key_prefix}{lock_key}"
        signal_key = f"{full_key}:signal"
        token = owner_id or self.instance_token

        # Try to acquire lock
        result = self.redis.set(
            name=full_key,
            value=token,  # Store owner token for verification
            nx=True,
            ex=ttl_seconds,
        )

        if result or not blocking:
            return bool(result)

        # Blocking mode: wait for signal
        start_time = time.time()
        remaining_timeout = timeout

        while True:
            # Wait for signal using BLPOP (efficient, no spinloop)
            result = self.redis.blpop(signal_key, timeout=remaining_timeout or 0)

            # Retry acquisition
            acquired = self.redis.set(
                name=full_key,
                value=token,
                nx=True,
                ex=ttl_seconds,
            )

            if acquired:
                return True

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                remaining_timeout = timeout - elapsed
                if remaining_timeout <= 0:
                    return False

    def release(self, lock_key: str, owner_id: str | None = None) -> bool:
        """
        Release distributed lock with ownership verification.

        Implementation:
        - Uses Lua script for atomic GET + DEL + LPUSH
        - Verifies owner_id matches before deleting
        - Signals waiting processes via LPUSH to signal key

        Args:
            lock_key: Lock identifier (will be prefixed)
            owner_id: Owner identifier (uses instance token if None)

        Returns:
            True if lock released, False if lock doesn't exist or not owner
        """
        full_key = f"{self.key_prefix}{lock_key}"
        signal_key = f"{full_key}:signal"
        token = owner_id or self.instance_token

        # Atomic release with ownership verification using Lua
        result = self.redis.eval(
            LUA_RELEASE_SCRIPT,
            2,  # num keys
            full_key,  # KEYS[1]
            signal_key,  # KEYS[2]
            token,  # ARGV[1]
        )

        return result == 1

    def extend(
        self,
        lock_key: str,
        ttl_seconds: int,
        owner_id: str | None = None,
    ) -> bool:
        """
        Extend lock TTL with ownership verification.

        Implementation:
        - Uses Lua script for atomic GET + EXPIRE
        - Verifies owner_id matches before extending

        Args:
            lock_key: Lock identifier (will be prefixed)
            ttl_seconds: New TTL duration in seconds
            owner_id: Owner identifier (uses instance token if None)

        Returns:
            True if extended, False if lock doesn't exist or not owner
        """
        full_key = f"{self.key_prefix}{lock_key}"
        token = owner_id or self.instance_token

        # Atomic extend with ownership verification using Lua
        result = self.redis.eval(
            LUA_EXTEND_SCRIPT,
            1,  # num keys
            full_key,  # KEYS[1]
            token,  # ARGV[1]
            ttl_seconds,  # ARGV[2]
        )

        return result == 1

    def reset(self, lock_key: str) -> bool:
        """
        Forcibly delete lock without ownership verification.

        ⚠️ WARNING: Bypasses ownership checks! Use with caution.

        Args:
            lock_key: Lock identifier (will be prefixed)

        Returns:
            True if deleted, False if lock didn't exist
        """
        full_key = f"{self.key_prefix}{lock_key}"
        deleted = self.redis.delete(full_key)
        return deleted > 0
