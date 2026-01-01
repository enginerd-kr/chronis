"""In-memory lock adapter for testing."""

import threading
import time
import uuid

from chronis.adapters.base import LockAdapter


class InMemoryLockAdapter(LockAdapter):
    """
    In-memory lock adapter with ownership tracking (for local development/testing).

    Features:
    - Lock ownership tracking via UUID tokens
    - Thread-safe operations using threading.Lock
    - Condition.wait() based efficient blocking mode
    - TTL simulation with expiry timestamps
    - Full feature parity with Redis adapter

    Note:
        This adapter is for testing and local development only.
        For production use, switch to Redis or DynamoDB adapters.

    Example:
        >>> lock = InMemoryLockAdapter()
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
        ...     lock.extend("my-lock", ttl_seconds=60)  # Extend TTL
        ...     lock.release("my-lock")
    """

    def __init__(self) -> None:
        """Initialize in-memory lock adapter."""
        # Store locks as: lock_key -> (owner_id, expiry_time)
        self._locks: dict[str, tuple[str, float]] = {}
        self._mutex = threading.Lock()  # Global lock for _locks dict
        # Store conditions for blocking: lock_key -> Condition
        self._conditions: dict[str, threading.Condition] = {}
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
        Acquire in-memory lock with ownership tracking.

        Implementation:
        - Stores (owner_id, expiry_time) tuple
        - Thread-safe using threading.Lock
        - Blocking mode uses Condition.wait() (no spinloop)

        Args:
            lock_key: Lock identifier
            ttl_seconds: Lock expiry time in seconds
            blocking: If True, wait for lock to become available
            timeout: Max wait time in seconds (None = wait forever)
            owner_id: Optional owner identifier (uses instance token if None)

        Returns:
            True if lock acquired, False otherwise
        """
        token = owner_id or self.instance_token
        expiry_time = time.time() + ttl_seconds

        # Ensure condition exists
        with self._mutex:
            if lock_key not in self._conditions:
                self._conditions[lock_key] = threading.Condition(self._mutex)

        condition = self._conditions[lock_key]

        # Acquire lock with condition
        with condition:
            # Clean up expired lock
            self._cleanup_expired_lock(lock_key)

            # Try to acquire
            if lock_key not in self._locks:
                self._locks[lock_key] = (token, expiry_time)
                return True

            if not blocking:
                return False

            # Blocking mode: wait with timeout
            start_time = time.time()
            remaining_timeout = timeout

            while lock_key in self._locks:
                # Check timeout
                if timeout is not None:
                    elapsed = time.time() - start_time
                    remaining_timeout = timeout - elapsed
                    if remaining_timeout <= 0:
                        return False

                # Wait for signal (released by release())
                condition.wait(timeout=remaining_timeout)

                # Clean up expired lock
                self._cleanup_expired_lock(lock_key)

                # Retry acquisition
                if lock_key not in self._locks:
                    self._locks[lock_key] = (token, expiry_time)
                    return True

        return False

    def release(self, lock_key: str, owner_id: str | None = None) -> bool:
        """
        Release in-memory lock with ownership verification.

        Implementation:
        - Verifies owner_id matches before releasing
        - Thread-safe using threading.Lock
        - Signals waiting threads via Condition.notify()

        Args:
            lock_key: Lock identifier
            owner_id: Owner identifier (uses instance token if None)

        Returns:
            True if lock released, False if lock doesn't exist or not owner
        """
        token = owner_id or self.instance_token

        if lock_key not in self._conditions:
            return False

        condition = self._conditions[lock_key]

        with condition:
            if lock_key not in self._locks:
                return False

            stored_owner, _ = self._locks[lock_key]

            # Verify ownership
            if stored_owner != token:
                return False

            # Release lock
            del self._locks[lock_key]

            # Signal one waiting thread
            condition.notify()

            return True

    def extend(
        self,
        lock_key: str,
        ttl_seconds: int,
        owner_id: str | None = None,
    ) -> bool:
        """
        Extend lock TTL with ownership verification.

        Implementation:
        - Verifies owner_id matches before extending
        - Updates expiry_time atomically

        Args:
            lock_key: Lock identifier
            ttl_seconds: New TTL duration in seconds
            owner_id: Owner identifier (uses instance token if None)

        Returns:
            True if extended, False if lock doesn't exist or not owner
        """
        token = owner_id or self.instance_token
        new_expiry_time = time.time() + ttl_seconds

        with self._mutex:
            if lock_key not in self._locks:
                return False

            stored_owner, _ = self._locks[lock_key]

            # Verify ownership
            if stored_owner != token:
                return False

            # Extend TTL
            self._locks[lock_key] = (stored_owner, new_expiry_time)
            return True

    def reset(self, lock_key: str) -> bool:
        """
        Forcibly delete lock without ownership verification.

        ⚠️ WARNING: Bypasses ownership checks! Use with caution.

        Args:
            lock_key: Lock identifier

        Returns:
            True if deleted, False if lock didn't exist
        """
        if lock_key not in self._conditions:
            return False

        condition = self._conditions[lock_key]

        with condition:
            if lock_key in self._locks:
                del self._locks[lock_key]
                # Signal all waiting threads
                condition.notify_all()
                return True
            return False

    def _cleanup_expired_lock(self, lock_key: str) -> None:
        """
        Clean up expired lock (must be called with condition held).

        Args:
            lock_key: Lock identifier to check
        """
        if lock_key in self._locks:
            _, expiry_time = self._locks[lock_key]
            if expiry_time < time.time():
                del self._locks[lock_key]
