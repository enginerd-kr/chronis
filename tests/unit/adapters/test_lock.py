"""Unit tests for Lock adapters."""

import time

import pytest

from chronis.adapters.lock import InMemoryLockAdapter


@pytest.fixture
def lock_adapter():
    """Create in-memory lock adapter."""
    return InMemoryLockAdapter()


class TestInMemoryLockAdapter:
    """Unit tests for InMemoryLockAdapter basic operations."""

    def test_acquire_lock(self, lock_adapter):
        """Test acquiring a lock."""
        result = lock_adapter.acquire("test-lock", ttl_seconds=5)
        assert result is True

    def test_acquire_already_held_lock(self, lock_adapter):
        """Test acquiring already held lock returns False."""
        lock_adapter.acquire("test-lock", ttl_seconds=5)
        result = lock_adapter.acquire("test-lock", ttl_seconds=5)
        assert result is False

    def test_release_lock(self, lock_adapter):
        """Test releasing a lock."""
        lock_adapter.acquire("test-lock", ttl_seconds=5)
        result = lock_adapter.release("test-lock")
        assert result is True

    def test_release_non_held_lock(self, lock_adapter):
        """Test releasing non-held lock returns False."""
        result = lock_adapter.release("test-lock")
        assert result is False

    def test_lock_expiration(self, lock_adapter):
        """Test lock expires after TTL."""
        lock_adapter.acquire("test-lock", ttl_seconds=1)

        # Wait for expiration
        time.sleep(1.1)

        # Should be able to acquire again
        result = lock_adapter.acquire("test-lock", ttl_seconds=5)
        assert result is True

    def test_multiple_locks(self, lock_adapter):
        """Test multiple different locks can be held."""
        result1 = lock_adapter.acquire("lock-1", ttl_seconds=5)
        result2 = lock_adapter.acquire("lock-2", ttl_seconds=5)

        assert result1 is True
        assert result2 is True

    def test_lock_reacquire_after_release(self, lock_adapter):
        """Test lock can be reacquired after release."""
        lock_adapter.acquire("test-lock", ttl_seconds=5)
        lock_adapter.release("test-lock")

        result = lock_adapter.acquire("test-lock", ttl_seconds=5)
        assert result is True
