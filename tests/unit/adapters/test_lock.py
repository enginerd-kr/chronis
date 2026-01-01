"""Unit tests for Lock adapters (including concurrency scenarios)."""

import threading
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


class TestLockOwnership:
    """Tests for lock ownership tracking and verification."""

    def test_different_owner_cannot_release(self, lock_adapter):
        """Test that different owner cannot release lock."""
        # Adapter 1 acquires lock
        owner1 = "owner-1"
        lock_adapter.acquire("test-lock", ttl_seconds=60, owner_id=owner1)

        # Adapter 2 tries to release (should fail)
        owner2 = "owner-2"
        result = lock_adapter.release("test-lock", owner_id=owner2)
        assert result is False

        # Lock should still be held
        result = lock_adapter.acquire("test-lock", ttl_seconds=60)
        assert result is False

    def test_same_owner_can_release(self, lock_adapter):
        """Test that same owner can release lock."""
        owner = "owner-1"
        lock_adapter.acquire("test-lock", ttl_seconds=60, owner_id=owner)

        # Same owner can release
        result = lock_adapter.release("test-lock", owner_id=owner)
        assert result is True

    def test_instance_token_ownership(self, lock_adapter):
        """Test instance token is used for ownership when owner_id not provided."""
        # Acquire without explicit owner_id (uses instance token)
        lock_adapter.acquire("test-lock", ttl_seconds=60)

        # Release without owner_id (uses same instance token)
        result = lock_adapter.release("test-lock")
        assert result is True

    def test_different_instances_cannot_interfere(self):
        """Test different lock adapter instances cannot interfere with each other's locks."""
        adapter1 = InMemoryLockAdapter()
        adapter2 = InMemoryLockAdapter()

        # Adapter 1 acquires lock
        adapter1.acquire("test-lock", ttl_seconds=60)

        # Adapter 2 cannot release adapter 1's lock
        result = adapter2.release("test-lock")
        assert result is False

    def test_different_owner_cannot_extend(self, lock_adapter):
        """Test that different owner cannot extend lock TTL."""
        owner1 = "owner-1"
        lock_adapter.acquire("test-lock", ttl_seconds=60, owner_id=owner1)

        # Different owner tries to extend
        owner2 = "owner-2"
        result = lock_adapter.extend("test-lock", ttl_seconds=120, owner_id=owner2)
        assert result is False


class TestLockExtend:
    """Tests for lock TTL extension."""

    def test_extend_lock_ttl(self, lock_adapter):
        """Test extending lock TTL."""
        lock_adapter.acquire("test-lock", ttl_seconds=2)

        # Extend TTL
        result = lock_adapter.extend("test-lock", ttl_seconds=10)
        assert result is True

        # Wait for original TTL to expire
        time.sleep(2.5)

        # Lock should still be held (extended)
        result = lock_adapter.acquire("test-lock", ttl_seconds=5)
        assert result is False

    def test_extend_non_existent_lock(self, lock_adapter):
        """Test extending non-existent lock returns False."""
        result = lock_adapter.extend("test-lock", ttl_seconds=60)
        assert result is False

    def test_extend_with_ownership_verification(self, lock_adapter):
        """Test extend verifies ownership."""
        owner = "owner-1"
        lock_adapter.acquire("test-lock", ttl_seconds=60, owner_id=owner)

        # Same owner can extend
        result = lock_adapter.extend("test-lock", ttl_seconds=120, owner_id=owner)
        assert result is True

        # Different owner cannot extend
        result = lock_adapter.extend("test-lock", ttl_seconds=180, owner_id="owner-2")
        assert result is False

    def test_extend_updates_expiry_time(self, lock_adapter):
        """Test extend actually updates expiry time."""
        lock_adapter.acquire("test-lock", ttl_seconds=1)

        # Extend before expiry
        time.sleep(0.5)
        lock_adapter.extend("test-lock", ttl_seconds=2)

        # Original TTL would have expired by now
        time.sleep(1)

        # But lock should still be held
        result = lock_adapter.acquire("test-lock", ttl_seconds=5)
        assert result is False


class TestBlockingMode:
    """Tests for blocking lock acquisition."""

    def test_blocking_acquire_waits_for_release(self, lock_adapter):
        """Test blocking acquire waits for lock to be released."""
        # Acquire lock
        lock_adapter.acquire("test-lock", ttl_seconds=60)

        def release_after_delay():
            time.sleep(0.5)
            lock_adapter.release("test-lock")

        # Start thread to release lock
        thread = threading.Thread(target=release_after_delay)
        thread.start()

        # Blocking acquire should wait and succeed
        start = time.time()
        result = lock_adapter.acquire("test-lock", ttl_seconds=60, blocking=True, timeout=2)
        elapsed = time.time() - start

        thread.join()

        assert result is True
        assert 0.4 < elapsed < 1.0  # Should wait ~0.5s

    def test_blocking_acquire_timeout(self, lock_adapter):
        """Test blocking acquire times out if lock not released."""
        # Acquire lock (not released)
        lock_adapter.acquire("test-lock", ttl_seconds=60)

        # Blocking acquire with timeout should fail
        start = time.time()
        result = lock_adapter.acquire("test-lock", ttl_seconds=60, blocking=True, timeout=0.5)
        elapsed = time.time() - start

        assert result is False
        assert 0.4 < elapsed < 0.7  # Should timeout around 0.5s

    def test_blocking_acquire_no_timeout(self, lock_adapter):
        """Test blocking acquire without timeout waits indefinitely."""
        lock_adapter.acquire("test-lock", ttl_seconds=60)

        def release_after_delay():
            time.sleep(1.0)
            lock_adapter.release("test-lock")

        thread = threading.Thread(target=release_after_delay)
        thread.start()

        # Should wait for release (no timeout)
        result = lock_adapter.acquire("test-lock", ttl_seconds=60, blocking=True, timeout=None)

        thread.join()

        assert result is True

    def test_non_blocking_returns_immediately(self, lock_adapter):
        """Test non-blocking acquire returns immediately."""
        lock_adapter.acquire("test-lock", ttl_seconds=60)

        # Non-blocking should return False immediately
        start = time.time()
        result = lock_adapter.acquire("test-lock", ttl_seconds=60, blocking=False)
        elapsed = time.time() - start

        assert result is False
        assert elapsed < 0.1  # Should be almost immediate


class TestLockReset:
    """Tests for forceful lock reset."""

    def test_reset_removes_lock(self, lock_adapter):
        """Test reset forcefully removes lock."""
        lock_adapter.acquire("test-lock", ttl_seconds=60)

        # Reset bypasses ownership
        result = lock_adapter.reset("test-lock")
        assert result is True

        # Lock should be available
        result = lock_adapter.acquire("test-lock", ttl_seconds=60)
        assert result is True

    def test_reset_non_existent_lock(self, lock_adapter):
        """Test reset on non-existent lock returns False."""
        result = lock_adapter.reset("test-lock")
        assert result is False

    def test_reset_signals_waiting_threads(self, lock_adapter):
        """Test reset signals waiting threads."""
        lock_adapter.acquire("test-lock", ttl_seconds=60)

        acquired = [False]

        def blocking_acquire():
            result = lock_adapter.acquire("test-lock", ttl_seconds=60, blocking=True, timeout=2)
            acquired[0] = result

        # Start blocking thread
        thread = threading.Thread(target=blocking_acquire)
        thread.start()

        time.sleep(0.2)  # Let thread start blocking

        # Reset should wake up the waiting thread
        lock_adapter.reset("test-lock")

        thread.join(timeout=1.0)

        assert acquired[0] is True


class TestRaceConditions:
    """Tests for race conditions in concurrent scenarios."""

    def test_concurrent_acquire_only_one_succeeds(self, lock_adapter):
        """Test only one thread succeeds when multiple try to acquire concurrently."""
        num_threads = 10
        success_count = [0]
        lock_obj = threading.Lock()

        def try_acquire():
            if lock_adapter.acquire("test-lock", ttl_seconds=5):
                with lock_obj:
                    success_count[0] += 1
                time.sleep(0.1)  # Hold lock briefly

        threads = [threading.Thread(target=try_acquire) for _ in range(num_threads)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # Only one thread should have acquired the lock
        assert success_count[0] == 1

    def test_release_and_acquire_race(self, lock_adapter):
        """Test release and acquire race condition."""
        iterations = 100
        errors = [0]

        def worker():
            for _ in range(iterations):
                if lock_adapter.acquire("test-lock", ttl_seconds=1):
                    time.sleep(0.001)
                    if not lock_adapter.release("test-lock"):
                        errors[0] += 1

        threads = [threading.Thread(target=worker) for _ in range(3)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # No errors should occur
        assert errors[0] == 0

    def test_ownership_prevents_cross_thread_release(self):
        """Test ownership prevents thread from releasing another thread's lock with different owner_id."""
        adapter = InMemoryLockAdapter()

        def thread1_work(results):
            # Thread 1 acquires with explicit owner_id
            adapter.acquire("test-lock", ttl_seconds=60, owner_id="thread-1")
            time.sleep(0.5)
            # Thread 1 still holds lock
            results["thread1_holding"] = True

        def thread2_work(results):
            time.sleep(0.2)  # Wait for thread1 to acquire
            # Thread 2 tries to release thread 1's lock with different owner_id
            released = adapter.release("test-lock", owner_id="thread-2")
            results["thread2_released"] = released

        results = {}

        t1 = threading.Thread(target=thread1_work, args=(results,))
        t2 = threading.Thread(target=thread2_work, args=(results,))

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # Thread 2 should NOT be able to release thread 1's lock (different owner_id)
        assert results["thread2_released"] is False
        assert results["thread1_holding"] is True
