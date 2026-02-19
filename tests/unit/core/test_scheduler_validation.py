"""Pure unit tests for PollingScheduler parameter validation."""

import pytest

from chronis.adapters.lock.memory import InMemoryLockAdapter
from chronis.adapters.storage.memory import InMemoryStorageAdapter
from chronis.core.schedulers.polling_scheduler import PollingScheduler


class TestPollingIntervalValidation:
    """Test polling_interval_seconds validation."""

    def test_polling_interval_below_minimum_raises_error(self):
        """Test that polling_interval < MIN_POLLING_INTERVAL raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            PollingScheduler(
                storage_adapter=InMemoryStorageAdapter(),
                lock_adapter=InMemoryLockAdapter(),
                polling_interval_seconds=0,  # Below MIN (1)
            )

        assert "must be >=" in str(exc_info.value)

    def test_polling_interval_above_maximum_raises_error(self):
        """Test that polling_interval > MAX_POLLING_INTERVAL raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            PollingScheduler(
                storage_adapter=InMemoryStorageAdapter(),
                lock_adapter=InMemoryLockAdapter(),
                polling_interval_seconds=4000,  # Above MAX (3600)
            )

        assert "should not exceed" in str(exc_info.value)


class TestLockTTLValidation:
    """Test lock_ttl_seconds validation."""

    def test_lock_ttl_less_than_2x_polling_raises_error(self):
        """Test that lock_ttl < 2x polling_interval raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            PollingScheduler(
                storage_adapter=InMemoryStorageAdapter(),
                lock_adapter=InMemoryLockAdapter(),
                polling_interval_seconds=10,
                lock_ttl_seconds=15,  # Less than 2x (20)
            )

        assert "at least 2x" in str(exc_info.value)
