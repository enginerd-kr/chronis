"""Tests for Fluent Builder API."""

from datetime import UTC, datetime, timedelta

import pytest

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler


@pytest.fixture
def scheduler():
    """Create scheduler for testing."""
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    sched = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,
    )
    yield sched
    if sched.is_running():
        sched.stop()


def dummy_job():
    """Dummy job function."""
    pass


class TestIntervalJobBuilder:
    """Tests for every() fluent API."""

    def test_every_seconds(self, scheduler):
        """Test creating interval job with seconds."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.every(seconds=30).run("dummy_job")

        assert job is not None
        assert job.trigger_type == "interval"
        assert job.trigger_args["seconds"] == 30

    def test_every_minutes(self, scheduler):
        """Test creating interval job with minutes."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.every(minutes=5).run("dummy_job")

        assert job.trigger_args["minutes"] == 5

    def test_every_with_multiple_units(self, scheduler):
        """Test creating interval job with multiple time units."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.every(hours=1, minutes=30).run("dummy_job")

        assert job.trigger_args["hours"] == 1
        assert job.trigger_args["minutes"] == 30

    def test_every_without_params_raises(self, scheduler):
        """Test that every() without parameters raises error."""
        with pytest.raises(ValueError, match="At least one interval parameter"):
            scheduler.every()

    def test_every_with_config_retry(self, scheduler):
        """Test interval job with retry configuration."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.every(minutes=5).config(retry=3, retry_delay=120).run("dummy_job")

        assert job.max_retries == 3
        assert job.retry_delay_seconds == 120

    def test_every_with_config_timeout(self, scheduler):
        """Test interval job with timeout."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.every(seconds=30).config(timeout=300).run("dummy_job")

        assert job.timeout_seconds == 300

    def test_every_with_config_metadata(self, scheduler):
        """Test interval job with metadata."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = (
            scheduler.every(minutes=5)
            .config(metadata={"tenant_id": "acme", "env": "prod"})
            .run("dummy_job")
        )

        assert job.metadata["tenant_id"] == "acme"
        assert job.metadata["env"] == "prod"

    def test_every_with_custom_id_and_name(self, scheduler):
        """Test interval job with custom ID and name."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = (
            scheduler.every(minutes=5)
            .config(job_id="custom-job-id", name="Custom Job Name")
            .run("dummy_job")
        )

        assert job.job_id == "custom-job-id"
        assert job.name == "Custom Job Name"

    def test_every_with_function_kwargs(self, scheduler):
        """Test interval job with function keyword arguments."""

        def job_with_args(x, y, z=None):
            pass

        scheduler.register_job_function("job_with_args", job_with_args)

        job = scheduler.every(seconds=30).run("job_with_args", x=1, y=2, z="hello")

        assert job is not None
        # kwargs are stored in storage, not exposed in JobInfo
        stored = scheduler.storage.get_job(job.job_id)
        assert stored["kwargs"]["x"] == 1
        assert stored["kwargs"]["y"] == 2
        assert stored["kwargs"]["z"] == "hello"


class TestCronJobBuilder:
    """Tests for on() fluent API."""

    def test_on_daily(self, scheduler):
        """Test creating daily job using on()."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.on(hour=9, minute=30).run("dummy_job")

        assert job.trigger_type == "cron"
        assert job.trigger_args["hour"] == 9
        assert job.trigger_args["minute"] == 30

    def test_on_hourly(self, scheduler):
        """Test creating hourly job using on()."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.on(minute=15).run("dummy_job")

        assert job.trigger_type == "cron"
        assert job.trigger_args["minute"] == 15

    def test_on_weekly_with_string_day(self, scheduler):
        """Test creating weekly job with string day."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.on(day_of_week="mon", hour=9).run("dummy_job")

        assert job.trigger_type == "cron"
        assert job.trigger_args["day_of_week"] == "mon"
        assert job.trigger_args["hour"] == 9

    def test_on_weekly_with_int_day(self, scheduler):
        """Test creating weekly job with integer day."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.on(day_of_week=0, hour=9, minute=30).run("dummy_job")

        assert job.trigger_args["day_of_week"] == 0

    def test_on_monthly(self, scheduler):
        """Test creating monthly job using on()."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.on(day=1, hour=0, minute=0).run("dummy_job")

        assert job.trigger_type == "cron"
        assert job.trigger_args["day"] == 1

    def test_on_with_timezone(self, scheduler):
        """Test cron job with timezone."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.on(hour=9).config(timezone="Asia/Seoul").run("dummy_job")

        assert job.timezone == "Asia/Seoul"


class TestDateJobBuilder:
    """Tests for once() and at() fluent API."""

    def test_once_with_string(self, scheduler):
        """Test creating one-time job with string date."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.once(when="2030-01-20T10:00:00").run("dummy_job")

        assert job.trigger_type == "date"

    def test_once_with_datetime(self, scheduler):
        """Test creating one-time job with datetime object."""
        scheduler.register_job_function("dummy_job", dummy_job)

        run_date = datetime.now(UTC) + timedelta(days=1)
        job = scheduler.once(when=run_date).run("dummy_job")

        assert job.trigger_type == "date"

    def test_once_with_config(self, scheduler):
        """Test once() with config options."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.once(when="2030-01-20T10:00:00").config(retry=2).run("dummy_job")

        assert job.trigger_type == "date"
        assert job.max_retries == 2

    def test_once_with_full_config(self, scheduler):
        """Test one-time job with full configuration."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = (
            scheduler.once(when="2030-01-20T10:00:00")
            .config(
                job_id="notification-123",
                name="Send Notification",
                retry=2,
                timeout=60,
                priority=8,
                metadata={"user_id": "user-1"},
            )
            .run("dummy_job")
        )

        assert job.job_id == "notification-123"
        assert job.name == "Send Notification"
        assert job.max_retries == 2
        assert job.timeout_seconds == 60
        assert job.priority == 8
        assert job.metadata["user_id"] == "user-1"


class TestConfigFirstPattern:
    """Tests for config-first ordering (config before trigger)."""

    def test_config_then_every(self, scheduler):
        """Test config() before every()."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.config(retry=3, timeout=300).every(minutes=5).run("dummy_job")

        assert job.max_retries == 3
        assert job.timeout_seconds == 300
        assert job.trigger_type == "interval"
        assert job.trigger_args["minutes"] == 5

    def test_config_then_on(self, scheduler):
        """Test config() before on()."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = scheduler.config(job_id="daily-job").on(hour=9).run("dummy_job")

        assert job.job_id == "daily-job"
        assert job.trigger_type == "cron"

    def test_config_then_once(self, scheduler):
        """Test config() before once()."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = (
            scheduler.config(metadata={"key": "value"})
            .once(when="2030-01-20T10:00:00")
            .run("dummy_job")
        )

        assert job.metadata["key"] == "value"
        assert job.trigger_type == "date"

    def test_multiple_config_calls(self, scheduler):
        """Test multiple config() calls are merged."""
        scheduler.register_job_function("dummy_job", dummy_job)

        job = (
            scheduler.config(retry=3)
            .config(timeout=300)
            .every(minutes=5)
            .config(metadata={"key": "value"})
            .run("dummy_job")
        )

        assert job.max_retries == 3
        assert job.timeout_seconds == 300
        assert job.metadata["key"] == "value"


class TestRunWithoutTriggerRaises:
    """Tests for error handling when run() is called without trigger."""

    def test_config_only_then_run_raises(self, scheduler):
        """Test that config().run() without trigger raises error."""
        scheduler.register_job_function("dummy_job", dummy_job)

        with pytest.raises(ValueError, match="No trigger configured"):
            scheduler.config(retry=3).run("dummy_job")


class TestMethodChaining:
    """Tests for method chaining behavior."""

    def test_full_chain_trigger_first(self, scheduler):
        """Test full method chain with trigger first."""
        scheduler.register_job_function("sync_data", dummy_job)

        job = (
            scheduler.every(minutes=5)
            .config(
                job_id="sync-job",
                name="Data Sync",
                retry=3,
                retry_delay=60,
                timeout=300,
                priority=7,
                metadata={"tenant_id": "acme", "env": "production"},
                timezone="UTC",
            )
            .run("sync_data")
        )

        assert job.job_id == "sync-job"
        assert job.name == "Data Sync"
        assert job.max_retries == 3
        assert job.retry_delay_seconds == 60
        assert job.timeout_seconds == 300
        assert job.priority == 7
        assert job.metadata["tenant_id"] == "acme"
        assert job.metadata["env"] == "production"
        assert job.timezone == "UTC"

    def test_full_chain_config_first(self, scheduler):
        """Test full method chain with config first."""
        scheduler.register_job_function("sync_data", dummy_job)

        job = (
            scheduler.config(
                job_id="sync-job",
                retry=3,
                timeout=300,
            )
            .every(minutes=5)
            .run("sync_data")
        )

        assert job.job_id == "sync-job"
        assert job.max_retries == 3
        assert job.timeout_seconds == 300
        assert job.trigger_args["minutes"] == 5


class TestFluentAndTraditionalAPICoexistence:
    """Tests for coexistence of fluent and traditional APIs."""

    def test_fluent_and_traditional_api_coexist(self, scheduler):
        """Test that fluent and traditional APIs can coexist."""
        scheduler.register_job_function("dummy", dummy_job)

        # Create with fluent API
        scheduler.every(seconds=30).config(job_id="fluent-job").run("dummy")

        # Create with traditional API
        scheduler.create_interval_job(
            func="dummy",
            job_id="traditional-job",
            seconds=60,
        )

        jobs = scheduler.query_jobs()
        job_ids = {j.job_id for j in jobs}

        assert "fluent-job" in job_ids
        assert "traditional-job" in job_ids

    def test_fluent_job_stored_in_storage(self, scheduler):
        """Test that fluent-created jobs are stored correctly."""
        scheduler.register_job_function("dummy", dummy_job)

        job = scheduler.every(seconds=30).run("dummy")

        # Verify in storage
        stored = scheduler.storage.get_job(job.job_id)
        assert stored is not None
        assert stored["trigger_type"] == "interval"
        assert stored["trigger_args"]["seconds"] == 30

    def test_fluent_job_can_be_paused(self, scheduler):
        """Test that fluent-created jobs can be paused."""
        scheduler.register_job_function("dummy", dummy_job)

        job = scheduler.every(minutes=5).run("dummy")

        # Pause should work
        result = scheduler.pause_job(job.job_id)
        assert result is True

        stored = scheduler.storage.get_job(job.job_id)
        assert stored["status"] == "paused"

    def test_fluent_job_metadata_searchable(self, scheduler):
        """Test that metadata from fluent API is searchable."""
        scheduler.register_job_function("dummy", dummy_job)

        scheduler.every(minutes=5).config(metadata={"tenant_id": "acme"}).run("dummy")

        # Query by metadata
        jobs = scheduler.query_jobs(filters={"metadata.tenant_id": "acme"})

        assert len(jobs) == 1
        assert jobs[0].metadata["tenant_id"] == "acme"
