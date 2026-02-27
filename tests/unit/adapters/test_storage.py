"""Unit tests for Storage adapters."""

import pytest

from chronis.adapters.base import JobStorageAdapter
from chronis.adapters.storage import InMemoryStorageAdapter
from chronis.core.state import JobStatus


@pytest.fixture
def storage():
    """Create in-memory storage adapter."""
    return InMemoryStorageAdapter()


class TestInMemoryStorageCreate:
    """Test create operations."""

    def test_create_job(self, storage, sample_job_data):
        """Test creating a job."""
        created = storage.create_job(sample_job_data)

        assert created["job_id"] == "test-job-1"
        assert created["name"] == "Test Job"
        assert created["status"] == JobStatus.SCHEDULED.value

    def test_create_duplicate_raises_error(self, storage, sample_job_data):
        """Test creating duplicate job raises error."""
        storage.create_job(sample_job_data)

        # Try to create duplicate
        with pytest.raises(ValueError, match="already exists"):
            storage.create_job(sample_job_data)


class TestInMemoryStorageRead:
    """Test read operations."""

    def test_get_job(self, storage, sample_job_data):
        """Test getting a job by ID."""
        storage.create_job(sample_job_data)

        retrieved = storage.get_job("test-job-1")
        assert retrieved is not None
        assert retrieved["job_id"] == "test-job-1"

    def test_get_nonexistent_job(self, storage):
        """Test getting non-existent job returns None."""
        result = storage.get_job("nonexistent")
        assert result is None

    def test_query_jobs_all(self, storage, sample_job_data):
        """Test querying all jobs."""
        # Create multiple jobs
        for i in range(3):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            storage.create_job(data)

        jobs = storage.query_jobs()
        assert len(jobs) == 3

    def test_query_jobs_with_filters(self, storage, sample_job_data):
        """Test querying jobs with filters."""
        # Create jobs with different statuses
        for i, status in enumerate([JobStatus.SCHEDULED, JobStatus.FAILED, JobStatus.SCHEDULED]):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            data["status"] = status.value
            storage.create_job(data)

        # Query only scheduled jobs
        scheduled = storage.query_jobs(filters={"status": JobStatus.SCHEDULED.value})
        assert len(scheduled) == 2

    def test_query_jobs_with_limit(self, storage, sample_job_data):
        """Test querying with limit."""
        # Create 5 jobs
        for i in range(5):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            storage.create_job(data)

        jobs = storage.query_jobs(limit=3)
        assert len(jobs) == 3

    def test_count_jobs(self, storage, sample_job_data):
        """Test counting jobs."""
        # Create 3 jobs
        for i in range(3):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            storage.create_job(data)

        count = storage.count_jobs()
        assert count == 3

    def test_count_jobs_with_filters(self, storage, sample_job_data):
        """Test counting with filters."""
        # Create jobs with different statuses
        for i, status in enumerate([JobStatus.SCHEDULED, JobStatus.FAILED, JobStatus.SCHEDULED]):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            data["status"] = status.value
            storage.create_job(data)

        scheduled_count = storage.count_jobs(filters={"status": JobStatus.SCHEDULED.value})
        assert scheduled_count == 2


class TestInMemoryStorageUpdate:
    """Test update operations."""

    def test_update_job(self, storage, sample_job_data):
        """Test updating a job."""
        storage.create_job(sample_job_data)

        # Update status
        result = storage.update_job("test-job-1", {"status": JobStatus.RUNNING.value})
        assert result["status"] == JobStatus.RUNNING.value

    def test_update_nonexistent_job(self, storage):
        """Test updating non-existent job raises error."""
        with pytest.raises(ValueError, match="not found"):
            storage.update_job("nonexistent", {"status": JobStatus.RUNNING.value})


class TestInMemoryStorageDelete:
    """Test delete operations."""

    def test_delete_job(self, storage, sample_job_data):
        """Test deleting a job."""
        storage.create_job(sample_job_data)

        result = storage.delete_job("test-job-1")
        assert result is True
        assert storage.get_job("test-job-1") is None

    def test_delete_nonexistent_job(self, storage):
        """Test deleting non-existent job returns False."""
        result = storage.delete_job("nonexistent")
        assert result is False


class TestStorageAdapterContract:
    """Test that JobStorageAdapter enforces required method implementations."""

    def test_compare_and_swap_job_is_abstract(self):
        """compare_and_swap_job must be abstract - adapters must implement it."""
        assert hasattr(JobStorageAdapter.compare_and_swap_job, "__isabstractmethod__")
        assert JobStorageAdapter.compare_and_swap_job.__isabstractmethod__ is True

    def test_cannot_instantiate_without_compare_and_swap(self):
        """Adapter without compare_and_swap_job raises TypeError."""

        class IncompleteAdapter(JobStorageAdapter):
            def create_job(self, job_data):
                pass

            def get_job(self, job_id):
                pass

            def update_job(self, job_id, updates):
                pass

            def delete_job(self, job_id):
                pass

            def query_jobs(self, filters=None, limit=None, offset=None):
                pass

        with pytest.raises(TypeError, match="compare_and_swap_job"):
            IncompleteAdapter()


class TestQueryJobsFilters:
    """Test query_jobs filter capabilities."""

    def _make_job(self, storage, job_id, **overrides):
        from chronis.core.state.enums import TriggerType
        from chronis.utils.time import utc_now

        data = {
            "job_id": job_id,
            "name": f"Job {job_id}",
            "trigger_type": TriggerType.INTERVAL.value,
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": JobStatus.SCHEDULED.value,
            "next_run_time": utc_now().isoformat(),
            "next_run_time_local": utc_now().isoformat(),
            "metadata": {},
            "created_at": utc_now().isoformat(),
            "updated_at": utc_now().isoformat(),
        }
        data.update(overrides)
        return storage.create_job(data)

    def test_next_run_time_lte_filter(self, storage):
        """next_run_time_lte filters jobs with next_run_time <= cutoff."""
        from datetime import timedelta

        from chronis.utils.time import utc_now

        now = utc_now()
        past = (now - timedelta(minutes=5)).isoformat()
        future = (now + timedelta(minutes=5)).isoformat()

        self._make_job(storage, "past-job", next_run_time=past)
        self._make_job(storage, "future-job", next_run_time=future)

        jobs = storage.query_jobs(filters={"next_run_time_lte": now.isoformat()})
        job_ids = [j["job_id"] for j in jobs]
        assert "past-job" in job_ids
        assert "future-job" not in job_ids

    def test_updated_at_lte_filter(self, storage):
        """updated_at_lte filters jobs with updated_at <= cutoff."""
        from datetime import timedelta

        from chronis.utils.time import utc_now

        now = utc_now()
        old = (now - timedelta(hours=1)).isoformat()
        recent = (now + timedelta(seconds=10)).isoformat()

        self._make_job(storage, "old-job", updated_at=old)
        self._make_job(storage, "recent-job", updated_at=recent)

        cutoff = now.isoformat()
        jobs = storage.query_jobs(filters={"updated_at_lte": cutoff})
        job_ids = [j["job_id"] for j in jobs]
        assert "old-job" in job_ids
        assert "recent-job" not in job_ids

    def test_metadata_prefix_filter(self, storage):
        """metadata.* prefix filters by metadata key-value."""
        self._make_job(storage, "tenant-a", metadata={"tenant_id": "acme"})
        self._make_job(storage, "tenant-b", metadata={"tenant_id": "globex"})
        self._make_job(storage, "no-tenant", metadata={})

        jobs = storage.query_jobs(filters={"metadata.tenant_id": "acme"})
        assert len(jobs) == 1
        assert jobs[0]["job_id"] == "tenant-a"

    def test_offset_pagination(self, storage):
        """offset skips the first N results."""
        from datetime import timedelta

        from chronis.utils.time import utc_now

        now = utc_now()
        for i in range(5):
            self._make_job(
                storage,
                f"job-{i}",
                next_run_time=(now + timedelta(seconds=i)).isoformat(),
            )

        all_jobs = storage.query_jobs()
        offset_jobs = storage.query_jobs(offset=2)

        assert len(all_jobs) == 5
        assert len(offset_jobs) == 3
        assert offset_jobs[0]["job_id"] == all_jobs[2]["job_id"]

    def test_sorting_by_next_run_time(self, storage):
        """Results are sorted by next_run_time ascending."""
        from datetime import timedelta

        from chronis.utils.time import utc_now

        now = utc_now()
        self._make_job(storage, "late", next_run_time=(now + timedelta(minutes=10)).isoformat())
        self._make_job(storage, "early", next_run_time=(now - timedelta(minutes=10)).isoformat())
        self._make_job(storage, "mid", next_run_time=now.isoformat())

        jobs = storage.query_jobs()
        assert [j["job_id"] for j in jobs] == ["early", "mid", "late"]

    def test_none_next_run_time_sorted_first(self, storage):
        """Jobs with None next_run_time sort before jobs with timestamps."""
        from chronis.utils.time import utc_now

        now = utc_now()
        self._make_job(storage, "has-time", next_run_time=now.isoformat())
        self._make_job(storage, "no-time", next_run_time=None)

        jobs = storage.query_jobs()
        assert jobs[0]["job_id"] == "no-time"
        assert jobs[1]["job_id"] == "has-time"

    def test_next_run_time_lte_excludes_none(self, storage):
        """Jobs with None next_run_time are excluded by next_run_time_lte filter."""
        from chronis.utils.time import utc_now

        now = utc_now()
        self._make_job(storage, "has-time", next_run_time=now.isoformat())
        self._make_job(storage, "no-time", next_run_time=None)

        jobs = storage.query_jobs(filters={"next_run_time_lte": now.isoformat()})
        job_ids = [j["job_id"] for j in jobs]
        assert "has-time" in job_ids
        assert "no-time" not in job_ids

    def test_combined_status_and_next_run_time_lte(self, storage):
        """Combining status + next_run_time_lte filters (core polling query)."""
        from datetime import timedelta

        from chronis.utils.time import utc_now

        now = utc_now()
        past = (now - timedelta(minutes=1)).isoformat()

        self._make_job(storage, "ready", status="scheduled", next_run_time=past)
        self._make_job(storage, "paused", status="paused", next_run_time=past)
        self._make_job(
            storage,
            "future",
            status="scheduled",
            next_run_time=(now + timedelta(hours=1)).isoformat(),
        )

        jobs = storage.query_jobs(
            filters={"status": "scheduled", "next_run_time_lte": now.isoformat()}
        )
        assert len(jobs) == 1
        assert jobs[0]["job_id"] == "ready"


class TestGetJobsBatch:
    """Test get_jobs_batch() operations."""

    def _make_job(self, storage, job_id):
        from chronis.core.state.enums import TriggerType
        from chronis.utils.time import utc_now

        return storage.create_job(
            {
                "job_id": job_id,
                "name": f"Job {job_id}",
                "trigger_type": TriggerType.INTERVAL.value,
                "trigger_args": {"seconds": 30},
                "timezone": "UTC",
                "status": JobStatus.SCHEDULED.value,
                "next_run_time": utc_now().isoformat(),
                "next_run_time_local": utc_now().isoformat(),
                "metadata": {},
                "created_at": utc_now().isoformat(),
                "updated_at": utc_now().isoformat(),
            }
        )

    def test_batch_returns_existing_jobs(self, storage):
        """get_jobs_batch returns only existing jobs."""
        self._make_job(storage, "job-1")
        self._make_job(storage, "job-2")

        result = storage.get_jobs_batch(["job-1", "job-2"])
        assert len(result) == 2
        assert "job-1" in result
        assert "job-2" in result

    def test_batch_excludes_nonexistent(self, storage):
        """Nonexistent job IDs are excluded from result."""
        self._make_job(storage, "job-1")

        result = storage.get_jobs_batch(["job-1", "nonexistent"])
        assert len(result) == 1
        assert "job-1" in result
        assert "nonexistent" not in result

    def test_batch_empty_input(self, storage):
        """Empty list returns empty dict."""
        result = storage.get_jobs_batch([])
        assert result == {}

    def test_batch_partial_existence(self, storage):
        """Partial matches return only found jobs."""
        self._make_job(storage, "a")
        self._make_job(storage, "c")

        result = storage.get_jobs_batch(["a", "b", "c", "d"])
        assert set(result.keys()) == {"a", "c"}


class TestCompareAndSwap:
    """Test compare_and_swap_job operations."""

    def test_cas_success(self, storage, sample_job_data):
        """CAS succeeds when expected_values match current data."""
        storage.create_job(sample_job_data)

        success, updated = storage.compare_and_swap_job(
            job_id="test-job-1",
            expected_values={"status": JobStatus.SCHEDULED.value},
            updates={"status": JobStatus.RUNNING.value},
        )

        assert success is True
        assert updated is not None
        assert updated["status"] == JobStatus.RUNNING.value

    def test_cas_failure_status_mismatch(self, storage, sample_job_data):
        """CAS fails when status does not match expected value."""
        storage.create_job(sample_job_data)

        success, updated = storage.compare_and_swap_job(
            job_id="test-job-1",
            expected_values={"status": JobStatus.RUNNING.value},
            updates={"status": JobStatus.FAILED.value},
        )

        assert success is False
        assert updated is None

        # Verify data was not changed
        job = storage.get_job("test-job-1")
        assert job["status"] == JobStatus.SCHEDULED.value

    def test_cas_failure_next_run_time_mismatch(self, storage, sample_job_data):
        """CAS fails when next_run_time does not match expected value."""
        storage.create_job(sample_job_data)

        success, updated = storage.compare_and_swap_job(
            job_id="test-job-1",
            expected_values={"next_run_time": "2000-01-01T00:00:00+00:00"},
            updates={"status": JobStatus.RUNNING.value},
        )

        assert success is False
        assert updated is None

    def test_cas_nonexistent_job_raises_error(self, storage):
        """CAS raises ValueError for a nonexistent job."""
        with pytest.raises(ValueError, match="not found"):
            storage.compare_and_swap_job(
                job_id="nonexistent",
                expected_values={"status": JobStatus.SCHEDULED.value},
                updates={"status": JobStatus.RUNNING.value},
            )

    def test_cas_updates_updated_at(self, storage, sample_job_data):
        """CAS automatically refreshes updated_at on success."""
        storage.create_job(sample_job_data)
        original = storage.get_job("test-job-1")
        original_updated_at = original["updated_at"]

        import time

        time.sleep(0.01)  # Ensure time difference

        success, updated = storage.compare_and_swap_job(
            job_id="test-job-1",
            expected_values={"status": JobStatus.SCHEDULED.value},
            updates={"status": JobStatus.RUNNING.value},
        )

        assert success is True
        assert updated["updated_at"] != original_updated_at
        assert updated["updated_at"] > original_updated_at

    def test_cas_atomicity_data_unchanged_on_failure(self, storage, sample_job_data):
        """CAS leaves all fields unchanged when comparison fails."""
        storage.create_job(sample_job_data)
        original = storage.get_job("test-job-1")

        storage.compare_and_swap_job(
            job_id="test-job-1",
            expected_values={"status": JobStatus.RUNNING.value},
            updates={
                "status": JobStatus.FAILED.value,
                "next_run_time": "2099-12-31T23:59:59+00:00",
            },
        )

        after = storage.get_job("test-job-1")
        assert after == original

    def test_cas_thread_safety(self, storage, sample_job_data):
        """Exactly one thread succeeds when 10 threads race to CAS the same job."""
        import threading

        storage.create_job(sample_job_data)

        results = []
        barrier = threading.Barrier(10)

        def attempt_cas():
            barrier.wait()
            success, _ = storage.compare_and_swap_job(
                job_id="test-job-1",
                expected_values={"status": JobStatus.SCHEDULED.value},
                updates={"status": JobStatus.RUNNING.value},
            )
            results.append(success)

        threads = [threading.Thread(target=attempt_cas) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results.count(True) == 1
        assert results.count(False) == 9

        # Verify final state
        job = storage.get_job("test-job-1")
        assert job["status"] == JobStatus.RUNNING.value
