"""Unit tests for FileStorage adapter."""

import json
import threading

import pytest

from chronis.contrib.adapters.storage.file import FileStorage
from chronis.core.state import JobStatus
from chronis.core.state.enums import TriggerType
from chronis.utils.time import utc_now


@pytest.fixture
def storage(tmp_path):
    """Create file storage adapter using pytest's tmp_path."""
    file_path = tmp_path / "test_jobs.json"
    return FileStorage(file_path=file_path)


def _make_job(storage, job_id, **overrides):
    """Helper to create a job with defaults."""
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


class TestFileStorageCreate:
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

        with pytest.raises(ValueError, match="already exists"):
            storage.create_job(sample_job_data)


class TestFileStorageRead:
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
        for i in range(3):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            storage.create_job(data)

        jobs = storage.query_jobs()
        assert len(jobs) == 3

    def test_query_jobs_with_filters(self, storage, sample_job_data):
        """Test querying jobs with filters."""
        for i, status in enumerate([JobStatus.SCHEDULED, JobStatus.FAILED, JobStatus.SCHEDULED]):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            data["status"] = status.value
            storage.create_job(data)

        scheduled = storage.query_jobs(filters={"status": JobStatus.SCHEDULED.value})
        assert len(scheduled) == 2

    def test_query_jobs_with_limit(self, storage, sample_job_data):
        """Test querying with limit."""
        for i in range(5):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            storage.create_job(data)

        jobs = storage.query_jobs(limit=3)
        assert len(jobs) == 3

    def test_count_jobs(self, storage, sample_job_data):
        """Test counting jobs."""
        for i in range(3):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            storage.create_job(data)

        count = storage.count_jobs()
        assert count == 3

    def test_count_jobs_with_filters(self, storage, sample_job_data):
        """Test counting with filters."""
        for i, status in enumerate([JobStatus.SCHEDULED, JobStatus.FAILED, JobStatus.SCHEDULED]):
            data = sample_job_data.copy()
            data["job_id"] = f"test-job-{i}"
            data["status"] = status.value
            storage.create_job(data)

        scheduled_count = storage.count_jobs(filters={"status": JobStatus.SCHEDULED.value})
        assert scheduled_count == 2


class TestFileStorageUpdate:
    """Test update operations."""

    def test_update_job(self, storage, sample_job_data):
        """Test updating a job."""
        storage.create_job(sample_job_data)

        result = storage.update_job("test-job-1", {"status": JobStatus.RUNNING.value})
        assert result["status"] == JobStatus.RUNNING.value

    def test_update_nonexistent_job(self, storage):
        """Test updating non-existent job raises error."""
        with pytest.raises(ValueError, match="not found"):
            storage.update_job("nonexistent", {"status": JobStatus.RUNNING.value})


class TestFileStorageDelete:
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


class TestFileStoragePersistence:
    """Test file persistence behavior."""

    def test_data_survives_reinstantiation(self, tmp_path, sample_job_data):
        """Data persists after creating a new FileStorage instance."""
        file_path = tmp_path / "persist_test.json"

        storage1 = FileStorage(file_path=file_path)
        storage1.create_job(sample_job_data)

        storage2 = FileStorage(file_path=file_path)
        retrieved = storage2.get_job("test-job-1")

        assert retrieved is not None
        assert retrieved["job_id"] == "test-job-1"
        assert retrieved["name"] == "Test Job"

    def test_empty_file_created_on_init(self, tmp_path):
        """auto_create=True creates an empty JSON file."""
        file_path = tmp_path / "new_file.json"
        assert not file_path.exists()

        FileStorage(file_path=file_path)

        assert file_path.exists()
        data = json.loads(file_path.read_text())
        assert data == {}

    def test_auto_create_false_raises_if_missing(self, tmp_path):
        """auto_create=False raises FileNotFoundError for missing file."""
        file_path = tmp_path / "missing.json"

        with pytest.raises(FileNotFoundError, match="Storage file not found"):
            FileStorage(file_path=file_path, auto_create=False)

    def test_corrupt_file_raises_on_load(self, tmp_path):
        """Invalid JSON raises ValueError on load."""
        file_path = tmp_path / "corrupt.json"
        file_path.write_text("not valid json {{{")

        with pytest.raises(ValueError, match="Invalid JSON"):
            FileStorage(file_path=file_path)

    def test_non_dict_json_raises_on_load(self, tmp_path):
        """JSON that is not a dict raises ValueError."""
        file_path = tmp_path / "array.json"
        file_path.write_text("[1, 2, 3]")

        with pytest.raises(ValueError, match="Expected JSON object"):
            FileStorage(file_path=file_path)

    def test_parent_directories_created(self, tmp_path):
        """Parent directories are created if they don't exist."""
        file_path = tmp_path / "a" / "b" / "c" / "jobs.json"

        FileStorage(file_path=file_path)

        assert file_path.exists()

    def test_update_persisted_to_file(self, tmp_path, sample_job_data):
        """Updates are persisted to file."""
        file_path = tmp_path / "update_test.json"

        storage = FileStorage(file_path=file_path)
        storage.create_job(sample_job_data)
        storage.update_job("test-job-1", {"status": JobStatus.RUNNING.value})

        storage2 = FileStorage(file_path=file_path)
        job = storage2.get_job("test-job-1")
        assert job["status"] == JobStatus.RUNNING.value

    def test_delete_persisted_to_file(self, tmp_path, sample_job_data):
        """Deletes are persisted to file."""
        file_path = tmp_path / "delete_test.json"

        storage = FileStorage(file_path=file_path)
        storage.create_job(sample_job_data)
        storage.delete_job("test-job-1")

        storage2 = FileStorage(file_path=file_path)
        assert storage2.get_job("test-job-1") is None

    def test_tuple_args_preserved_after_reload(self, tmp_path):
        """Tuple args field is preserved after JSON round-trip."""
        file_path = tmp_path / "tuple_test.json"

        storage = FileStorage(file_path=file_path)
        _make_job(storage, "tuple-job", args=(1, "two", 3))

        storage2 = FileStorage(file_path=file_path)
        job = storage2.get_job("tuple-job")
        assert job["args"] == (1, "two", 3)
        assert isinstance(job["args"], tuple)


class TestFileStorageQueryFilters:
    """Test query_jobs filter capabilities."""

    def test_next_run_time_lte_filter(self, storage):
        """next_run_time_lte filters jobs with next_run_time <= cutoff."""
        from datetime import timedelta

        now = utc_now()
        past = (now - timedelta(minutes=5)).isoformat()
        future = (now + timedelta(minutes=5)).isoformat()

        _make_job(storage, "past-job", next_run_time=past)
        _make_job(storage, "future-job", next_run_time=future)

        jobs = storage.query_jobs(filters={"next_run_time_lte": now.isoformat()})
        job_ids = [j["job_id"] for j in jobs]
        assert "past-job" in job_ids
        assert "future-job" not in job_ids

    def test_updated_at_lte_filter(self, storage):
        """updated_at_lte filters jobs with updated_at <= cutoff."""
        from datetime import timedelta

        now = utc_now()
        old = (now - timedelta(hours=1)).isoformat()
        recent = (now + timedelta(seconds=10)).isoformat()

        _make_job(storage, "old-job", updated_at=old)
        _make_job(storage, "recent-job", updated_at=recent)

        cutoff = now.isoformat()
        jobs = storage.query_jobs(filters={"updated_at_lte": cutoff})
        job_ids = [j["job_id"] for j in jobs]
        assert "old-job" in job_ids
        assert "recent-job" not in job_ids

    def test_metadata_prefix_filter(self, storage):
        """metadata.* prefix filters by metadata key-value."""
        _make_job(storage, "tenant-a", metadata={"tenant_id": "acme"})
        _make_job(storage, "tenant-b", metadata={"tenant_id": "globex"})
        _make_job(storage, "no-tenant", metadata={})

        jobs = storage.query_jobs(filters={"metadata.tenant_id": "acme"})
        assert len(jobs) == 1
        assert jobs[0]["job_id"] == "tenant-a"

    def test_offset_pagination(self, storage):
        """offset skips the first N results."""
        from datetime import timedelta

        now = utc_now()
        for i in range(5):
            _make_job(
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

        now = utc_now()
        _make_job(storage, "late", next_run_time=(now + timedelta(minutes=10)).isoformat())
        _make_job(storage, "early", next_run_time=(now - timedelta(minutes=10)).isoformat())
        _make_job(storage, "mid", next_run_time=now.isoformat())

        jobs = storage.query_jobs()
        assert [j["job_id"] for j in jobs] == ["early", "mid", "late"]

    def test_none_next_run_time_sorted_first(self, storage):
        """Jobs with None next_run_time sort before jobs with timestamps."""
        now = utc_now()
        _make_job(storage, "has-time", next_run_time=now.isoformat())
        _make_job(storage, "no-time", next_run_time=None)

        jobs = storage.query_jobs()
        assert jobs[0]["job_id"] == "no-time"
        assert jobs[1]["job_id"] == "has-time"

    def test_next_run_time_lte_excludes_none(self, storage):
        """Jobs with None next_run_time are excluded by next_run_time_lte filter."""
        now = utc_now()
        _make_job(storage, "has-time", next_run_time=now.isoformat())
        _make_job(storage, "no-time", next_run_time=None)

        jobs = storage.query_jobs(filters={"next_run_time_lte": now.isoformat()})
        job_ids = [j["job_id"] for j in jobs]
        assert "has-time" in job_ids
        assert "no-time" not in job_ids

    def test_combined_status_and_next_run_time_lte(self, storage):
        """Combining status + next_run_time_lte filters (core polling query)."""
        from datetime import timedelta

        now = utc_now()
        past = (now - timedelta(minutes=1)).isoformat()

        _make_job(storage, "ready", status="scheduled", next_run_time=past)
        _make_job(storage, "paused", status="paused", next_run_time=past)
        _make_job(
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


class TestFileStorageCAS:
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
        import time

        storage.create_job(sample_job_data)
        original = storage.get_job("test-job-1")
        original_updated_at = original["updated_at"]

        time.sleep(0.01)

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

    def test_cas_persisted_to_file(self, tmp_path, sample_job_data):
        """CAS changes are persisted to file."""
        file_path = tmp_path / "cas_test.json"

        storage = FileStorage(file_path=file_path)
        storage.create_job(sample_job_data)

        storage.compare_and_swap_job(
            job_id="test-job-1",
            expected_values={"status": JobStatus.SCHEDULED.value},
            updates={"status": JobStatus.RUNNING.value},
        )

        storage2 = FileStorage(file_path=file_path)
        job = storage2.get_job("test-job-1")
        assert job["status"] == JobStatus.RUNNING.value

    def test_cas_thread_safety(self, storage, sample_job_data):
        """Exactly one thread succeeds when 10 threads race to CAS the same job."""
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

        job = storage.get_job("test-job-1")
        assert job["status"] == JobStatus.RUNNING.value


class TestFileStorageBatch:
    """Test get_jobs_batch() operations."""

    def test_batch_returns_existing_jobs(self, storage):
        """get_jobs_batch returns only existing jobs."""
        _make_job(storage, "job-1")
        _make_job(storage, "job-2")

        result = storage.get_jobs_batch(["job-1", "job-2"])
        assert len(result) == 2
        assert "job-1" in result
        assert "job-2" in result

    def test_batch_excludes_nonexistent(self, storage):
        """Nonexistent job IDs are excluded from result."""
        _make_job(storage, "job-1")

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
        _make_job(storage, "a")
        _make_job(storage, "c")

        result = storage.get_jobs_batch(["a", "b", "c", "d"])
        assert set(result.keys()) == {"a", "c"}
