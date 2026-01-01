"""Unit tests for Storage adapters."""

import pytest

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
