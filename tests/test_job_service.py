"""Tests for JobService."""

from chronis.adapters.storage.memory import InMemoryStorageAdapter
from chronis.core.common.exceptions import JobAlreadyExistsError, JobNotFoundError
from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobDefinition
from chronis.core.query import jobs_by_metadata, jobs_by_status, scheduled_jobs
from chronis.core.services import JobService
from chronis.core.state import JobStatus


class TestJobService:
    """Tests for JobService."""

    def setup_method(self):
        """Setup test fixtures."""
        self.storage = InMemoryStorageAdapter()
        self.service = JobService(storage=self.storage, verbose=False)

    def test_create_job(self):
        """Test creating a job."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )

        job_info = self.service.create(job_def)

        assert job_info.job_id == "test-1"
        assert job_info.name == "Test Job"
        assert job_info.status == JobStatus.SCHEDULED

    def test_create_duplicate_job_raises_error(self):
        """Test that creating duplicate job raises error."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )

        self.service.create(job_def)

        # Try to create again
        try:
            self.service.create(job_def)
            raise AssertionError("Should have raised JobAlreadyExistsError")
        except JobAlreadyExistsError:
            pass

    def test_get_job(self):
        """Test getting a job."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )

        self.service.create(job_def)
        job_info = self.service.get("test-1")

        assert job_info is not None
        assert job_info.job_id == "test-1"

    def test_get_nonexistent_job_returns_none(self):
        """Test getting nonexistent job returns None."""
        job_info = self.service.get("nonexistent")

        assert job_info is None

    def test_query_with_spec(self):
        """Test querying jobs with specification."""

        def test_func():
            pass

        # Create multiple jobs
        for i in range(3):
            job_def = JobDefinition(
                job_id=f"test-{i}",
                name=f"Test Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
            )
            self.service.create(job_def)

        # Query all scheduled jobs
        jobs = self.service.query(filters=scheduled_jobs())

        assert len(jobs) == 3

    def test_query_with_raw_filters(self):
        """Test querying jobs with raw filters (backward compatibility)."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )
        self.service.create(job_def)

        # Query with raw filters
        jobs = self.service.query(filters={"status": "scheduled"})

        assert len(jobs) == 1

    def test_query_with_limit(self):
        """Test querying with limit."""

        def test_func():
            pass

        # Create 5 jobs
        for i in range(5):
            job_def = JobDefinition(
                job_id=f"test-{i}",
                name=f"Test Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
            )
            self.service.create(job_def)

        # Query with limit
        jobs = self.service.query(filters=scheduled_jobs(), limit=3)

        assert len(jobs) == 3

    def test_update_job(self):
        """Test updating a job."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )
        self.service.create(job_def)

        # Update name
        updated = self.service.update("test-1", name="Updated Job")

        assert updated.name == "Updated Job"

    def test_update_nonexistent_job_raises_error(self):
        """Test updating nonexistent job raises error."""
        try:
            self.service.update("nonexistent", name="New Name")
            raise AssertionError("Should have raised JobNotFoundError")
        except JobNotFoundError:
            pass

    def test_update_with_no_changes(self):
        """Test update with no changes returns current job."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )
        created = self.service.create(job_def)

        # Update with no parameters
        updated = self.service.update("test-1")

        assert updated.job_id == created.job_id
        assert updated.name == created.name

    def test_delete_job(self):
        """Test deleting a job."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )
        self.service.create(job_def)

        # Delete
        result = self.service.delete("test-1")

        assert result is True
        assert self.service.get("test-1") is None

    def test_delete_nonexistent_job(self):
        """Test deleting nonexistent job returns False."""
        result = self.service.delete("nonexistent")

        assert result is False

    def test_exists(self):
        """Test checking if job exists."""

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )
        self.service.create(job_def)

        assert self.service.exists("test-1") is True
        assert self.service.exists("nonexistent") is False

    def test_count(self):
        """Test counting jobs."""

        def test_func():
            pass

        # Create 3 jobs
        for i in range(3):
            job_def = JobDefinition(
                job_id=f"test-{i}",
                name=f"Test Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
            )
            self.service.create(job_def)

        # Count all
        total = self.service.count()
        assert total == 3

        # Count scheduled
        scheduled_count = self.service.count(filters=scheduled_jobs())
        assert scheduled_count == 3

    def test_count_with_specific_status(self):
        """Test counting jobs by status."""

        def test_func():
            pass

        # Create job
        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )
        self.service.create(job_def)

        # Update to failed
        self.service.update("test-1", status=JobStatus.FAILED)

        # Count failed jobs
        failed_count = self.service.count(filters=jobs_by_status(JobStatus.FAILED))
        assert failed_count == 1

        # Count scheduled jobs
        scheduled_count = self.service.count(filters=scheduled_jobs())
        assert scheduled_count == 0


class TestJobServiceIntegration:
    """Integration tests for JobService."""

    def setup_method(self):
        """Setup test fixtures."""
        self.storage = InMemoryStorageAdapter()
        self.service = JobService(storage=self.storage, verbose=False)

    def test_complete_crud_lifecycle(self):
        """Test complete CRUD lifecycle."""

        def test_func():
            pass

        # Create
        job_def = JobDefinition(
            job_id="lifecycle-test",
            name="Lifecycle Test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"minutes": 5},
            func=test_func,
        )
        created = self.service.create(job_def)
        assert created.job_id == "lifecycle-test"

        # Read
        retrieved = self.service.get("lifecycle-test")
        assert retrieved is not None
        assert retrieved.name == "Lifecycle Test"

        # Update
        updated = self.service.update("lifecycle-test", name="Updated Name")
        assert updated.name == "Updated Name"

        # Delete
        deleted = self.service.delete("lifecycle-test")
        assert deleted is True

        # Verify deletion
        assert self.service.get("lifecycle-test") is None

    def test_query_filtering(self):
        """Test query with multiple filters."""

        def test_func():
            pass

        # Create jobs with different metadata
        for i in range(3):
            job_def = JobDefinition(
                job_id=f"tenant-a-{i}",
                name=f"Tenant A Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
                metadata={"tenant": "a", "priority": "high"},
            )
            self.service.create(job_def)

        for i in range(2):
            job_def = JobDefinition(
                job_id=f"tenant-b-{i}",
                name=f"Tenant B Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
                metadata={"tenant": "b", "priority": "low"},
            )
            self.service.create(job_def)

        # Query by metadata
        tenant_a_jobs = self.service.query(filters=jobs_by_metadata("tenant", "a"))

        assert len(tenant_a_jobs) == 3
        for job in tenant_a_jobs:
            assert job.metadata["tenant"] == "a"
