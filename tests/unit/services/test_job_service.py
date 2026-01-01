"""Pure unit tests for JobService using mocked storage."""


import pytest

from chronis.core.common.exceptions import JobAlreadyExistsError, JobNotFoundError
from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobDefinition
from chronis.core.services import JobService
from chronis.core.state import JobStatus


class TestJobServiceCreate:
    """Test JobService.create() with mocked storage."""

    def test_create_job_calls_storage_and_returns_job_info(self, mock_storage):
        """Test that create calls storage.create_job and returns JobInfo."""
        service = JobService(storage=mock_storage)

        # Mock storage to return job data
        mock_storage.create_job.return_value = {
            "job_id": "test-1",
            "name": "Test Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": "scheduled",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
            "func_name": "test_func",
            "args": [],
            "kwargs": {},
        }

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )

        result = service.create(job_def)

        # Verify storage was called
        mock_storage.create_job.assert_called_once()

        # Verify result
        assert result.job_id == "test-1"
        assert result.name == "Test Job"
        assert result.status == JobStatus.SCHEDULED

    def test_create_duplicate_raises_job_already_exists_error(self, mock_storage):
        """Test that creating duplicate job raises JobAlreadyExistsError."""
        service = JobService(storage=mock_storage)

        # Mock storage to raise ValueError (duplicate)
        mock_storage.create_job.side_effect = ValueError("Job test-1 already exists")

        def test_func():
            pass

        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
        )

        with pytest.raises(JobAlreadyExistsError):
            service.create(job_def)

    def test_create_logs_in_verbose_mode(self, mock_storage, mock_logger):
        """Test that create logs in verbose mode."""
        service = JobService(storage=mock_storage, logger=mock_logger, verbose=True)

        mock_storage.create_job.return_value = {
            "job_id": "test-1",
            "name": "Test",
            "trigger_type": "interval",
            "trigger_args": {},
            "timezone": "UTC",
            "status": "scheduled",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
            "func_name": "test",
            "args": [],
            "kwargs": {},
        }

        job_def = JobDefinition(
            job_id="test-1",
            name="Test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={},
            func=lambda: None,
        )

        service.create(job_def)

        # Verify logging
        mock_logger.info.assert_called_once()


class TestJobServiceGet:
    """Test JobService.get() with mocked storage."""

    def test_get_existing_job_returns_job_info(self, mock_storage):
        """Test that get returns JobInfo for existing job."""
        service = JobService(storage=mock_storage)

        mock_storage.get_job.return_value = {
            "job_id": "test-1",
            "name": "Test Job",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": "scheduled",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
            "func_name": "test_func",
            "args": [],
            "kwargs": {},
        }

        result = service.get("test-1")

        mock_storage.get_job.assert_called_once_with("test-1")
        assert result is not None
        assert result.job_id == "test-1"

    def test_get_nonexistent_job_returns_none(self, mock_storage):
        """Test that get returns None for nonexistent job."""
        service = JobService(storage=mock_storage)

        mock_storage.get_job.return_value = None

        result = service.get("nonexistent")

        assert result is None


class TestJobServiceUpdate:
    """Test JobService.update() with mocked storage."""

    def test_update_with_fields_calls_storage(self, mock_storage):
        """Test that update calls storage.update_job with proper data."""
        service = JobService(storage=mock_storage)

        mock_storage.update_job.return_value = {
            "job_id": "test-1",
            "name": "Updated Name",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": "scheduled",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
            "func_name": "test",
            "args": [],
            "kwargs": {},
        }

        result = service.update("test-1", name="Updated Name")

        # Verify storage was called with name and updated_at
        mock_storage.update_job.assert_called_once()
        call_args = mock_storage.update_job.call_args
        assert call_args[0][0] == "test-1"
        assert "name" in call_args[0][1]
        assert call_args[0][1]["name"] == "Updated Name"
        assert "updated_at" in call_args[0][1]

        assert result.name == "Updated Name"

    def test_update_nonexistent_job_raises_error(self, mock_storage):
        """Test that updating nonexistent job raises JobNotFoundError."""
        service = JobService(storage=mock_storage)

        mock_storage.update_job.side_effect = ValueError("Job not found")

        with pytest.raises(JobNotFoundError):
            service.update("nonexistent", name="New Name")

    def test_update_with_no_changes_gets_current_job(self, mock_storage):
        """Test that update with no parameters fetches current job."""
        service = JobService(storage=mock_storage)

        mock_storage.get_job.return_value = {
            "job_id": "test-1",
            "name": "Test",
            "trigger_type": "interval",
            "trigger_args": {},
            "timezone": "UTC",
            "status": "scheduled",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
            "func_name": "test",
            "args": [],
            "kwargs": {},
        }

        service.update("test-1")

        # Should call get_job, not update_job
        mock_storage.get_job.assert_called_once_with("test-1")
        mock_storage.update_job.assert_not_called()

    def test_update_with_no_changes_nonexistent_raises_error(self, mock_storage):
        """Test that update with no changes on nonexistent job raises error."""
        service = JobService(storage=mock_storage)

        mock_storage.get_job.return_value = None

        with pytest.raises(JobNotFoundError):
            service.update("nonexistent")


class TestJobServiceDelete:
    """Test JobService.delete() with mocked storage."""

    def test_delete_existing_job_returns_true(self, mock_storage):
        """Test that deleting existing job returns True."""
        service = JobService(storage=mock_storage)

        mock_storage.delete_job.return_value = True

        result = service.delete("test-1")

        mock_storage.delete_job.assert_called_once_with("test-1")
        assert result is True

    def test_delete_nonexistent_job_returns_false(self, mock_storage):
        """Test that deleting nonexistent job returns False."""
        service = JobService(storage=mock_storage)

        mock_storage.delete_job.return_value = False

        result = service.delete("nonexistent")

        assert result is False


class TestJobServiceQuery:
    """Test JobService.query() with mocked storage."""

    def test_query_calls_storage_with_filters(self, mock_storage):
        """Test that query passes filters to storage."""
        service = JobService(storage=mock_storage)

        mock_storage.query_jobs.return_value = []

        filters = {"status": "scheduled"}
        service.query(filters=filters, limit=10)

        mock_storage.query_jobs.assert_called_once_with(filters=filters, limit=10)

    def test_query_returns_list_of_job_info(self, mock_storage):
        """Test that query returns list of JobInfo objects."""
        service = JobService(storage=mock_storage)

        mock_storage.query_jobs.return_value = [
            {
                "job_id": "test-1",
                "name": "Job 1",
                "trigger_type": "interval",
                "trigger_args": {},
                "timezone": "UTC",
                "status": "scheduled",
                "metadata": {},
                "created_at": "2024-01-01T12:00:00+00:00",
                "updated_at": "2024-01-01T12:00:00+00:00",
                "func_name": "test",
                "args": [],
                "kwargs": {},
            }
        ]

        result = service.query()

        assert len(result) == 1
        assert result[0].job_id == "test-1"


class TestJobServiceExists:
    """Test JobService.exists() with mocked storage."""

    def test_exists_returns_true_when_job_found(self, mock_storage):
        """Test that exists returns True when job is found."""
        service = JobService(storage=mock_storage)

        mock_storage.get_job.return_value = {
            "job_id": "test-1",
            "name": "Test",
            "trigger_type": "interval",
            "trigger_args": {},
            "timezone": "UTC",
            "status": "scheduled",
            "metadata": {},
            "created_at": "2024-01-01T12:00:00+00:00",
            "updated_at": "2024-01-01T12:00:00+00:00",
            "func_name": "test",
            "args": [],
            "kwargs": {},
        }

        result = service.exists("test-1")

        assert result is True

    def test_exists_returns_false_when_job_not_found(self, mock_storage):
        """Test that exists returns False when job not found."""
        service = JobService(storage=mock_storage)

        mock_storage.get_job.return_value = None

        result = service.exists("nonexistent")

        assert result is False


class TestJobServiceCount:
    """Test JobService.count() with mocked storage."""

    def test_count_calls_storage_with_filters(self, mock_storage):
        """Test that count passes filters to storage."""
        service = JobService(storage=mock_storage)

        mock_storage.count_jobs.return_value = 5

        filters = {"status": "scheduled"}
        result = service.count(filters=filters)

        mock_storage.count_jobs.assert_called_once_with(filters=filters)
        assert result == 5

    def test_count_without_filters(self, mock_storage):
        """Test count without filters."""
        service = JobService(storage=mock_storage)

        mock_storage.count_jobs.return_value = 10

        result = service.count()

        mock_storage.count_jobs.assert_called_once_with(filters=None)
        assert result == 10
