"""Pure unit tests for SchedulingOrchestrator with mocks."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

from chronis.core.services.scheduling_orchestrator import SchedulingOrchestrator


class TestPollAndEnqueueQueueFull:
    """Test poll_and_enqueue when queue is full."""

    def test_queue_full_logs_warning_and_returns_zero(
        self, mock_storage, mock_job_queue, mock_logger
    ):
        """Test that full queue logs warning and skips polling."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
        )

        # Mock queue as full
        mock_job_queue.get_available_slots.return_value = 0
        mock_job_queue.get_status.return_value = {"size": 10, "max_size": 10}

        result = orchestrator.poll_and_enqueue()

        # Should return 0 and not query storage
        assert result == 0
        mock_storage.query_jobs.assert_not_called()

        # Should log warning
        mock_logger.warning.assert_called_once()
        assert "queue is full" in mock_logger.warning.call_args[0][0].lower()

    def test_queue_available_polls_storage(self, mock_storage, mock_job_queue, mock_logger):
        """Test that available queue polls storage for ready jobs."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
        )

        mock_job_queue.get_available_slots.return_value = 5
        mock_storage.query_jobs.return_value = []

        with patch("chronis.utils.time.utc_now") as mock_utc_now:
            mock_utc_now.return_value = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)

            result = orchestrator.poll_and_enqueue()

        # Should query storage
        mock_storage.query_jobs.assert_called_once()

        # No jobs found, should return 0
        assert result == 0


class TestPollAndEnqueueVerboseLogging:
    """Test verbose logging in poll_and_enqueue."""

    def test_verbose_mode_logs_found_jobs(self, mock_storage, mock_job_queue, mock_logger):
        """Test that verbose mode logs when jobs are found."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
            verbose=True,  # Verbose mode
        )

        mock_job_queue.get_available_slots.return_value = 10
        mock_job_queue.add_job.return_value = True

        # Return 2 jobs (less than 10, so normally wouldn't log)
        mock_storage.query_jobs.return_value = [
            {"job_id": "job-1"},
            {"job_id": "job-2"},
        ]

        with patch("chronis.utils.time.utc_now"):
            orchestrator.poll_and_enqueue()

        # Verbose mode should log even for small number of jobs
        mock_logger.info.assert_called()
        assert "Found ready jobs" in mock_logger.info.call_args[0][0]

    def test_many_jobs_logs_even_without_verbose(self, mock_storage, mock_job_queue, mock_logger):
        """Test that finding >=10 jobs logs even without verbose."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
            verbose=False,  # Not verbose
        )

        mock_job_queue.get_available_slots.return_value = 20
        mock_job_queue.add_job.return_value = True

        # Return 10+ jobs
        mock_storage.query_jobs.return_value = [{"job_id": f"job-{i}"} for i in range(10)]

        with patch("chronis.utils.time.utc_now"):
            orchestrator.poll_and_enqueue()

        # Should log because count >= 10
        mock_logger.info.assert_called()
        assert "Found ready jobs" in mock_logger.info.call_args[0][0]


class TestPollAndEnqueueJobAddFailure:
    """Test handling when job cannot be added to queue."""

    def test_failed_to_add_job_logs_warning(self, mock_storage, mock_job_queue, mock_logger):
        """Test that failure to add job logs warning."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
        )

        mock_job_queue.get_available_slots.return_value = 10
        mock_job_queue.add_job.return_value = False  # Failed to add

        mock_storage.query_jobs.return_value = [{"job_id": "job-1"}]

        with patch("chronis.utils.time.utc_now"):
            result = orchestrator.poll_and_enqueue()

        # Should return 0 (no jobs added)
        assert result == 0

        # Should log warning
        mock_logger.warning.assert_called()
        assert "Failed to add job to queue" in mock_logger.warning.call_args[0][0]


class TestGetNextJobFromQueue:
    """Test get_next_job_from_queue method."""

    def test_get_next_job_from_empty_queue_returns_none(
        self, mock_storage, mock_job_queue, mock_logger
    ):
        """Test that get_next_job_from_queue returns None when queue is empty."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
        )

        mock_job_queue.is_empty.return_value = True

        result = orchestrator.get_next_job_from_queue()

        assert result is None
        mock_job_queue.get_next_job.assert_not_called()

    def test_get_next_job_from_queue_returns_job(self, mock_storage, mock_job_queue, mock_logger):
        """Test that get_next_job_from_queue delegates to queue."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
        )

        mock_job_queue.is_empty.return_value = False
        mock_job_queue.get_next_job.return_value = {"job_id": "job-1"}

        result = orchestrator.get_next_job_from_queue()

        mock_job_queue.get_next_job.assert_called_once()
        assert result == {"job_id": "job-1"}


class TestMarkJobCompleted:
    """Test mark_job_completed method."""

    def test_mark_job_completed_calls_queue_mark_completed(
        self, mock_storage, mock_job_queue, mock_logger
    ):
        """Test that mark_job_completed delegates to queue."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
        )

        mock_job_queue.mark_completed = Mock()

        orchestrator.mark_job_completed("job-1")

        mock_job_queue.mark_completed.assert_called_once_with("job-1")


class TestGetQueueStatus:
    """Test get_queue_status method."""

    def test_get_queue_status_returns_from_queue(self, mock_storage, mock_job_queue, mock_logger):
        """Test that get_queue_status delegates to job_queue."""
        orchestrator = SchedulingOrchestrator(
            storage=mock_storage,
            job_queue=mock_job_queue,
            logger=mock_logger,
        )

        mock_job_queue.get_status.return_value = {"size": 5, "max_size": 10}

        result = orchestrator.get_queue_status()

        assert result == {"size": 5, "max_size": 10}
