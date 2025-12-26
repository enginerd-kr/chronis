"""Tests for SchedulingOrchestrator."""

import logging
from datetime import timedelta

from chronis.adapters.storage.memory import InMemoryStorageAdapter
from chronis.core.common.types import TriggerType
from chronis.core.job_queue import JobQueue
from chronis.core.jobs.definition import JobDefinition
from chronis.core.services import JobService, SchedulingOrchestrator
from chronis.utils.logging import ContextLogger
from chronis.utils.time import utc_now


class TestSchedulingOrchestrator:
    """Tests for SchedulingOrchestrator."""

    def setup_method(self):
        """Setup test fixtures."""
        self.storage = InMemoryStorageAdapter()
        self.job_queue = JobQueue(max_queue_size=10)

        # Create real logger for ContextLogger
        base_logger = logging.getLogger("test")
        base_logger.setLevel(logging.WARNING)  # Suppress logs in tests
        self.logger = ContextLogger(base_logger)

        self.orchestrator = SchedulingOrchestrator(
            storage=self.storage, job_queue=self.job_queue, logger=self.logger, verbose=False
        )
        self.job_service = JobService(storage=self.storage)

    def test_poll_and_enqueue_empty_storage(self):
        """Test polling when storage is empty."""
        added = self.orchestrator.poll_and_enqueue()

        assert added == 0
        assert self.orchestrator.is_queue_empty() is True

    def test_poll_and_enqueue_ready_jobs(self):
        """Test polling and enqueuing ready jobs."""

        def test_func():
            pass

        # Create job with past next_run_time
        past_time = utc_now() - timedelta(seconds=10)
        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
            next_run_time=past_time,
        )
        self.job_service.create(job_def)

        # Poll
        added = self.orchestrator.poll_and_enqueue()

        assert added == 1
        assert self.orchestrator.is_queue_empty() is False

    def test_poll_and_enqueue_future_jobs(self):
        """Test that future jobs are not enqueued."""

        def test_func():
            pass

        # Create job with future next_run_time
        future_time = utc_now() + timedelta(seconds=60)
        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
            next_run_time=future_time,
        )
        self.job_service.create(job_def)

        # Poll
        added = self.orchestrator.poll_and_enqueue()

        assert added == 0
        assert self.orchestrator.is_queue_empty() is True

    def test_poll_respects_queue_limit(self):
        """Test that polling respects queue capacity."""

        def test_func():
            pass

        # Create 15 ready jobs (more than queue capacity of 10)
        past_time = utc_now() - timedelta(seconds=10)
        for i in range(15):
            job_def = JobDefinition(
                job_id=f"test-{i}",
                name=f"Test Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
                next_run_time=past_time,
            )
            self.job_service.create(job_def)

        # Poll
        added = self.orchestrator.poll_and_enqueue()

        # Should only add up to queue capacity
        assert added <= 10

    def test_get_next_job_from_queue(self):
        """Test getting next job from queue."""

        def test_func():
            pass

        # Create and enqueue job
        past_time = utc_now() - timedelta(seconds=10)
        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
            next_run_time=past_time,
        )
        self.job_service.create(job_def)
        self.orchestrator.poll_and_enqueue()

        # Get next job
        job_data = self.orchestrator.get_next_job_from_queue()

        assert job_data is not None
        assert job_data["job_id"] == "test-1"

    def test_get_next_job_from_empty_queue(self):
        """Test getting from empty queue returns None."""
        job_data = self.orchestrator.get_next_job_from_queue()

        assert job_data is None

    def test_mark_job_completed(self):
        """Test marking job as completed."""

        def test_func():
            pass

        # Create and enqueue job
        past_time = utc_now() - timedelta(seconds=10)
        job_def = JobDefinition(
            job_id="test-1",
            name="Test Job",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=test_func,
            next_run_time=past_time,
        )
        self.job_service.create(job_def)
        self.orchestrator.poll_and_enqueue()

        # Get and mark as running
        job_data = self.orchestrator.get_next_job_from_queue()
        assert job_data is not None

        # Mark completed
        self.orchestrator.mark_job_completed("test-1")

        # Should be removed from in-flight set
        status = self.orchestrator.get_queue_status()
        assert status["in_flight_jobs"] == 0

    def test_get_queue_status(self):
        """Test getting queue status."""

        def test_func():
            pass

        # Create jobs
        past_time = utc_now() - timedelta(seconds=10)
        for i in range(3):
            job_def = JobDefinition(
                job_id=f"test-{i}",
                name=f"Test Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
                next_run_time=past_time,
            )
            self.job_service.create(job_def)

        self.orchestrator.poll_and_enqueue()

        status = self.orchestrator.get_queue_status()

        assert status["pending_jobs"] == 3
        assert status["in_flight_jobs"] == 0
        assert status["available_slots"] == 7

    def test_poll_when_queue_full(self):
        """Test polling when queue is full."""

        def test_func():
            pass

        # Fill queue to capacity
        past_time = utc_now() - timedelta(seconds=10)
        for i in range(10):
            job_def = JobDefinition(
                job_id=f"test-{i}",
                name=f"Test Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
                next_run_time=past_time,
            )
            self.job_service.create(job_def)

        self.orchestrator.poll_and_enqueue()

        # Create more jobs
        for i in range(10, 15):
            job_def = JobDefinition(
                job_id=f"test-{i}",
                name=f"Test Job {i}",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=test_func,
                next_run_time=past_time,
            )
            self.job_service.create(job_def)

        # Try to poll again - should return 0 (queue full)
        added = self.orchestrator.poll_and_enqueue()

        assert added == 0
