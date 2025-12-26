"""Tests for JobLifecycleManager."""

from datetime import datetime, timedelta

import pytest

from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobInfo
from chronis.core.lifecycle import JobLifecycleManager
from chronis.core.state import JobStatus
from chronis.utils.time import utc_now


def create_test_job(
    status: JobStatus = JobStatus.SCHEDULED,
    trigger_type: TriggerType = TriggerType.INTERVAL,
    next_run_time: datetime | None = "default",
) -> JobInfo:
    """Helper to create test job."""
    if next_run_time == "default":
        next_run_time = utc_now() - timedelta(seconds=1)  # Past time

    data = {
        "job_id": "test-job-1",
        "name": "Test Job",
        "trigger_type": trigger_type.value,
        "trigger_args": {"seconds": 10},
        "timezone": "UTC",
        "status": status.value,
        "next_run_time": next_run_time.isoformat() if next_run_time else None,
        "metadata": {},
        "created_at": utc_now().isoformat(),
        "updated_at": utc_now().isoformat(),
    }
    return JobInfo(data)


class TestCanExecute:
    """Tests for can_execute method."""

    def test_scheduled_job_with_past_run_time_can_execute(self):
        """Scheduled job with past run time can execute."""
        past_time = utc_now() - timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=past_time)

        assert JobLifecycleManager.can_execute(job) is True

    def test_scheduled_job_with_future_run_time_cannot_execute(self):
        """Scheduled job with future run time cannot execute."""
        future_time = utc_now() + timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=future_time)

        assert JobLifecycleManager.can_execute(job) is False

    def test_running_job_cannot_execute(self):
        """Running job cannot execute (duplicate prevention)."""
        job = create_test_job(status=JobStatus.RUNNING)

        assert JobLifecycleManager.can_execute(job) is False

    def test_pending_job_can_execute(self):
        """Pending job can execute."""
        job = create_test_job(status=JobStatus.PENDING)

        assert JobLifecycleManager.can_execute(job) is True

    def test_failed_job_can_execute(self):
        """Failed job can execute (retry)."""
        job = create_test_job(status=JobStatus.FAILED)

        assert JobLifecycleManager.can_execute(job) is True


class TestIsReadyForExecution:
    """Tests for is_ready_for_execution method."""

    def test_scheduled_job_ready_when_time_passed(self):
        """Scheduled job is ready when time has passed."""
        past_time = utc_now() - timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=past_time)

        assert JobLifecycleManager.is_ready_for_execution(job) is True

    def test_scheduled_job_not_ready_when_time_future(self):
        """Scheduled job is not ready when time is in future."""
        future_time = utc_now() + timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=future_time)

        assert JobLifecycleManager.is_ready_for_execution(job) is False

    def test_running_job_not_ready(self):
        """Running job is not ready."""
        job = create_test_job(status=JobStatus.RUNNING)

        assert JobLifecycleManager.is_ready_for_execution(job) is False

    def test_failed_job_not_ready(self):
        """Failed job is not considered ready (needs explicit retry)."""
        job = create_test_job(status=JobStatus.FAILED)

        assert JobLifecycleManager.is_ready_for_execution(job) is False

    def test_job_without_next_run_time_not_ready(self):
        """Job without next_run_time is not ready."""
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=None)

        assert JobLifecycleManager.is_ready_for_execution(job) is False

    def test_custom_current_time(self):
        """Can check readiness against custom time."""
        run_time = utc_now() + timedelta(seconds=60)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=run_time)

        # Not ready now
        assert JobLifecycleManager.is_ready_for_execution(job) is False

        # Ready at custom future time
        future_check_time = run_time + timedelta(seconds=1)
        assert JobLifecycleManager.is_ready_for_execution(job, future_check_time) is True


class TestDetermineNextStatusAfterExecution:
    """Tests for determine_next_status_after_execution method."""

    def test_successful_interval_job_becomes_scheduled(self):
        """Successful interval job transitions to SCHEDULED."""
        job = create_test_job(trigger_type=TriggerType.INTERVAL)

        next_status = JobLifecycleManager.determine_next_status_after_execution(
            job, execution_succeeded=True
        )

        assert next_status == JobStatus.SCHEDULED

    def test_successful_cron_job_becomes_scheduled(self):
        """Successful cron job transitions to SCHEDULED."""
        job = create_test_job(trigger_type=TriggerType.CRON)

        next_status = JobLifecycleManager.determine_next_status_after_execution(
            job, execution_succeeded=True
        )

        assert next_status == JobStatus.SCHEDULED

    def test_successful_date_job_returns_none(self):
        """Successful date job returns None (should be deleted)."""
        job = create_test_job(trigger_type=TriggerType.DATE)

        next_status = JobLifecycleManager.determine_next_status_after_execution(
            job, execution_succeeded=True
        )

        assert next_status is None

    def test_failed_job_becomes_failed(self):
        """Failed job transitions to FAILED regardless of trigger type."""
        for trigger_type in [TriggerType.INTERVAL, TriggerType.CRON, TriggerType.DATE]:
            job = create_test_job(trigger_type=trigger_type)

            next_status = JobLifecycleManager.determine_next_status_after_execution(
                job, execution_succeeded=False
            )

            assert next_status == JobStatus.FAILED


class TestShouldDeleteAfterExecution:
    """Tests for should_delete_after_execution method."""

    def test_date_trigger_should_delete(self):
        """DATE trigger jobs should be deleted after execution."""
        job = create_test_job(trigger_type=TriggerType.DATE)

        assert JobLifecycleManager.should_delete_after_execution(job) is True

    def test_interval_trigger_should_not_delete(self):
        """INTERVAL trigger jobs should not be deleted."""
        job = create_test_job(trigger_type=TriggerType.INTERVAL)

        assert JobLifecycleManager.should_delete_after_execution(job) is False

    def test_cron_trigger_should_not_delete(self):
        """CRON trigger jobs should not be deleted."""
        job = create_test_job(trigger_type=TriggerType.CRON)

        assert JobLifecycleManager.should_delete_after_execution(job) is False


class TestCanRetry:
    """Tests for can_retry method."""

    def test_failed_job_can_retry(self):
        """Failed job can be retried."""
        job = create_test_job(status=JobStatus.FAILED)

        assert JobLifecycleManager.can_retry(job) is True

    def test_scheduled_job_cannot_retry(self):
        """Scheduled job cannot be retried (not failed)."""
        job = create_test_job(status=JobStatus.SCHEDULED)

        assert JobLifecycleManager.can_retry(job) is False

    def test_running_job_cannot_retry(self):
        """Running job cannot be retried."""
        job = create_test_job(status=JobStatus.RUNNING)

        assert JobLifecycleManager.can_retry(job) is False


class TestValidateStateTransition:
    """Tests for validate_state_transition method."""

    def test_valid_transitions(self):
        """All valid state transitions should pass."""
        valid_transitions = [
            (JobStatus.PENDING, JobStatus.SCHEDULED),
            (JobStatus.PENDING, JobStatus.RUNNING),
            (JobStatus.SCHEDULED, JobStatus.RUNNING),
            (JobStatus.SCHEDULED, JobStatus.FAILED),
            (JobStatus.RUNNING, JobStatus.SCHEDULED),
            (JobStatus.RUNNING, JobStatus.FAILED),
            (JobStatus.FAILED, JobStatus.SCHEDULED),
            (JobStatus.FAILED, JobStatus.RUNNING),
        ]

        for current, next_status in valid_transitions:
            assert JobLifecycleManager.validate_state_transition(current, next_status) is True

    def test_invalid_transitions(self):
        """Invalid state transitions should raise ValueError."""
        invalid_transitions = [
            (JobStatus.RUNNING, JobStatus.PENDING),
            (JobStatus.SCHEDULED, JobStatus.PENDING),
            (JobStatus.FAILED, JobStatus.PENDING),
        ]

        for current, next_status in invalid_transitions:
            with pytest.raises(ValueError, match="Invalid state transition"):
                JobLifecycleManager.validate_state_transition(current, next_status)
