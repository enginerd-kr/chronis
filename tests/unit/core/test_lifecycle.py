"""Tests for job lifecycle helpers."""

from datetime import datetime, timedelta

from chronis.core import lifecycle
from chronis.core.common.types import TriggerType
from chronis.core.jobs.definition import JobInfo
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
        "next_run_time_local": next_run_time.isoformat() if next_run_time else None,
        "metadata": {},
        "created_at": utc_now().isoformat(),
        "updated_at": utc_now().isoformat(),
    }
    return JobInfo.from_dict(data)


class TestCanExecute:
    """Tests for can_execute method."""

    def test_scheduled_job_with_past_run_time_can_execute(self):
        """Scheduled job with past run time can execute."""
        past_time = utc_now() - timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=past_time)

        assert lifecycle.can_execute(job) is True

    def test_scheduled_job_with_future_run_time_cannot_execute(self):
        """Scheduled job with future run time cannot execute."""
        future_time = utc_now() + timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=future_time)

        assert lifecycle.can_execute(job) is False

    def test_running_job_cannot_execute(self):
        """Running job cannot execute (duplicate prevention)."""
        job = create_test_job(status=JobStatus.RUNNING)

        assert lifecycle.can_execute(job) is False

    def test_pending_job_can_execute(self):
        """Pending job can execute."""
        job = create_test_job(status=JobStatus.PENDING)

        assert lifecycle.can_execute(job) is True

    def test_failed_job_can_execute(self):
        """Failed job can execute (retry)."""
        job = create_test_job(status=JobStatus.FAILED)

        assert lifecycle.can_execute(job) is True


class TestIsReadyForExecution:
    """Tests for is_ready_for_execution method."""

    def test_scheduled_job_ready_when_time_passed(self):
        """Scheduled job is ready when time has passed."""
        past_time = utc_now() - timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=past_time)

        assert lifecycle.is_ready_for_execution(job) is True

    def test_scheduled_job_not_ready_when_time_future(self):
        """Scheduled job is not ready when time is in future."""
        future_time = utc_now() + timedelta(seconds=10)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=future_time)

        assert lifecycle.is_ready_for_execution(job) is False

    def test_running_job_not_ready(self):
        """Running job is not ready."""
        job = create_test_job(status=JobStatus.RUNNING)

        assert lifecycle.is_ready_for_execution(job) is False

    def test_failed_job_not_ready(self):
        """Failed job is not considered ready (needs explicit retry)."""
        job = create_test_job(status=JobStatus.FAILED)

        assert lifecycle.is_ready_for_execution(job) is False

    def test_job_without_next_run_time_not_ready(self):
        """Job without next_run_time is not ready."""
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=None)

        assert lifecycle.is_ready_for_execution(job) is False

    def test_custom_current_time(self):
        """Can check readiness against custom time."""
        run_time = utc_now() + timedelta(seconds=60)
        job = create_test_job(status=JobStatus.SCHEDULED, next_run_time=run_time)

        # Not ready now
        assert lifecycle.is_ready_for_execution(job) is False

        # Ready at custom future time
        future_check_time = run_time + timedelta(seconds=1)
        assert lifecycle.is_ready_for_execution(job, future_check_time) is True


class TestDetermineNextStatusAfterExecution:
    """Tests for determine_next_status_after_execution method."""

    def test_successful_interval_job_becomes_scheduled(self):
        """Successful interval job transitions to SCHEDULED."""
        next_status = lifecycle.determine_next_status_after_execution(
            TriggerType.INTERVAL, execution_succeeded=True
        )

        assert next_status == JobStatus.SCHEDULED

    def test_successful_cron_job_becomes_scheduled(self):
        """Successful cron job transitions to SCHEDULED."""
        next_status = lifecycle.determine_next_status_after_execution(
            TriggerType.CRON, execution_succeeded=True
        )

        assert next_status == JobStatus.SCHEDULED

    def test_successful_date_job_returns_none(self):
        """Successful date job returns None (should be deleted)."""
        next_status = lifecycle.determine_next_status_after_execution(
            TriggerType.DATE, execution_succeeded=True
        )

        assert next_status is None

    def test_failed_job_becomes_failed(self):
        """Failed job transitions to FAILED regardless of trigger type."""
        for trigger_type in [TriggerType.INTERVAL, TriggerType.CRON, TriggerType.DATE]:
            next_status = lifecycle.determine_next_status_after_execution(
                trigger_type, execution_succeeded=False
            )

            assert next_status == JobStatus.FAILED
