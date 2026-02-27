"""Unit tests for JobDefinition Pydantic model and JobInfo."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from chronis.core.jobs.definition import JobDefinition, JobInfo
from chronis.core.state import JobStatus
from chronis.core.state.enums import TriggerType


class TestJobDefinitionValidators:
    """Test Pydantic validators on JobDefinition."""

    def test_invalid_timezone_raises_error(self):
        """잘못된 timezone → ValueError."""
        with pytest.raises(ValidationError, match="timezone"):
            JobDefinition(
                job_id="test",
                name="test",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=lambda: None,
                timezone="Invalid/Zone",
            )

    def test_priority_below_range_raises_error(self):
        """priority -1 → ValueError."""
        with pytest.raises(ValidationError, match="Priority"):
            JobDefinition(
                job_id="test",
                name="test",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=lambda: None,
                priority=-1,
            )

    def test_priority_above_range_raises_error(self):
        """priority 11 → ValueError."""
        with pytest.raises(ValidationError, match="Priority"):
            JobDefinition(
                job_id="test",
                name="test",
                trigger_type=TriggerType.INTERVAL,
                trigger_args={"seconds": 30},
                func=lambda: None,
                priority=11,
            )

    def test_priority_boundary_values_accepted(self):
        """priority 0과 10은 허용."""
        job0 = JobDefinition(
            job_id="t0",
            name="t",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=lambda: None,
            priority=0,
        )
        job10 = JobDefinition(
            job_id="t10",
            name="t",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=lambda: None,
            priority=10,
        )
        assert job0.priority == 0
        assert job10.priority == 10

    def test_default_args_converts_none_to_tuple(self):
        """None args → 빈 tuple."""
        job = JobDefinition(
            job_id="test",
            name="test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=lambda: None,
            args=None,
        )
        assert job.args == ()

    def test_default_dicts_converts_none_to_dict(self):
        """None kwargs/metadata → 빈 dict."""
        job = JobDefinition(
            job_id="test",
            name="test",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=lambda: None,
            kwargs=None,
            metadata=None,
        )
        assert job.kwargs == {}
        assert job.metadata == {}


class TestModelPostInit:
    """Test model_post_init if_missed defaults."""

    def test_date_defaults_to_run_once(self):
        """DATE trigger → if_missed='run_once'."""
        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.DATE,
            trigger_args={"run_date": "2025-01-01 09:00:00"},
            func=lambda: None,
        )
        assert job.if_missed == "run_once"

    def test_cron_defaults_to_skip(self):
        """CRON trigger → if_missed='skip'."""
        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.CRON,
            trigger_args={"hour": 9},
            func=lambda: None,
        )
        assert job.if_missed == "skip"

    def test_interval_defaults_to_run_once(self):
        """INTERVAL trigger → if_missed='run_once'."""
        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=lambda: None,
        )
        assert job.if_missed == "run_once"

    def test_explicit_override_preserved(self):
        """명시적 if_missed 지정 시 기본값 무시."""
        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.CRON,
            trigger_args={"hour": 9},
            func=lambda: None,
            if_missed="run_all",
        )
        assert job.if_missed == "run_all"


class TestToDict:
    """Test JobDefinition.to_dict()."""

    def test_callable_func_converted_to_qualified_name(self):
        """callable func → module.name 변환."""

        def my_function():
            pass

        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=my_function,
        )
        d = job.to_dict()
        assert "my_function" in d["func_name"]
        assert "." in d["func_name"]  # module.name format

    def test_string_func_preserved(self):
        """string func → 그대로 유지."""
        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func="my_module.my_func",
        )
        d = job.to_dict()
        assert d["func_name"] == "my_module.my_func"

    def test_next_run_time_auto_calculated_when_none(self):
        """next_run_time None → 자동 계산."""
        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=lambda: None,
        )
        d = job.to_dict()
        assert d["next_run_time"] is not None
        assert d["status"] == "scheduled"

    def test_to_dict_includes_all_fields(self):
        """to_dict에 필수 필드가 모두 포함."""
        job = JobDefinition(
            job_id="t",
            name="t",
            trigger_type=TriggerType.INTERVAL,
            trigger_args={"seconds": 30},
            func=lambda: None,
        )
        d = job.to_dict()
        required_keys = {
            "job_id",
            "name",
            "trigger_type",
            "trigger_args",
            "timezone",
            "func_name",
            "args",
            "kwargs",
            "status",
            "next_run_time",
            "next_run_time_local",
            "metadata",
            "max_retries",
            "retry_delay_seconds",
            "retry_count",
            "timeout_seconds",
            "priority",
            "if_missed",
            "misfire_threshold_seconds",
            "last_run_time",
            "last_scheduled_time",
            "created_at",
            "updated_at",
        }
        assert required_keys.issubset(d.keys())


class TestCanExecute:
    """Test JobInfo.can_execute() and is_ready_for_execution()."""

    def _make_job_info(self, **overrides):
        defaults = {
            "job_id": "t",
            "name": "t",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": JobStatus.SCHEDULED,
            "next_run_time": None,
            "next_run_time_local": None,
            "metadata": {},
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
        defaults.update(overrides)
        return JobInfo(**defaults)

    def test_paused_cannot_execute(self):
        """PAUSED 상태 → can_execute() False."""
        info = self._make_job_info(status=JobStatus.PAUSED)
        assert info.can_execute() is False

    def test_running_cannot_execute(self):
        """RUNNING 상태 → can_execute() False."""
        info = self._make_job_info(status=JobStatus.RUNNING)
        assert info.can_execute() is False

    def test_scheduled_with_past_time_can_execute(self):
        """SCHEDULED + 과거 next_run_time → can_execute() True."""
        from datetime import timedelta

        past = datetime.now(UTC) - timedelta(minutes=5)
        info = self._make_job_info(
            status=JobStatus.SCHEDULED,
            next_run_time=past,
        )
        assert info.can_execute() is True

    def test_scheduled_with_future_time_cannot_execute(self):
        """SCHEDULED + 미래 next_run_time → can_execute() False."""
        from datetime import timedelta

        future = datetime.now(UTC) + timedelta(hours=1)
        info = self._make_job_info(
            status=JobStatus.SCHEDULED,
            next_run_time=future,
        )
        assert info.can_execute() is False

    def test_scheduled_no_next_run_time_can_execute(self):
        """SCHEDULED + next_run_time None → can_execute() True."""
        info = self._make_job_info(status=JobStatus.SCHEDULED)
        assert info.can_execute() is True


class TestFromDict:
    """Test JobInfo.from_dict()."""

    def test_from_dict_parses_timestamps(self):
        """from_dict가 ISO timestamp을 datetime으로 파싱."""
        data = {
            "job_id": "t",
            "name": "t",
            "trigger_type": "interval",
            "trigger_args": {"seconds": 30},
            "timezone": "UTC",
            "status": "scheduled",
            "next_run_time": "2025-01-01T09:00:00+00:00",
            "next_run_time_local": "2025-01-01T09:00:00+00:00",
            "metadata": {},
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
        }
        info = JobInfo.from_dict(data)
        assert isinstance(info.next_run_time, datetime)
        assert isinstance(info.created_at, datetime)
        assert info.status == JobStatus.SCHEDULED

    def test_from_dict_handles_missing_optional_fields(self):
        """from_dict가 선택 필드 누락 시 기본값 사용."""
        data = {
            "job_id": "t",
            "name": "t",
            "trigger_type": "interval",
            "trigger_args": {},
            "status": "scheduled",
            "metadata": {},
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
        }
        info = JobInfo.from_dict(data)
        assert info.timezone == "UTC"
        assert info.max_retries == 0
        assert info.priority == 5
        assert info.next_run_time is None
        assert info.last_run_time is None

    def test_from_dict_invalid_status_raises_error(self):
        """잘못된 status 문자열 → ValueError."""
        data = {
            "job_id": "t",
            "name": "t",
            "trigger_type": "interval",
            "trigger_args": {},
            "status": "invalid_status",
            "metadata": {},
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
        }
        with pytest.raises(ValueError):
            JobInfo.from_dict(data)


class TestDetermineNextStatus:
    """Test JobInfo.determine_next_status_after_execution()."""

    def test_date_success_returns_none(self):
        """DATE + 성공 → None (삭제 대상)."""
        result = JobInfo.determine_next_status_after_execution("date", True)
        assert result is None

    def test_interval_success_returns_scheduled(self):
        """INTERVAL + 성공 → SCHEDULED."""
        result = JobInfo.determine_next_status_after_execution("interval", True)
        assert result == JobStatus.SCHEDULED

    def test_cron_success_returns_scheduled(self):
        """CRON + 성공 → SCHEDULED."""
        result = JobInfo.determine_next_status_after_execution("cron", True)
        assert result == JobStatus.SCHEDULED

    def test_any_failure_returns_failed(self):
        """어떤 trigger_type이든 실패 → FAILED."""
        for tt in ["date", "interval", "cron"]:
            result = JobInfo.determine_next_status_after_execution(tt, False)
            assert result == JobStatus.FAILED
