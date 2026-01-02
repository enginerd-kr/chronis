"""Execution History Example using Success/Failure Handlers.

This example demonstrates how to implement execution history tracking
using Chronis's success and failure handlers. No built-in history table
is needed - handlers provide execution event notifications.

Key concepts:
- Use success handler to log successful executions
- Use failure handler to log failed executions
- Query execution history for monitoring and debugging
- Generate execution statistics

Note: Handlers cannot track execution duration (start/end time) since
they are called only after job completes. For duration tracking, add
timing code within your job functions or use APM tools.
"""

import time
from datetime import UTC, datetime, timedelta

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler

# =============================================================================
# Execution History Storage
# =============================================================================


class ExecutionHistory:
    """Simple execution history using in-memory list."""

    def __init__(self):
        """Initialize execution history."""
        self.executions = []
        self._id_counter = 0

    def get_success_handler(self):
        """Return success handler for scheduler."""
        return self._on_success

    def get_failure_handler(self):
        """Return failure handler for scheduler."""
        return self._on_failure

    def _on_success(self, job_id: str, job_info):
        """Success handler - log execution.

        Args:
            job_id: Job ID that succeeded
            job_info: JobInfo snapshot at success time
        """
        self._id_counter += 1

        entry = {
            "id": self._id_counter,
            "job_id": job_id,
            "job_name": job_info.name,
            "status": "success",
            "executed_at": datetime.now(UTC).isoformat(),
            "trigger_type": job_info.trigger_type,
            "metadata": job_info.metadata,
        }

        self.executions.append(entry)
        print(f"📝 History: Logged successful execution of '{job_info.name}'")

    def _on_failure(self, job_id: str, error: Exception, job_info):
        """Failure handler - log execution.

        Args:
            job_id: Job ID that failed
            error: Exception that occurred
            job_info: JobInfo snapshot at failure time
        """
        self._id_counter += 1

        entry = {
            "id": self._id_counter,
            "job_id": job_id,
            "job_name": job_info.name,
            "status": "failed",
            "error_type": type(error).__name__,
            "error_message": str(error),
            "executed_at": datetime.now(UTC).isoformat(),
            "trigger_type": job_info.trigger_type,
            "metadata": job_info.metadata,
            "retry_count": job_info.retry_count,
        }

        self.executions.append(entry)
        print(f"❌ History: Logged failed execution of '{job_info.name}' ({type(error).__name__})")

    def get_history(self, job_id=None, status=None, limit=100):
        """Get execution history with optional filters.

        Args:
            job_id: Filter by job ID (None = all jobs)
            status: Filter by status ('success' or 'failed', None = all)
            limit: Maximum number of executions to return

        Returns:
            List of execution records (most recent first)
        """
        filtered = self.executions

        if job_id:
            filtered = [e for e in filtered if e["job_id"] == job_id]

        if status:
            filtered = [e for e in filtered if e["status"] == status]

        # Sort by executed_at descending (most recent first)
        filtered.sort(key=lambda x: x["executed_at"], reverse=True)
        return filtered[:limit]

    def get_statistics(self, job_id=None):
        """Get execution statistics.

        Args:
            job_id: Calculate stats for specific job (None = all jobs)

        Returns:
            Dictionary with statistics
        """
        executions = (
            self.executions if not job_id else [e for e in self.executions if e["job_id"] == job_id]
        )

        total = len(executions)
        success_count = len([e for e in executions if e["status"] == "success"])
        failed_count = total - success_count

        return {
            "total_executions": total,
            "successful": success_count,
            "failed": failed_count,
            "success_rate": (success_count / total * 100) if total > 0 else 0,
        }


# =============================================================================
# Example Jobs
# =============================================================================


def data_sync_job(source: str):
    """Job that syncs data from source."""
    print(f"[{datetime.now()}] Syncing data from {source}...")
    time.sleep(0.5)


def report_generation_job(report_type: str):
    """Job that generates reports."""
    print(f"[{datetime.now()}] Generating {report_type} report...")
    time.sleep(0.3)


def flaky_api_job():
    """Job that fails occasionally (simulates flaky API)."""
    import random

    if random.random() < 0.5:  # 50% failure rate
        raise ConnectionError("API timeout")
    print(f"[{datetime.now()}] API call succeeded")


# =============================================================================
# Main Example
# =============================================================================


def main():
    """Demonstrate execution history tracking."""
    print("=" * 70)
    print("Execution History Example")
    print("=" * 70)

    # 1. Setup execution history
    print("\n1. Setting up execution history...")
    history = ExecutionHistory()
    print("   ✓ Execution history initialized")

    # 2. Setup scheduler with history handlers
    print("\n2. Creating scheduler with execution history handlers...")
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,
        on_success=history.get_success_handler(),
        on_failure=history.get_failure_handler(),
        verbose=False,
    )
    print("   ✓ Scheduler created with execution history tracking")

    # 3. Register job functions
    print("\n3. Registering job functions...")
    scheduler.register_job_function("data_sync", data_sync_job)
    scheduler.register_job_function("report_gen", report_generation_job)
    scheduler.register_job_function("flaky_api", flaky_api_job)
    print("   ✓ Registered 3 job functions")

    # 4. Create jobs
    print("\n4. Creating jobs...")

    # Interval job - runs multiple times
    job1 = scheduler.create_interval_job(
        func="data_sync",
        seconds=3,
        kwargs={"source": "database"},
        metadata={"team": "data-eng", "priority": "high"},
    )
    print(f"   ✓ Created interval job: {job1.job_id}")

    # Date jobs - run once
    for i in range(3):
        scheduler.create_date_job(
            func="report_gen",
            run_date=datetime.now(UTC) + timedelta(seconds=i + 1),
            kwargs={"report_type": f"daily_{i + 1}"},
            metadata={"report_id": f"RPT_{i + 1}"},
        )
    print("   ✓ Created 3 report generation jobs")

    # Flaky job with retries
    job_flaky = scheduler.create_date_job(
        func="flaky_api",
        run_date=datetime.now(UTC) + timedelta(seconds=2),
        max_retries=3,
        retry_delay_seconds=1,
        metadata={"api": "external"},
    )
    print(f"   ✓ Created flaky job: {job_flaky.job_id}")

    # 5. Start scheduler
    print("\n5. Starting scheduler...")
    scheduler.start()
    print("   ✓ Scheduler started\n")
    print("-" * 70)
    print("Watching job execution...\n")

    # 6. Wait for jobs to execute
    time.sleep(12)

    # 7. Stop scheduler
    print("\n" + "-" * 70)
    print("\n6. Stopping scheduler...")
    scheduler.stop()
    print("   ✓ Scheduler stopped")

    # 8. Query execution history
    print("\n7. Querying execution history...")
    all_executions = history.get_history()
    print(f"   ✓ Total executions logged: {len(all_executions)}")

    if all_executions:
        print("\n   Recent executions (last 10):")
        for i, execution in enumerate(all_executions[:10], 1):
            status_icon = "✅" if execution["status"] == "success" else "❌"
            print(f"\n   {i}. {status_icon} {execution['job_name']}")
            print(f"      Job ID: {execution['job_id']}")
            print(f"      Status: {execution['status']}")
            print(f"      Time: {execution['executed_at']}")
            if execution["status"] == "failed":
                print(f"      Error: {execution['error_type']}: {execution['error_message']}")
                print(f"      Retry Count: {execution['retry_count']}")

    # 9. Get statistics
    print("\n8. Execution Statistics:")
    stats = history.get_statistics()
    print(f"   Total Executions: {stats['total_executions']}")
    print(f"   Successful: {stats['successful']}")
    print(f"   Failed: {stats['failed']}")
    print(f"   Success Rate: {stats['success_rate']:.1f}%")

    # 10. Query by job
    print("\n9. Querying history for specific job...")
    sync_job_history = history.get_history(job_id=job1.job_id)
    print(f"   ✓ Data sync job executed {len(sync_job_history)} times")

    # 11. Query failures only
    print("\n10. Querying failed executions only...")
    failed = history.get_history(status="failed")
    print(f"   ✓ Total failures: {len(failed)}")
    if failed:
        print("\n   Failed execution details:")
        for failure in failed:
            print(f"      - {failure['job_name']}: {failure['error_type']}")

    print("\n" + "=" * 70)
    print("Example Complete!")
    print("=" * 70)

    print("\n💡 Key Takeaways:")
    print("   1. Use on_success handler to log successful executions")
    print("   2. Use on_failure handler to log failed executions")
    print("   3. Store execution events in any backend (DB, Redis, file, etc.)")
    print("   4. Query history for monitoring and debugging")
    print("   5. Generate statistics for success rate tracking")
    print("\n⚠️  Limitation:")
    print("   - Cannot track execution duration (handlers called after completion)")
    print("   - For duration, add timing in job function or use APM tools")


if __name__ == "__main__":
    main()
