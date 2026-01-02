"""Dead Letter Queue (DLQ) Example using Failure Handlers.

This example demonstrates how to implement a Dead Letter Queue pattern
using Chronis's failure handler mechanism. No built-in DLQ is needed -
the failure handler provides full access to job information.

Key concepts:
- Store func_name in metadata for job recreation
- Use failure handler to save failed jobs to DLQ
- Implement retry logic by recreating jobs from DLQ
"""

import time
from datetime import UTC, datetime, timedelta

from chronis import InMemoryLockAdapter, InMemoryStorageAdapter, PollingScheduler

# =============================================================================
# DLQ Storage (In-Memory for simplicity, but could be PostgreSQL, Redis, etc.)
# =============================================================================


class SimpleDLQ:
    """Simple DLQ implementation using in-memory list."""

    def __init__(self):
        """Initialize DLQ with in-memory storage."""
        self.failed_jobs = []
        self._id_counter = 0

    def get_failure_handler(self):
        """Return failure handler for scheduler.

        Usage:
            dlq = SimpleDLQ()
            scheduler = PollingScheduler(
                ...,
                on_failure=dlq.get_failure_handler()
            )
        """
        return self._on_failure

    def _on_failure(self, job_id: str, error: Exception, job_info):
        """Failure handler - save to DLQ.

        Called automatically when a job fails after all retries.

        Args:
            job_id: Job ID that failed
            error: Exception that occurred
            job_info: JobInfo snapshot at failure time
        """
        self._id_counter += 1
        func_name = job_info.metadata.get("func_name")

        entry = {
            "id": self._id_counter,
            "job_id": job_id,
            "job_name": job_info.name,
            "func_name": func_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "trigger_type": job_info.trigger_type,
            "trigger_args": job_info.trigger_args,
            "metadata": job_info.metadata,
            "retry_count": job_info.retry_count,
            "failed_at": datetime.now(UTC).isoformat(),
            "status": "pending",
        }

        self.failed_jobs.append(entry)

        print(f"💀 DLQ: Saved failed job '{job_info.name}' (error: {type(error).__name__})")

    def get_failed_jobs(self, status="pending", limit=100):
        """Get failed jobs from DLQ.

        Args:
            status: Filter by status ('pending', 'retried', 'abandoned')
            limit: Maximum number of jobs to return

        Returns:
            List of failed job records
        """
        filtered = [job for job in self.failed_jobs if job["status"] == status]
        # Sort by failed_at descending
        filtered.sort(key=lambda x: x["failed_at"], reverse=True)
        return filtered[:limit]

    def retry_failed_job(self, dlq_id: int, scheduler):
        """Retry a failed job from DLQ.

        Creates a new job with the same configuration and marks the DLQ entry as retried.

        Args:
            dlq_id: DLQ entry ID to retry
            scheduler: Scheduler instance to create new job

        Returns:
            New job ID if created, None if not found
        """
        # Find entry
        entry = None
        for job in self.failed_jobs:
            if job["id"] == dlq_id and job["status"] == "pending":
                entry = job
                break

        if not entry:
            return None

        func_name = entry["func_name"]
        metadata = entry["metadata"]

        # Create new job (schedule for 5 seconds from now)
        new_job = scheduler.create_date_job(
            func=func_name,
            run_date=datetime.now(UTC) + timedelta(seconds=5),
            metadata=metadata,
        )

        # Mark DLQ entry as retried
        entry["status"] = "retried"

        print(f"🔄 DLQ: Retrying job (new job_id: {new_job.job_id})")
        return new_job.job_id

    def get_statistics(self):
        """Get DLQ statistics."""
        stats = {}
        for job in self.failed_jobs:
            status = job["status"]
            stats[status] = stats.get(status, 0) + 1
        return stats


# =============================================================================
# Example Jobs
# =============================================================================


def reliable_job():
    """Job that always succeeds."""
    print(f"[{datetime.now()}] ✅ Reliable job executed successfully")


def flaky_job(item_id: int):
    """Job that fails occasionally."""
    import random

    if random.random() < 0.7:  # 70% failure rate
        raise ConnectionError(f"Network timeout processing item {item_id}")
    print(f"[{datetime.now()}] ✅ Processed item {item_id}")


def always_failing_job(order_id: int):
    """Job that always fails (for demonstration)."""
    raise ValueError(f"Payment gateway error for order {order_id}")


# =============================================================================
# Main Example
# =============================================================================


def main():
    """Demonstrate DLQ pattern."""
    print("=" * 70)
    print("Dead Letter Queue (DLQ) Example")
    print("=" * 70)

    # 1. Setup DLQ
    print("\n1. Setting up DLQ...")
    dlq = SimpleDLQ()
    print("   ✓ DLQ initialized with in-memory storage")

    # 2. Setup scheduler with DLQ failure handler
    print("\n2. Creating scheduler with DLQ failure handler...")
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,
        on_failure=dlq.get_failure_handler(),  # ← DLQ integration
        verbose=False,
    )
    print("   ✓ Scheduler created with DLQ failure handler")

    # 3. Register job functions
    print("\n3. Registering job functions...")
    scheduler.register_job_function("reliable_job", reliable_job)
    scheduler.register_job_function("flaky_job", flaky_job)
    scheduler.register_job_function("always_failing_job", always_failing_job)
    print("   ✓ Registered 3 job functions")

    # 4. Create jobs (IMPORTANT: store func_name in metadata!)
    print("\n4. Creating jobs...")
    print("   ⚠️  NOTE: func_name must be stored in metadata for DLQ retry!")

    # Job that will succeed
    job1 = scheduler.create_date_job(
        func="reliable_job",
        run_date=datetime.now(UTC) + timedelta(seconds=2),
        metadata={"func_name": "reliable_job"},  # ← Required for DLQ!
    )
    print(f"   ✓ Created reliable job: {job1.job_id}")

    # Job that will fail (after retries)
    job2 = scheduler.create_date_job(
        func="always_failing_job",
        run_date=datetime.now(UTC) + timedelta(seconds=3),
        kwargs={"order_id": 12345},
        max_retries=2,
        retry_delay_seconds=1,
        metadata={"func_name": "always_failing_job", "tenant_id": "acme"},  # ← Required!
    )
    print(f"   ✓ Created failing job: {job2.job_id} (will retry 2 times)")

    # Flaky job (might succeed or fail)
    job3 = scheduler.create_date_job(
        func="flaky_job",
        run_date=datetime.now(UTC) + timedelta(seconds=4),
        kwargs={"item_id": 999},
        max_retries=3,
        retry_delay_seconds=1,
        metadata={"func_name": "flaky_job"},  # ← Required!
    )
    print(f"   ✓ Created flaky job: {job3.job_id} (will retry 3 times)")

    # 5. Start scheduler
    print("\n5. Starting scheduler...")
    scheduler.start()
    print("   ✓ Scheduler started\n")
    print("-" * 70)
    print("Watching job execution...\n")

    # 6. Wait for jobs to execute
    time.sleep(15)

    # 7. Stop scheduler
    print("\n" + "-" * 70)
    print("\n6. Stopping scheduler...")
    scheduler.stop()
    print("   ✓ Scheduler stopped")

    # 8. Check DLQ
    print("\n7. Checking DLQ...")
    failed_jobs = dlq.get_failed_jobs()
    print(f"   ✓ Failed jobs in DLQ: {len(failed_jobs)}")

    if failed_jobs:
        print("\n   Failed Job Details:")
        for i, job in enumerate(failed_jobs, 1):
            print(f"\n   Entry {i}:")
            print(f"      ID: {job['id']}")
            print(f"      Job Name: {job['job_name']}")
            print(f"      Function: {job['func_name']}")
            print(f"      Error: {job['error_type']}: {job['error_message']}")
            print(f"      Retry Count: {job['retry_count']}")
            print(f"      Failed At: {job['failed_at']}")
            print(f"      Status: {job['status']}")

        # 9. Retry from DLQ
        print("\n8. Retrying failed jobs from DLQ...")
        for job in failed_jobs:
            if job["func_name"]:  # Only retry if func_name is stored
                new_job_id = dlq.retry_failed_job(job["id"], scheduler)
                if new_job_id:
                    print(f"   ✓ Retrying DLQ entry {job['id']} → new job {new_job_id}")
            else:
                print(f"   ⚠️  Cannot retry entry {job['id']} (no func_name in metadata)")

        # Note: Retried jobs are now scheduled in storage
        print("\n9. Retried jobs are now scheduled (would execute on next scheduler run)")

    # 10. DLQ Statistics
    print("\n10. DLQ Statistics:")
    stats = dlq.get_statistics()
    for status, count in stats.items():
        print(f"   - {status}: {count}")

    print("\n" + "=" * 70)
    print("Example Complete!")
    print("=" * 70)

    print("\n💡 Key Takeaways:")
    print("   1. Store func_name in metadata when creating jobs")
    print("   2. Use on_failure handler to save failed jobs to DLQ")
    print("   3. Query DLQ to find failed jobs and their errors")
    print("   4. Retry failed jobs by creating new jobs from DLQ data")
    print("   5. No built-in DLQ needed - failure handler provides everything!")


if __name__ == "__main__":
    main()
