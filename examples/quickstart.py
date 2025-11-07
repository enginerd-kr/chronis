"""Chronis Quick Start Example."""

import time
from datetime import datetime

from chronis import (
    InMemoryLockAdapter,
    InMemoryStorageAdapter,
    JobDefinition,
    JobStatus,
    PollingScheduler,
    TriggerType,
)


def send_email():
    """Example job function - send email."""
    print(f"[{datetime.now()}] Sending email...")


def cleanup_logs():
    """Example job function - cleanup logs."""
    print(f"[{datetime.now()}] Cleaning up logs...")


def generate_report(report_type: str):
    """Example job function - generate report."""
    print(f"[{datetime.now()}] Generating {report_type} report...")


def main():
    """Main function to demonstrate Chronis usage."""
    print("=== Chronis Quick Start ===\n")

    # 1. Create storage and lock adapters
    print("1. Creating adapters...")
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    # 2. Create scheduler
    print("2. Creating scheduler...")
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=5,  # Poll every 5 seconds
        lock_ttl_seconds=60,
    )

    # 3. Register job functions
    print("3. Registering job functions...")
    scheduler.register_job_function("send_email", send_email)
    scheduler.register_job_function("cleanup_logs", cleanup_logs)
    scheduler.register_job_function("generate_report", generate_report)

    # 4. Create jobs
    print("4. Creating jobs...\n")

    # Interval job (every 10 seconds)
    job1 = JobDefinition(
        job_id="email-001",
        name="Email Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 10},
        func="send_email",
        timezone="UTC",
    )
    scheduler.create_job(job1)
    print(f"   ✓ Created job: {job1.name} (runs every 10 seconds)")

    # Cron job (at specific time - example: every minute at :30 seconds)
    job2 = JobDefinition(
        job_id="cleanup-001",
        name="Log Cleanup Job",
        trigger_type=TriggerType.CRON,
        trigger_args={"minute": "*", "second": "30"},  # Every minute at :30
        func="cleanup_logs",
        timezone="UTC",
    )
    scheduler.create_job(job2)
    print(f"   ✓ Created job: {job2.name} (runs every minute at :30)")

    # Job with parameters
    job3 = JobDefinition(
        job_id="report-001",
        name="Report Generation Job",
        trigger_type=TriggerType.INTERVAL,
        trigger_args={"seconds": 15},
        func="generate_report",
        args=("daily",),
        timezone="UTC",
    )
    scheduler.create_job(job3)
    print(f"   ✓ Created job: {job3.name} (runs every 15 seconds)\n")

    # 5. List jobs
    print("5. Listing scheduled jobs:")
    jobs = scheduler.list_jobs(status=JobStatus.SCHEDULED)
    for job in jobs:
        print(f"   - {job.name} (ID: {job.job_id}, Status: {job.status.value})")
        print(f"     Next run: {job.next_run_time}")

    # 6. Start scheduler
    print("\n6. Starting scheduler...")
    scheduler.start()
    print("   ✓ Scheduler started (running in background)\n")

    # 7. Let it run for a while
    print("7. Running for 30 seconds...")
    print("   (Watch the job execution logs below)\n")
    print("=" * 60)

    try:
        time.sleep(30)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")

    # 8. Stop scheduler
    print("\n" + "=" * 60)
    print("\n8. Stopping scheduler...")
    scheduler.stop()
    print("   ✓ Scheduler stopped\n")

    # 9. Check job status
    print("9. Final job status:")
    jobs = scheduler.list_jobs()
    for job in jobs:
        print(f"   - {job.name}: Status={job.status.value}")

    # 10. Demonstrate state transitions
    print("\n10. Demonstrating state transitions:")
    print("   Pausing email job...")
    scheduler.pause_job("email-001")
    paused_job = scheduler.get_job("email-001")
    print(f"   ✓ Email job status: {paused_job.status.value}")

    print("   Resuming email job...")
    scheduler.resume_job("email-001")
    resumed_job = scheduler.get_job("email-001")
    print(f"   ✓ Email job status: {resumed_job.status.value}")

    print("\n=== Example Complete ===")


if __name__ == "__main__":
    main()
