"""Chronis Quick Start Example - Simplified API."""

import time
from datetime import datetime, timedelta

from chronis import (
    InMemoryLockAdapter,
    InMemoryStorageAdapter,
    JobStatus,
    PollingScheduler,
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
    print("=== Chronis Quick Start - Simplified API ===\n")

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

    # 4. Create jobs using simplified API (TriggerType hidden)
    print("4. Creating jobs with simplified API...\n")

    # Interval job - runs every 10 seconds
    job1 = scheduler.create_interval_job(
        job_id="email-001",
        name="Send Email (Interval)",
        func="send_email",
        seconds=10,
    )
    print(f"   ✓ Created interval job: {job1.name} (runs every 10 seconds)")

    # Cron job - runs daily at 9 AM
    job2 = scheduler.create_cron_job(
        job_id="cleanup-001",
        name="Cleanup Logs (Daily 9 AM)",
        func="cleanup_logs",
        hour=9,
        minute=0,
        timezone="Asia/Seoul",
    )
    print(f"   ✓ Created cron job: {job2.name} (runs daily at 9 AM Seoul time)")

    # Interval job - runs every 15 seconds
    job3 = scheduler.create_interval_job(
        job_id="report-001",
        name="Generate Report",
        func="generate_report",
        seconds=15,
        kwargs={"report_type": "sales"},
    )
    print(f"   ✓ Created interval job: {job3.name} (runs every 15 seconds)\n")

    # Date job - runs once after 30 seconds
    future_time = datetime.now() + timedelta(seconds=30)
    job4 = scheduler.create_date_job(
        job_id="oneshot-001",
        name="One-time Email",
        func="send_email",
        run_date=future_time,
    )
    print(f"   ✓ Created date job: {job4.name} (runs once at {future_time})\n")

    # 5. List scheduled jobs
    print("5. Listing scheduled jobs:")
    jobs = scheduler.list_jobs(status=JobStatus.SCHEDULED)
    for job in jobs:
        print(f"   - {job.name} (ID: {job.job_id}, Status: {job.status.value})")
        print(f"     Next run: {job.next_run_time}")

    # 6. Start scheduler
    print("\n6. Starting scheduler...")
    scheduler.start()
    print("   ✓ Scheduler started (polling every 5 seconds)")

    # 7. Let it run for 40 seconds
    print("\n7. Running scheduler for 40 seconds...")
    print("   (Watch the jobs execute)\n")
    time.sleep(40)

    # 8. Stop scheduler
    print("\n8. Stopping scheduler...")
    scheduler.stop()
    print("   ✓ Scheduler stopped")

    # 9. Final job status
    print("\n9. Final job status:")
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
