"""Chronis Quick Start Example - Simplified API."""

import time
from datetime import datetime, timedelta

from chronis import (
    InMemoryLockAdapter,
    InMemoryStorageAdapter,
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

    # 4. Create jobs using AI-friendly API (auto-generated IDs)
    print("4. Creating jobs with AI-friendly API (auto-generated IDs)...\n")

    # Interval job - runs every 10 seconds
    job1 = scheduler.create_interval_job(
        func="send_email",
        seconds=10,
    )
    print(f"   ✓ Created interval job: {job1.name}")
    print(f"     ID: {job1.job_id} (auto-generated)")

    # Cron job - runs daily at 9 AM (with explicit name)
    job2 = scheduler.create_cron_job(
        func="cleanup_logs",
        name="Cleanup Logs (Daily 9 AM)",
        hour=9,
        minute=0,
        timezone="Asia/Seoul",
    )
    print(f"   ✓ Created cron job: {job2.name}")
    print(f"     ID: {job2.job_id} (auto-generated)")

    # Interval job - runs every 15 seconds (with kwargs)
    job3 = scheduler.create_interval_job(
        func="generate_report",
        seconds=15,
        kwargs={"report_type": "sales"},
    )
    print(f"   ✓ Created interval job: {job3.name}")
    print(f"     ID: {job3.job_id} (auto-generated)\n")

    # Date job - runs once after 30 seconds
    future_time = datetime.now() + timedelta(seconds=30)
    job4 = scheduler.create_date_job(
        func="send_email",
        run_date=future_time,
    )
    print(f"   ✓ Created date job: {job4.name}")
    print(f"     ID: {job4.job_id} (auto-generated)")
    print(f"     Scheduled for: {future_time}\n")

    # 5. Query all jobs
    print("5. Querying all jobs...")
    all_jobs = scheduler.query_jobs()
    print(f"   ✓ Total jobs created: {len(all_jobs)}")
    for job in all_jobs:
        print(f"      - {job.job_id}: {job.name} ({job.status.value}) - {job.trigger_type}")

    # 6. Start scheduler
    print("\n6. Starting scheduler...")
    scheduler.start()
    print("   ✓ Scheduler started (polling every 5 seconds)")

    # 7. Let it run for 40 seconds
    print("\n7. Running scheduler for 40 seconds...")
    print("   (Watch the jobs execute)\n")
    time.sleep(40)

    # 8. Check one-time job was deleted after execution
    print("\n8. Checking one-time job status...")
    oneshot_job = scheduler.get_job(job4.job_id)  # Use the auto-generated ID
    if oneshot_job is None:
        print("   ✓ One-time job was automatically deleted after execution")
    else:
        print(f"   ℹ One-time job status: {oneshot_job.status.value}")

    # 9. Stop scheduler
    print("\n9. Stopping scheduler...")
    scheduler.stop()
    print("   ✓ Scheduler stopped")

    # 10. Query remaining jobs
    print("\n10. Querying remaining jobs...")
    remaining_jobs = scheduler.query_jobs()
    print(f"   ✓ Remaining jobs: {len(remaining_jobs)}")
    for job in remaining_jobs:
        print(f"      - {job.job_id}: {job.name} ({job.status.value})")

    print("\n=== Example Complete ===")


if __name__ == "__main__":
    main()
