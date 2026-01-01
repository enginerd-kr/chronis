"""Example: Using PostgreSQL adapter with Chronis.

This example demonstrates how to use PostgreSQL for job storage.

Requirements:
    pip install chronis[postgres]
    # Or manually: pip install psycopg2-binary

PostgreSQL Server:
    docker-compose up -d
    # Or: brew install postgresql && brew services start postgresql
"""

import time

import psycopg2

from chronis import PollingScheduler
from chronis.adapters.lock import InMemoryLockAdapter
from chronis.contrib.storage import PostgreSQLStorageAdapter


def send_notification():
    """Example job function."""
    print(f"[{time.strftime('%H:%M:%S')}] Sending notification...")


def generate_report():
    """Another example job function."""
    print(f"[{time.strftime('%H:%M:%S')}] Generating report...")


def main():
    """Run the example."""
    print("=== Chronis PostgreSQL Adapter Example ===\n")

    # 1. Connect to PostgreSQL
    print("1. Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="scheduler",
            user="scheduler",
            password="scheduler_pass",
        )
        print("   ✓ PostgreSQL connection successful\n")
    except psycopg2.OperationalError as e:
        print(f"   ✗ PostgreSQL connection failed: {e}")
        print("   Start PostgreSQL: docker-compose up -d")
        return

    try:
        # 2. Create adapters
        print("2. Creating adapters...")
        storage = PostgreSQLStorageAdapter(conn)
        lock = InMemoryLockAdapter()  # Use Redis for production
        print("   ✓ Adapters created")
        print("   ✓ Table 'chronis_jobs' created (if not exists)\n")

        # 3. Create scheduler
        print("3. Creating scheduler...")
        scheduler = PollingScheduler(
            storage_adapter=storage, lock_adapter=lock, polling_interval_seconds=5, verbose=True
        )
        print("   ✓ Scheduler created\n")

        # 4. Register functions
        print("4. Registering job functions...")
        scheduler.register_job_function("send_notification", send_notification)
        scheduler.register_job_function("generate_report", generate_report)
        print("   ✓ Functions registered\n")

        # 5. Create jobs
        print("5. Creating jobs...")

        # Interval job
        job1 = scheduler.create_interval_job(
            func="send_notification",
            job_id="notification-job",
            name="Notification Sender",
            seconds=15,
            metadata={"tenant_id": "acme", "priority": "high"},
        )
        print(f"   ✓ Created: {job1.job_id}")

        # Cron job
        job2 = scheduler.create_cron_job(
            func="generate_report",
            job_id="report-job",
            name="Report Generator",
            minute="*/2",  # Every 2 minutes
            metadata={"tenant_id": "acme", "type": "daily_report"},
        )
        print(f"   ✓ Created: {job2.job_id}\n")

        # 6. Start scheduler
        print("6. Starting scheduler...")
        scheduler.start()
        print("   ✓ Scheduler started\n")

        # 7. Monitor
        print("7. Running for 30 seconds... (Ctrl+C to stop)\n")

        for i in range(6):
            time.sleep(5)

            # Query jobs from PostgreSQL
            jobs = scheduler.query_jobs()
            print(f"   [{i * 5}s] Jobs in database: {len(jobs)}")

            for job in jobs:
                print(
                    f"      - {job.job_id}: {job.status.value} "
                    f"(next: {job.next_run_time_local or 'N/A'})"
                )

            # Query database directly
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM chronis_jobs")
                count = cursor.fetchone()[0]
                print(f"   [{i * 5}s] PostgreSQL row count: {count}\n")

    except KeyboardInterrupt:
        print("\n   Interrupted by user")

    finally:
        # 8. Cleanup
        print("\n8. Cleaning up...")
        scheduler.stop()
        print("   ✓ Scheduler stopped")

        # Delete jobs
        scheduler.delete_job("notification-job")
        scheduler.delete_job("report-job")
        print("   ✓ Jobs deleted")

        # Verify cleanup
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM chronis_jobs")
            remaining = cursor.fetchone()[0]
            if remaining > 0:
                print(f"   ⚠ {remaining} jobs remaining in database")
                cursor.execute("DELETE FROM chronis_jobs")
                conn.commit()
                print("   ✓ Cleaned up remaining jobs")
            else:
                print("   ✓ All jobs cleaned up")

        conn.close()
        print("\n=== Example completed ===")


if __name__ == "__main__":
    main()
