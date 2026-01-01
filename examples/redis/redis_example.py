"""Example: Using Redis adapters with Chronis.

This example demonstrates how to use Redis for distributed storage and locking.

Requirements:
    pip install chronis[redis]
    # Or manually: pip install redis

Redis Server:
    docker run -d -p 6379:6379 redis:latest
    # Or: brew install redis && redis-server
"""

import time

import redis

from chronis import PollingScheduler
from chronis.contrib.lock import RedisLockAdapter
from chronis.contrib.storage import RedisStorageAdapter


def send_email():
    """Example job function."""
    print(f"[{time.strftime('%H:%M:%S')}] Sending email...")


def process_data():
    """Another example job function."""
    print(f"[{time.strftime('%H:%M:%S')}] Processing data...")


def main():
    """Run the example."""
    print("=== Chronis Redis Adapter Example ===\n")

    # 1. Connect to Redis
    print("1. Connecting to Redis...")
    redis_client = redis.Redis(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=True,  # Important for JSON serialization
    )

    # Test connection
    try:
        redis_client.ping()
        print("   ✓ Redis connection successful\n")
    except redis.ConnectionError:
        print("   ✗ Redis connection failed")
        print("   Start Redis: docker run -d -p 6379:6379 redis:latest")
        return

    # 2. Create adapters
    print("2. Creating Redis adapters...")
    storage = RedisStorageAdapter(redis_client, key_prefix="example:jobs:")
    lock = RedisLockAdapter(redis_client, key_prefix="example:lock:")
    print("   ✓ Adapters created\n")

    # 3. Create scheduler
    print("3. Creating scheduler...")
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=5,
        verbose=True,
    )
    print("   ✓ Scheduler created\n")

    # 4. Register functions
    print("4. Registering job functions...")
    scheduler.register_job_function("send_email", send_email)
    scheduler.register_job_function("process_data", process_data)
    print("   ✓ Functions registered\n")

    # 5. Create jobs
    print("5. Creating jobs...")

    # Interval job - runs every 10 seconds
    job1 = scheduler.create_interval_job(
        func="send_email", job_id="email-job", name="Email Sender", seconds=10
    )
    print(f"   ✓ Created interval job: {job1.job_id}")

    # Cron job - runs every minute
    job2 = scheduler.create_cron_job(
        func="process_data", job_id="processor-job", name="Data Processor", minute="*"
    )
    print(f"   ✓ Created cron job: {job2.job_id}\n")

    # 6. Start scheduler
    print("6. Starting scheduler...")
    scheduler.start()
    print("   ✓ Scheduler started (running in background)\n")

    try:
        # 7. Monitor jobs
        print("7. Running for 30 seconds... (Ctrl+C to stop)\n")

        for i in range(6):
            time.sleep(5)

            # Query jobs
            jobs = scheduler.query_jobs()
            print(f"   [{i * 5}s] Active jobs: {len(jobs)}")

            for job in jobs:
                print(
                    f"      - {job.job_id}: {job.status.value} "
                    f"(next: {job.next_run_time_local or 'N/A'})"
                )

            # Check Redis directly
            keys = redis_client.keys("example:*")
            print(f"   [{i * 5}s] Redis keys: {len(keys)}")
            print()

    except KeyboardInterrupt:
        print("\n   Interrupted by user")

    finally:
        # 8. Cleanup
        print("\n8. Cleaning up...")
        scheduler.stop()
        print("   ✓ Scheduler stopped")

        # Delete jobs
        scheduler.delete_job("email-job")
        scheduler.delete_job("processor-job")
        print("   ✓ Jobs deleted")

        # Verify Redis cleanup
        remaining_keys = redis_client.keys("example:*")
        if remaining_keys:
            print(f"   ⚠ {len(remaining_keys)} keys remaining in Redis")
            for key in remaining_keys:
                redis_client.delete(key)
            print("   ✓ Cleaned up remaining keys")
        else:
            print("   ✓ All Redis keys cleaned up")

        print("\n=== Example completed ===")


if __name__ == "__main__":
    main()
