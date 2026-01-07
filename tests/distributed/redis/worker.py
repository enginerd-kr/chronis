"""Worker script for Docker Compose distributed test.

Each container runs this script as an independent pod.
"""

import os
import signal
import sys
import time

import redis

from chronis import PollingScheduler
from chronis.contrib.adapters.lock import RedisLockAdapter
from chronis.contrib.adapters.storage import RedisStorageAdapter

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_requested
    shutdown_requested = True
    print(f"\n[{POD_NAME}] Shutdown signal received")


def job_function(job_num: int):
    """Job executed by scheduler."""

    def job():
        timestamp = time.strftime("%H:%M:%S.%f")[:-3]

        # Track execution in Redis
        r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
        count = r.incr(f"docker:job-{job_num}:count")

        # Record which pod executed
        r.rpush("docker:executions", f"{timestamp}|{POD_NAME}|job-{job_num}|{count}")

        print(f"[{timestamp}] ⚙️  Job-{job_num} execution #{count} by {POD_NAME}")
        time.sleep(0.2)

    return job


# Configuration from environment
POD_NAME = os.getenv("POD_NAME", f"pod-{os.getpid()}")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
IS_LEADER = os.getenv("IS_LEADER", "false") == "true"

# Setup signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def main():
    """Run scheduler worker."""
    print(f"\n{'=' * 60}")
    print(f"{POD_NAME} - Starting")
    print(f"{'=' * 60}")
    print(f"Redis: {REDIS_HOST}")
    print(f"Leader: {IS_LEADER}")
    print(f"PID: {os.getpid()}\n")

    # Connect to Redis
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=6379,
        db=0,
        decode_responses=True,
    )

    # Wait for Redis
    for i in range(10):
        try:
            redis_client.ping()
            print(f"[{POD_NAME}] ✓ Redis connected\n")
            break
        except redis.ConnectionError:
            print(f"[{POD_NAME}] Waiting for Redis... ({i + 1}/10)")
            time.sleep(1)
    else:
        print(f"[{POD_NAME}] ✗ Redis connection failed")
        return 1

    # Create adapters
    storage = RedisStorageAdapter(redis_client, key_prefix="docker:jobs:")
    lock = RedisLockAdapter(redis_client, key_prefix="docker:lock:")

    # Create scheduler
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Fast polling for testing
        executor_interval_seconds=0.5,
        max_workers=10,
        max_queue_size=50,
        verbose=False,
    )

    # Register job functions
    print(f"[{POD_NAME}] Registering 5 job functions...")
    for i in range(5):
        scheduler.register_job_function(f"job_{i}", job_function(i))

    # Only leader creates jobs
    if IS_LEADER:
        print(f"[{POD_NAME}] 🎖️  LEADER - Creating 5 jobs...\n")

        # Clean up previous runs
        for key in redis_client.keys("docker:*"):
            redis_client.delete(key)

        # Create jobs
        for i in range(5):
            scheduler.create_interval_job(
                func=f"job_{i}",
                job_id=f"job-{i}",
                name=f"Job {i}",
                seconds=5,  # Every 5 seconds
            )

        print(f"[{POD_NAME}] ✓ 5 jobs created (every 5 seconds)\n")
    else:
        # Followers wait for leader
        print(f"[{POD_NAME}] Waiting for leader to create jobs...\n")
        time.sleep(3)

    # Start scheduler
    scheduler.start()
    print(f"[{POD_NAME}] ✓ Scheduler started\n")

    # Run until shutdown signal
    try:
        iteration = 0
        while not shutdown_requested:
            time.sleep(5)
            iteration += 1

            # Show periodic status
            status = scheduler.get_queue_status()
            print(
                f"[{POD_NAME}] T+{iteration * 5}s: "
                f"{status['pending_jobs']} pending, "
                f"{status['in_flight_jobs']} in-flight"
            )

    except KeyboardInterrupt:
        print(f"\n[{POD_NAME}] Interrupted")

    finally:
        # Graceful shutdown
        print(f"\n[{POD_NAME}] Stopping scheduler...")
        result = scheduler.stop(timeout=10)
        print(f"[{POD_NAME}] ✓ Stopped (async jobs: {result['async_jobs_completed']})")

        # Leader cleanup
        if IS_LEADER:
            time.sleep(2)  # Wait for followers
            print(f"\n[{POD_NAME}] Cleaning up jobs...")
            for i in range(5):
                try:
                    scheduler.delete_job(f"job-{i}")
                except Exception as e:
                    print(f"[{POD_NAME}] Warning: {e}")

            print(f"[{POD_NAME}] ✓ Cleanup complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
