"""Stress test worker for Redis-based distributed scheduler.

Configurable via environment variables:
- JOB_COUNT: Number of jobs to create (default: 100)
- JOB_INTERVAL_MS: Job execution interval in milliseconds (default: 1000)
- JOB_DURATION_MS: Simulated job execution time in milliseconds (default: 50)
- TEST_DURATION_SEC: Total test duration in seconds (default: 60)
"""

import os
import signal
import socket
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


def create_job_function(job_id: str, duration_ms: int, interval_ms: int):
    """Create a job function that records metrics."""

    def job():
        actual_time = time.time()

        # Record metrics in Redis
        r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

        # Get job creation time and calculate expected scheduled time
        job_created = float(r.get(f"stress:job:{job_id}:created") or actual_time)
        exec_count = int(r.get(f"stress:job:{job_id}:count") or 0)

        # Expected scheduled time = creation_time + ((exec_count + 1) * interval)
        # First run (exec_count=0) is at creation_time + interval
        interval_sec = interval_ms / 1000.0
        scheduled_time = job_created + ((exec_count + 1) * interval_sec)

        # Latency = how late the job ran compared to scheduled time
        latency_ms = int((actual_time - scheduled_time) * 1000)
        if latency_ms < 0:
            latency_ms = 0  # Job ran early (clock skew or first run)

        # Simulate work
        time.sleep(duration_ms / 1000.0)

        end_time = time.time()
        execution_time_ms = int((end_time - actual_time) * 1000)

        # Increment execution count
        r.incr("stress:total_executions")
        r.incr(f"stress:job:{job_id}:count")
        r.incr(f"stress:pod:{POD_NAME}:count")

        # Record detailed execution for latency analysis
        execution_data = f"{actual_time:.6f}|{scheduled_time:.6f}|{latency_ms}|{execution_time_ms}|{POD_NAME}|{job_id}"
        r.rpush("stress:executions", execution_data)

        # Track latency for percentile calculation
        r.rpush("stress:latencies", str(latency_ms))

        if int(r.get("stress:total_executions") or 0) % 100 == 0:
            print(
                f"[{POD_NAME}] Executed {job_id} (latency: {latency_ms}ms, duration: {execution_time_ms}ms)"
            )

    return job


# Configuration from environment
# Use hostname (set by Docker Compose) or fallback to POD_NAME env var
POD_NAME = os.getenv("POD_NAME", socket.gethostname())
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
IS_LEADER = os.getenv("IS_LEADER", "false") == "true"
JOB_COUNT = int(os.getenv("JOB_COUNT", "100"))
JOB_INTERVAL_MS = int(os.getenv("JOB_INTERVAL_MS", "1000"))
JOB_DURATION_MS = int(os.getenv("JOB_DURATION_MS", "50"))
TEST_DURATION_SEC = int(os.getenv("TEST_DURATION_SEC", "60"))

# Setup signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def main():
    """Run stress test worker."""
    print(f"\n{'=' * 60}")
    print(f"STRESS TEST - {POD_NAME}")
    print(f"{'=' * 60}")
    print(f"Redis: {REDIS_HOST}")
    print(f"Leader: {IS_LEADER}")
    print(f"Jobs: {JOB_COUNT}")
    print(f"Interval: {JOB_INTERVAL_MS}ms")
    print(f"Job duration: {JOB_DURATION_MS}ms")
    print(f"Test duration: {TEST_DURATION_SEC}s")
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
            print(f"[{POD_NAME}] Redis connected\n")
            break
        except redis.ConnectionError:
            print(f"[{POD_NAME}] Waiting for Redis... ({i + 1}/10)")
            time.sleep(1)
    else:
        print(f"[{POD_NAME}] Redis connection failed")
        return 1

    # Create adapters
    storage = RedisStorageAdapter(redis_client, key_prefix="stress:jobs:")
    lock = RedisLockAdapter(redis_client, key_prefix="stress:lock:")

    # Create scheduler with tuned settings for stress test
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Poll every 1 second
        executor_interval_seconds=0.5,  # Execute queued jobs every 1 second
        max_workers=50,
        max_queue_size=500,
        verbose=False,
    )

    # Register job functions
    print(f"[{POD_NAME}] Registering {JOB_COUNT} job functions...")
    for i in range(JOB_COUNT):
        job_id = f"job-{i:04d}"
        scheduler.register_job_function(
            job_id, create_job_function(job_id, JOB_DURATION_MS, JOB_INTERVAL_MS)
        )

    # Only leader creates jobs
    if IS_LEADER:
        print(f"[{POD_NAME}] LEADER - Initializing stress test...\n")

        # Clean up previous runs
        for key in redis_client.keys("stress:*"):
            redis_client.delete(key)

        # Record test start time
        test_start = time.time()
        redis_client.set("stress:test_start", str(test_start))
        redis_client.set("stress:config:job_count", str(JOB_COUNT))
        redis_client.set("stress:config:interval_ms", str(JOB_INTERVAL_MS))
        redis_client.set("stress:config:duration_ms", str(JOB_DURATION_MS))
        redis_client.set("stress:config:test_duration", str(TEST_DURATION_SEC))

        # Create jobs with distributed start times
        # Spread jobs across the interval so they become ready gradually
        interval_seconds = JOB_INTERVAL_MS / 1000.0
        stagger_delay = interval_seconds / JOB_COUNT  # Time between each job creation
        base_time = time.time()
        print(
            f"[{POD_NAME}] Creating {JOB_COUNT} jobs (staggered over {interval_seconds}s, delay: {stagger_delay * 1000:.1f}ms)..."
        )

        for i in range(JOB_COUNT):
            job_id = f"job-{i:04d}"
            job_creation_time = time.time()
            # Record actual creation time for latency calculation
            redis_client.set(f"stress:job:{job_id}:created", str(job_creation_time))
            scheduler.create_interval_job(
                func=job_id,
                job_id=job_id,
                name=f"Stress Job {i}",
                seconds=interval_seconds,
            )
            # Sleep to stagger the actual creation time (and thus next_run_time)
            if i < JOB_COUNT - 1:
                time.sleep(stagger_delay)

        creation_time = time.time() - base_time
        print(f"[{POD_NAME}] {JOB_COUNT} jobs created in {creation_time:.1f}s\n")

        # Leader also waits for followers to be ready
        # This ensures fair distribution from the start
        wait_time = 15
        print(f"[{POD_NAME}] Waiting {wait_time}s for all followers to be ready...\n")
        time.sleep(wait_time)
    else:
        # Followers wait for leader to finish staggered job creation
        wait_time = max(15, (JOB_INTERVAL_MS / 1000.0) + 5)  # interval + buffer
        print(f"[{POD_NAME}] Waiting {wait_time}s for leader to create jobs...\n")
        time.sleep(wait_time)

    # Start scheduler
    scheduler.start()
    print(f"[{POD_NAME}] Scheduler started\n")

    # Run for test duration
    try:
        start_time = time.time()
        last_status_time = start_time

        while not shutdown_requested:
            elapsed = time.time() - start_time

            # Check if test duration reached
            if elapsed >= TEST_DURATION_SEC:
                print(f"\n[{POD_NAME}] Test duration ({TEST_DURATION_SEC}s) reached")
                break

            # Show status every 10 seconds
            if time.time() - last_status_time >= 10:
                total_execs = int(redis_client.get("stress:total_executions") or 0)
                my_execs = int(redis_client.get(f"stress:pod:{POD_NAME}:count") or 0)
                status = scheduler.get_queue_status()

                print(
                    f"[{POD_NAME}] T+{int(elapsed)}s: "
                    f"total={total_execs}, mine={my_execs}, "
                    f"pending={status['pending_jobs']}, in-flight={status['in_flight_jobs']}"
                )
                last_status_time = time.time()

            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[{POD_NAME}] Interrupted")

    finally:
        # Record test end time
        if IS_LEADER:
            redis_client.set("stress:test_end", str(time.time()))

        # Graceful shutdown
        print(f"\n[{POD_NAME}] Stopping scheduler...")
        scheduler.stop()
        print(f"[{POD_NAME}] Stopped")

        # Leader cleanup
        if IS_LEADER:
            time.sleep(3)  # Wait for followers
            print(f"\n[{POD_NAME}] Cleaning up jobs...")
            for i in range(JOB_COUNT):
                try:
                    scheduler.delete_job(f"job-{i:04d}")
                except Exception:
                    pass
            print(f"[{POD_NAME}] Cleanup complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
