"""Stress test worker for PostgreSQL + Redis distributed scheduler.

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

import psycopg2
import redis

from chronis import PollingScheduler
from chronis.contrib.adapters.lock import RedisLockAdapter
from chronis.contrib.adapters.storage import PostgreSQLStorageAdapter

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_requested
    shutdown_requested = True
    print(f"\n[{POD_NAME}] Shutdown signal received")


def create_job_function(job_id: str, duration_ms: int, interval_ms: int, pg_conn_params: dict):
    """Create a job function that records metrics."""

    def job():
        actual_time = time.time()

        # Get job creation time and execution count from Redis for latency calculation
        r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
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
        r.incr(f"stress:job:{job_id}:count")

        # Record metrics in PostgreSQL
        try:
            conn = psycopg2.connect(**pg_conn_params)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO stress_executions
                    (job_id, pod_name, actual_time, scheduled_time, latency_ms, execution_time_ms)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (job_id, POD_NAME, actual_time, scheduled_time, latency_ms, execution_time_ms),
                )
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"[{POD_NAME}] Failed to record metrics: {e}")

        # Log progress periodically
        count = r.incr("stress:total_executions")
        if count % 100 == 0:
            print(f"[{POD_NAME}] Executed {job_id} (latency: {latency_ms}ms, total: {count})")

    return job


# Configuration from environment
# Use hostname (set by Docker Compose) or fallback to POD_NAME env var
POD_NAME = os.getenv("POD_NAME", socket.gethostname())
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
IS_LEADER = os.getenv("IS_LEADER", "false") == "true"
JOB_COUNT = int(os.getenv("JOB_COUNT", "100"))
JOB_INTERVAL_MS = int(os.getenv("JOB_INTERVAL_MS", "1000"))
JOB_DURATION_MS = int(os.getenv("JOB_DURATION_MS", "50"))
TEST_DURATION_SEC = int(os.getenv("TEST_DURATION_SEC", "60"))

PG_CONN_PARAMS = {
    "host": POSTGRES_HOST,
    "port": 5432,
    "dbname": "chronis",
    "user": "chronis",
    "password": "chronis",
}

# Setup signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def setup_metrics_table(conn):
    """Create metrics table for stress test."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stress_executions (
                id SERIAL PRIMARY KEY,
                job_id TEXT NOT NULL,
                pod_name TEXT NOT NULL,
                actual_time DOUBLE PRECISION NOT NULL,
                scheduled_time DOUBLE PRECISION NOT NULL,
                latency_ms INTEGER NOT NULL,
                execution_time_ms INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stress_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.commit()


def main():
    """Run stress test worker."""
    print(f"\n{'=' * 60}")
    print(f"STRESS TEST - {POD_NAME}")
    print(f"{'=' * 60}")
    print(f"PostgreSQL: {POSTGRES_HOST}")
    print(f"Redis: {REDIS_HOST}")
    print(f"Leader: {IS_LEADER}")
    print(f"Jobs: {JOB_COUNT}")
    print(f"Interval: {JOB_INTERVAL_MS}ms")
    print(f"Job duration: {JOB_DURATION_MS}ms")
    print(f"Test duration: {TEST_DURATION_SEC}s")
    print(f"PID: {os.getpid()}\n")

    # Connect to PostgreSQL
    storage_conn = None
    for i in range(10):
        try:
            storage_conn = psycopg2.connect(**PG_CONN_PARAMS)
            print(f"[{POD_NAME}] PostgreSQL connected")
            break
        except psycopg2.OperationalError:
            print(f"[{POD_NAME}] Waiting for PostgreSQL... ({i + 1}/10)")
            time.sleep(1)
    else:
        print(f"[{POD_NAME}] PostgreSQL connection failed")
        return 1

    # Connect to Redis
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
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
    storage = PostgreSQLStorageAdapter(storage_conn, auto_migrate=True)
    lock = RedisLockAdapter(redis_client, key_prefix="stress:lock:")

    # Create scheduler with tuned settings for stress test
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=1,  # Poll every 1 second
        executor_interval_seconds=1,  # Execute queued jobs every 1 second
        max_workers=50,
        max_queue_size=500,
        verbose=False,
    )

    # Register job functions
    print(f"[{POD_NAME}] Registering {JOB_COUNT} job functions...")
    for i in range(JOB_COUNT):
        job_id = f"job-{i:04d}"
        scheduler.register_job_function(
            job_id, create_job_function(job_id, JOB_DURATION_MS, JOB_INTERVAL_MS, PG_CONN_PARAMS)
        )

    # Only leader initializes test
    if IS_LEADER:
        print(f"[{POD_NAME}] LEADER - Initializing stress test...\n")

        # Setup metrics table
        setup_metrics_table(storage_conn)

        # Clean up previous runs
        with storage_conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE stress_executions")
            cursor.execute("DELETE FROM stress_config")
            storage_conn.commit()

        # Clean Redis counter
        redis_client.delete("stress:total_executions")

        # Record test configuration
        test_start = time.time()
        with storage_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO stress_config (key, value) VALUES (%s, %s)",
                ("test_start", str(test_start)),
            )
            cursor.execute(
                "INSERT INTO stress_config (key, value) VALUES (%s, %s)",
                ("job_count", str(JOB_COUNT)),
            )
            cursor.execute(
                "INSERT INTO stress_config (key, value) VALUES (%s, %s)",
                ("interval_ms", str(JOB_INTERVAL_MS)),
            )
            cursor.execute(
                "INSERT INTO stress_config (key, value) VALUES (%s, %s)",
                ("duration_ms", str(JOB_DURATION_MS)),
            )
            cursor.execute(
                "INSERT INTO stress_config (key, value) VALUES (%s, %s)",
                ("test_duration", str(TEST_DURATION_SEC)),
            )
            storage_conn.commit()

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
                status = scheduler.get_queue_status()

                print(
                    f"[{POD_NAME}] T+{int(elapsed)}s: "
                    f"total={total_execs}, "
                    f"pending={status['pending_jobs']}, in-flight={status['in_flight_jobs']}"
                )
                last_status_time = time.time()

            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[{POD_NAME}] Interrupted")

    finally:
        # Record test end time
        if IS_LEADER:
            with storage_conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO stress_config (key, value) VALUES (%s, %s) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    ("test_end", str(time.time())),
                )
                storage_conn.commit()

        # Graceful shutdown
        print(f"\n[{POD_NAME}] Stopping scheduler...")
        result = scheduler.stop(timeout=30)
        print(f"[{POD_NAME}] Stopped (completed: {result['async_jobs_completed']})")

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

        # Close connection
        if storage_conn:
            storage_conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
