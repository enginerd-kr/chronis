"""Analyze stress test results from PostgreSQL."""

import os
import time

import psycopg2


def percentile(sorted_list: list[int], p: float) -> int:
    """Calculate percentile from sorted list."""
    if not sorted_list:
        return 0
    k = (len(sorted_list) - 1) * p / 100
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_list) else f
    return int(sorted_list[f] + (k - f) * (sorted_list[c] - sorted_list[f]))


def main():
    """Analyze stress test results."""
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")

    # Wait for data
    time.sleep(2)

    # Connect
    conn = psycopg2.connect(
        host=postgres_host,
        port=5432,
        dbname="chronis",
        user="chronis",
        password="chronis",
    )

    print("\n" + "=" * 70)
    print("STRESS TEST RESULTS (PostgreSQL)")
    print("=" * 70)

    # Get test configuration
    with conn.cursor() as cursor:
        cursor.execute("SELECT key, value FROM stress_config")
        config = dict(cursor.fetchall())

    job_count = int(config.get("job_count", 0))
    interval_ms = int(config.get("interval_ms", 0))
    duration_ms = int(config.get("duration_ms", 0))
    test_duration = int(config.get("test_duration", 0))
    test_start = float(config.get("test_start", 0))
    test_end = float(config.get("test_end", time.time()))
    actual_duration = test_end - test_start

    print("\nConfiguration:")
    print(f"  Jobs: {job_count}")
    print(f"  Interval: {interval_ms}ms")
    print(f"  Job duration: {duration_ms}ms")
    print(f"  Test duration: {test_duration}s (actual: {actual_duration:.1f}s)")

    # Get total executions
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM stress_executions")
        total_executions = cursor.fetchone()[0]

    if total_executions == 0:
        print("\nNo executions recorded!")
        conn.close()
        return

    # Calculate expected executions
    # First execution happens at T=interval (not T=0), so:
    # - Run 1 at T=interval
    # - Run 2 at T=2*interval
    # - Run N at T=N*interval
    # Jobs at exactly T=test_duration boundary may not complete
    # Use conservative formula: floor((test_duration - 1) / interval)
    interval_sec = interval_ms / 1000
    expected_per_job = max(1, int((test_duration - 1) / interval_sec))
    expected_total = job_count * expected_per_job

    print("\nExecution Summary:")
    print(f"  Total executions: {total_executions}")
    print(f"  Expected (approx): {expected_total}")
    print(f"  Throughput: {total_executions / actual_duration:.1f} jobs/sec")

    # Get latencies for percentile analysis
    with conn.cursor() as cursor:
        cursor.execute("SELECT latency_ms FROM stress_executions ORDER BY latency_ms")
        latencies = [row[0] for row in cursor.fetchall()]

    if latencies:
        print("\nLatency (scheduled -> actual execution):")
        print(f"  Min:  {min(latencies)}ms")
        print(f"  P50:  {percentile(latencies, 50)}ms")
        print(f"  P95:  {percentile(latencies, 95)}ms")
        print(f"  P99:  {percentile(latencies, 99)}ms")
        print(f"  Max:  {max(latencies)}ms")
        print(f"  Avg:  {sum(latencies) / len(latencies):.1f}ms")

    # Get per-pod stats
    print("\nWorker Distribution:")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT pod_name, COUNT(*) as count
            FROM stress_executions
            GROUP BY pod_name
            ORDER BY pod_name
            """
        )
        pod_counts = dict(cursor.fetchall())

    total_from_pods = sum(pod_counts.values())
    for pod_name in sorted(pod_counts.keys()):
        count = pod_counts[pod_name]
        pct = (count / total_from_pods * 100) if total_from_pods > 0 else 0
        print(f"  {pod_name}: {count} ({pct:.1f}%)")

    # Check for uneven distribution
    if pod_counts:
        avg_per_pod = total_from_pods / len(pod_counts)
        max_deviation = max(abs(c - avg_per_pod) / avg_per_pod * 100 for c in pod_counts.values())
        if max_deviation > 30:
            print(f"  Warning: Uneven distribution (max deviation: {max_deviation:.1f}%)")

    # Check for missed jobs
    print("\nJob Execution Check:")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT job_id, COUNT(*) as count
            FROM stress_executions
            GROUP BY job_id
            """
        )
        job_counts = dict(cursor.fetchall())

    missed_jobs = job_count - len(job_counts)
    low_execution_jobs = sum(1 for c in job_counts.values() if c < expected_per_job * 0.5)

    print(f"  Jobs with 0 executions: {missed_jobs}")
    print(f"  Jobs with <50% expected: {low_execution_jobs}")

    # Check for duplicates (executions within 100ms of each other for same job)
    print("\nDuplicate Check:")
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT job_id, actual_time
            FROM stress_executions
            ORDER BY job_id, actual_time
            """
        )
        executions = cursor.fetchall()

    job_times = {}
    for job_id, actual_time in executions:
        if job_id not in job_times:
            job_times[job_id] = []
        job_times[job_id].append(actual_time)

    duplicate_count = 0
    for _job_id, times in job_times.items():
        times.sort()
        for i in range(len(times) - 1):
            if (times[i + 1] - times[i]) < 0.1:  # Within 100ms
                duplicate_count += 1

    if duplicate_count > 0:
        print(f"  Potential duplicates: {duplicate_count}")
    else:
        print("  No duplicates detected")

    # Final verdict
    print(f"\n{'=' * 70}")

    passed = True

    # Check throughput
    if total_executions >= expected_total * 0.9:
        print(f"PASS: Throughput ({total_executions / actual_duration:.1f} jobs/sec)")
    else:
        print(f"WARN: Low throughput - {total_executions} vs expected {expected_total}")
        passed = False

    # Check latency
    p99_latency = percentile(latencies, 99) if latencies else 0
    if p99_latency < 1000:  # P99 < 1 second
        print(f"PASS: P99 latency ({p99_latency}ms < 1000ms)")
    else:
        print(f"WARN: High P99 latency ({p99_latency}ms)")
        passed = False

    # Check missed jobs
    if missed_jobs == 0:
        print("PASS: No missed jobs")
    else:
        print(f"FAIL: {missed_jobs} jobs never executed")
        passed = False

    # Check duplicates
    if duplicate_count == 0:
        print("PASS: No duplicate executions")
    else:
        print(f"WARN: {duplicate_count} potential duplicates")

    print(f"{'=' * 70}")

    if passed:
        print("OVERALL: PASS")
    else:
        print("OVERALL: ISSUES DETECTED - Review warnings above")

    # Cleanup
    print("\nCleaning up test data...")
    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE stress_executions")
        cursor.execute("DELETE FROM stress_config")
        conn.commit()
    print("Done\n")

    conn.close()


if __name__ == "__main__":
    main()
