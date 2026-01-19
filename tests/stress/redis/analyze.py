"""Analyze stress test results from Redis."""

import os
import time

import redis


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
    redis_host = os.getenv("REDIS_HOST", "localhost")

    # Wait for data
    time.sleep(2)

    # Connect
    r = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

    print("\n" + "=" * 70)
    print("STRESS TEST RESULTS")
    print("=" * 70)

    # Get test configuration
    job_count = int(r.get("stress:config:job_count") or 0)
    interval_ms = int(r.get("stress:config:interval_ms") or 0)
    duration_ms = int(r.get("stress:config:duration_ms") or 0)
    test_duration = int(r.get("stress:config:test_duration") or 0)

    test_start = float(r.get("stress:test_start") or 0)
    test_end = float(r.get("stress:test_end") or time.time())
    actual_duration = test_end - test_start

    print("\nConfiguration:")
    print(f"  Jobs: {job_count}")
    print(f"  Interval: {interval_ms}ms")
    print(f"  Job duration: {duration_ms}ms")
    print(f"  Test duration: {test_duration}s (actual: {actual_duration:.1f}s)")

    # Get total executions
    total_executions = int(r.get("stress:total_executions") or 0)

    if total_executions == 0:
        print("\nNo executions recorded!")
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
    latencies_raw = r.lrange("stress:latencies", 0, -1)
    latencies = sorted([int(x) for x in latencies_raw])

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
    pod_counts = {}
    for key in r.keys("stress:pod:*:count"):
        pod_name = key.split(":")[2]
        count = int(r.get(key) or 0)
        pod_counts[pod_name] = count

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

    # Check for missed jobs (jobs with 0 executions)
    print("\nJob Execution Check:")
    missed_jobs = 0
    low_execution_jobs = 0

    for i in range(job_count):
        job_id = f"job-{i:04d}"
        count = int(r.get(f"stress:job:{job_id}:count") or 0)
        if count == 0:
            missed_jobs += 1
        elif count < expected_per_job * 0.5:
            low_execution_jobs += 1

    print(f"  Jobs with 0 executions: {missed_jobs}")
    print(f"  Jobs with <50% expected: {low_execution_jobs}")

    # Check for duplicates (executions within 100ms of each other for same job)
    print("\nDuplicate Check:")
    executions = r.lrange("stress:executions", 0, -1)

    job_times = {}
    for exec_str in executions:
        parts = exec_str.split("|")
        if len(parts) != 6:
            continue
        actual_time, scheduled_time, latency, duration, pod, job_id = parts
        if job_id not in job_times:
            job_times[job_id] = []
        job_times[job_id].append(float(actual_time))

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
    print("\nCleaning up Redis data...")
    for key in r.keys("stress:*"):
        r.delete(key)
    print("Done\n")


if __name__ == "__main__":
    main()
