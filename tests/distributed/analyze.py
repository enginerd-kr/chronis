"""Analyze distributed test results from Redis."""

import os
import time

import redis


def main():
    """Analyze execution results."""
    redis_host = os.getenv("REDIS_HOST", "localhost")

    # Wait a moment for final data
    time.sleep(2)

    # Connect
    r = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

    print("\n" + "=" * 70)
    print("DISTRIBUTED TEST RESULTS")
    print("=" * 70)

    # Get all executions
    executions = r.lrange("docker:executions", 0, -1)

    if not executions:
        print("\n⚠️  No executions recorded!")
        print("Make sure the test ran for at least 30 seconds.")
        return

    print(f"\nTotal executions: {len(executions)}")

    # Parse executions
    by_job = {}
    by_pod = {}
    timeline = []

    for exec_str in executions:
        parts = exec_str.split("|")
        if len(parts) != 4:
            continue

        timestamp, pod_name, job_id, count = parts
        timeline.append((timestamp, pod_name, job_id, int(count)))

        # Count by job
        if job_id not in by_job:
            by_job[job_id] = 0
        by_job[job_id] += 1

        # Count by pod
        if pod_name not in by_pod:
            by_pod[pod_name] = 0
        by_pod[pod_name] += 1

    # Job analysis
    print("\nExecutions per job:")
    for job_id in sorted(by_job.keys()):
        count = by_job[job_id]
        expected = 6  # ~30 seconds / 5 second interval
        status = "✓" if count <= expected + 1 else "⚠️"
        print(f"  {status} {job_id}: {count} times (expected: ~{expected})")

    # Pod analysis
    print("\nExecutions per pod:")
    for pod_name in sorted(by_pod.keys()):
        count = by_pod[pod_name]
        print(f"  {pod_name}: {count} times")

    # Check for duplicates (same job within 1 second)
    print("\nChecking for near-simultaneous duplicates...")
    duplicates = []

    job_times = {}
    for timestamp, pod_name, job_id, _count in timeline:
        if job_id not in job_times:
            job_times[job_id] = []
        job_times[job_id].append((timestamp, pod_name))

    for job_id, times in job_times.items():
        for i in range(len(times) - 1):
            t1, pod1 = times[i]
            t2, pod2 = times[i + 1]

            # Parse time (HH:MM:SS.mmm)
            try:
                h1, m1, s1 = t1.split(":")
                h2, m2, s2 = t2.split(":")

                time1 = int(h1) * 3600 + int(m1) * 60 + float(s1)
                time2 = int(h2) * 3600 + int(m2) * 60 + float(s2)

                # Within 1 second = suspicious
                if abs(time2 - time1) < 1.0:
                    duplicates.append((job_id, t1, t2, pod1, pod2))
            except Exception:
                pass

    if duplicates:
        print(f"⚠️  Found {len(duplicates)} potential duplicates:")
        for job_id, t1, t2, pod1, pod2 in duplicates[:10]:  # Show first 10
            print(f"   {job_id}: {t1} ({pod1}) → {t2} ({pod2})")
        if len(duplicates) > 10:
            print(f"   ... and {len(duplicates) - 10} more")
    else:
        print("✅ No duplicates found!")

    # Timeline sample
    print("\nExecution timeline (first 20):")
    for i, (timestamp, pod_name, job_id, _count) in enumerate(timeline[:20], 1):
        print(f"  {i:2d}. {timestamp} | {pod_name:20s} | {job_id}")

    # Final verdict
    total = len(executions)
    expected = 5 * 6  # 5 jobs × 6 times
    print(f"\n{'=' * 70}")

    if total <= expected + 5:
        print(f"✅ PASS: Total executions ({total}) within acceptable range")
    else:
        print(f"⚠️  WARNING: Total executions ({total}) higher than expected (~{expected})")

    if not duplicates:
        print("✅ PASS: No duplicate executions detected")
    else:
        print(f"❌ FAIL: {len(duplicates)} duplicate executions found")

    print("=" * 70)

    # Cleanup
    print("\nCleaning up Redis data...")
    for key in r.keys("docker:*"):
        r.delete(key)
    print("✓ Done\n")


if __name__ == "__main__":
    main()
