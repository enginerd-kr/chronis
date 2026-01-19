# Stress Test (PostgreSQL + Redis)

Stress test for the distributed scheduler using PostgreSQL for storage and Redis for locking.

## Architecture

- **PostgreSQL**: Job storage and metrics persistence
- **Redis**: Distributed lock coordination
- **1 Leader Pod**: Creates jobs and runs scheduler
- **N Follower Pods**: Run schedulers competing for jobs (default: 3, scalable via `--scale`)

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `JOB_COUNT` | 100 | Number of concurrent jobs |
| `JOB_INTERVAL_MS` | 1000 | Job execution interval (ms) |
| `JOB_DURATION_MS` | 50 | Simulated job execution time (ms) |
| `TEST_DURATION_SEC` | 60 | Total test duration (seconds) |

## Usage

### Quick Test (100 jobs, 60 seconds, 4 pods)

```bash
cd tests/stress/postgresql
docker-compose up --build
```

This runs 1 leader pod + 3 follower pods by default.

### Scale Worker Count

```bash
# Run with 1 leader + 5 followers (6 total pods)
docker-compose up --build --scale scheduler-follower=5

# Run with 1 leader + 10 followers (11 total pods)
docker-compose up --build --scale scheduler-follower=10

# Run with just the leader (single pod)
docker-compose up --build --scale scheduler-follower=0
```

### Custom Configuration

```bash
# High load: 20,000 jobs with 60s interval across 8 pods
JOB_COUNT=20000 JOB_INTERVAL_MS=60000 \
docker-compose up --build --scale scheduler-follower=7

# Long duration: 10,000 jobs for 5 minutes across 5 pods
JOB_COUNT=10000 TEST_DURATION_SEC=300 \
docker-compose up --build --scale scheduler-follower=4

# Slow jobs: 1,000 jobs with 200ms execution time
JOB_COUNT=1000 JOB_DURATION_MS=200 \
docker-compose up --build --scale scheduler-follower=3
```

### Run Analysis

```bash
# After test completes
docker-compose run --rm analyzer
```

### Cleanup

```bash
docker-compose down -v
```

## Expected Output

```
STRESS TEST RESULTS (PostgreSQL)
======================================================================

Configuration:
  Jobs: 100
  Interval: 1000ms
  Job duration: 50ms
  Test duration: 60s (actual: 60.2s)

Execution Summary:
  Total executions: 5892
  Expected (approx): 6000
  Throughput: 97.9 jobs/sec

Latency (scheduled -> actual execution):
  Min:  2ms
  P50:  12ms
  P95:  45ms
  P99:  89ms
  Max:  156ms
  Avg:  18.3ms

Worker Distribution:
  scheduler-1: 1478 (25.1%)
  scheduler-2: 1456 (24.7%)
  scheduler-3: 1489 (25.3%)
  scheduler-leader: 1469 (24.9%)

Job Execution Check:
  Jobs with 0 executions: 0
  Jobs with <50% expected: 0

Duplicate Check:
  No duplicates detected

======================================================================
PASS: Throughput (97.9 jobs/sec)
PASS: P99 latency (89ms < 1000ms)
PASS: No missed jobs
PASS: No duplicate executions
======================================================================
OVERALL: PASS
```

## Comparison with Redis-only

| Aspect | Redis-only | PostgreSQL + Redis |
|--------|------------|-------------------|
| Storage | Redis | PostgreSQL |
| Locking | Redis | Redis |
| Durability | Limited | Full ACID |
| Query flexibility | Limited | Full SQL |
| Recommended for | Caching, ephemeral jobs | Production, persistent jobs |

## Stress Test Scenarios

### 1. High Job Count

```bash
JOB_COUNT=500 JOB_INTERVAL_MS=1000 docker-compose up --build
```

Tests: PostgreSQL connection pooling, lock contention

### 2. High Frequency

```bash
JOB_COUNT=50 JOB_INTERVAL_MS=100 docker-compose up --build
```

Tests: Database write throughput, polling efficiency

### 3. Long Running Jobs

```bash
JOB_COUNT=100 JOB_DURATION_MS=500 docker-compose up --build
```

Tests: Connection timeout handling, thread pool management

### 4. Extended Duration

```bash
JOB_COUNT=100 TEST_DURATION_SEC=600 docker-compose up --build
```

Tests: Connection pool stability, memory leaks, long-term reliability
