# Distributed Scheduler Test

This example simulates a Kubernetes environment using Docker Compose.

## Architecture

- **Redis**: Shared storage & distributed lock
- **scheduler-leader**: Leader pod that creates jobs
- **scheduler-1,2,3**: Follower pods (4 scheduler instances total)
- **analyzer**: Result analysis container

## Usage

### 1. Run Test

```bash
cd tests/distributed

# Start all containers (runs for ~30 seconds)
docker-compose up --build

# Stop with Ctrl+C or wait for automatic completion
```

### 2. View Logs

```bash
# View specific pod logs
docker-compose logs -f scheduler-leader
docker-compose logs -f scheduler-1

# View all logs
docker-compose logs
```

### 3. Analyze Results

```bash
# Run analysis after test completes
docker-compose run --rm analyzer

# Or run during test in separate terminal
docker-compose --profile analyze up analyzer
```

### 4. Cleanup

```bash
docker-compose down
```

## Expected Results (Success)

```
DISTRIBUTED TEST RESULTS
======================================================================

Total executions: 30
Number of pods: 4

Executions per job:
  ✓ job-0: 6 times (expected: ~6)
  ✓ job-1: 6 times
  ✓ job-2: 6 times
  ✓ job-3: 6 times
  ✓ job-4: 6 times

Executions per pod:
  scheduler-leader: 8 times
  scheduler-1: 7 times
  scheduler-2: 8 times
  scheduler-3: 7 times

Checking for near-simultaneous duplicates...
✅ No duplicates found!

======================================================================
✅ PASS: Total executions (30) within acceptable range
✅ PASS: No duplicate executions detected
======================================================================
```

## Verification Points

1. **Each job executes ~6 times** (30 seconds / 5 second interval)
2. **Executions distributed across pods**
3. **No duplicate executions** (same job not executed twice within 1 second)

## Troubleshooting

### Redis connection failed

```bash
# Check Redis status
docker-compose ps redis

# View Redis logs
docker-compose logs redis
```

### Build errors

```bash
# Rebuild without cache
docker-compose build --no-cache
```

### Stale data from previous runs

```bash
# Complete cleanup including volumes
docker-compose down -v
```

## Advanced: Scale Up

Test with more scheduler instances:

```bash
# Scale scheduler-1 to 5 instances
docker-compose up --build --scale scheduler-1=5
```

## What This Tests

This distributed test validates:

- **Distributed locking**: Redis locks prevent duplicate execution across pods
- **Exactly-once execution**: Each job executes only once per trigger
- **Load distribution**: Work is distributed across multiple scheduler instances
- **Race condition prevention**: Status and next_run_time updates prevent races

This simulates a real Kubernetes deployment with multiple scheduler pods sharing Redis for coordination.
