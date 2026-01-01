# Adapter Implementation Guide

Guide for implementing custom storage and lock adapters.

## Storage Adapter Interface

Implement `JobStorageAdapter` with these methods:

```python
from chronis.adapters.base import JobStorageAdapter

class MyStorageAdapter(JobStorageAdapter):
    def create_job(self, job_data: dict) -> dict:
        """Create and return job."""

    def get_job(self, job_id: str) -> dict | None:
        """Get job by ID."""

    def update_job(self, job_id: str, updates: dict) -> dict:
        """Update and return job."""

    def delete_job(self, job_id: str) -> bool:
        """Delete job, return success."""

    def query_jobs(
        self,
        filters: dict | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        """Query jobs with filters."""

    def update_job_run_times(
        self,
        job_id: str,
        scheduled_time: str,
        actual_time: str,
        next_run_time: str | None,
    ) -> dict:
        """Update execution times."""
```

## Job Data Structure

### Required Fields

All adapters must store and return these fields:

```python
{
    "job_id": str,
    "status": str,  # "scheduled", "running", "paused", "completed", "failed"
    "next_run_time": str,  # ISO format UTC (e.g., "2025-01-15T10:00:00Z")

    # Misfire handling - CRITICAL for misfire detection
    "if_missed": str,  # "skip" | "run_once" | "run_all"
    "misfire_threshold_seconds": int,
    "last_run_time": str | None,  # Last actual execution time
    "last_scheduled_time": str | None,  # Last scheduled time
}
```

**IMPORTANT**: `query_jobs()` must return ALL misfire fields for misfire detection to work.

### Optional Fields

```python
{
    "metadata": dict,  # User-defined metadata for multi-tenancy, filtering
    "name": str,
    "trigger_type": str,
    "trigger_args": dict,
}
```

## Query Filters

Common filter patterns used by Chronis:

```python
# Scheduler polling (most common)
filters = {
    "status": "scheduled",
    "next_run_time_lte": "2025-01-15T10:00:00Z"
}

# Multi-tenant filtering
filters = {
    "metadata.tenant_id": "acme",
    "status": "scheduled"
}
```

**Filter interpretation is adapter-specific.** Implement based on your storage capabilities.

## Implementation Examples

### PostgreSQL

```python
import psycopg2
from chronis.adapters.base import JobStorageAdapter

class PostgreSQLAdapter(JobStorageAdapter):
    def query_jobs(self, filters=None, limit=None, offset=None):
        query = "SELECT * FROM jobs WHERE 1=1"
        params = []

        if filters:
            if "status" in filters:
                query += " AND status = %s"
                params.append(filters["status"])
            if "next_run_time_lte" in filters:
                query += " AND next_run_time <= %s"
                params.append(filters["next_run_time_lte"])
            if "metadata.tenant_id" in filters:
                query += " AND metadata->>'tenant_id' = %s"
                params.append(filters["metadata.tenant_id"])

        query += " ORDER BY next_run_time"
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"

        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
```

Schema:

```sql
CREATE TABLE jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50),
    next_run_time TIMESTAMPTZ,
    metadata JSONB
);

CREATE INDEX idx_status_time ON jobs(status, next_run_time);
CREATE INDEX idx_metadata_gin ON jobs USING GIN(metadata);
```

### Redis

```python
import redis
import json
from chronis.adapters.base import JobStorageAdapter

class RedisAdapter(JobStorageAdapter):
    def __init__(self, host="localhost", port=6379):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)

    def create_job(self, job_data):
        job_id = job_data["job_id"]
        self.redis.hset(f"job:{job_id}", mapping=self._serialize(job_data))
        if job_data.get("status") == "scheduled":
            self.redis.zadd("jobs:scheduled", {job_id: job_data["next_run_time"]})
        return job_data

    def query_jobs(self, filters=None, limit=None, offset=None):
        if filters and filters.get("status") == "scheduled":
            max_time = filters.get("next_run_time_lte", "+inf")
            job_ids = self.redis.zrangebyscore(
                "jobs:scheduled", "-inf", max_time,
                start=offset or 0, num=limit or 100
            )
            return [self._deserialize(self.redis.hgetall(f"job:{jid}"))
                    for jid in job_ids]
        return []

    def _serialize(self, data):
        return {k: json.dumps(v) if isinstance(v, dict) else v
                for k, v in data.items()}

    def _deserialize(self, data):
        result = {}
        for k, v in data.items():
            try:
                result[k] = json.loads(v)
            except:
                result[k] = v
        return result
```

## Lock Adapter Interface

Implement `JobLockAdapter` for distributed locking:

```python
from chronis.adapters.base import JobLockAdapter

class MyLockAdapter(JobLockAdapter):
    def acquire_lock(self, job_id: str, timeout: int = 30) -> bool:
        """
        Acquire exclusive lock for job execution.

        Returns True if lock acquired, False otherwise.
        Lock should auto-expire after timeout seconds.
        """

    def release_lock(self, job_id: str) -> bool:
        """
        Release lock after job execution.

        Returns True if lock released, False if lock didn't exist.
        """
```

### Redis Lock Example

```python
import redis
from redis.lock import Lock

class RedisLockAdapter(JobLockAdapter):
    def __init__(self, host="localhost", port=6379):
        self.redis = redis.Redis(host=host, port=port)

    def acquire_lock(self, job_id: str, timeout: int = 30) -> bool:
        lock = Lock(self.redis, f"lock:{job_id}", timeout=timeout)
        return lock.acquire(blocking=False)

    def release_lock(self, job_id: str) -> bool:
        lock = Lock(self.redis, f"lock:{job_id}")
        try:
            lock.release()
            return True
        except:
            return False
```

## Performance & Indexing

### Required Indexes

For efficient scheduler polling:

```sql
-- PostgreSQL
CREATE INDEX idx_status_time ON jobs(status, next_run_time);

-- Also needed for metadata queries
CREATE INDEX idx_metadata_gin ON jobs USING GIN(metadata);
```

### Multi-Tenancy Optimization

For tenant-isolated queries, add:

```sql
-- Single tenant queries
CREATE INDEX idx_tenant ON jobs((metadata->>'tenant_id'));

-- Tenant + status queries
CREATE INDEX idx_tenant_status_time
  ON jobs((metadata->>'tenant_id'), status, next_run_time);
```

### Scaling Considerations

- **PostgreSQL**: Index-based queries scale well to millions of jobs
- **Redis**: Use sorted sets (ZSET) for time-based queries; SCAN operations slow down after ~10k jobs
- **DynamoDB**: Design partition keys around tenant_id or status for optimal GSI performance

## Testing Your Adapter

Essential test cases:

```python
def test_storage_adapter():
    adapter = MyStorageAdapter()

    # 1. Create and retrieve job
    job = adapter.create_job({
        "job_id": "test",
        "status": "scheduled",
        "next_run_time": "2025-01-15T10:00:00Z",
        "if_missed": "run_once",
        "misfire_threshold_seconds": 60,
        "last_run_time": None,
        "last_scheduled_time": None,
        "metadata": {"tenant_id": "acme"}
    })
    assert adapter.get_job("test")["job_id"] == "test"

    # 2. Query by status and time
    jobs = adapter.query_jobs(filters={
        "status": "scheduled",
        "next_run_time_lte": "2025-12-31T23:59:59Z"
    })
    assert len(jobs) == 1

    # 3. Query by metadata
    jobs = adapter.query_jobs(filters={"metadata.tenant_id": "acme"})
    assert len(jobs) == 1

    # 4. Update job
    updated = adapter.update_job("test", {"status": "running"})
    assert updated["status"] == "running"

    # 5. Update run times
    updated = adapter.update_job_run_times(
        "test",
        scheduled_time="2025-01-15T10:00:00Z",
        actual_time="2025-01-15T10:00:05Z",
        next_run_time="2025-01-16T10:00:00Z"
    )
    assert updated["last_run_time"] == "2025-01-15T10:00:05Z"

    # 6. Delete job
    assert adapter.delete_job("test") == True
    assert adapter.get_job("test") is None
```

## Reference Implementations

- **In-Memory**: [`chronis/adapters/storage/memory.py`](../chronis/adapters/storage/memory.py), [`chronis/adapters/lock/memory.py`](../chronis/adapters/lock/memory.py)
- **PostgreSQL**: [`chronis/contrib/storage/postgres.py`](../chronis/contrib/storage/postgres.py)
- **Redis**: [`chronis/contrib/storage/redis.py`](../chronis/contrib/storage/redis.py), [`chronis/contrib/lock/redis.py`](../chronis/contrib/lock/redis.py)
