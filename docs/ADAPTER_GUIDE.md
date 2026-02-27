# Adapter Implementation Guide

Guide for implementing custom storage and lock adapters.

## Storage Adapter Interface

Implement `JobStorageAdapter` with these **6 required methods**:

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

    def compare_and_swap_job(
        self,
        job_id: str,
        expected_values: dict,
        updates: dict,
    ) -> tuple[bool, dict | None]:
        """Atomically update job only if current values match expected values.

        MUST be atomic to prevent duplicate job execution in distributed deployments.

        Examples:
        - Redis: WATCH/MULTI/EXEC transaction
        - PostgreSQL: UPDATE ... WHERE conditions
        - DynamoDB: ConditionExpression in UpdateItem
        - InMemory: Direct dict comparison (single-process only)
        """

    # Optional: Override for optimization
    # def count_jobs(self, filters=None):
    #     """Count jobs. Default: len(query_jobs(filters))."""
```

### Optional Method Overrides

**count_jobs** (Recommended for large datasets):

- **Default**: `len(query_jobs(filters))`
- **Override for**: Performance optimization (> 10k jobs)
- **Examples**: SQL COUNT(*), Redis SCARD

## Job Data Structure

### Required Fields

All adapters must store and return these fields:

```python
{
    "job_id": str,
    "status": str,  # "pending" | "scheduled" | "running" | "paused" | "failed"
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

Implement `LockAdapter` with these methods:

```python
from chronis.adapters.base import LockAdapter
import uuid

class MyLockAdapter(LockAdapter):
    def __init__(self):
        # MUST generate unique instance token
        self.instance_token = str(uuid.uuid4())

    def acquire(
        self,
        lock_key: str,
        ttl_seconds: int,
        blocking: bool = False,
        timeout: float | None = None,
        owner_id: str | None = None,
    ) -> bool:
        """
        Acquire distributed lock.

        Requirements:
        - MUST store owner_id with lock (for verification)
        - MUST be atomic (no race conditions)
        - MUST respect TTL (auto-expire)
        - SHOULD support blocking mode efficiently

        Returns True if acquired, False otherwise.
        """

    def release(self, lock_key: str, owner_id: str | None = None) -> bool:
        """
        Release distributed lock.

        Requirements:
        - MUST verify owner_id before release
        - MUST be atomic (check + delete)
        - MUST return False if not owner

        Returns True if released, False if not owner or doesn't exist.
        """

    def extend(
        self,
        lock_key: str,
        ttl_seconds: int,
        owner_id: str | None = None,
    ) -> bool:
        """
        Extend lock TTL.

        Requirements:
        - MUST verify owner_id before extending
        - MUST be atomic (check + extend)

        Returns True if extended, False if not owner or doesn't exist.
        """

    def reset(self, lock_key: str) -> bool:
        """
        Forcibly delete lock (crash recovery).

        ⚠️ WARNING: Bypasses ownership checks!

        Returns True if deleted, False if didn't exist.
        """
```

### Redis Lock Implementation

```python
import redis
import uuid
import time
from chronis.adapters.base import LockAdapter

# Lua script for atomic release with ownership verification
LUA_RELEASE_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    redis.call('del', KEYS[1])
    redis.call('lpush', KEYS[2], '1')
    redis.call('expire', KEYS[2], 1)
    return 1
"""

# Lua script for atomic extend with ownership verification
LUA_EXTEND_SCRIPT = """
    local token = redis.call('get', KEYS[1])
    if not token or token ~= ARGV[1] then
        return 0
    end
    redis.call('expire', KEYS[1], ARGV[2])
    return 1
"""

class RedisLockAdapter(LockAdapter):
    def __init__(self, redis_client, key_prefix="chronis:lock:"):
        self.redis = redis_client
        self.key_prefix = key_prefix
        self.instance_token = str(uuid.uuid4())

    def acquire(
        self,
        lock_key: str,
        ttl_seconds: int,
        blocking: bool = False,
        timeout: float | None = None,
        owner_id: str | None = None,
    ) -> bool:
        full_key = f"{self.key_prefix}{lock_key}"
        signal_key = f"{full_key}:signal"
        token = owner_id or self.instance_token

        # Try to acquire (store owner token)
        result = self.redis.set(full_key, token, nx=True, ex=ttl_seconds)

        if result or not blocking:
            return bool(result)

        # Blocking mode: wait for signal with BLPOP
        start = time.time()
        remaining = timeout

        while True:
            self.redis.blpop(signal_key, timeout=remaining or 0)

            if self.redis.set(full_key, token, nx=True, ex=ttl_seconds):
                return True

            if timeout:
                elapsed = time.time() - start
                remaining = timeout - elapsed
                if remaining <= 0:
                    return False

    def release(self, lock_key: str, owner_id: str | None = None) -> bool:
        full_key = f"{self.key_prefix}{lock_key}"
        signal_key = f"{full_key}:signal"
        token = owner_id or self.instance_token

        # Atomic release with ownership verification
        result = self.redis.eval(LUA_RELEASE_SCRIPT, 2, full_key, signal_key, token)
        return result == 1

    def extend(
        self,
        lock_key: str,
        ttl_seconds: int,
        owner_id: str | None = None,
    ) -> bool:
        full_key = f"{self.key_prefix}{lock_key}"
        token = owner_id or self.instance_token

        # Atomic extend with ownership verification
        result = self.redis.eval(LUA_EXTEND_SCRIPT, 1, full_key, token, ttl_seconds)
        return result == 1

    def reset(self, lock_key: str) -> bool:
        full_key = f"{self.key_prefix}{lock_key}"
        return self.redis.delete(full_key) > 0
```

### Key Implementation Details

**Ownership Tracking**:

- Each adapter instance has a unique `instance_token` (UUID)
- Lock value stores the owner token (not just "1")
- Release/extend verify ownership before operation

**Atomic Operations**:

- Redis: Lua scripts for check-and-delete/extend
- DynamoDB: Conditional expressions
- InMemory: threading.Lock for atomicity

**Blocking Mode**:

- Redis: BLPOP on signal key (efficient, no spinloop)
- InMemory: Condition.wait() for thread signaling
- DynamoDB: Sleep polling (no BLPOP equivalent)

**Signal Mechanism**:

- On release, push to `{lock_key}:signal` list
- Waiting processes BLPOP on signal key
- Signal key expires after 1 second

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

    # 5. Update execution times (using update_job)
    updated = adapter.update_job(
        "test",
        {
            "last_scheduled_time": "2025-01-15T10:00:00Z",
            "last_run_time": "2025-01-15T10:00:05Z",
            "next_run_time": "2025-01-16T10:00:00Z",
        }
    )
    assert updated["last_run_time"] == "2025-01-15T10:00:05Z"

    # 6. Delete job
    assert adapter.delete_job("test") == True
    assert adapter.get_job("test") is None
```

## Reference Implementations

### Storage Adapters

- **In-Memory**: [`chronis/adapters/storage/memory.py`](../chronis/adapters/storage/memory.py)
- **PostgreSQL**: [`chronis/contrib/adapters/storage/postgres/adapter.py`](../chronis/contrib/adapters/storage/postgres/adapter.py)
- **Redis**: [`chronis/contrib/adapters/storage/redis.py`](../chronis/contrib/adapters/storage/redis.py)

### Lock Adapters

- **In-Memory**: [`chronis/adapters/lock/memory.py`](../chronis/adapters/lock/memory.py)
- **Redis**: [`chronis/contrib/adapters/lock/redis.py`](../chronis/contrib/adapters/lock/redis.py)
