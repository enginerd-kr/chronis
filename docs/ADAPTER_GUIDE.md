# Storage Adapter Implementation Guide

This guide helps you implement custom storage adapters for Chronis.

## Interface Contract

All storage adapters must implement the `JobStorageAdapter` abstract class:

```python
from chronis.adapters.base import JobStorageAdapter
from typing import Any

class MyCustomAdapter(JobStorageAdapter):
    def create_job(self, job_data: dict[str, Any]) -> dict[str, Any]:
        """Create a job in storage."""
        pass

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Get a job by ID."""
        pass

    def update_job(self, job_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        """Update a job."""
        pass

    def delete_job(self, job_id: str) -> bool:
        """Delete a job."""
        pass

    def query_jobs(
        self,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query jobs with filters."""
        pass
```

## Job Data Structure

### Required Fields

```python
{
    "job_id": "unique-job-id",      # str
    "status": "scheduled",           # str: "scheduled", "running", "completed", etc.
    "next_run_time": "2025-01-15T10:00:00Z",  # str (ISO format, UTC)
}
```

### Optional Fields

```python
{
    "metadata": {                    # dict (user-defined)
        "tenant_id": "acme",        # Multi-tenancy
        "priority": "high",          # Custom tags
        # ... any other fields
    },
    "name": "Daily Report",
    "trigger_type": "interval",
    "trigger_args": {"hours": 24},
    # ... adapter-specific fields
}
```

## Filter Implementation

The `query_jobs()` method receives filters as a dictionary. Common patterns:

```python
filters = {
    "status": "scheduled",
    "next_run_time_lte": "2025-01-15T10:00:00Z",
    "metadata.tenant_id": "acme",
    "metadata.priority": "high"
}
```

**Filter interpretation is adapter-specific.** Implement based on your storage capabilities.

## Implementation Examples

### Example 1: SQL Database (PostgreSQL)

```python
from chronis.adapters.base import JobStorageAdapter
import psycopg2

class PostgreSQLAdapter(JobStorageAdapter):
    def __init__(self, connection_string: str):
        self.conn = psycopg2.connect(connection_string)

    def query_jobs(
        self,
        filters: dict | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        query = "SELECT * FROM scheduled_jobs WHERE 1=1"
        params = []

        if filters:
            if "status" in filters:
                query += " AND status = %s"
                params.append(filters["status"])

            if "next_run_time_lte" in filters:
                query += " AND next_run_time <= %s"
                params.append(filters["next_run_time_lte"])

            # Metadata filters (JSONB support)
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

**Schema:**

```sql
CREATE TABLE scheduled_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50),
    next_run_time TIMESTAMPTZ,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Recommended indexes
CREATE INDEX idx_status_time ON scheduled_jobs (status, next_run_time);
CREATE INDEX idx_tenant ON scheduled_jobs ((metadata->>'tenant_id'));
```

### Example 2: NoSQL Database (DynamoDB)

```python
import boto3
from chronis.adapters.base import JobStorageAdapter

class DynamoDBAdapter(JobStorageAdapter):
    def __init__(self, table_name: str, region: str = "us-east-1"):
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)

    def query_jobs(
        self,
        filters: dict | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        # Option 1: Single-tenant query using PK
        if filters and "metadata.tenant_id" in filters:
            response = self.table.query(
                KeyConditionExpression="PK = :pk",
                FilterExpression="#status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":pk": filters["metadata.tenant_id"],
                    ":status": filters.get("status", "scheduled")
                },
                Limit=limit or 100
            )
            return response['Items']

        # Option 2: Cross-tenant query using GSI
        response = self.table.query(
            IndexName="status-time-index",
            KeyConditionExpression="GSI1_PK = :status AND GSI1_SK <= :time",
            ExpressionAttributeValues={
                ":status": filters.get("status", "scheduled"),
                ":time": filters.get("next_run_time_lte", "9999-12-31T23:59:59Z")
            },
            Limit=limit or 100
        )
        return response['Items']
```

**Table Schema:**

```python
# Primary Key
PK = metadata["tenant_id"]  # or "JOBS" for single-tenant
SK = f"JOB#{job_id}"

# GSI for cross-tenant queries
GSI1_PK = status
GSI1_SK = next_run_time
```

### Example 3: Key-Value Store (Redis)

```python
import redis
import json
from chronis.adapters.base import JobStorageAdapter

class RedisAdapter(JobStorageAdapter):
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)

    def create_job(self, job_data: dict) -> dict:
        job_id = job_data["job_id"]
        # Store job as hash
        self.redis.hset(f"job:{job_id}", mapping=self._serialize(job_data))
        # Add to sorted set for efficient querying
        if job_data.get("status") == "scheduled":
            timestamp = job_data["next_run_time"]
            self.redis.zadd("jobs:scheduled", {job_id: timestamp})
        return job_data

    def query_jobs(
        self,
        filters: dict | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict]:
        # Use sorted set for time-based queries
        if filters and filters.get("status") == "scheduled":
            max_time = filters.get("next_run_time_lte", "+inf")
            job_ids = self.redis.zrangebyscore(
                "jobs:scheduled",
                "-inf",
                max_time,
                start=offset or 0,
                num=limit or 100
            )
            jobs = [self._deserialize(self.redis.hgetall(f"job:{jid}"))
                    for jid in job_ids]

            # Filter by metadata if needed
            if "metadata.tenant_id" in filters:
                tenant = filters["metadata.tenant_id"]
                jobs = [j for j in jobs
                        if j.get("metadata", {}).get("tenant_id") == tenant]

            return jobs
        # Fallback to scan
        return self._scan_all(filters, limit)

    def _serialize(self, data: dict) -> dict:
        """Serialize nested dicts to JSON strings."""
        return {k: json.dumps(v) if isinstance(v, dict) else v
                for k, v in data.items()}

    def _deserialize(self, data: dict) -> dict:
        """Deserialize JSON strings back to dicts."""
        result = {}
        for k, v in data.items():
            try:
                result[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                result[k] = v
        return result
```

## Performance Guidelines

### Indexing Strategy

**Required indexes:**
- `status` + `next_run_time` (for scheduler polling)

**Optional indexes (multi-tenancy):**
- `metadata.tenant_id`
- `metadata.tenant_id` + `status` + `next_run_time`

### Query Optimization

```python
# ✅ Good: Indexed query
filters = {"status": "scheduled", "next_run_time_lte": current_time}

# ⚠️ Acceptable: Metadata query (index recommended)
filters = {"metadata.tenant_id": "acme", "status": "scheduled"}

# ❌ Avoid: Complex metadata queries without index
filters = {
    "metadata.field1": "value1",
    "metadata.field2": "value2",
    "metadata.field3": "value3"
}
```

### Partition Size

- **PostgreSQL/DynamoDB**: Index-based queries, minimal size constraints
- **Redis**: SCAN-based queries benefit from partitions < 1,000-10,000 items
- **In-Memory**: Limited by available memory

## Testing Your Adapter

```python
import unittest
from datetime import datetime

class TestMyAdapter(unittest.TestCase):
    def setUp(self):
        self.adapter = MyCustomAdapter()

    def test_create_and_get_job(self):
        job_data = {
            "job_id": "test-job",
            "status": "scheduled",
            "next_run_time": "2025-01-15T10:00:00Z",
            "metadata": {"tenant_id": "test"}
        }
        created = self.adapter.create_job(job_data)
        retrieved = self.adapter.get_job("test-job")
        self.assertEqual(created["job_id"], retrieved["job_id"])

    def test_query_by_status(self):
        self.adapter.create_job({
            "job_id": "job1",
            "status": "scheduled",
            "next_run_time": "2025-01-15T10:00:00Z"
        })
        jobs = self.adapter.query_jobs(filters={"status": "scheduled"})
        self.assertEqual(len(jobs), 1)

    def test_query_by_metadata(self):
        self.adapter.create_job({
            "job_id": "job1",
            "status": "scheduled",
            "next_run_time": "2025-01-15T10:00:00Z",
            "metadata": {"tenant_id": "acme"}
        })
        jobs = self.adapter.query_jobs(
            filters={"metadata.tenant_id": "acme"}
        )
        self.assertEqual(len(jobs), 1)
```

## Common Patterns

### Multi-Tenancy

```python
# Pattern 1: Tenant ID in metadata
job_data["metadata"] = {"tenant_id": "acme"}

# Pattern 2: Hierarchical keys
job_data["metadata"] = {
    "org_id": "acme",
    "team_id": "engineering",
    "project_id": "website"
}

# Pattern 3: Composite partition key (DynamoDB)
job_data["metadata"] = {
    "partition_key": f"tenant:{tenant_id}:project:{project_id}"
}
```

### Time-based Queries

```python
# Scheduler polling
filters = {
    "status": "scheduled",
    "next_run_time_lte": utc_now().isoformat()
}

# Tenant-specific polling
filters = {
    "metadata.tenant_id": "acme",
    "status": "scheduled",
    "next_run_time_lte": utc_now().isoformat()
}
```

## Reference Implementation

See [`chronis/adapters/storage/memory.py`](storage/memory.py) for the complete InMemoryStorageAdapter implementation.
