# PostgreSQL Adapter Example

This example demonstrates using Chronis with PostgreSQL for job storage.

## Quick Start

### 1. Start PostgreSQL

```bash
# Using Docker Compose (recommended)
docker-compose up -d

# Wait for database to be ready
docker-compose logs -f postgres
# Look for: "database system is ready to accept connections"
```

### 2. Install Dependencies

```bash
# Install Chronis with PostgreSQL support
pip install chronis[postgres]

# Or with uv
uv pip install chronis[postgres]
```

### 3. Run Example

```bash
python examples/postgres/postgres_example.py

# Or with uv
uv run python examples/postgres/postgres_example.py
```

## What This Example Shows

- Creating PostgreSQL-based storage adapter
- Automatic table and index creation
- Creating and managing jobs with PostgreSQL persistence
- Querying jobs with indexes
- Metadata filtering with JSONB
- Cleanup and resource management

## Database Schema

The adapter automatically creates:

```sql
-- Jobs table
CREATE TABLE chronis_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    status VARCHAR(50),
    next_run_time TIMESTAMP,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_chronis_jobs_status ON chronis_jobs(status);
CREATE INDEX idx_chronis_jobs_next_run_time ON chronis_jobs(next_run_time);
CREATE INDEX idx_chronis_jobs_metadata ON chronis_jobs USING GIN(metadata);
```

## Inspecting Data

### Using psql

```bash
# Connect to database
docker exec -it postgres-postgres-1 psql -U scheduler -d scheduler

# Or if using docker-compose
docker-compose exec postgres psql -U scheduler -d scheduler
```

### SQL Queries

```sql
-- View all jobs
SELECT job_id, status, next_run_time FROM chronis_jobs;

-- View job data
SELECT data FROM chronis_jobs WHERE job_id = 'notification-job';

-- Query by status
SELECT job_id, status FROM chronis_jobs WHERE status = 'scheduled';

-- Query by metadata
SELECT job_id, metadata
FROM chronis_jobs
WHERE metadata @> '{"tenant_id": "acme"}';

-- Check indexes
\di chronis_jobs*

-- Table size
SELECT pg_size_pretty(pg_total_relation_size('chronis_jobs'));
```

## Advanced Usage

### Connection Pooling

```python
from psycopg2 import pool

# Create connection pool
connection_pool = pool.ThreadedConnectionPool(
    minconn=2,
    maxconn=20,
    host='localhost',
    database='scheduler',
    user='scheduler',
    password='scheduler_pass'
)

# Get connection
conn = connection_pool.getconn()
storage = PostgreSQLStorageAdapter(conn)

# Return connection when done
connection_pool.putconn(conn)
```

### High Availability

```yaml
# docker-compose.yml with replication
services:
  postgres-primary:
    image: postgres:16-alpine
    environment:
      POSTGRES_REPLICATION_MODE: master
    ports:
      - "5432:5432"

  postgres-replica:
    image: postgres:16-alpine
    environment:
      POSTGRES_REPLICATION_MODE: slave
      POSTGRES_MASTER_SERVICE_HOST: postgres-primary
    ports:
      - "5433:5432"
```

### Environment Variables

```bash
# .env file
DB_HOST=localhost
DB_PORT=5432
DB_NAME=scheduler
DB_USER=scheduler
DB_PASSWORD=scheduler_pass
```

```python
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT', 5432)),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
```

## Monitoring

### Query Performance

```sql
-- Enable query logging
ALTER DATABASE scheduler SET log_min_duration_statement = 100;

-- View slow queries
SELECT query, calls, mean_exec_time, max_exec_time
FROM pg_stat_statements
WHERE query LIKE '%chronis_jobs%'
ORDER BY mean_exec_time DESC;
```

### Index Usage

```sql
-- Check if indexes are being used
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read
FROM pg_stat_user_indexes
WHERE tablename = 'chronis_jobs';
```

## Optimization Tips

1. **Partition large tables**
   ```sql
   -- Partition by status
   CREATE TABLE chronis_jobs_scheduled PARTITION OF chronis_jobs
       FOR VALUES IN ('scheduled');
   ```

2. **Vacuum regularly**
   ```sql
   VACUUM ANALYZE chronis_jobs;
   ```

3. **Add composite indexes**
   ```sql
   CREATE INDEX idx_status_time ON chronis_jobs(status, next_run_time);
   ```

## Cleanup

```bash
# Stop database
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v
```

## See Also

- [PostgreSQL Adapter Guide](../../chronis/contrib/postgres/README.md)
- [Adapter Implementation Guide](../../docs/ADAPTER_GUIDE.md)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
