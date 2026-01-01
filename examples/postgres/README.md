# PostgreSQL Example

Demonstrates PostgreSQL storage with JSONB schema and indexes.

## Quick Start

```bash
# 1. Start PostgreSQL
docker-compose up -d

# 2. Install dependencies
pip install chronis[postgres]

# 3. Run example
python postgres_example.py
```

## Schema

Table created automatically with JSONB support:

```sql
CREATE TABLE chronis_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    status VARCHAR(50),
    next_run_time TIMESTAMP,
    metadata JSONB
);
```

Indexes on `status`, `next_run_time`, and `metadata` (GIN).

## Inspect Data

```bash
# Connect to database
docker-compose exec postgres psql -U scheduler -d scheduler

# View jobs
SELECT job_id, status, next_run_time FROM chronis_jobs;

# Query by metadata
SELECT * FROM chronis_jobs WHERE metadata @> '{"tenant_id": "acme"}';
```

## Cleanup

```bash
docker-compose down -v
```
