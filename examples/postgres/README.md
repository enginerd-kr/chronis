# PostgreSQL Example

Demonstrates PostgreSQL storage with automatic database migrations, JSONB schema, and optimized indexes.

## Quick Start

```bash
# 1. Start PostgreSQL
docker-compose up -d

# 2. Install dependencies
pip install chronis[postgres]

# 3. Run example
python postgres_example.py
```

## Database Migrations

Chronis uses a **Flyway-style migration system** for PostgreSQL:

- ✅ Automatic schema creation on first run
- ✅ Version-controlled SQL migrations
- ✅ Migration history tracking
- ✅ Checksum verification
- ✅ Automatic rollback on failure

### Migration Files

Located in `chronis/contrib/storage/postgres/migrations/`:

```
V001__initial_schema.sql    # Creates tables and indexes
```

### Migration History

Tracked in `chronis_migration_history` table:

```sql
SELECT version, description, applied_at, execution_time_ms
FROM chronis_migration_history
ORDER BY version;
```

## Schema

Tables created automatically via migrations:

### 1. Job Storage (`chronis_jobs`)

```sql
CREATE TABLE chronis_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    status VARCHAR(50),
    next_run_time TIMESTAMP,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Indexes:**
- `idx_chronis_jobs_status` - Status queries
- `idx_chronis_jobs_next_run_time` - Scheduling queries
- `idx_chronis_jobs_metadata` (GIN) - JSONB metadata queries

### 2. Migration History (`chronis_migration_history`)

```sql
CREATE TABLE chronis_migration_history (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    filename TEXT NOT NULL,
    applied_at TIMESTAMP DEFAULT NOW(),
    checksum TEXT,
    execution_time_ms INTEGER
);
```

## Inspect Data

```bash
# Connect to database
docker-compose exec postgres psql -U scheduler -d scheduler

# View jobs
SELECT job_id, status, next_run_time FROM chronis_jobs;

# Query by metadata (JSONB)
SELECT * FROM chronis_jobs WHERE metadata @> '{"tenant_id": "acme"}';

# Check migration history
SELECT version, description, applied_at
FROM chronis_migration_history
ORDER BY version;
```

## Advanced Usage

### Custom Migrations

You can provide your own migration directory:

```python
from pathlib import Path
from chronis.contrib.storage import PostgreSQLStorageAdapter

storage = PostgreSQLStorageAdapter(
    conn,
    migrations_dir="my_custom_migrations"
)
```

### Manual Migration Control

```python
from chronis.contrib.storage.postgres import MigrationRunner

# Check status
runner = MigrationRunner(conn, Path("chronis/contrib/storage/postgres/migrations"))
status = runner.status()
print(f"Applied: {status['applied_count']}, Pending: {status['pending_count']}")

# Run migrations manually
runner.migrate()
```

### Disable Auto-Migration

```python
storage = PostgreSQLStorageAdapter(
    conn,
    auto_migrate=False  # Don't run migrations on init
)
```

## Cleanup

```bash
# Stop and remove containers
docker-compose down -v
```

## Production Considerations

1. **Connection Pooling**: Use `psycopg2.pool` or pgBouncer
2. **Migrations**: Review and test migrations before production
3. **Monitoring**: Track migration execution times
4. **Backups**: Regular PostgreSQL backups before schema changes
