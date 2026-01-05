# Chronis Database Migrations

This directory contains versioned SQL migration files for the PostgreSQL storage adapter.

## File Naming Convention

Migration files must follow this format:

```
V{version}__{description}.sql
```

- `V`: Required prefix
- `{version}`: Zero-padded version number (e.g., 001, 002, 010)
- `__`: Double underscore separator
- `{description}`: Snake_case description
- `.sql`: File extension

### Examples

```
V001__initial_schema.sql
V002__add_locks_table.sql
V003__add_job_indexes.sql
V010__add_retry_tracking.sql
```

## How It Works

1. **Version Tracking**: The system maintains a `chronis_migration_history` table that tracks which migrations have been applied.

2. **Automatic Execution**: When you initialize the PostgreSQL adapter with a migrations directory, pending migrations are automatically executed in order.

3. **Idempotent**: Use `IF NOT EXISTS` clauses in your SQL to make migrations safe to re-run.

## Usage

### Automatic Migration (Recommended)

```python
import psycopg2
from chronis.contrib.adapters.storage.postgres import PostgreSQLStorageAdapter

conn = psycopg2.connect(...)

# Automatically runs pending migrations on initialization
storage = PostgreSQLStorageAdapter(
    conn,
    migrations_dir="chronis/contrib/adapters/storage/postgres/migrations"
)
```

### Manual Migration Control

```python
from chronis.contrib.adapters.storage.postgres import MigrationRunner
from pathlib import Path

runner = MigrationRunner(conn, Path("chronis/contrib/adapters/storage/postgres/migrations"))

# Check status
status = runner.status()
print(f"Applied: {status['applied_count']}, Pending: {status['pending_count']}")

# Run migrations manually
runner.migrate()

# Run up to specific version
runner.migrate(target_version=5)
```

### Disable Auto-Migration

```python
# Initialize without running migrations
storage = PostgreSQLStorageAdapter(
    conn,
    migrations_dir="chronis/contrib/adapters/storage/postgres/migrations",
    auto_migrate=False
)
```

## Creating New Migrations

1. Find the next available version number:
   ```bash
   ls chronis/contrib/adapters/storage/postgres/migrations/V*.sql | tail -1
   ```

2. Create a new file with the next version:
   ```bash
   touch chronis/contrib/adapters/storage/postgres/migrations/V002__add_feature.sql
   ```

3. Write your SQL migration:
   ```sql
   -- Add new column
   ALTER TABLE chronis_jobs ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT 0;

   -- Add index
   CREATE INDEX IF NOT EXISTS idx_chronis_jobs_priority
   ON chronis_jobs(priority);
   ```

4. Test your migration:
   ```python
   runner = MigrationRunner(conn, Path("chronis/contrib/adapters/storage/postgres/migrations"))
   runner.migrate()
   ```

## Best Practices

1. **Use Transactions**: Each migration runs in a transaction and rolls back on failure.

2. **Be Idempotent**: Always use `IF NOT EXISTS` or `IF EXISTS` to make migrations safe to re-run.

3. **Test First**: Test migrations on a development database before production.

4. **Don't Modify Applied Migrations**: Once a migration is applied, create a new migration instead of editing the old one.

5. **Keep Migrations Small**: One logical change per migration for easier rollback and debugging.

## Troubleshooting

### Migration Fails

If a migration fails, it's automatically rolled back. Fix the SQL and try again:

```python
runner.migrate()  # Will retry failed migration
```

### Check Migration Status

```python
status = runner.status()

print("Applied migrations:")
for m in status['applied']:
    print(f"  V{m['version']:03d}: {m['description']}")

print("\nPending migrations:")
for m in status['pending']:
    print(f"  V{m['version']:03d}: {m['description']}")
```

### Skip Auto-Migration

If you need more control, disable auto-migration and run manually:

```python
storage = PostgreSQLStorageAdapter(
    conn,
    migrations_dir="chronis/contrib/adapters/storage/migrations",
    auto_migrate=False
)

# Later, run migrations when ready
runner = MigrationRunner(conn, Path("chronis/contrib/adapters/storage/migrations"))
runner.migrate()
```
