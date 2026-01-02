# Chronis Examples

Example code demonstrating Chronis usage.

## Basic Examples

### [quickstart.py](quickstart.py)

In-memory scheduler with interval and cron jobs.

```bash
python examples/quickstart.py
```

### [multitenant.py](multitenant.py)

Multi-tenant job scheduling with metadata filtering.

```bash
python examples/multitenant.py
```

### [dead_letter_queue.py](dead_letter_queue.py)

Dead Letter Queue (DLQ) pattern using failure handlers.

Demonstrates how to:

- Capture failed jobs with full error context
- Store failed jobs in DLQ (in-memory example, easily adaptable to PostgreSQL/Redis)
- Query and retry failed jobs
- Track DLQ statistics

```bash
python examples/dead_letter_queue.py
```

## Adapter Examples

### [redis/](redis/)

Redis-based storage and distributed locking.

```bash
cd examples/redis
docker-compose up -d
python redis_example.py
```

See [redis/README.md](redis/README.md) for details.

### [postgres/](postgres/)

PostgreSQL storage with JSONB schema.

```bash
cd examples/postgres
docker-compose up -d
python postgres_example.py
```

See [postgres/README.md](postgres/README.md) for details.

## Setup

```bash
# Install with dependencies
pip install chronis[all]

# Or install from source
pip install -e ".[all]"
```
