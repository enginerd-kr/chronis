# Chronis Examples

This directory contains example code demonstrating how to use Chronis.

## Examples

### Basic Examples

#### 1. [quickstart.py](quickstart.py)
Basic example showing fundamental Chronis usage with in-memory adapters.

```bash
python examples/quickstart.py
```

Features:
- In-memory storage and locks
- Interval and cron jobs
- Job monitoring

#### 2. [multitenant.py](multitenant.py)
Multi-tenant job scheduling with metadata filtering.

```bash
python examples/multitenant.py
```

Features:
- Tenant isolation with metadata
- Per-tenant job queries
- Multi-tenant best practices

### Adapter Examples

#### 3. [redis/](redis/)
Using Redis for storage and distributed locking.

```bash
cd examples/redis
docker-compose up -d
python redis_example.py
```

Features:
- Redis storage adapter
- Distributed locking
- Docker Compose setup
- Indexed queries

See [redis/README.md](redis/README.md) for details.

#### 4. [postgres/](postgres/)
Using PostgreSQL for persistent storage.

```bash
cd examples/postgres
docker-compose up -d
python postgres_example.py
```

Features:
- PostgreSQL storage adapter
- JSONB-based schema
- Indexed queries
- Docker Compose setup
- Schema initialization

See [postgres/README.md](postgres/README.md) for details.

## Running Examples

### Prerequisites

```bash
# Install Chronis
pip install -e .

# Or with optional dependencies
pip install -e ".[redis]"      # For Redis examples
pip install -e ".[postgres]"   # For PostgreSQL examples
pip install -e ".[all]"        # For all examples
```

### Using uv

```bash
# Install dependencies
uv sync

# Run examples
uv run python examples/quickstart.py
uv run python examples/redis/redis_example.py
uv run python examples/postgres/postgres_example.py
```

## Docker Support

Redis and PostgreSQL examples include Docker Compose files:

```bash
# Start infrastructure
cd examples/redis && docker-compose up -d
cd examples/postgres && docker-compose up -d

# Stop infrastructure
docker-compose down

# Remove data volumes
docker-compose down -v
```
