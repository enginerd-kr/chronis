# Redis Adapter Example

This example demonstrates using Chronis with Redis for distributed storage and locking.

## Quick Start

### 1. Start Redis

```bash
# Using Docker Compose (recommended)
docker-compose up -d

# Or using Docker directly
docker run -d -p 6379:6379 redis:7-alpine

# Or using Homebrew (macOS)
brew install redis
redis-server
```

### 2. Install Dependencies

```bash
# Install Chronis with Redis support
pip install chronis[redis]

# Or with uv
uv pip install chronis[redis]
```

### 3. Run Example

```bash
python examples/redis/redis_example.py

# Or with uv
uv run python examples/redis/redis_example.py
```

## What This Example Shows

- Creating Redis-based storage and lock adapters
- Connecting to Redis server
- Creating and managing jobs with Redis persistence
- Distributed locking across multiple scheduler instances
- Monitoring jobs in Redis
- Cleanup and resource management

## Redis Data Structure

After running the example, you can inspect Redis:

```bash
# Connect to Redis CLI
redis-cli

# List all Chronis keys
KEYS example:*

# View a job
HGET example:jobs:email-job data

# View a lock
GET example:lock:email-job
```

## Running Multiple Schedulers

Test distributed execution:

```bash
# Terminal 1
python examples/redis/redis_example.py

# Terminal 2
python examples/redis/redis_example.py

# Jobs will execute only once (distributed locking works)
```

## Customization

The Redis adapters in `chronis/contrib/` are reference implementations.

To customize:
1. Copy the adapter code to your project
2. Modify schema to fit your needs
3. Add custom monitoring and metrics
4. Adjust key prefixes and TTLs
5. Optimize for your scale

See `chronis/contrib/lock/redis.py` and `chronis/contrib/storage/redis.py` for implementation details.

## Cleanup

```bash
# Stop Redis
docker-compose down

# Remove volumes
docker-compose down -v
```
