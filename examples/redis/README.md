# Redis Example

Demonstrates Redis-based storage and distributed locking.

## Quick Start

```bash
# 1. Start Redis
docker-compose up -d

# 2. Install dependencies
pip install chronis[redis]

# 3. Run example
python redis_example.py
```

## Inspect Redis Data

```bash
redis-cli

# List jobs
KEYS example:*

# View job data
HGET example:jobs:email-job data
```

## Test Distributed Locking

Run multiple instances - jobs execute only once:

```bash
# Terminal 1
python redis_example.py

# Terminal 2
python redis_example.py
```

## Cleanup

```bash
docker-compose down -v
```
