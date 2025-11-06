# Chronis Examples

This directory contains example code demonstrating how to use Chronis.

## Quick Start

Run the basic example:

```bash
python examples/quickstart.py
```

This example demonstrates:
- Creating a scheduler with in-memory storage and locks
- Registering job functions
- Creating interval and cron jobs
- Starting and stopping the scheduler
- Listing and monitoring jobs

## Examples

### 1. quickstart.py
Basic example showing fundamental Chronis usage with in-memory adapters.

## Running Examples

Make sure you have installed Chronis with dependencies:

```bash
# Using pip
pip install -e .

# Using uv
uv sync
uv run python examples/quickstart.py
```
