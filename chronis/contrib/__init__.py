"""
Community-contributed adapters for Chronis.

These are reference implementations maintained by the Chronis team.
You can use them as-is or customize for your needs.

## Installation

These adapters require optional dependencies:

```bash
# Redis adapters
pip install chronis[redis]

# All adapters
pip install chronis[all]
```

## Usage

```python
# Redis adapters (optional)
try:
    from chronis.contrib.adapters.lock import RedisLockAdapter
    from chronis.contrib.adapters.storage import RedisStorageAdapter
except ImportError:
    # Install with: pip install chronis[redis]
    pass
```

## Customization

These implementations are starting points. To customize:
1. Review the code to understand the schema and design decisions
2. Copy and modify to match your specific requirements
3. Add your own optimizations (indexing, caching, etc.)
4. Test thoroughly with your infrastructure

## Maintenance Policy

- We maintain these adapters with best-effort support
- Breaking changes will be documented in CHANGELOG
- Community contributions welcome
- For critical use cases, consider maintaining your own fork
"""
