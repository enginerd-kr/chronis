# CLAUDE.md

## PR review rules

- Apply `needs-test` when meaningful logic changed without sufficient tests.
- Apply `risk-high` for auth, permission, billing, migration, deletion, security, or concurrency changes.
- Apply `needs-human-review` for architectural changes or unclear intent.
- Apply `safe-small-change` only for clearly low-risk small diffs.
- Never invent labels not already approved by the repository.
- Prioritize correctness, security, and regression risk over style nits.
