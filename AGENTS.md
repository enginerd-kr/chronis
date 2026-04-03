# AGENTS.md

## Review guidelines

- Don't log secrets, tokens, or PII.
- Verify that authentication and authorization checks exist at every required boundary.
- Flag broad `except Exception` blocks unless they re-raise or clearly justify recovery.
- Check that network and external I/O calls have explicit timeouts and error handling.
- Verify that functions handle `None`, empty inputs, and invalid data safely.
- Check that mutable default arguments are not used.
- Ensure DB writes are transactional where partial failure would be dangerous.
- Flag blocking I/O inside async functions.
- Require tests for bug fixes and critical edge cases.
- Prioritize correctness, security, and operational risk over cosmetic style comments.
