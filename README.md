# Deduplicating Executor for Agent Tool Calls

Executes tool calls with request deduplication: identical in-flight calls share one future, plus batching windows and idempotency keys.

**Stack:** Python 3.12 · stdlib only · unittest

## Problem statement

When several agents (or retries) fire the same tool call concurrently, only one execution should happen.  `submit` registers a call under an idempotency key: the first submitter executes, late submitters with the same key block on the same future and receive the same result.

## Design decisions

- `submit_batch` collects calls for a short window and executes the unique keys once each.
- Failures are cached briefly so a retry storm does not hammer a failing tool; successes are not cached (pair with a result cache for that).

## Usage example

```python
from src.dedup_executor import DedupExecutor

ex = DedupExecutor()
print(ex.submit("search:ai", lambda: ["r1", "r2"]))  # ['r1', 'r2']
```

## Layout

- `README.md`
- `src/dedup_executor.py`
- `tests/test_dedup_executor.py`
- `LICENSE`
- `.gitignore`

## Setup

No third-party dependencies — Python 3.11+ standard library only.

```bash
cd agentic-dedup-executor
python -m unittest discover -s tests -v
```

## License

MIT — see [LICENSE](LICENSE).
