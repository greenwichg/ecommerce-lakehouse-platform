"""Source data generators.

Each generator is a self-contained Python module exposing a Click CLI:

    python -m generators.orders --start-date YYYY-MM-DD --end-date YYYY-MM-DD

Generators are idempotent: identical args produce identical output. This
matters for backfills (re-running a date range yields the same data) and
for tests (we can re-run and compare).
"""
