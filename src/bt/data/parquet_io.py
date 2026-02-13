"""Utilities for resilient parquet IO with pandas/pyarrow."""


def ensure_pyarrow_parquet() -> None:
    """Ensure ``pyarrow.parquet`` is attached for pandas parquet backends.

    Some runtimes import ``pyarrow.parquet`` without exposing it on
    ``pyarrow.parquet`` attribute access; pandas can rely on that attribute.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        return

    pa.parquet = pq
