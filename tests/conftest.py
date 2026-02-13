import pytest

def _ensure_pyarrow_parquet_attr():
    """
    Pandas' PyArrow parquet backend calls `pyarrow.parquet.*` via the
    `pyarrow` module attribute (pa.parquet). In some environments the
    submodule can be importable but not attached as an attribute, and
    in a full test suite something may reset it (reload/monkeypatch).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    pa.parquet = pq  # force attach every time

def pytest_configure(config):
    # Run as early as possible (before collection finishes / plugins run)
    try:
        _ensure_pyarrow_parquet_attr()
    except Exception:
        pass

@pytest.fixture(autouse=True)
def _fix_pyarrow_parquet_attr():
    # Re-attach before every test in case earlier tests reset pyarrow
    try:
        _ensure_pyarrow_parquet_attr()
    except Exception:
        pass
    yield
