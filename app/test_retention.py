import pytest

from app import retention_worker


class DummyConn:
    """
    Very simple dummy connection / cursor to avoid real DB.
    Works as a context manager so it can be used with:

        with get_connection() as conn:
            ...

    """
    def __init__(self, rows=None):
        self.rows = rows or []
        self.closed = False

    # context manager methods
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def cursor(self, cursor_factory=None):
        return DummyCursor(self.rows)

    def close(self):
        self.closed = True


class DummyCursor:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def execute(self, query, params=None):
        # just store what was executed for potential inspection
        self.queries.append((query, params))

    def fetchall(self):
        return self.rows


def test_get_retention_history_empty(monkeypatch):
    """
    History: no deleted rows → count==0
    """
    dummy_conn = DummyConn(rows=[])

    # monkeypatch the real DB connection helper used in retention_worker
    def fake_get_connection():
        return dummy_conn

    monkeypatch.setattr(retention_worker, "get_connection", fake_get_connection)

    result = retention_worker.get_retention_history(limit=10)

    assert result["count"] == 0
    assert result["items"] == []


def test_notify_retention_prints(capfd):
    """
    Ensure _notify_retention prints something meaningful.
    """
    summary = {"files_deleted": 3, "total_size_bytes": 1024 * 1024 * 2}
    retention_worker._notify_retention(summary)

    out, err = capfd.readouterr()
    assert "Retention removed 3 files" in out
    assert "total_size_mb=" in out


# Skeletons for future tests (to be implemented later)

@pytest.mark.skip(reason="to be implemented")
def test_preview_mode(monkeypatch):
    """
    TODO: mock DB + S3 to verify preview logic without deletes.
    """
    pass


@pytest.mark.skip(reason="to be implemented")
def test_delete_enabled(monkeypatch):
    """
    TODO: simulate delete mode with mocked S3 + DB.
    """
    pass


@pytest.mark.skip(reason="to be implemented")
def test_missing_s3_file(monkeypatch):
    """
    TODO: ensure missing file is handled correctly and not crashing.
    """
    pass
