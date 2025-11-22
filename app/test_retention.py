import types
import pytest

from app import retention_worker


class DummyConn:
    """
    Very simple dummy connection / cursor to avoid real DB.
    You can extend this or replace with your own mock library later.
    """
    def __init__(self, rows=None):
        self.rows = rows or []
        self.closed = False

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
        self.queries.append((query, params))

    def fetchall(self):
        return self.rows


def test_get_retention_history_empty(monkeypatch):
    """
    History: no deleted rows → count==0
    """
    dummy_conn = DummyConn(rows=[])

    def fake_get_conn():
        return dummy_conn

    monkeypatch.setattr(retention_worker, "_get_conn", fake_get_conn)

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


# Skeletons for future tests (you can fill later):

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
