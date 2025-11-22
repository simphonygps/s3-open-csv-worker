import datetime as dt

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


def test_preview_mode(monkeypatch):
    """
    Verify preview mode aggregates candidates correctly and uses check_s3_exists.
    """
    now = dt.datetime.now(dt.timezone.utc)

    # Fake DB candidates: 2 rows
    def fake_fetch_db_candidates(threshold, limit):
        return [
            {
                "bucket": "simphony-dev",
                "object_key": "csv-worker/a.csv",
                "rows_total": 10,
                "processed_finished_at": now - dt.timedelta(days=1),
                "size_bytes": 100,
            },
            {
                "bucket": "simphony-dev",
                "object_key": "csv-worker/b.csv",
                "rows_total": 5,
                "processed_finished_at": now - dt.timedelta(days=2),
                "size_bytes": 200,
            },
        ]

    # First exists, second missing
    def fake_check_s3_exists(bucket, key):
        return key.endswith("a.csv")

    monkeypatch.setattr(retention_worker, "fetch_db_candidates", fake_fetch_db_candidates)
    monkeypatch.setattr(retention_worker, "check_s3_exists", fake_check_s3_exists)

    # Force deterministic env
    monkeypatch.setenv("RETENTION_DAYS", "0")
    monkeypatch.setenv("RETENTION_LIMIT", "100")
    monkeypatch.setenv("RETENTION_DELETE_ENABLED", "false")

    preview = retention_worker.get_retention_preview()

    assert preview["summary"]["total_candidates"] == 2
    assert preview["summary"]["s3_exists"] == 1
    assert preview["summary"]["s3_missing_or_error"] == 1

    candidates = preview["candidates"]
    assert len(candidates) == 2
    # First should have s3_exists=True, second False
    assert candidates[0]["s3_exists"] or candidates[1]["s3_exists"]
    assert not (candidates[0]["s3_exists"] and candidates[1]["s3_exists"])


def test_delete_enabled(monkeypatch, capfd):
    """
    Ensure that when delete mode is enabled, run_retention_check:
    - calls s3.delete_object for existing candidates
    - calls _mark_deleted_success
    - does NOT treat them as missing
    """
    now = dt.datetime.now(dt.timezone.utc)

    # One candidate that exists in S3
    def fake_fetch_db_candidates(threshold, limit):
        return [
            {
                "bucket": "simphony-dev",
                "object_key": "csv-worker/existing.csv",
                "rows_total": 1,
                "processed_finished_at": now - dt.timedelta(days=1),
                "size_bytes": 123,
            }
        ]

    def fake_check_s3_exists(bucket, key):
        return True

    delete_calls = []
    mark_deleted_calls = []
    update_status_calls = []

    class DummyS3:
        def delete_object(self, Bucket, Key):
            delete_calls.append((Bucket, Key))

    def fake_mark_deleted_success(bucket, key, deleted_reason, deletion_mode):
        mark_deleted_calls.append(
            (bucket, key, deleted_reason, deletion_mode)
        )

    def fake_update_status(bucket, key, status, error_code=None, error_message=None):
        update_status_calls.append(
            (bucket, key, status, error_code, error_message)
        )

    monkeypatch.setattr(retention_worker, "fetch_db_candidates", fake_fetch_db_candidates)
    monkeypatch.setattr(retention_worker, "check_s3_exists", fake_check_s3_exists)
    monkeypatch.setattr(retention_worker, "s3", DummyS3())
    monkeypatch.setattr(retention_worker, "_mark_deleted_success", fake_mark_deleted_success)
    monkeypatch.setattr(retention_worker, "_update_status", fake_update_status)

    # Enable delete mode and notifications
    monkeypatch.setenv("RETENTION_DELETE_ENABLED", "true")
    monkeypatch.setenv("RETENTION_NOTIFY_ENABLED", "false")  # to avoid extra prints in this test

    retention_worker.run_retention_check()

    out, err = capfd.readouterr()
    assert "Retention delete summary" in out

    # Assert S3 delete was called once
    assert delete_calls == [("simphony-dev", "csv-worker/existing.csv")]

    # Assert mark_deleted_success called once with correct bucket/key
    assert len(mark_deleted_calls) == 1
    assert mark_deleted_calls[0][0] == "simphony-dev"
    assert mark_deleted_calls[0][1] == "csv-worker/existing.csv"

    # No 'missing' status updates expected here
    assert all(call[2] != "missing" for call in update_status_calls)


def test_missing_s3_file(monkeypatch, capfd):
    """
    Ensure that when S3 object is missing, run_retention_check:
    - does NOT call s3.delete_object
    - calls _update_status with status='missing'
    """
    now = dt.datetime.now(dt.timezone.utc)

    def fake_fetch_db_candidates(threshold, limit):
        return [
            {
                "bucket": "simphony-dev",
                "object_key": "csv-worker/missing.csv",
                "rows_total": 1,
                "processed_finished_at": now - dt.timedelta(days=1),
                "size_bytes": 456,
            }
        ]

    def fake_check_s3_exists(bucket, key):
        return False  # simulate missing in S3

    delete_calls = []
    update_status_calls = []

    class DummyS3:
        def delete_object(self, Bucket, Key):
            delete_calls.append((Bucket, Key))

    def fake_update_status(bucket, key, status, error_code=None, error_message=None):
        update_status_calls.append(
            (bucket, key, status, error_code, error_message)
        )

    monkeypatch.setattr(retention_worker, "fetch_db_candidates", fake_fetch_db_candidates)
    monkeypatch.setattr(retention_worker, "check_s3_exists", fake_check_s3_exists)
    monkeypatch.setattr(retention_worker, "s3", DummyS3())
    monkeypatch.setattr(retention_worker, "_update_status", fake_update_status)

    monkeypatch.setenv("RETENTION_DELETE_ENABLED", "true")
    monkeypatch.setenv("RETENTION_NOTIFY_ENABLED", "false")

    retention_worker.run_retention_check()

    out, err = capfd.readouterr()
    assert "marked status='missing' in DB" in out

    # S3 delete should never be called
    assert delete_calls == []

    # There should be at least one update_status call with status='missing'
    assert any(call[2] == "missing" for call in update_status_calls)
