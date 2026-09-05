from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from contextswarm_mini.cps import CPSStore
from contextswarm_mini.profiling import PROFILE_FILENAME, RunProfiler

ROOT = Path(__file__).resolve().parents[1]


class _FailingContext:
    def __init__(self, *, enter_error: BaseException | None = None,
                 exit_error: BaseException | None = None,
                 suppress: bool = False) -> None:
        self.enter_error = enter_error
        self.exit_error = exit_error
        self.suppress = suppress
        self.entered = False
        self.exited = False

    def __enter__(self):
        if self.enter_error is not None:
            raise self.enter_error
        self.entered = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exited = True
        if self.exit_error is not None:
            raise self.exit_error
        return self.suppress


class _Profiler:
    enabled = True

    def __init__(self, context: _FailingContext) -> None:
        self.context = context

    def span(self, name: str, **fields: object):
        del name, fields
        return self.context


class _FactoryFailProfiler:
    enabled = True

    def span(self, name: str, **fields: object):
        del name, fields
        raise RuntimeError("sink factory")


def _store_with_context(context: _FailingContext) -> CPSStore:
    # _profile_span only needs these two attributes.  Avoid opening a SQLite
    # database so this regression test remains focused on sink semantics.
    store = CPSStore.__new__(CPSStore)
    store._profiling_enabled = True
    store.profiler = _Profiler(context)
    return store


class CPSProfileSpanTests(unittest.TestCase):
    @staticmethod
    def _rows(path: Path) -> list[dict[str, object]]:
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def test_sink_enter_failure_is_fail_open(self) -> None:
        context = _FailingContext(enter_error=RuntimeError("sink enter"))
        store = _store_with_context(context)
        observed: list[str] = []

        with store._profile_span("cps.test"):
            observed.append("business ran")

        self.assertEqual(observed, ["business ran"])
        self.assertFalse(context.entered)
        self.assertFalse(context.exited)

    def test_sink_exit_failure_is_fail_open(self) -> None:
        context = _FailingContext(exit_error=RuntimeError("sink exit"))
        store = _store_with_context(context)
        observed: list[str] = []

        with store._profile_span("cps.test"):
            observed.append("business ran")

        self.assertEqual(observed, ["business ran"])
        self.assertTrue(context.entered)
        self.assertTrue(context.exited)

    def test_business_exception_is_never_suppressed_by_sink(self) -> None:
        context = _FailingContext(suppress=True)
        store = _store_with_context(context)

        with self.assertRaisesRegex(ValueError, "business failure"):
            with store._profile_span("cps.test"):
                raise ValueError("business failure")

        self.assertTrue(context.entered)
        self.assertTrue(context.exited)

    def test_sink_factory_failure_is_fail_open(self) -> None:
        store = CPSStore.__new__(CPSStore)
        store._profiling_enabled = True
        store.profiler = _FactoryFailProfiler()
        with store._profile_span("cps.test"):
            pass

    def test_rolled_back_write_reports_zero_rows_written(self) -> None:
        """SQLite total_changes must not count mutations that never commit."""

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-cps-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root / "profile",
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="cps-rollback",
            )
            profiler.start()
            store = CPSStore(root / "cps.sqlite3", profiler=profiler)
            with self.assertRaisesRegex(RuntimeError, "synthetic body failure"):
                with store._write_transaction("body_failure") as db:
                    db.execute(
                        "INSERT INTO events(event_id,event_type,payload,created_at) "
                        "VALUES(?,?,?,?)",
                        ("event-body-failure", "synthetic", "{}", "now"),
                    )
                    raise RuntimeError("synthetic body failure")
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            terminal = [
                row
                for row in rows
                if row["event"] == "cps.write.commit"
                and row.get("db_operation") == "body_failure"
            ]
            self.assertEqual(len(terminal), 1)
            self.assertEqual(terminal[0]["status"], "error")
            self.assertEqual(terminal[0]["reason"], "body_failed")
            self.assertEqual(terminal[0]["rows_written"], 0)

    def test_commit_failure_reports_zero_rows_written(self) -> None:
        """A failed COMMIT is not a durable write even if SQLite changed rows."""

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-cps-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root / "profile",
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="cps-commit-failure",
            )
            profiler.start()
            store = CPSStore(root / "cps.sqlite3", profiler=profiler)
            with patch.object(
                store,
                "_commit_write",
                side_effect=sqlite3.OperationalError("synthetic commit failure"),
            ):
                with self.assertRaisesRegex(sqlite3.OperationalError, "synthetic commit failure"):
                    with store._write_transaction("commit_failure") as db:
                        db.execute(
                            "INSERT INTO events(event_id,event_type,payload,created_at) "
                            "VALUES(?,?,?,?)",
                            ("event-commit-failure", "synthetic", "{}", "now"),
                        )
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            terminal = [
                row
                for row in rows
                if row["event"] == "cps.write.commit"
                and row.get("db_operation") == "commit_failure"
            ]
            self.assertEqual(len(terminal), 1)
            self.assertEqual(terminal[0]["status"], "error")
            self.assertEqual(terminal[0]["reason"], "commit_failed")
            self.assertEqual(terminal[0]["rows_written"], 0)


if __name__ == "__main__":
    unittest.main()
