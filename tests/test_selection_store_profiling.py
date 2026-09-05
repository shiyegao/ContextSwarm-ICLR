from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from contextswarm_mini.profiling import PROFILE_FILENAME, RunProfiler
from contextswarm_mini.config import SelectionConfig
from contextswarm_mini.cps import CPSStore
from contextswarm_mini.selection_runtime import SelectionRuntime
from contextswarm_mini.selection_store import SelectionStore


ROOT = Path(__file__).resolve().parents[1]


class SelectionStoreProfilingTests(unittest.TestCase):
    """Failure and phase contracts for opt-in SelectionStore diagnostics."""

    @staticmethod
    def _rows(path: Path) -> list[dict[str, object]]:
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def _profiler(self, root: Path, run_id: str) -> RunProfiler:
        profiler = RunProfiler(
            root / "profile",
            enabled=True,
            heartbeat_interval_seconds=60,
            run_id=run_id,
        )
        profiler.start()
        return profiler

    def test_selection_low_level_events_inherit_search_scope(self) -> None:
        """Every low-level selection stage must join one logical attempt."""

        config = SelectionConfig(
            enabled=True,
            selector_name="random",
            selector_version="figure3_v1",
            visibility="project_shared",
            trace_slot_limit=4,
            context_token_budget=4096,
            tokenizer="unicode_word_v1",
            seed=11,
            tie_break="trace_id_asc",
            policy_params={"sample_without_replacement": True},
            direct_messages=False,
            candidate_transfer=False,
        )
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-selection-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = self._profiler(root, "selection-scope-run")
            cps = CPSStore(root / "cps.sqlite3", profiler=profiler)
            cps.create_piece(
                task_id="task-scope",
                author="source",
                kind="note",
                title="scope trace",
                body="selection attribution",
            )
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            runtime = SelectionRuntime(
                cps,
                store,
                config,
                run_id="selection-scope-run",
                profiler=profiler,
            )
            result = runtime.search(
                task_id="task-scope",
                actor_id="actor-scope",
                episode=7,
                query="scope",
                request_key="scope-request",
            )
            self.assertTrue(result["ok"])
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            expected_events = {
                "selection.eligible.read",
                "selection.eligible.filter",
                "selection.eligible.query_terms",
                "selection.eligible.materialize",
                "trace.project.query",
                "trace.project.read",
                "selection.snapshot",
                "selection.persist.readback",
                "selection.persist.readback.query",
                "selection.sqlite.connect",
            }
            scoped = {
                row["event"]
                for row in rows
                if row.get("task_id") == "task-scope"
                and row.get("actor_id") == "actor-scope"
                and row.get("episode") == 7
            }
            self.assertTrue(expected_events.issubset(scoped), (expected_events - scoped))

            projection_queries = [
                row
                for row in rows
                if row["event"] == "trace.project.query"
                and row.get("task_id") == "task-scope"
            ]
            self.assertTrue(projection_queries)
            self.assertTrue(
                all(
                    isinstance(row.get("projection_call_index"), int)
                    and isinstance(row.get("trace_set_sha256"), str)
                    for row in projection_queries
                )
            )

    def test_connection_failure_has_one_terminal_persist_event_and_no_queue_leak(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-selection-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = self._profiler(root, "connect-failure")
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            with patch.object(
                store,
                "_connect",
                side_effect=sqlite3.OperationalError("synthetic connect failure"),
            ):
                with self.assertRaises(sqlite3.OperationalError):
                    with store._write("connect_failure"):
                        self.fail("connection failure should prevent transaction body")
            self.assertEqual(store._write_waiters, 0)
            self.assertEqual(store._write_active, 0)
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            ends = [
                row
                for row in rows
                if row["event"] == "selection.persist.end"
                and row.get("operation") == "connect_failure"
            ]
            self.assertEqual(len(ends), 1)
            self.assertEqual(ends[0]["status"], "error")
            self.assertEqual(ends[0]["error_kind"], "OperationalError")
            self.assertEqual(ends[0]["lock_queue_depth"], 0)

    def test_begin_failure_has_terminal_event_and_releases_waiter(self) -> None:
        class BeginFailConnection:
            total_changes = 0
            in_transaction = False

            def execute(self, statement: str, *parameters: object) -> object:
                del parameters
                if statement == "BEGIN IMMEDIATE":
                    raise sqlite3.OperationalError("synthetic begin failure")
                raise AssertionError(f"unexpected statement: {statement}")

        @contextmanager
        def fake_db(*, operation: str = "generic"):
            del operation
            yield BeginFailConnection()

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-selection-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = self._profiler(root, "begin-failure")
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            with patch.object(store, "_db", side_effect=fake_db):
                with self.assertRaises(sqlite3.OperationalError):
                    with store._write("begin_failure"):
                        self.fail("BEGIN failure should prevent transaction body")
            self.assertEqual(store._write_waiters, 0)
            self.assertEqual(store._write_active, 0)
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            ends = [
                row
                for row in rows
                if row["event"] == "selection.persist.end"
                and row.get("operation") == "begin_failure"
            ]
            self.assertEqual(len(ends), 1)
            self.assertEqual(ends[0]["status"], "error")
            self.assertEqual(ends[0]["error_kind"], "OperationalError")
            self.assertGreaterEqual(float(ends[0]["lock_wait_seconds"]), 0.0)
            self.assertEqual(ends[0]["rows_written"], 0)

    def test_body_failure_closes_lock_episode_and_does_not_report_committed_rows(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-selection-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = self._profiler(root, "body-failure")
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            with self.assertRaisesRegex(RuntimeError, "synthetic body failure"):
                with store._write("body_failure") as db:
                    # The statement is deliberately valid; the raised error
                    # exercises rollback after the writer lock is acquired.
                    db.execute("SELECT 1")
                    raise RuntimeError("synthetic body failure")
            self.assertEqual(store._write_waiters, 0)
            self.assertEqual(store._write_active, 0)
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            ends = [
                row
                for row in rows
                if row["event"] == "selection.persist.end"
                and row.get("operation") == "body_failure"
            ]
            self.assertEqual(len(ends), 1)
            self.assertEqual(ends[0]["status"], "error")
            self.assertEqual(ends[0]["error_kind"], "RuntimeError")
            self.assertEqual(ends[0]["rows_written"], 0)
            self.assertGreaterEqual(float(ends[0]["lock_hold_seconds"]), 0.0)

    def test_commit_failure_has_terminal_event_after_rollback(self) -> None:
        class CommitFailConnection:
            def __init__(self) -> None:
                self._db = sqlite3.connect(":memory:", isolation_level=None)

            @property
            def total_changes(self) -> int:
                return int(self._db.total_changes)

            @property
            def in_transaction(self) -> bool:
                return bool(self._db.in_transaction)

            def execute(self, statement: str, *parameters: object) -> object:
                if statement == "COMMIT":
                    raise sqlite3.OperationalError("synthetic commit failure")
                return self._db.execute(statement, *parameters)

            def close(self) -> None:
                self._db.close()

        @contextmanager
        def fake_db(*, operation: str = "generic"):
            del operation
            connection = CommitFailConnection()
            try:
                yield connection
            finally:
                connection.close()

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-selection-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = self._profiler(root, "commit-failure")
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            with patch.object(store, "_db", side_effect=fake_db):
                with self.assertRaises(sqlite3.OperationalError):
                    with store._write("commit_failure") as db:
                        db.execute("CREATE TABLE t (value INTEGER)")
                        db.execute("INSERT INTO t(value) VALUES (1)")
            self.assertEqual(store._write_waiters, 0)
            self.assertEqual(store._write_active, 0)
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            ends = [
                row
                for row in rows
                if row["event"] == "selection.persist.end"
                and row.get("operation") == "commit_failure"
            ]
            self.assertEqual(len(ends), 1)
            self.assertEqual(ends[0]["status"], "error")
            self.assertEqual(ends[0]["error_kind"], "OperationalError")
            self.assertEqual(ends[0]["rows_written"], 0)

    def test_read_snapshot_labels_deferred_begin_separately_from_lock_wait(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-selection-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = self._profiler(root, "read-semantics")
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            store.summary()
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            summary = next(
                row
                for row in rows
                if row["event"] == "selection.read.end"
                and row.get("operation") == "summary"
            )
            self.assertEqual(summary["read_mode"], "deferred_wal")
            self.assertEqual(summary["read_lock_wait_seconds"], 0)
            self.assertEqual(summary["lock_wait_seconds"], 0)
            self.assertGreaterEqual(
                float(summary["read_transaction_seconds"]),
                float(summary["read_scope_seconds"]),
            )
            # ``begin_seconds`` is intentionally a separate field.  Older
            # profilers may drop it until the schema allow-list is upgraded;
            # when present it must not be reported as lock contention.
            if "begin_seconds" in summary:
                self.assertGreaterEqual(float(summary["begin_seconds"]), 0.0)

    def test_record_search_has_distinct_call_and_transaction_events(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-selection-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = self._profiler(root, "persist-layers")
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            config = store.register_selector_config(
                selector_name="profiling", config={"seed": 1}
            )
            store.record_search(
                request_key="persist-layers",
                task_id="task",
                actor_id="actor",
                selector_config_id=config["selector_config_id"],
                query={"text": "q"},
                comparison_identity="comparison",
                snapshot_identity="snapshot",
                pool_identity="pool",
                rankings=[{"trace_id": "trace", "rank": 1, "selected": True}],
            )
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            transaction_starts = [
                row
                for row in rows
                if row["event"] == "selection.persist.start"
                and row.get("operation") == "record_search"
            ]
            transaction_ends = [
                row
                for row in rows
                if row["event"] == "selection.persist.end"
                and row.get("operation") == "record_search"
            ]
            call_starts = [
                row
                for row in rows
                if row["event"] == "selection.persist.call.start"
            ]
            call_ends = [
                row
                for row in rows
                if row["event"] == "selection.persist.call.end"
            ]
            self.assertEqual(len(transaction_starts), 1)
            self.assertEqual(len(transaction_ends), 1)
            self.assertEqual(len(call_starts), 1)
            self.assertEqual(len(call_ends), 1)
            self.assertEqual(transaction_ends[0]["status"], "ok")
            self.assertEqual(call_ends[0]["status"], "ok")


if __name__ == "__main__":
    unittest.main()
