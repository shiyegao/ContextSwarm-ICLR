from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from contextswarm_mini.cps import CPSStore
from contextswarm_mini.selection_store import SelectionStore


ROOT = Path(__file__).resolve().parents[1]


class _RecordingProfiler:
    """Small in-memory sink used to observe the profiling call boundary."""

    enabled = True

    def __init__(self, tracker: dict[str, object]) -> None:
        self.tracker = tracker
        self.events: list[tuple[str, dict[str, object]]] = []

    def emit(self, event: str, **fields: object) -> None:
        active = self.tracker.get("active")
        in_transaction = False
        if active is not None:
            in_transaction = bool(getattr(active, "in_transaction", False))
        self.events.append((event, dict(fields)))
        self.tracker.setdefault("event_in_transaction", {})[event] = in_transaction


class _DisabledProfiler:
    enabled = False

    def emit(self, event: str, **fields: object) -> None:
        raise AssertionError(f"disabled profiler unexpectedly emitted {event}: {fields}")


class _TrackingConnection:
    """Delegate a sqlite connection while exposing its transaction state."""

    def __init__(self, connection: sqlite3.Connection, tracker: dict[str, object]) -> None:
        self._connection = connection
        self._tracker = tracker
        tracker["active"] = self

    @property
    def in_transaction(self) -> bool:
        return bool(self._connection.in_transaction)

    @property
    def total_changes(self) -> int:
        return int(self._connection.total_changes)

    def execute(self, statement: str, *parameters: object):
        result = self._connection.execute(statement, *parameters)
        return result

    def executescript(self, script: str):
        return self._connection.executescript(script)

    def close(self) -> None:
        try:
            self._connection.close()
        finally:
            if self._tracker.get("active") is self:
                self._tracker["active"] = None


def _track_connections(store: object, tracker: dict[str, object]) -> None:
    original_connect = store._connect

    def tracked_connect(*, operation: str = "generic"):
        connection = original_connect(operation=operation)
        return _TrackingConnection(connection, tracker)

    store._connect = tracked_connect


class ProfileIoLockBoundaryTests(unittest.TestCase):
    def test_selection_lock_and_readback_events_are_emitted_after_close(self) -> None:
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            tracker: dict[str, object] = {"event_in_transaction": {}}
            profiler = _RecordingProfiler(tracker)
            store = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            _track_connections(store, tracker)

            with store._write("lock_boundary") as db:
                db.execute("SELECT 1").fetchone()

            config = store.register_selector_config(
                selector_name="lock-boundary", config={"seed": 1}
            )
            store.record_search(
                request_key="lock-boundary",
                task_id="task",
                actor_id="actor",
                selector_config_id=config["selector_config_id"],
                query={"text": "q"},
                comparison_identity="comparison",
                snapshot_identity="snapshot",
                pool_identity="pool",
                rankings=[{"trace_id": "trace", "rank": 1, "selected": True}],
            )

            states = tracker["event_in_transaction"]
            self.assertFalse(states["selection.persist.lock"])
            self.assertFalse(states["selection.persist.end"])
            self.assertFalse(states["selection.persist.readback.query"])
            self.assertFalse(states["selection.persist.readback"])

            names = [event for event, _ in profiler.events]
            self.assertLess(names.index("selection.persist.lock"), names.index("selection.persist.end"))

    def test_cps_lock_event_is_emitted_after_commit_or_rollback(self) -> None:
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            tracker: dict[str, object] = {"event_in_transaction": {}}
            profiler = _RecordingProfiler(tracker)
            store = CPSStore(root / "cps.sqlite3", profiler=profiler)
            _track_connections(store, tracker)

            with store._write_transaction("lock_boundary") as db:
                db.execute(
                    "INSERT INTO events(event_id,event_type,payload,created_at) VALUES(?,?,?,?)",
                    ("lock-boundary", "synthetic", "{}", "now"),
                )

            states = tracker["event_in_transaction"]
            self.assertFalse(states["cps.write.lock"])
            self.assertFalse(states["cps.write.commit"])

            # The rollback path must retain a terminal event and the same
            # outside-lock guarantee.
            with self.assertRaisesRegex(RuntimeError, "rollback boundary"):
                with store._write_transaction("rollback_boundary"):
                    raise RuntimeError("rollback boundary")
            self.assertFalse(states["cps.write.lock"])
            self.assertFalse(states["cps.write.commit"])

    def test_disabled_writes_do_not_read_clock_or_emit_profile_events(self) -> None:
        profiler = _DisabledProfiler()
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            with patch(
                "contextswarm_mini.cps.time.monotonic",
                side_effect=AssertionError("disabled clock"),
            ), patch(
                "contextswarm_mini.selection_store.time.monotonic",
                side_effect=AssertionError("disabled clock"),
            ):
                selection = SelectionStore(root / "selection.sqlite3", profiler=profiler)
                cps = CPSStore(root / "cps.sqlite3", profiler=profiler)
                with selection._write("disabled") as db:
                    db.execute("SELECT 1").fetchone()
                with cps._write_transaction("disabled") as db:
                    db.execute("SELECT 1").fetchone()


if __name__ == "__main__":
    unittest.main()
