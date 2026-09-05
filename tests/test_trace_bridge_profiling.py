from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from contextswarm_mini.allocation_projection import TraceProjectionLimits
from contextswarm_mini.allocation_trace_bridge import (
    SelectionStoreTraceSource,
    TraceProjectionSnapshotPage,
    TraceProjectionBridge,
)
from contextswarm_mini.profiling import PROFILE_FILENAME, RunProfiler

ROOT = Path(__file__).resolve().parents[1]


_FEEDBACK_VALUES = {
    "useful": 1.0,
    "not_useful": -1.0,
    "misleading": -1.0,
    "stale": -1.0,
    "unsafe": -1.0,
    "duplicate": -1.0,
    "diagnostic_useful": 1.0,
    "needs_refinement": -1.0,
    "not_used": 0.0,
    "route_attempted": 0.0,
    "route_improving": 1.0,
}


class _SnapshotSource:
    def __init__(self) -> None:
        self.calls = 0

    def read_allocation_projection_snapshot(self, task_ids, *, as_of_watermark, cursor, limit):
        del task_ids, as_of_watermark, cursor, limit
        self.calls += 1
        return TraceProjectionSnapshotPage(
            records=(
                {
                    "sequence": 1,
                    "record_id": "record-1",
                    "task_id": "task-a",
                    "kind": "piece_snapshot",
                    "trace_id": "trace-1",
                    "lineage_id": "lineage-1",
                    "active": True,
                },
            ),
            trace_watermark="trace-cut-1",
            source_watermark="source-cut-1",
            snapshot_id="snapshot-1",
        )


class _ReorderedSnapshotSource:
    """Return the same normalized trace set in the caller's task order."""

    def read_allocation_projection_snapshot(self, task_ids, *, as_of_watermark, cursor, limit):
        del as_of_watermark, cursor, limit
        records = tuple(
            {
                "sequence": index,
                "record_id": f"record-{task_id}",
                "task_id": task_id,
                "kind": "piece_snapshot",
                "trace_id": f"trace-{task_id}",
                "lineage_id": f"lineage-{task_id}",
                "active": True,
            }
            for index, task_id in enumerate(task_ids, start=1)
        )
        return TraceProjectionSnapshotPage(
            records=records,
            trace_watermark="trace-cut-reordered",
        )


class _ReplaySnapshotSource:
    """Replay one page exactly before returning its terminal page."""

    def read_allocation_projection_snapshot(self, task_ids, *, as_of_watermark, cursor, limit):
        del task_ids, as_of_watermark, limit
        record = {
            "sequence": 1,
            "record_id": "replayed-record",
            "task_id": "task-a",
            "kind": "frontier",
            "lineage_id": "lineage-a",
        }
        if not cursor:
            return TraceProjectionSnapshotPage(
                records=(record,),
                trace_watermark="trace-cut-replay",
                next_cursor="retry",
                complete=False,
            )
        return TraceProjectionSnapshotPage(
            records=(record,),
            trace_watermark="trace-cut-replay",
            complete=True,
        )


class TraceBridgeProfilingTests(unittest.TestCase):
    def _rows(self, path: Path) -> list[dict[str, object]]:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    def test_projection_materialize_hashes_and_reuse_are_separate(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-trace-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root / "profile",
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="trace-bridge-test",
            )
            profiler.start()
            source = _SnapshotSource()
            bridge = TraceProjectionBridge(profiler=profiler)
            first = bridge.read(["task-a"], store=source)
            second = bridge.read(["task-a"], store=source)
            profiler.close()

            self.assertEqual(first.watermark, second.watermark)
            self.assertNotEqual(first.trace_watermark_sha256, "")
            self.assertNotEqual(first.source_snapshot_sha256, "")
            self.assertNotEqual(
                first.trace_watermark_sha256, first.source_snapshot_sha256
            )
            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            project_end = [row for row in rows if row["event"] == "trace.bridge.project.end"]
            materialize = [row for row in rows if row["event"] == "trace.bridge.materialize"]
            summaries = [row for row in rows if row["event"] == "trace.bridge.summary"]
            self.assertEqual(len(project_end), 2)
            self.assertEqual(len(materialize), 2)
            self.assertEqual(summaries[-1]["reuse_count"], 1)
            self.assertEqual(
                summaries[-1]["trace_watermark_sha256"], first.trace_watermark_sha256
            )
            self.assertEqual(
                summaries[-1]["source_snapshot_sha256"], first.source_snapshot_sha256
            )

    def test_sqlite_projection_reports_schema_and_data_queries(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-trace-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            db_path = root / "selection.sqlite3"
            db = sqlite3.connect(db_path)
            db.executescript(
                """
                CREATE TABLE search_events (search_event_id TEXT PRIMARY KEY, task_id TEXT, query_json TEXT);
                CREATE TABLE exposures (exposure_id TEXT PRIMARY KEY, search_event_id TEXT, actor_id TEXT);
                CREATE TABLE exposure_items (exposure_item_id TEXT PRIMARY KEY, exposure_id TEXT, trace_id TEXT);
                CREATE TABLE feedback_events (
                    feedback_event_id TEXT PRIMARY KEY, exposure_item_id TEXT, trace_id TEXT,
                    actor_id TEXT, event_class TEXT, feedback_kind TEXT, terminal INTEGER,
                    effective INTEGER, payload_json TEXT
                );
                INSERT INTO search_events VALUES ('search-1', 'task-a', '{}');
                INSERT INTO exposures VALUES ('exposure-1', 'search-1', 'worker-1');
                INSERT INTO exposure_items VALUES ('item-1', 'exposure-1', 'trace-1');
                """
            )
            db.commit()
            db.close()
            profiler = RunProfiler(
                root / "profile",
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="trace-sqlite-test",
            )
            profiler.start()
            source = SelectionStoreTraceSource(
                db_path,
                feedback_values=_FEEDBACK_VALUES,
                profiler=profiler,
            )
            records, _watermark = source.read_complete_records(["task-a"])
            self.assertEqual(len(records), 1)
            profiler.close()
            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            queries = [row for row in rows if row["event"] == "trace.bridge.sqlite.query"]
            aggregate = next(row for row in rows if row["event"] == "trace.bridge.sqlite")
            self.assertEqual([row["query_name"] for row in queries], ["schema", "exposure", "feedback"])
            self.assertEqual([row["query_index"] for row in queries], [1, 2, 3])
            self.assertEqual(aggregate["query_count"], 3)
            self.assertEqual(aggregate["read_lock_wait_seconds"], 0)
            self.assertIn("begin_seconds", aggregate)

    def test_reuse_uses_normalized_trace_set_and_reports_call_index(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-trace-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root / "profile",
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="trace-bridge-reuse-test",
            )
            profiler.start()
            bridge = TraceProjectionBridge(profiler=profiler)
            bridge.read(["task-a", "task-b"], store=_ReorderedSnapshotSource())
            bridge.read(["task-b", "task-a"], store=_ReorderedSnapshotSource())
            profiler.close()

            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            summaries = [row for row in rows if row["event"] == "trace.bridge.summary"]
            self.assertEqual([row["projection_call_index"] for row in summaries], [1, 2])
            self.assertEqual([row["projection_calls"] for row in summaries], [1, 2])
            self.assertEqual(
                summaries[0]["trace_set_sha256"], summaries[1]["trace_set_sha256"]
            )
            self.assertEqual(summaries[1]["reuse_count"], 1)
            # Projection output identity is also order-independent; it is kept
            # separate from the trace-set key used for reuse accounting.
            self.assertEqual(
                summaries[0]["projection_snapshot_sha256"],
                summaries[1]["projection_snapshot_sha256"],
            )

    def test_exact_replayed_snapshot_page_does_not_consume_record_bound(self) -> None:
        view = TraceProjectionBridge(
            limits=TraceProjectionLimits(max_records=1)
        ).read(["task-a"], store=_ReplaySnapshotSource())
        self.assertEqual(view.source, "selection_store_snapshot")
        self.assertEqual(view.for_task("task-a").frontier_count, 1)

    def test_profiling_off_bridge_reads_without_profiling_clock(self) -> None:
        class DisabledProfiler:
            enabled = False

        with patch(
            "contextswarm_mini.allocation_trace_bridge.time.monotonic",
            side_effect=AssertionError("profiling clock used while disabled"),
        ):
            view = TraceProjectionBridge(profiler=DisabledProfiler()).read(
                ["task-a"], store=_SnapshotSource()
            )
        self.assertEqual(view.source, "selection_store_snapshot")

    def test_public_zero_reason_is_bounded(self) -> None:
        private = "/private/selection.sqlite3: SQL details"
        view = TraceProjectionBridge().zero(["task-a"], reason=private)
        self.assertEqual(view.fallback_reason, "trace_store_unavailable")
        self.assertNotIn("private", repr(view))
        self.assertNotIn("SQL", view.watermark)


if __name__ == "__main__":
    unittest.main()
