from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from threading import Barrier
import unittest

from contextswarm_mini.selection_artifacts import (
    reconstruct_selection_chains,
    validate_selection_store_export,
)
from contextswarm_mini.selection_store import (
    RequestKeyConflictError,
    SelectionStore,
)


class SelectionStoreCandidatePoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.path = self.root / "selection.sqlite3"
        self.store = SelectionStore(self.path)
        self.config = self.store.register_selector_config(
            selector_name="nustigmergy",
            config={"selector_version": "figure3_v1", "kappa": 1.0},
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _candidate(trace_id: str, *, body: str, exposure_count: int) -> dict:
        return {
            "trace_id": trace_id,
            "source_task_id": "source-task",
            "task_family": "formal",
            "author_id": "worker-source",
            "scope_key": "project_shared",
            "visibility": "project_shared",
            "kind": "knowledge",
            "title": f"title {trace_id}",
            "body": body,
            "tags": ["lemma", trace_id],
            "created_at": "2026-08-23T00:00:00Z",
            "commit_seq": 17 if trace_id == "trace-z" else 11,
            "lifecycle": "active",
            "cluster_id": f"cluster-{trace_id}",
            "content_sha256": hashlib.sha256(body.encode()).hexdigest(),
            "token_count": 9,
            "evidence": {"verifier_count": 2.0},
            "relations": {"supports": 1},
            "relevance": 0.75,
            "evidence_score": 2.0,
            "structure_score": 1.0,
            "state_score": 1.0,
            "lineage_id": "source-task",
            "feedback": {
                "exposure_count": exposure_count,
                "effective_terminal_count": 1,
                "kind_counts": {"useful": 1},
                "signed_weight_sum": 1.0,
                "positive_count": 1,
                "negative_count": 0,
            },
        }

    def _kwargs(self, request_key: str = "search-with-pool") -> dict:
        candidates = [
            self._candidate("trace-z", body="zeta proof", exposure_count=3),
            self._candidate("trace-a", body="alpha proof", exposure_count=1),
        ]
        return {
            "request_key": request_key,
            "task_id": "task-1",
            "actor_id": "worker-1",
            "selector_config_id": self.config["selector_config_id"],
            "query": {"text": "lemma", "search_ordinal": 4},
            "comparison_identity": {"contract": "fixed"},
            "snapshot_identity": {"snapshot": 17},
            "pool_identity": {"eligible": ["trace-a", "trace-z"]},
            "rankings": [
                {
                    "trace_id": "trace-z",
                    "rank": 2,
                    "selected": False,
                    "component_scores": {"interaction": 0.25},
                },
                {
                    "trace_id": "trace-a",
                    "rank": 1,
                    "selected": True,
                    "component_scores": {"interaction": 0.5},
                    "payload": {"token_count": 9, "total_score": 1.5},
                },
            ],
            # Deliberately reverse the input order.  The store owns canonical
            # pool order, independent of Python/CPS read ordering.
            "eligible_candidates": candidates,
            "snapshot_watermarks": {
                "cps": {"pieces_rowid": 17},
                "selection": {
                    "exposure_item_rowid": 3,
                    "feedback_event_rowid": 2,
                },
            },
        }

    def _legacy_child_rows(self, request_key: str) -> dict:
        """Create a modern selection and restore the pre-dedup child column."""

        first = self.store.record_search(**self._kwargs(request_key))
        db = sqlite3.connect(self.path)
        try:
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                (
                    json.dumps(
                        self._kwargs(request_key)["snapshot_watermarks"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            db.commit()
        finally:
            db.close()
        return first

    def test_pool_rows_are_stable_complete_and_restart_idempotent(self) -> None:
        first = self.store.record_search(**self._kwargs())
        self.assertFalse(first["idempotent"])
        self.assertEqual(
            [row["trace_id"] for row in first["candidates"]],
            ["trace-a", "trace-z"],
        )
        self.assertEqual(
            [row["pool_order"] for row in first["candidates"]], [1, 2]
        )
        candidate = first["candidates"][0]
        self.assertEqual(candidate["candidate_payload"]["body"], "alpha proof")
        self.assertEqual(candidate["candidate_payload"]["relevance"], 0.75)
        self.assertEqual(candidate["candidate_payload"]["lineage_id"], "source-task")
        self.assertEqual(candidate["feedback_snapshot"]["exposure_count"], 1)
        self.assertEqual(
            candidate["snapshot_watermarks"], self._kwargs()["snapshot_watermarks"]
        )
        self.assertEqual(
            first["search_event"]["snapshot_watermarks"],
            self._kwargs()["snapshot_watermarks"],
        )
        for name in (
            "eligible_candidates_sha256",
            "snapshot_watermarks_sha256",
        ):
            self.assertRegex(first["search_event"][name], r"^[0-9a-f]{64}$")

        candidate_ids = [row["search_candidate_id"] for row in first["candidates"]]
        reopened = SelectionStore(self.path)
        retry_kwargs = self._kwargs()
        retry_kwargs["eligible_candidates"] = list(
            reversed(retry_kwargs["eligible_candidates"])
        )
        retry = reopened.record_search(**retry_kwargs)
        self.assertTrue(retry["idempotent"])
        self.assertEqual(
            [row["search_candidate_id"] for row in retry["candidates"]],
            candidate_ids,
        )

    def test_candidate_watermark_projection_is_independent_per_row(self) -> None:
        """Derived compatibility fields retain the old mutable-object shape."""

        chain = self.store.record_search(**self._kwargs("projection-copy"))
        parent = chain["search_event"]["snapshot_watermarks"]
        first = chain["candidates"][0]["snapshot_watermarks"]
        second = chain["candidates"][1]["snapshot_watermarks"]
        self.assertEqual(first, parent)
        self.assertEqual(second, parent)
        self.assertIsNot(first, parent)
        self.assertIsNot(second, parent)
        self.assertIsNot(first["selection"], second["selection"])
        first["selection"]["feedback_event_rowid"] = 999
        self.assertEqual(parent["selection"]["feedback_event_rowid"], 2)
        self.assertEqual(second["selection"]["feedback_event_rowid"], 2)

    def test_summary_and_jsonl_export_include_replayable_pool(self) -> None:
        search = self.store.record_search(**self._kwargs())
        summary = self.store.summary()
        self.assertEqual(summary["counts"]["search_candidates"], 2)

        destination = self.root / "selection_events.jsonl"
        export = self.store.export_jsonl(destination)
        self.assertEqual(export["record_type_counts"]["search_candidate"], 2)
        rows = [
            json.loads(line)
            for line in destination.read_text(encoding="utf-8").splitlines()
        ]
        record_types = [row["record_type"] for row in rows]
        self.assertLess(
            record_types.index("search_event"),
            record_types.index("search_candidate"),
        )
        self.assertLess(
            record_types.index("search_candidate"),
            record_types.index("search_ranking"),
        )
        exported = [
            row["record"] for row in rows if row["record_type"] == "search_candidate"
        ]
        self.assertEqual(
            {row["search_candidate_id"] for row in exported},
            {row["search_candidate_id"] for row in search["candidates"]},
        )
        self.assertTrue(all("candidate_payload" in row for row in exported))
        self.assertTrue(all("feedback_snapshot" in row for row in exported))
        # The immutable watermark is now emitted once on the parent
        # search_event.  Legacy exports may still carry a candidate copy, but
        # fresh exports omit the redundant field.
        self.assertTrue(all("snapshot_watermarks" not in row for row in exported))
        parent = next(
            row["record"]
            for row in rows
            if row["record_type"] == "search_event"
            and row["record"]["search_event_id"] == search["search_event"]["search_event_id"]
        )
        self.assertEqual(parent["snapshot_watermarks"], self._kwargs()["snapshot_watermarks"])

    def test_request_key_conflicts_on_pool_payload_feedback_or_watermark(self) -> None:
        self.store.record_search(**self._kwargs())
        for field in ("body", "feedback", "watermark"):
            with self.subTest(field=field):
                changed = self._kwargs()
                if field == "body":
                    changed["eligible_candidates"][0]["body"] = "different proof"
                elif field == "feedback":
                    changed["eligible_candidates"][0]["feedback"][
                        "exposure_count"
                    ] = 99
                else:
                    changed["snapshot_watermarks"]["selection"][
                        "feedback_event_rowid"
                    ] = 99
                with self.assertRaises(RequestKeyConflictError) as raised:
                    self.store.record_search(**changed)
                expected = (
                    "snapshot_watermarks"
                    if field == "watermark"
                    else "eligible_candidates"
                )
                self.assertIn(expected, raised.exception.mismatched_fields)

    def test_pool_contract_rejects_partial_or_unreplayable_inputs(self) -> None:
        missing_watermark = self._kwargs("missing-watermark")
        missing_watermark.pop("snapshot_watermarks")
        with self.assertRaisesRegex(ValueError, "supplied together"):
            self.store.record_search(**missing_watermark)

        missing_candidate = self._kwargs("missing-candidate")
        missing_candidate["eligible_candidates"] = [
            missing_candidate["eligible_candidates"][0]
        ]
        with self.assertRaisesRegex(ValueError, "absent from eligible_candidates"):
            self.store.record_search(**missing_candidate)

        duplicate = self._kwargs("duplicate-candidate")
        duplicate["eligible_candidates"].append(
            dict(duplicate["eligible_candidates"][0])
        )
        with self.assertRaisesRegex(ValueError, "must be unique"):
            self.store.record_search(**duplicate)

    def test_reopen_migrates_legacy_candidate_watermarks_and_preserves_replay(self) -> None:
        """An old v1 table is rewritten without losing its parent watermark."""

        first = self.store.record_search(**self._kwargs("legacy-reopen"))
        watermark = self._kwargs("legacy-reopen")["snapshot_watermarks"]
        canonical = json.dumps(watermark, sort_keys=True, separators=(",", ":"))
        db = sqlite3.connect(self.path)
        try:
            # Recreate the pre-dedup physical layout and pre-pool parent
            # columns.  The next SelectionStore opener must add the parent
            # columns, verify the child copies, then drop only the redundant
            # child column.
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                (canonical,),
            )
            db.execute("ALTER TABLE search_events DROP COLUMN snapshot_watermarks_json")
            db.execute("ALTER TABLE search_events DROP COLUMN snapshot_watermarks_sha256")
            db.commit()
        finally:
            db.close()

        reopened = SelectionStore(self.path)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            self.assertNotIn("snapshot_watermarks_json", columns)
            foreign_keys = db.execute("PRAGMA foreign_key_list(search_candidates)").fetchall()
            self.assertTrue(
                any(row[2] == "search_events" and row[6] == "CASCADE" for row in foreign_keys)
            )
            indexes = {
                row[1] for row in db.execute("PRAGMA index_list(search_candidates)")
            }
            self.assertIn("search_candidates_trace", indexes)
            parent = db.execute(
                """SELECT snapshot_watermarks_json, snapshot_watermarks_sha256
                     FROM search_events WHERE search_event_id = ?""",
                (first["search_event"]["search_event_id"],),
            ).fetchone()
        finally:
            db.close()
        self.assertEqual(json.loads(parent[0]), watermark)
        self.assertRegex(parent[1], r"^[0-9a-f]{64}$")
        replay = reopened.record_search(**self._kwargs("legacy-reopen"))
        self.assertTrue(replay["idempotent"])
        self.assertEqual(replay["search_event"]["snapshot_watermarks"], watermark)
        self.assertEqual(
            [row["snapshot_watermarks"] for row in replay["candidates"]],
            [watermark, watermark],
        )

    def test_reopen_rebuilds_missing_pool_digest_and_preserves_candidate_payloads(self) -> None:
        """Legacy rows with no parent pool hash remain exportable and replayable."""

        request_key = "legacy-missing-pool-hash"
        first = self.store.record_search(**self._kwargs(request_key))
        expected_candidates = [
            {
                "search_candidate_id": row["search_candidate_id"],
                "candidate_sha256": row["candidate_sha256"],
                "candidate_payload": row["candidate_payload"],
                "feedback_snapshot": row["feedback_snapshot"],
            }
            for row in first["candidates"]
        ]
        before_export = self.root / "legacy-missing-pool-hash.before.jsonl"
        self.store.export_jsonl(before_export)
        before_summary = validate_selection_store_export(before_export)
        before_chains = reconstruct_selection_chains(before_export)

        db = sqlite3.connect(self.path)
        try:
            # Recreate a pre-pool parent schema while retaining the legacy
            # per-candidate watermark copies.  The migration must add all
            # parent fields, reconstruct the pool digest from payloads, and
            # only then drop the redundant child column.
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                (
                    json.dumps(
                        self._kwargs()["snapshot_watermarks"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            for column in (
                "eligible_candidates_sha256",
                "snapshot_watermarks_sha256",
                "snapshot_watermarks_json",
            ):
                db.execute(f"ALTER TABLE search_events DROP COLUMN {column}")
            db.commit()
        finally:
            db.close()

        reopened = SelectionStore(self.path)
        after_export = self.root / "legacy-missing-pool-hash.after.jsonl"
        reopened.export_jsonl(after_export)
        after_summary = validate_selection_store_export(after_export)
        after_chains = reconstruct_selection_chains(after_export)
        self.assertEqual(after_summary, before_summary)
        self.assertEqual(after_chains, before_chains)

        replay = reopened.record_search(**self._kwargs(request_key))
        self.assertTrue(replay["idempotent"])
        actual_candidates = [
            {
                "search_candidate_id": row["search_candidate_id"],
                "candidate_sha256": row["candidate_sha256"],
                "candidate_payload": row["candidate_payload"],
                "feedback_snapshot": row["feedback_snapshot"],
            }
            for row in replay["candidates"]
        ]
        self.assertEqual(actual_candidates, expected_candidates)

        db = sqlite3.connect(self.path)
        try:
            parent_hash = db.execute(
                "SELECT eligible_candidates_sha256 FROM search_events "
                "WHERE search_event_id = ?",
                (first["search_event"]["search_event_id"],),
            ).fetchone()[0]
            child_columns = {
                row[1]
                for row in db.execute("PRAGMA table_info(search_candidates)")
            }
        finally:
            db.close()
        self.assertEqual(parent_hash, first["search_event"]["eligible_candidates_sha256"])
        self.assertNotIn("snapshot_watermarks_json", child_columns)

    def test_reopen_migration_fails_closed_on_conflicting_legacy_watermarks(self) -> None:
        self.store.record_search(**self._kwargs("legacy-conflict"))
        db = sqlite3.connect(self.path)
        try:
            for column in (
                "eligible_candidates_sha256",
                "snapshot_watermarks_sha256",
                "snapshot_watermarks_json",
            ):
                db.execute(f"ALTER TABLE search_events DROP COLUMN {column}")
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                (json.dumps(self._kwargs()["snapshot_watermarks"], separators=(",", ":")),),
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ? "
                "WHERE pool_order = 2",
                (json.dumps({"selection": {"feedback_event_rowid": 999}}),),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaisesRegex(ValueError, "conflicting snapshot watermarks"):
            SelectionStore(self.path)
        # The failed migration leaves the legacy column and rows available for
        # repair/retry instead of silently discarding the contradictory copy.
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            event_columns = {
                row[1] for row in db.execute("PRAGMA table_info(search_events)")
            }
            self.assertIn("snapshot_watermarks_json", columns)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM search_candidates").fetchone()[0], 2
            )
        finally:
            db.close()
        self.assertTrue(
            {
                "eligible_candidates_sha256",
                "snapshot_watermarks_sha256",
                "snapshot_watermarks_json",
            }.isdisjoint(event_columns)
        )

    def test_reopen_preserves_pre_pool_legacy_rows(self) -> None:
        """Rows recorded before candidate-pool support remain replayable."""

        kwargs = self._kwargs("legacy-no-pool")
        kwargs.pop("eligible_candidates")
        kwargs.pop("snapshot_watermarks")
        first = self.store.record_search(**kwargs)
        db = sqlite3.connect(self.path)
        try:
            # Simulate the original pre-pool parent schema and an empty legacy
            # candidate table.  There is no child watermark to promote.
            db.execute("ALTER TABLE search_events DROP COLUMN snapshot_watermarks_json")
            db.execute("ALTER TABLE search_events DROP COLUMN snapshot_watermarks_sha256")
            db.execute("ALTER TABLE search_events DROP COLUMN eligible_candidates_sha256")
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.commit()
        finally:
            db.close()

        reopened = SelectionStore(self.path)
        chain = reopened.get_search(first["search_event"]["search_event_id"])
        self.assertIsNotNone(chain)
        assert chain is not None
        self.assertEqual(chain["candidates"], [])
        self.assertEqual(chain["search_event"]["snapshot_watermarks"], {})
        retry = reopened.record_search(**kwargs)
        self.assertTrue(retry["idempotent"])

    def test_concurrent_legacy_reopeners_do_not_race_schema_drop(self) -> None:
        """Only one opener performs DROP COLUMN; waiters re-check the schema."""

        self.store.record_search(**self._kwargs("legacy-concurrent"))
        db = sqlite3.connect(self.path)
        try:
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                (
                    json.dumps(
                        self._kwargs()["snapshot_watermarks"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            db.commit()
        finally:
            db.close()

        barrier = Barrier(8)

        def reopen(_: int) -> SelectionStore:
            barrier.wait()
            return SelectionStore(self.path)

        with ThreadPoolExecutor(max_workers=8) as pool:
            reopened = list(pool.map(reopen, range(8)))
        self.assertEqual(len(reopened), 8)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(search_candidates)")
            }
        finally:
            db.close()
        self.assertNotIn("snapshot_watermarks_json", columns)

    def test_concurrent_pre_pool_openers_do_not_race_parent_column_adds(self) -> None:
        """All parent ALTERs are serialized when reopening a pre-pool DB."""

        db = sqlite3.connect(self.path)
        try:
            for column in (
                "eligible_candidates_sha256",
                "snapshot_watermarks_sha256",
                "snapshot_watermarks_json",
            ):
                db.execute(f"ALTER TABLE search_events DROP COLUMN {column}")
            db.commit()
        finally:
            db.close()

        barrier = Barrier(16)

        def reopen(_: int) -> SelectionStore:
            barrier.wait()
            return SelectionStore(self.path)

        with ThreadPoolExecutor(max_workers=16) as pool:
            reopened = list(pool.map(reopen, range(16)))
        self.assertEqual(len(reopened), 16)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(search_events)")
            }
        finally:
            db.close()
        self.assertTrue(
            {
                "eligible_candidates_sha256",
                "snapshot_watermarks_sha256",
                "snapshot_watermarks_json",
            }.issubset(columns)
        )

    def test_reopen_migration_rejects_invalid_parent_digest(self) -> None:
        self.store.record_search(**self._kwargs("legacy-bad-digest"))
        db = sqlite3.connect(self.path)
        try:
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                (
                    json.dumps(
                        self._kwargs()["snapshot_watermarks"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            db.execute(
                "UPDATE search_events SET snapshot_watermarks_sha256 = ?",
                ("0" * 64,),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaisesRegex(ValueError, "parent digest is invalid"):
            SelectionStore(self.path)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(search_candidates)")
            }
        finally:
            db.close()
        self.assertIn("snapshot_watermarks_json", columns)

    def test_reopen_migration_rejects_malformed_child_watermark(self) -> None:
        self.store.record_search(**self._kwargs("legacy-bad-json"))
        db = sqlite3.connect(self.path)
        try:
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                ("{not-json",),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaisesRegex(ValueError, "invalid snapshot watermarks JSON"):
            SelectionStore(self.path)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(search_candidates)")
            }
        finally:
            db.close()
        self.assertIn("snapshot_watermarks_json", columns)

    def test_reopen_migration_rejects_parent_child_value_mismatch(self) -> None:
        self.store.record_search(**self._kwargs("legacy-parent-mismatch"))
        db = sqlite3.connect(self.path)
        try:
            db.execute(
                "ALTER TABLE search_candidates ADD COLUMN "
                "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
            )
            db.execute(
                "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                (
                    json.dumps(
                        self._kwargs()["snapshot_watermarks"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            parent = {"selection": {"feedback_event_rowid": 999}}
            db.execute(
                "UPDATE search_events SET snapshot_watermarks_json = ?, "
                "snapshot_watermarks_sha256 = ?",
                (
                    json.dumps(parent, sort_keys=True, separators=(",", ":")),
                    hashlib.sha256(
                        json.dumps(parent, sort_keys=True, separators=(",", ":")).encode()
                    ).hexdigest(),
                ),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaisesRegex(ValueError, "parent and candidate values differ"):
            SelectionStore(self.path)

    def test_reopen_migration_rejects_corrupt_candidate_metadata(self) -> None:
        """Dropping the watermark column must not hide bad candidate rows."""

        first = self._legacy_child_rows("legacy-corrupt-candidate-payload")
        db = sqlite3.connect(self.path)
        try:
            payload = json.loads(
                db.execute(
                    "SELECT candidate_payload_json FROM search_candidates "
                    "WHERE pool_order = 1"
                ).fetchone()[0]
            )
            payload["body"] = "tampered after persistence"
            db.execute(
                "UPDATE search_candidates SET candidate_payload_json = ? "
                "WHERE pool_order = 1",
                (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaisesRegex(ValueError, "candidate payload hash mismatch"):
            SelectionStore(self.path)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1]
                for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            self.assertIn("snapshot_watermarks_json", columns)
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM search_candidates").fetchone()[0],
                len(first["candidates"]),
            )
        finally:
            db.close()

    def test_reopen_migration_rejects_candidate_trace_or_feedback_mismatch(self) -> None:
        """Candidate metadata joins are validated before the legacy DROP."""

        for field in ("trace", "feedback"):
            with self.subTest(field=field):
                temporary = tempfile.TemporaryDirectory()
                try:
                    path = Path(temporary.name) / "selection.sqlite3"
                    store = SelectionStore(path)
                    config = store.register_selector_config(
                        selector_name="nustigmergy",
                        config={"selector_version": "figure3_v1", "kappa": 1.0},
                    )
                    kwargs = self._kwargs(f"legacy-corrupt-{field}")
                    kwargs["selector_config_id"] = config["selector_config_id"]
                    first = store.record_search(**kwargs)
                    db = sqlite3.connect(path)
                    try:
                        db.execute(
                            "ALTER TABLE search_candidates ADD COLUMN "
                            "snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}'"
                        )
                        db.execute(
                            "UPDATE search_candidates SET snapshot_watermarks_json = ?",
                            (
                                json.dumps(
                                    kwargs["snapshot_watermarks"],
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                            ),
                        )
                        if field == "trace":
                            payload = json.loads(
                                db.execute(
                                    "SELECT candidate_payload_json FROM search_candidates "
                                    "WHERE pool_order = 1"
                                ).fetchone()[0]
                            )
                            payload["trace_id"] = "unexpected-trace"
                            db.execute(
                                "UPDATE search_candidates SET candidate_payload_json = ?, "
                                "candidate_sha256 = ? WHERE pool_order = 1",
                                (
                                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                                    hashlib.sha256(
                                        json.dumps(
                                            payload,
                                            sort_keys=True,
                                            separators=(",", ":"),
                                        ).encode()
                                    ).hexdigest(),
                                ),
                            )
                        else:
                            db.execute(
                                "UPDATE search_candidates SET feedback_snapshot_json = ? "
                                "WHERE pool_order = 1",
                                ('{"exposure_count":999}',),
                            )
                        db.commit()
                    finally:
                        db.close()

                    expected = (
                        "candidate payload trace_id mismatch"
                        if field == "trace"
                        else "candidate feedback snapshot mismatch"
                    )
                    with self.assertRaisesRegex(ValueError, expected):
                        SelectionStore(path)
                    db = sqlite3.connect(path)
                    try:
                        columns = {
                            row[1]
                            for row in db.execute(
                                "PRAGMA table_info(search_candidates)"
                            )
                        }
                    finally:
                        db.close()
                    self.assertIn("snapshot_watermarks_json", columns)
                    self.assertEqual(len(first["candidates"]), 2)
                finally:
                    temporary.cleanup()

    def test_reopen_migration_rejects_parent_pool_digest_without_child_rows(self) -> None:
        """A non-empty pool identity must have replayable candidate rows."""

        first = self._legacy_child_rows("legacy-dangling-pool-digest")
        db = sqlite3.connect(self.path)
        try:
            db.execute("DELETE FROM search_candidates")
            db.commit()
        finally:
            db.close()

        with self.assertRaisesRegex(ValueError, "pool digest but no candidate rows"):
            SelectionStore(self.path)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1]
                for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            self.assertIn("snapshot_watermarks_json", columns)
            self.assertEqual(
                db.execute(
                    "SELECT eligible_candidates_sha256 FROM search_events "
                    "WHERE search_event_id = ?",
                    (first["search_event"]["search_event_id"],),
                ).fetchone()[0],
                first["search_event"]["eligible_candidates_sha256"],
            )
        finally:
            db.close()

    def test_reopen_migration_rejects_invalid_parent_pool_digest(self) -> None:
        """A stale parent pool digest must not be certified by child removal."""

        self._legacy_child_rows("legacy-invalid-pool-digest")
        db = sqlite3.connect(self.path)
        try:
            db.execute(
                "UPDATE search_events SET eligible_candidates_sha256 = ?",
                ("0" * 64,),
            )
            db.commit()
        finally:
            db.close()

        with self.assertRaisesRegex(ValueError, "parent digest is invalid"):
            SelectionStore(self.path)
        db = sqlite3.connect(self.path)
        try:
            columns = {
                row[1]
                for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            self.assertIn("snapshot_watermarks_json", columns)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
