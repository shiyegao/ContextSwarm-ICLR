from __future__ import annotations

import json
import hashlib
from pathlib import Path
import tempfile
import unittest

from contextswarm_mini.selection_artifacts import (
    ArtifactValidationError,
    collect_paired_trace_metrics,
    reconstruct_selection_chains,
    summarize_selection_store_export,
    validate_selection_store_export,
)
from contextswarm_mini.selection_store import SelectionStore


class TraceSelectionArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = SelectionStore(self.root / "selection.sqlite3")
        self.config = self.store.register_selector_config(
            selector_name="nustigmergy",
            config={"selector_version": "figure3_v1", "score_precision": 6},
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _populate(self, *, comparison: str = "comparison", trace: str = "trace-1") -> Path:
        search = self.store.record_search(
            request_key="search-1",
            task_id="task-1",
            actor_id="worker-1",
            selector_config_id=self.config["selector_config_id"],
            query={"text": "lemma", "episode": 2},
            comparison_identity=comparison,
            snapshot_identity={"watermark": 9},
            pool_identity={"traces": [trace]},
            rankings=[
                {
                    "trace_id": trace,
                    "rank": 1,
                    "selected": True,
                    "component_scores": {"relevance": 0.8, "interaction": 0.2},
                    "payload": {"token_count": 4, "total_score": 1.0},
                },
                {
                    "trace_id": "trace-dropped",
                    "rank": 2,
                    "selected": False,
                    "component_scores": {},
                    "payload": {"drop_reason": "token_budget"},
                },
            ],
        )
        self.store.record_feedback(
            request_key="feedback-progress",
            exposure_item_id=search["items"][0]["exposure_item_id"],
            actor_id="worker-1",
            trace_id=trace,
            feedback_kind="route_attempted",
            origin="worker",
            terminal=False,
            payload={"value": 0},
        )
        self.store.record_feedback(
            request_key="feedback-terminal",
            exposure_item_id=search["items"][0]["exposure_item_id"],
            actor_id="worker-1",
            trace_id=trace,
            feedback_kind="useful",
            origin="worker",
            payload={"value": 1},
        )
        self.store.record_feedback(
            request_key="feedback-conflict",
            exposure_item_id=search["items"][0]["exposure_item_id"],
            actor_id="worker-1",
            trace_id=trace,
            feedback_kind="stale",
            origin="worker",
            payload={"value": -1},
        )
        self.store.record_verifier_evidence(
            request_key="evidence-1",
            trace_id=trace,
            verifier_id="judge",
            status="verified",
            evidence={"receipt": "r1"},
            task_id="task-1",
        )
        self.store.record_maintenance_event(
            request_key="maintenance-1",
            trace_id=trace,
            actor_id="runner",
            maintenance_kind="reviewed",
            payload={"reason": "audit"},
        )
        self.store.record_relation(
            request_key="relation-1",
            source_trace_id=trace,
            target_trace_id="trace-dropped",
            relation_kind="supports",
            actor_id="runner",
        )
        path = self.root / "selection_events.jsonl"
        self.store.export_jsonl(path)
        return path

    @staticmethod
    def _rows(path: Path) -> list[dict]:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    def test_valid_export_reconstructs_complete_trace_chain(self) -> None:
        path = self._populate()
        summary = validate_selection_store_export(path)
        self.assertEqual(summary["counts"]["search_events"], 1)
        self.assertEqual(summary["counts"]["exposure_items"], 1)
        self.assertEqual(summary["counts"]["effective_feedback_events"], 1)
        self.assertEqual(summary["counts"]["conflicting_terminal_feedback_events"], 1)
        self.assertEqual(summary["comparison_contract_ids"], [summary["comparison_contract_ids"][0]])
        self.assertEqual(summary["usage"]["delivered_trace_context_tokens"], 4)
        chains = reconstruct_selection_chains(path)
        self.assertEqual(len(chains), 1)
        chain = chains[0]
        self.assertEqual(chain["search_event"]["selector_config_id"], self.config["selector_config_id"])
        self.assertEqual(chain["ranking"]["trace_id"], "trace-1")
        self.assertEqual(chain["exposure_item"]["trace_id"], "trace-1")
        self.assertEqual(chain["effective_terminal_feedback"]["feedback_kind"], "useful")
        self.assertEqual(len(chain["feedback_events"]), 3)

    def test_validator_rejects_orphan_and_actor_or_selected_mismatch(self) -> None:
        original = self._rows(self._populate())
        rows = json.loads(json.dumps(original))
        exposure = next(row for row in rows if row["record_type"] == "exposure")
        exposure["record"]["actor_id"] = "other-worker"
        with self.assertRaisesRegex(ArtifactValidationError, "exposure actor"):
            validate_selection_store_export(rows)

        rows = json.loads(json.dumps(original))
        item = next(row for row in rows if row["record_type"] == "exposure_item")
        item["record"]["trace_id"] = "trace-dropped"
        with self.assertRaisesRegex(ArtifactValidationError, "trace/rank"):
            validate_selection_store_export(rows)

    def test_validator_rejects_duplicate_effective_terminal_and_bad_conflict(self) -> None:
        original = self._rows(self._populate())
        rows = json.loads(json.dumps(original))
        feedback = [row for row in rows if row["record_type"] == "feedback_event"]
        winner = next(row for row in feedback if row["record"]["effective"])
        duplicate = json.loads(json.dumps(winner))
        duplicate["record"]["feedback_event_id"] = "feedback_event_" + "z" * 64
        duplicate["record"]["request_key"] = "feedback-duplicate"
        insert_at = next(index for index, row in enumerate(rows) if row["record_type"] == "verifier_evidence")
        rows.insert(insert_at, duplicate)
        with self.assertRaisesRegex(ArtifactValidationError, "multiple effective"):
            validate_selection_store_export(rows)

        rows = json.loads(json.dumps(original))
        conflict = next(row for row in rows if row["record_type"] == "feedback_event" and row["record"]["request_key"] == "feedback-conflict")
        conflict["record"]["conflicts_with_feedback_event_id"] = "missing-winner"
        with self.assertRaisesRegex(ArtifactValidationError, "missing event"):
            validate_selection_store_export(rows)

    def test_summary_accepts_rows_and_paired_trace_metrics_fail_closed(self) -> None:
        left_path = self._populate(comparison="fixed-contract", trace="left-trace")
        left_rows = self._rows(left_path)
        left_summary = summarize_selection_store_export(left_rows)
        self.assertEqual(left_summary, validate_selection_store_export(left_rows))

        other = SelectionStore(self.root / "right.sqlite3")
        right_config = other.register_selector_config(
            selector_name="recency", config={"selector_version": "figure3_v1"}
        )
        right_search = other.record_search(
            request_key="search-1",
            task_id="task-1",
            actor_id="worker-1",
            selector_config_id=right_config["selector_config_id"],
            query={"text": "lemma"},
            comparison_identity="fixed-contract",
            snapshot_identity="same",
            pool_identity="same",
            rankings=[{"trace_id": "right-trace", "rank": 1, "selected": True, "payload": {"token_count": 2}}],
        )
        right_path = self.root / "right.jsonl"
        other.export_jsonl(right_path)
        report = collect_paired_trace_metrics(left_path, right_path)
        self.assertEqual(report["comparison_contract_ids"], left_summary["comparison_contract_ids"])
        self.assertEqual(report["differences"]["delivered_trace_context_tokens"], 2)
        self.assertEqual(report["differences"]["feedback_event_count"], 3)

        rows = self._rows(right_path)
        search = next(row for row in rows if row["record_type"] == "search_event")
        search["record"]["comparison_sha256"] = "f" * 64
        with self.assertRaisesRegex(ArtifactValidationError, "comparison"):
            collect_paired_trace_metrics(left_path, rows)

    def test_optional_eligible_pool_records_are_validated_and_counted(self) -> None:
        rows = self._rows(self._populate())
        search = next(row for row in rows if row["record_type"] == "search_event")
        watermarks = {"commit_seq": 4}
        search["record"]["snapshot_watermarks"] = watermarks
        search["record"]["snapshot_watermarks_sha256"] = hashlib.sha256(
            json.dumps(watermarks, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        ranking = next(row for row in rows if row["record_type"] == "search_ranking" and row["record"]["trace_id"] == "trace-1")
        candidate = {
            "schema": rows[0]["schema"],
            "record_type": "search_candidate",
            "record": {
                "search_candidate_id": "search_candidate_" + "1" * 64,
                "search_event_id": search["record"]["search_event_id"],
                "trace_id": "trace-1",
                "pool_order": 1,
                "candidate_payload": {
                    "trace_id": "trace-1",
                    "title": "lemma",
                    "body": "proof",
                    "feedback": {"exposure_count": 1},
                },
                "feedback_snapshot": {"exposure_count": 1},
                "snapshot_watermarks": {"commit_seq": 4},
            },
        }
        candidate2 = json.loads(json.dumps(candidate))
        candidate2["record"]["search_candidate_id"] = "search_candidate_" + "2" * 64
        candidate2["record"]["trace_id"] = "trace-dropped"
        candidate2["record"]["pool_order"] = 2
        candidate2["record"]["candidate_payload"]["trace_id"] = "trace-dropped"
        for row in (candidate, candidate2):
            row["record"]["candidate_sha256"] = hashlib.sha256(
                json.dumps(
                    row["record"]["candidate_payload"], sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
        search["record"]["eligible_candidates_sha256"] = hashlib.sha256(
            json.dumps(
                [candidate["record"]["candidate_payload"], candidate2["record"]["candidate_payload"]],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        insert_at = next(index for index, row in enumerate(rows) if row["record_type"] == "search_ranking")
        rows.insert(insert_at, candidate)
        rows.insert(insert_at + 1, candidate2)
        summary = validate_selection_store_export(rows)
        self.assertEqual(summary["record_type_counts"]["search_candidate"], 2)
        self.assertEqual(summary["usage"]["eligible_candidate_count"], 2)

    def test_unicode_candidate_payload_hash_matches_selection_store(self) -> None:
        config = self.store.register_selector_config(
            selector_name="unicode", config={"label": "证明"}
        )
        search = self.store.record_search(
            request_key="unicode-search",
            task_id="task-1",
            actor_id="worker-1",
            selector_config_id=config["selector_config_id"],
            query={"text": "证明"},
            comparison_identity={"合同": "固定"},
            snapshot_identity={"水位": 1},
            pool_identity={"池": "甲"},
            eligible_candidates=[
                {"trace_id": "unicode-trace", "title": "引理", "body": "证明"}
            ],
            snapshot_watermarks={"提交": 1},
            rankings=[
                {
                    "trace_id": "unicode-trace",
                    "rank": 1,
                    "selected": True,
                    "payload": {"token_count": 2},
                }
            ],
        )
        self.assertEqual(search["candidates"][0]["candidate_payload"]["title"], "引理")
        path = self.root / "unicode.jsonl"
        self.store.export_jsonl(path)
        validate_selection_store_export(path)

    def test_validator_rejects_non_json_numeric_values(self) -> None:
        rows = self._rows(self._populate())
        search = next(row for row in rows if row["record_type"] == "search_event")
        search["record"]["query"] = {"score": float("nan")}
        with self.assertRaisesRegex(ArtifactValidationError, "canonical JSON"):
            validate_selection_store_export(rows)

    def test_validator_accepts_legacy_candidate_watermark_and_rejects_mismatch(self) -> None:
        """New exports omit child watermarks; old exports remain auditable."""

        search = self.store.record_search(
            request_key="pool-legacy-export",
            task_id="task-1",
            actor_id="worker-1",
            selector_config_id=self.config["selector_config_id"],
            query={"text": "lemma"},
            comparison_identity="comparison",
            snapshot_identity={"watermark": 4},
            pool_identity={"traces": ["pool-a", "pool-b"]},
            eligible_candidates=[
                {"trace_id": "pool-a", "title": "a", "body": "proof a"},
                {"trace_id": "pool-b", "title": "b", "body": "proof b"},
            ],
            snapshot_watermarks={"commit_seq": 4},
            rankings=[
                {"trace_id": "pool-a", "rank": 1, "selected": True},
                {"trace_id": "pool-b", "rank": 2, "selected": False},
            ],
        )
        path = self.root / "pool-export.jsonl"
        self.store.export_jsonl(path)
        rows = self._rows(path)
        candidates = [row for row in rows if row["record_type"] == "search_candidate"]
        self.assertEqual(len(candidates), 2)
        self.assertTrue(all("snapshot_watermarks" not in row["record"] for row in candidates))
        validate_selection_store_export(rows)

        parent = next(row for row in rows if row["record_type"] == "search_event")["record"]
        watermark = parent["snapshot_watermarks"]
        for row in candidates:
            row["record"]["snapshot_watermarks"] = dict(watermark)
        validate_selection_store_export(rows)

        partial_parent = json.loads(json.dumps(rows))
        partial_search = next(
            row for row in partial_parent if row["record_type"] == "search_event"
        )
        del partial_search["record"]["snapshot_watermarks_sha256"]
        with self.assertRaisesRegex(ArtifactValidationError, "pool snapshot fields must be complete"):
            validate_selection_store_export(partial_parent)

        partial_empty_parent = json.loads(json.dumps(rows))
        partial_empty_search = next(
            row for row in partial_empty_parent if row["record_type"] == "search_event"
        )
        partial_empty_search["record"]["snapshot_watermarks"] = {}
        del partial_empty_search["record"]["snapshot_watermarks_sha256"]
        del partial_empty_search["record"]["eligible_candidates_sha256"]
        with self.assertRaisesRegex(ArtifactValidationError, "pool snapshot fields must be complete"):
            validate_selection_store_export(partial_empty_parent)

        del candidates[0]["record"]["snapshot_watermarks"]
        with self.assertRaisesRegex(ArtifactValidationError, "all present or all absent"):
            validate_selection_store_export(rows)
        candidates[0]["record"]["snapshot_watermarks"] = dict(watermark)
        candidates[0]["record"]["snapshot_watermarks"] = {"commit_seq": 999}
        with self.assertRaisesRegex(ArtifactValidationError, "watermark mismatch"):
            validate_selection_store_export(rows)


if __name__ == "__main__":
    unittest.main()
