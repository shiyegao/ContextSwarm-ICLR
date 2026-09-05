from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout

from scripts.audit_profiling import (
    PROFILE_SCHEMA_VERSION,
    TARGETS,
    audit_profiling,
    main,
)


ROOT = Path(__file__).resolve().parents[1]


def _row(sequence: int, event: str, **fields: object) -> dict[str, object]:
    return {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "sequence": sequence,
        "at": "2026-08-29T00:00:00+00:00",
        "monotonic_ns": sequence,
        "event": event,
        # Keep ordinary fixtures attributable to one sanitized scope.  Tests
        # that exercise missing/split correlation explicitly override these
        # fields with ``None`` or a second scope.
        "run_id": "run-a",
        "task_id": "task-a",
        "actor_id": "actor-a",
        "episode": 1,
        **fields,
    }


def _write_profile(root: Path, rows: list[dict[str, object]], *, metadata: dict[str, object] | None = None) -> Path:
    profile = root / "profiling.jsonl"
    with profile.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    if metadata is not None:
        (root / "run_meta.json").write_text(json.dumps(metadata), encoding="utf-8")
    return profile


def _clean_rows(*, mode: str = "mono", policy: str = "uniform", selection: bool = False, maximum: int = 1) -> list[dict[str, object]]:
    events: list[tuple[str, dict[str, object]]] = [
        ("profile.start", {}),
        (
            "run.configuration",
            {
                "mode": mode,
                "allocation_policy": policy,
                "selection_enabled": selection,
                "max_parallel": maximum,
            },
        ),
        ("run.start", {}),
        ("attempt.lifecycle.start", {"component": "runner_wrapper"}),
        # The supervision boundary and the concrete agent invocation are
        # separate spans.  Both are required for a real agent-vs-wrapper
        # baseline; a lifecycle summary alone must not satisfy the contract.
        ("attempt.agent.invoke.start", {}),
        ("attempt.agent.invoke.end", {"status": "ok", "elapsed_seconds": 0.2}),
        ("agent.start", {"component": "pi_agent_wrapper"}),
        ("agent.end", {"status": "ok", "elapsed_seconds": 0.2}),
        ("attempt.lifecycle.end", {"status": "ok", "wall_seconds": 0.3}),
        # A complete agent-vs-wrapper baseline needs both the logical agent
        # lifecycle and at least one process/resource observation.  Keep the
        # fixture representative of the contract so conjunction coverage does
        # not accidentally regress to a "one event is enough" check.
        ("resource.process", {"pid": 4242, "process_tree_count": 1, "rss_bytes": 100}),
        # ``resource.sample`` is the run-wide aggregate.  Real aggregate rows
        # intentionally carry no task/actor/episode attribution; the audit
        # must keep them auxiliary rather than joining them to an attempt.
        (
            "resource.sample",
            {
                "run_id": None,
                "task_id": None,
                "actor_id": None,
                "episode": None,
                "process_tree_count": 1,
                "rss_bytes": 100,
            },
        ),
        ("judge.execute.start", {}),
        ("judge.execute.end", {"status": "ok", "elapsed_seconds": 0.1}),
        ("judge.receipt", {"status": "PROVED", "score": 1}),
        ("drain.start", {}),
        ("drain.end", {"drained": True}),
        ("run.end", {"status": "ok"}),
        ("profile.end", {"elapsed_seconds": 0.5}),
    ]
    return [_row(index, event, **fields) for index, (event, fields) in enumerate(events, 1)]


class ProfilingAuditTests(unittest.TestCase):
    def test_clean_profile_has_all_targets_and_terminal_span_checks(self) -> None:
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            root = Path(temporary)
            profile = _write_profile(root, _clean_rows())
            report = audit_profiling(profile, run_meta={"runtime_provenance": {"image": "pinned"}})

        self.assertEqual(report["exit_code"], 0)
        self.assertTrue(report["ok"])
        self.assertEqual(set(report["coverage"]), set(TARGETS))
        self.assertEqual(report["coverage"]["agent_wrapper"], "present")
        self.assertEqual(report["coverage"]["selection"], "not_applicable")
        self.assertEqual(report["coverage"]["trace_projection"], "not_applicable")
        self.assertEqual(report["coverage"]["max_parallel"], "not_applicable")
        self.assertEqual(report["coverage"]["cps"], "not_applicable")
        self.assertEqual(report["coverage"]["judge"], "present")
        self.assertEqual(report["coverage"]["record_search_lock"], "not_applicable")
        self.assertEqual(report["coverage_status"]["record_search_lock"], "not_applicable")
        self.assertTrue(report["profile"]["sequence"]["valid"])
        self.assertTrue(report["profile"]["spans"]["valid"])
        self.assertTrue(report["profile"]["termination"]["valid"])
        self.assertEqual(report["dropped_fields"]["total"], 0)
        self.assertEqual(report["sensitive_fields"]["total"], 0)
        self.assertEqual(report["coverage_status"]["agent_wrapper"], "covered")
        agent_detail = report["coverage_detail"]["agent_wrapper"]
        self.assertTrue(agent_detail["goal_complete"])
        self.assertEqual(agent_detail["auxiliary_presence"]["resource.sample"], True)
        self.assertEqual(agent_detail["correlation"]["state"], "proven")

    def test_record_search_lock_requires_exact_lifecycle_and_scope(self) -> None:
        rows = _clean_rows(selection=True)
        # Keep the broader selection chain intentionally incomplete: this
        # test is specifically about the independent record_search writer
        # contract and must not pass merely because selection.summary exists.
        rows[3:3] = [
            _row(4, "selection.persist.start", operation="record_search"),
            _row(
                5,
                "selection.persist.lock",
                operation="record_search",
                status="acquired",
                lock_wait_seconds=0.01,
            ),
            _row(
                6,
                "selection.persist.end",
                operation="record_search",
                status="ok",
                lock_wait_seconds=0.01,
                lock_hold_seconds=0.02,
                transaction_seconds=0.02,
                rows_written=1,
            ),
        ]
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)

        detail = report["coverage_detail"]["record_search_lock"]
        self.assertEqual(report["coverage"]["record_search_lock"], "present")
        self.assertEqual(detail["required_families"], [
            "selection.persist.start",
            "selection.persist.lock",
            "selection.persist.end",
        ])
        self.assertTrue(detail["goal_complete"])
        self.assertEqual(detail["correlation"]["state"], "proven")

        # A lock marker without the full four-dimensional scope cannot be
        # joined to the endpoints.  The global event count remains complete,
        # but the target must become partial and fail the clean gate.
        broken = [dict(row) for row in rows]
        for row in broken:
            if row["event"] == "selection.persist.lock":
                row["episode"] = None
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), broken)
            broken_report = audit_profiling(profile)
        broken_detail = broken_report["coverage_detail"]["record_search_lock"]
        self.assertEqual(broken_report["coverage"]["record_search_lock"], "partial")
        self.assertFalse(broken_detail["goal_complete"])
        self.assertNotEqual(broken_detail["correlation"]["state"], "proven")

    def test_record_search_lock_without_writer_is_conditional(self) -> None:
        rows = _clean_rows(selection=True)
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)
        self.assertEqual(report["coverage"]["record_search_lock"], "conditional_missing")
        self.assertEqual(report["coverage_status"]["record_search_lock"], "conditional_missing")
        self.assertEqual(report["coverage_detail"]["record_search_lock"]["correlation"]["state"], "not_observed")
        self.assertEqual(report["exit_code"], 1)

    def test_record_search_lock_ignores_other_selection_write_operations(self) -> None:
        # The store reuses selection.persist.* for configuration and feedback
        # writes.  Those rows must not satisfy the dedicated record_search
        # target, even when all three lifecycle markers and a complete scope
        # are present.
        rows = _clean_rows(selection=True)
        rows[3:3] = [
            _row(4, "selection.persist.start", operation="register_selector_config"),
            _row(
                5,
                "selection.persist.lock",
                operation="register_selector_config",
                status="acquired",
            ),
            _row(
                6,
                "selection.persist.end",
                operation="register_selector_config",
                status="ok",
            ),
        ]
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)

        detail = report["coverage_detail"]["record_search_lock"]
        self.assertEqual(report["event_counts"]["selection.persist.start"], 1)
        self.assertEqual(report["coverage"]["record_search_lock"], "conditional_missing")
        self.assertFalse(detail["goal_complete"])
        self.assertEqual(detail["plumbing_presence"]["families"]["selection.persist.start"], False)
        self.assertEqual(detail["correlation"]["state"], "not_observed")
        # The shared persist lifecycle is also excluded from the generic
        # correlation diagnostics for the dedicated target; otherwise a
        # startup/config write could look like lock evidence in the top-level
        # report even though coverage correctly rejects it.
        self.assertFalse(
            any(key.endswith(":record_search_lock") for key in report["correlation"]["missing_dimensions"])
        )
        self.assertFalse(
            any(key.startswith("record_search_lock:") for key in report["correlation"]["unattributed_stage_rows"])
        )

        # Mixing one non-record marker into an otherwise complete transaction
        # must still fail: the operation predicate is applied per event, not
        # just to the first/outer lifecycle row.
        mixed = [dict(row) for row in rows]
        for row in mixed:
            if row["event"] == "selection.persist.lock":
                row["operation"] = "record_search"
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), mixed)
            mixed_report = audit_profiling(profile)
        mixed_detail = mixed_report["coverage_detail"]["record_search_lock"]
        self.assertEqual(mixed_report["coverage"]["record_search_lock"], "partial")
        self.assertFalse(mixed_detail["goal_complete"])
        self.assertEqual(mixed_detail["correlation"]["state"], "missing_required")

    def test_dropped_fields_and_unclosed_span_are_reported(self) -> None:
        rows = _clean_rows()
        rows.insert(
            -1,
            _row(
                len(rows),
                "selection.rank.start",
                dropped_fields=3,
            ),
        )
        # Re-number deliberately so the only defect here is the missing end.
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)

        self.assertEqual(report["exit_code"], 1)
        self.assertFalse(report["ok"])
        self.assertEqual(report["dropped_fields"]["total"], 3)
        self.assertEqual(report["dropped_fields"]["rows"], 1)
        self.assertGreaterEqual(report["profile"]["spans"]["open"], 1)
        self.assertFalse(report["profile"]["spans"]["valid"])
        issue_codes = {item["code"] for item in report["issues"]}
        self.assertIn("dropped_fields", issue_codes)
        self.assertIn("span_missing_end", issue_codes)

    def test_agent_wrapper_accepts_concrete_wrapper_variant_but_rejects_partial(self) -> None:
        rows = _clean_rows()
        # The mock/specialized runner may expose attempt.wrapper spans without
        # the recovery-only attempt.lifecycle envelope.  That variant still
        # satisfies the wrapper side of the target when resource observations
        # are present.
        rows = [row for row in rows if not str(row["event"]).startswith("attempt.lifecycle")]
        rows.insert(5, _row(6, "attempt.wrapper.dispatch.start"))
        rows.insert(6, _row(7, "attempt.wrapper.dispatch.end", status="ok"))
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)
        self.assertEqual(report["coverage"]["agent_wrapper"], "present")
        self.assertEqual(report["coverage_detail"]["agent_wrapper"]["missing_required_any_families"], [])

        # The aggregate sample is not an attempt requirement.  Removing the
        # attributed process-tree row, however, must make the goal partial.
        incomplete = [row for row in rows if row["event"] != "resource.process"]
        for index, row in enumerate(incomplete, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), incomplete)
            partial_report = audit_profiling(profile)
        self.assertEqual(partial_report["coverage"]["agent_wrapper"], "partial")
        self.assertEqual(partial_report["exit_code"], 1)
        self.assertTrue(
            partial_report["coverage_detail"]["agent_wrapper"]["auxiliary_presence"]["resource.sample"]
        )

    def test_trace_policy_makes_cps_progress_absence_conditional(self) -> None:
        rows = _clean_rows(mode="cps", policy="trace_state", selection=True, maximum=4)
        # Selection/trace bridge/CPS search are observed, but the ordinary
        # progress_snapshot branch is intentionally not emitted.
        rows[3:3] = [
            _row(4, "selection.runtime.start"),
            _row(5, "selection.runtime.end", status="ok"),
            # Trace-aware allocation uses the bridge projection chain.  The
            # query/page/project/materialize/summary conjunction below is
            # intentionally complete so only CPS is conditional in this arm.
            _row(6, "trace.bridge.page", page_count=1),
            _row(7, "trace.bridge.project.start"),
            _row(8, "trace.bridge.project.end", status="ok"),
            _row(9, "trace.bridge.materialize", rows=2),
            _row(10, "trace.bridge.sqlite.query", rows_scanned=2, query_seconds=0.01),
            _row(11, "trace.bridge.summary", page_count=1),
            _row(12, "cps.search.query", rows_scanned=2, query_seconds=0.01),
            _row(13, "resource.sample", rss_bytes=100, process_tree_count=2),
            _row(14, "attempt.admitted", active_slots=1),
            _row(15, "attempt.solver_slot_released", remaining_slots=3),
        ]
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)

        self.assertEqual(report["configuration"]["policy"], "trace_state")
        self.assertEqual(report["coverage"]["trace_projection"], "present")
        self.assertEqual(report["coverage"]["cps"], "conditional_missing")
        self.assertEqual(report["coverage_detail"]["cps"]["status"], "conditional_missing")
        self.assertNotIn("trace_progress_exclusive_violation", {item["code"] for item in report["issues"]})

    def test_uniform_cps_without_progress_is_required_missing(self) -> None:
        rows = _clean_rows(mode="cps", policy="uniform", selection=True, maximum=2)
        rows.insert(3, _row(4, "selection.snapshot", snapshot_count=1))
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)
        self.assertEqual(report["coverage"]["cps"], "missing")
        self.assertEqual(report["coverage_status"]["cps"], "missing_required")
        self.assertEqual(report["exit_code"], 1)

    def test_split_attempt_scopes_do_not_union_to_complete_agent_goal(self) -> None:
        # The global event set contains every required agent stage, but the
        # first attempt owns invoke/agent and the second owns resources.  A
        # run-level union would be a false ``present``; correlation must keep
        # the two canonical scopes separate.
        rows = [
            _row(1, "profile.start"),
            _row(2, "run.configuration", mode="mono", allocation_policy="uniform", selection_enabled=False, max_parallel=1),
            _row(3, "run.start"),
            _row(4, "attempt.lifecycle.start"),
            _row(5, "attempt.agent.invoke.start"),
            _row(6, "attempt.agent.invoke.end", status="ok"),
            _row(7, "agent.start"),
            _row(8, "agent.end", status="ok"),
            _row(9, "attempt.lifecycle.end", status="ok"),
            _row(10, "attempt.lifecycle.start", actor_id="actor-b"),
            _row(11, "resource.process", actor_id="actor-b", pid=4243, process_tree_count=1),
            _row(12, "resource.sample", actor_id="actor-b", process_tree_count=1),
            _row(13, "attempt.lifecycle.end", actor_id="actor-b", status="ok"),
            _row(14, "run.end", status="ok"),
            _row(15, "profile.end", elapsed_seconds=1.0),
        ]
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile, run_meta={"runtime_provenance": {"image": "pinned"}, "dry_run": True})

        detail = report["coverage_detail"]["agent_wrapper"]
        self.assertEqual(report["coverage"]["agent_wrapper"], "partial")
        self.assertFalse(detail["goal_complete"])
        self.assertEqual(detail["correlation"]["state"], "split_scope")
        self.assertEqual(detail["correlation"]["complete_scope_count"], 0)
        self.assertIn("correlation_split_scope", {item["code"] for item in report["issues"]})

    def test_same_scope_retry_attempts_are_not_combined(self) -> None:
        # Two retries reuse the same task/actor/episode.  Each contributes a
        # different half of the stage set; repeated lifecycle boundaries mark
        # the canonical scope as split_attempt rather than complete.
        rows = [
            _row(1, "profile.start"),
            _row(2, "run.configuration", mode="mono", allocation_policy="uniform", selection_enabled=False, max_parallel=1),
            _row(3, "run.start"),
            _row(4, "attempt.lifecycle.start"),
            _row(5, "attempt.agent.invoke.start"),
            _row(6, "attempt.agent.invoke.end", status="error"),
            _row(7, "agent.start"),
            _row(8, "agent.end", status="error"),
            _row(9, "attempt.lifecycle.end", status="error"),
            _row(10, "attempt.lifecycle.start"),
            _row(11, "resource.process", pid=4242, process_tree_count=1),
            _row(12, "resource.sample", process_tree_count=1),
            _row(13, "attempt.lifecycle.end", status="ok"),
            _row(14, "run.end", status="ok"),
            _row(15, "profile.end", elapsed_seconds=1.0),
        ]
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile, run_meta={"runtime_provenance": {"image": "pinned"}, "dry_run": True})

        correlation = report["coverage_detail"]["agent_wrapper"]["correlation"]
        self.assertEqual(report["coverage"]["agent_wrapper"], "partial")
        self.assertEqual(correlation["state"], "split_attempt")
        self.assertFalse(correlation["complete"])

    def test_missing_terminal_is_a_correlation_failure(self) -> None:
        rows = [row for row in _clean_rows() if row["event"] != "attempt.lifecycle.end"]
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile, run_meta={"runtime_provenance": {"image": "pinned"}, "dry_run": True})

        correlation = report["coverage_detail"]["agent_wrapper"]["correlation"]
        self.assertEqual(correlation["state"], "missing_terminal")
        self.assertFalse(report["coverage_detail"]["agent_wrapper"]["goal_complete"])
        self.assertIn("correlation_missing_terminal", {item["code"] for item in report["issues"]})

    def test_duplicate_replay_cannot_complete_scope(self) -> None:
        rows = _clean_rows()
        duplicate = next(row for row in rows if row["event"] == "agent.start").copy()
        rows.insert(6, duplicate)
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile, run_meta={"runtime_provenance": {"image": "pinned"}, "dry_run": True})

        correlation = report["coverage_detail"]["agent_wrapper"]["correlation"]
        self.assertEqual(correlation["state"], "duplicate_replay")
        self.assertGreaterEqual(report["correlation"]["duplicate_replay_count"], 1)
        self.assertFalse(correlation["complete"])

    def test_cps_start_end_from_different_runs_are_not_joined(self) -> None:
        rows = [
            _row(1, "profile.start", run_id="run-a"),
            _row(2, "run.configuration", run_id="run-a", mode="cps", allocation_policy="uniform", selection_enabled=False, max_parallel=2),
            _row(3, "run.start", run_id="run-a"),
            _row(4, "cps.progress.start", run_id="run-a"),
            _row(5, "cps.progress.query", run_id="run-a", rows_scanned=1),
            _row(6, "cps.progress.materialize", run_id="run-a", materialized_rows=1),
            _row(7, "cps.progress.summary", run_id="run-a", rows_scanned=1),
            _row(8, "cps.sqlite.connect", run_id="run-a", connect_seconds=0.01),
            # This endpoint would make the old global prefix check look
            # complete, but the run-level keyed pair must reject it.
            _row(9, "cps.progress.end", run_id="run-b", status="ok"),
            _row(10, "run.end", run_id="run-b", status="ok"),
            _row(11, "profile.end", run_id="run-b", elapsed_seconds=1.0),
        ]
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile, run_meta={"runtime_provenance": {"image": "pinned"}})

        correlation = report["coverage_detail"]["cps"]["correlation"]
        self.assertEqual(correlation["state"], "cross_run")
        self.assertFalse(correlation["complete"])
        self.assertEqual(report["coverage"]["cps"], "partial")
        self.assertIn("correlation_cross_run", {item["code"] for item in report["issues"]})

    def test_backpressure_events_are_allowlisted_and_backlog_limit_is_numeric(self) -> None:
        rows = _clean_rows()
        rows[9:9] = [
            _row(10, "judge.queue.wait", backlog_limit=7),
            _row(11, "judge.queue.expired", backlog_limit=7),
        ]
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)

        issue_codes = {item["code"] for item in report["issues"]}
        self.assertNotIn("unknown_event", issue_codes)
        self.assertEqual(report["event_counts"]["judge.queue.wait"], 1)
        self.assertEqual(report["event_counts"]["judge.queue.expired"], 1)
        self.assertEqual(
            report["aggregates"]["numeric"]["backlog_limit"]["count"],
            2,
        )
        self.assertEqual(report["profile"]["field_presence"]["backlog_limit"], 2)

    def test_sensitive_values_never_cross_report_boundary(self) -> None:
        rows = _clean_rows()
        rows.insert(
            3,
            _row(
                4,
                "security.check",
                prompt="PRIVATE_PROMPT_VALUE",
                candidate="PRIVATE_CANDIDATE_VALUE",
                endpoint="https://user:password@example.invalid/private",
                path="/private/secret/file",
            ),
        )
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)
        serialized = json.dumps(report, sort_keys=True)
        for value in (
            "PRIVATE_PROMPT_VALUE",
            "PRIVATE_CANDIDATE_VALUE",
            "example.invalid",
            "/private/secret/file",
            # Correlation keys are hashed internally and must not cross the
            # report boundary either.
            "run-a",
            "task-a",
            "actor-a",
        ):
            self.assertNotIn(value, serialized)
        self.assertGreater(report["sensitive_fields"]["total"], 0)
        self.assertIn("prompt", report["sensitive_fields"]["categories"])
        self.assertEqual(report["exit_code"], 1)

    def test_bounded_candidate_row_metric_is_not_sensitive(self) -> None:
        """Numeric preparation counters must not trip candidate-content checks."""

        rows = _clean_rows()
        rows.insert(
            3,
            _row(
                4,
                "selection.persist.end",
                operation="register_selector_config",
                status="ok",
                prepare_candidate_rows=0,
                prepare_ranking_rows=0,
                prepare_rows=0,
            ),
        )
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)

        self.assertEqual(report["sensitive_fields"]["total"], 0)
        self.assertNotIn("sensitive_field", {item["code"] for item in report["issues"]})

    def test_unknown_event_label_is_collapsed_without_echoing_label(self) -> None:
        rows = _clean_rows()
        # The wire grammar accepts this label, but it is not part of the
        # reviewed profiling event allowlist.  It must be counted only in the
        # bounded ``other`` bucket; retaining the original component would
        # let an attacker smuggle an identifier into the audit report.
        rows.insert(3, _row(4, "selection.private_blob", wall_seconds=0.01))
        for index, row in enumerate(rows, 1):
            row["sequence"] = index
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            profile = _write_profile(Path(temporary), rows)
            report = audit_profiling(profile)

        serialized = json.dumps(report, sort_keys=True)
        self.assertNotIn("selection.private_blob", serialized)
        self.assertEqual(report["event_counts"].get("other"), 1)
        self.assertEqual(report["counts"].get("other"), 1)
        issue_codes = {item["code"] for item in report["issues"]}
        self.assertIn("unknown_event", issue_codes)
        self.assertEqual(report["exit_code"], 1)

    def test_bad_json_has_input_exit_code_two_without_path_echo(self) -> None:
        with tempfile.TemporaryDirectory(prefix="audit-profile-", dir=ROOT) as temporary:
            root = Path(temporary)
            profile = root / "profiling.jsonl"
            profile.write_text("{not-json}\n", encoding="utf-8")
            report = audit_profiling(profile)
            output = io.StringIO()
            with redirect_stdout(output):
                code = main([str(profile), "--format", "text"])
        self.assertEqual(report["exit_code"], 2)
        self.assertEqual(code, 2)
        self.assertIn("profile audit", output.getvalue())
        self.assertNotIn(str(profile), output.getvalue())

    def test_source_uses_line_iterator_not_splitlines_buffering(self) -> None:
        source = (ROOT / "scripts" / "audit_profiling.py").read_text(encoding="utf-8")
        self.assertNotIn("read_text(encoding=\"utf-8\").splitlines()", source)
        self.assertIn("for line_number, line in enumerate(handle, 1)", source)


if __name__ == "__main__":
    unittest.main()
