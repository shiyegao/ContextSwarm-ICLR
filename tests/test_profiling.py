from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
import stat
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

from contextswarm_mini.config import load_config
from contextswarm_mini.cps import CPSStore
from contextswarm_mini.evaluator import candidate_sha256, task_contract_sha256
from contextswarm_mini.allocation_trace_bridge import TraceProjectionBridge
from contextswarm_mini.judge_broker import JudgeBroker
from contextswarm_mini.models import AgentResult, Task, Verdict
from contextswarm_mini.pi_agent import PiAgent
from contextswarm_mini.profiling import (
    PROFILE_FILENAME,
    PROFILE_SCHEMA_VERSION,
    RunProfiler,
    _safe_identifier,
)
from contextswarm_mini.runner import RunLogger, _run_solver_with_recovery
from contextswarm_mini.selection_runtime import SelectionRuntime
from contextswarm_mini.selection_store import SelectionStore


ROOT = Path(__file__).resolve().parents[1]


def _post(url: str, operation: str, payload: dict[str, object]) -> dict[str, object]:
    request = Request(
        f"{url}/{operation}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=3) as response:
        result = json.loads(response.read())
    assert isinstance(result, dict)
    return result


class _ReceiptEvaluator:
    is_mock_evaluator = True

    def __init__(self, status: str) -> None:
        self.status = status

    def expected_task_contract_sha256(self, task: Task) -> str:
        return task_contract_sha256(
            task,
            lean_env_id="mock",
            verification_profile="mock",
            judge_mode="mock",
        )

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        del deadline_monotonic
        return Verdict(
            task.slug,
            self.status,
            1.0 if self.status == "PROVED" else 0.0,
            0.01,
            response={"mock": True},
            candidate_sha256=candidate_sha256(candidate_code),
            task_contract_sha256=self.expected_task_contract_sha256(task),
            judge_job_id="job-1",
            cache_reused=self.status != "PROVED",
        )


def _receipt_task(root: Path) -> Task:
    return Task(
        slug="task",
        root=root,
        problem_text="problem",
        baseline_code="import Mathlib\ntheorem task : True := by trivial\n",
        metadata={"problem_id": "task", "theorem_name": "task"},
    )


class ProfilingTests(unittest.TestCase):
    def test_identifier_hashes_email_or_account_style_actor_id(self) -> None:
        self.assertEqual(_safe_identifier("agent-1"), "agent-1")
        redacted = _safe_identifier("alice@example.com")
        self.assertIsNotNone(redacted)
        self.assertTrue(str(redacted).startswith("opaque:"))
        self.assertNotEqual(redacted, "alice@example.com")

    """Small, disk-backed tests for the opt-in profiler boundary."""

    def _rows(self, path: Path) -> list[dict[str, object]]:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    def test_default_off_has_no_profile_file_or_sampler_and_logger_skips_profile_timing(self) -> None:
        # /tmp is a tmpfs in the test container.  Keep all fixtures under the
        # repository's disk-backed filesystem while still using tempfile for
        # automatic, recoverable cleanup.
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            with patch.dict(
                os.environ,
                {
                    "CONTEXTSWARM_PROFILE": "0",
                    "CONTEXTSWARM_RESOURCE_PROFILING": "0",
                    "CONTEXTSWARM_PROFILING": "0",
                },
                clear=False,
            ):
                profiler = RunProfiler.from_environment(root, run_id="run-1")
                self.assertFalse(profiler.enabled)
                self.assertIsNone(profiler._sampler_thread)
                profiler.start(root_pid=os.getpid())
                profiler.emit("disabled.event", prompt="must-not-be-written")
                self.assertEqual(profiler.sample_now(force=True), {})
                with patch(
                    "contextswarm_mini.profiling.time.monotonic",
                    side_effect=AssertionError("heartbeat clock used while disabled"),
                ):
                    profiler.heartbeat(force=True, pid=os.getpid())
                profiler.close()

                logger = RunLogger(root)
                # A disabled logger event still needs its ordinary JSONL
                # write, but must not take profiling-only clocks.
                with patch(
                    "contextswarm_mini.runner.time.monotonic",
                    side_effect=AssertionError("profiling clock used while disabled"),
                ):
                    logger.event("ordinary_event", value="ok")
                logger.close()

            self.assertFalse((root / PROFILE_FILENAME).exists())
            self.assertEqual({path.name for path in root.iterdir()}, {"events.jsonl"})
            self.assertNotIn(
                "contextswarm-profiler",
                {thread.name for thread in threading.enumerate()},
            )

    def test_enabled_schema_filters_sensitive_fields_and_records_span(self) -> None:
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-1",
            )
            profiler.start(root_pid=os.getpid())
            with profiler.span(
                "selection.rank",
                task_id="task-1",
                actor_id="agent-1",
                episode=0,
                candidate_count=2,
            ):
                profiler.emit(
                    "security.check",
                    task_id="task-1",
                    actor_id="agent-1",
                    status="ok",
                    accepted=True,
                    candidate_sha256="a" * 64,
                    prompt="PROMPT_SHOULD_NOT_APPEAR",
                    candidate="CANDIDATE_SHOULD_NOT_APPEAR",
                    secret="SECRET_SHOULD_NOT_APPEAR",
                    query="QUERY_SHOULD_NOT_APPEAR",
                    nested={"secret": "NESTED_SHOULD_NOT_APPEAR"},
                )
            profiler.sample_now(force=True)
            profiler.close()

            profile_path = root / PROFILE_FILENAME
            self.assertTrue(profile_path.is_file())
            self.assertEqual(stat.S_IMODE(profile_path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
            rows = self._rows(profile_path)
            self.assertGreaterEqual(len(rows), 5)
            self.assertTrue(all(row["schema_version"] == PROFILE_SCHEMA_VERSION for row in rows))
            self.assertEqual(
                [row["sequence"] for row in rows],
                list(range(1, len(rows) + 1)),
            )
            events = {str(row["event"]) for row in rows}
            self.assertIn("selection.rank.start", events)
            self.assertIn("selection.rank.end", events)
            self.assertIn("resource.sample", events)
            self.assertIn("resource.process.self", events)
            span_end = next(row for row in rows if row["event"] == "selection.rank.end")
            self.assertEqual(span_end["task_id"], "task-1")
            self.assertEqual(span_end["actor_id"], "agent-1")
            self.assertEqual(span_end["episode"], 0)
            self.assertIn("wall_seconds", span_end)
            self.assertIn("cpu_user_seconds", span_end)
            self.assertIn("cpu_system_seconds", span_end)
            sample = next(row for row in rows if row["event"] == "resource.sample")
            self.assertIn("sample_interval_seconds", sample)
            self.assertIn("cpu_utilization", sample)
            security = next(row for row in rows if row["event"] == "security.check")
            self.assertEqual(security["candidate_sha256"], "a" * 64)
            self.assertGreaterEqual(int(security.get("dropped_fields", 0)), 5)
            forbidden_keys = {"prompt", "candidate", "secret", "query", "nested"}
            self.assertTrue(forbidden_keys.isdisjoint(security))
            serialized = profile_path.read_text(encoding="utf-8")
            for value in (
                "PROMPT_SHOULD_NOT_APPEAR",
                "CANDIDATE_SHOULD_NOT_APPEAR",
                "SECRET_SHOULD_NOT_APPEAR",
                "QUERY_SHOULD_NOT_APPEAR",
                "NESTED_SHOULD_NOT_APPEAR",
            ):
                self.assertNotIn(value, serialized)

    def test_callsite_schema_fields_are_retained_without_allowlist_widening(self) -> None:
        """Keep the low-cardinality timing/query labels observable and bounded."""

        expected = {
            "begin_seconds": 0.012,
            "db_bytes_after": 4096,
            "evaluator_seconds": 0.34,
            "audit_seconds": 0.005,
            "query_name": "exposure",
            "query_index": 2,
            "scan_scope": "active_task",
            "method": "SELECT",
            "projection_seconds": 0.021,
            "recovery": "retry",
        }
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-schema",
            )
            profiler.emit(
                "schema.callsite",
                **expected,
                prompt="PROMPT_MUST_NOT_BE_WRITTEN",
                candidate="CANDIDATE_MUST_NOT_BE_WRITTEN",
                query="QUERY_MUST_NOT_BE_WRITTEN",
                secret="SECRET_MUST_NOT_BE_WRITTEN",
                nested={"body": "NESTED_MUST_NOT_BE_WRITTEN"},
                claim=object(),
            )
            profiler.close()

            rows = self._rows(root / PROFILE_FILENAME)
            row = next(item for item in rows if item["event"] == "schema.callsite")
            for key, value in expected.items():
                self.assertEqual(row[key], value)
            self.assertGreaterEqual(int(row.get("dropped_fields", 0)), 6)
            forbidden = {"prompt", "candidate", "query", "secret", "nested", "claim"}
            self.assertTrue(forbidden.isdisjoint(row))
            serialized = (root / PROFILE_FILENAME).read_text(encoding="utf-8")
            for value in (
                "PROMPT_MUST_NOT_BE_WRITTEN",
                "CANDIDATE_MUST_NOT_BE_WRITTEN",
                "QUERY_MUST_NOT_BE_WRITTEN",
                "SECRET_MUST_NOT_BE_WRITTEN",
                "NESTED_MUST_NOT_BE_WRITTEN",
            ):
                self.assertNotIn(value, serialized)

    def test_backpressure_notifications_keep_bounded_queue_limit(self) -> None:
        """Evaluator backlog events remain auditable at high concurrency."""

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-backpressure",
            )
            profiler.start(root_pid=os.getpid())
            profiler.observe_logger_event(
                "evaluation_backpressure_wait",
                {
                    "task_id": "task-queue",
                    "agent_id": "agent-queue",
                    "episode": 1,
                    "backlog_limit": 7,
                },
            )
            profiler.observe_logger_event(
                "evaluation_backpressure_expired",
                {
                    "task_id": "task-queue",
                    "agent_id": "agent-queue",
                    "episode": 1,
                    "backlog_limit": 7,
                },
            )
            profiler.close()

            rows = self._rows(root / PROFILE_FILENAME)
            queue_rows = [
                row
                for row in rows
                if row["event"] in {"judge.queue.wait", "judge.queue.expired"}
            ]
            self.assertEqual(
                [row["event"] for row in queue_rows],
                ["judge.queue.wait", "judge.queue.expired"],
            )
            self.assertTrue(
                all(
                    row.get("task_id") == "task-queue"
                    and row.get("actor_id") == "agent-queue"
                    and row.get("episode") == 1
                    for row in queue_rows
                )
            )
            self.assertEqual([row["backlog_limit"] for row in queue_rows], [7, 7])
            self.assertTrue(all("dropped_fields" not in row for row in queue_rows))

    def test_cgroup_scope_precedes_root_and_snapshot_never_returns_path(self) -> None:
        if os.name != "posix":
            self.skipTest("cgroup v2 probing is Unix-specific")
        scoped = Path("/sys/fs/cgroup/session.slice/run.scope")
        root = Path("/sys/fs/cgroup")
        candidates = RunProfiler._cgroup_candidates("0::/session.slice/run.scope\n")
        self.assertEqual(candidates[0], scoped)
        self.assertEqual(candidates[-1], root)

        # Exercise the actual reader with synthetic lookup roots.  Returned
        # metrics are scalar-only; the local cgroup path must not be present.
        with patch.object(RunProfiler, "_cgroup_candidates", return_value=(scoped, root)):
            snapshot = RunProfiler._cgroup_snapshot()
        rendered = json.dumps(snapshot, sort_keys=True)
        self.assertNotIn("session.slice", rendered)
        self.assertNotIn("/sys/fs/cgroup", rendered)
        self.assertTrue(all(not isinstance(value, (dict, list, str)) for value in snapshot.values()))

    def test_process_registration_is_attributed_and_close_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(root, enabled=True, heartbeat_interval_seconds=60, run_id="run-2")
            profiler.start(root_pid=os.getpid())
            profiler.register_process(
                os.getpid(),
                task_id="task-2",
                actor_id="agent-2",
                role="solver",
                episode=0,
            )
            profiler.sample_now(force=True)
            profiler.unregister_process(os.getpid(), status="exited")
            profiler.close()
            first_close = (root / PROFILE_FILENAME).read_text(encoding="utf-8")
            profiler.close()
            self.assertEqual(first_close, (root / PROFILE_FILENAME).read_text(encoding="utf-8"))

            rows = self._rows(root / PROFILE_FILENAME)
            registered = [row for row in rows if row["event"] == "resource.process.register"]
            unregistered = [row for row in rows if row["event"] == "resource.process.unregister"]
            samples = [row for row in rows if row["event"] == "resource.process"]
            self.assertTrue(registered)
            self.assertTrue(unregistered)
            self.assertTrue(samples)
            self.assertEqual(registered[-1]["task_id"], "task-2")
            self.assertEqual(registered[-1]["actor_id"], "agent-2")
            self.assertEqual(unregistered[-1]["task_id"], "task-2")
            self.assertEqual(unregistered[-1]["episode"], 0)
            self.assertIn("peak_rss_bytes", unregistered[-1])
            self.assertIn("peak_process_count", unregistered[-1])
            self.assertTrue(
                any(
                    row.get("task_id") == "task-2" and row.get("actor_id") == "agent-2"
                    for row in samples
                )
            )

    def test_forced_heartbeat_uses_terminal_tree_sample_not_global_sampler(self) -> None:
        """Pi attempt closeout must not walk every run root/artifact."""

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-terminal-boundary",
            )
            profiler.start(root_pid=os.getpid())
            pid = os.getpid()
            profiler.register_process(
                pid,
                task_id="task-terminal",
                actor_id="agent-terminal",
                role="solver",
                episode=0,
            )
            snapshot = {
                "pid": pid,
                "process_state": "R",
                "cpu_user_seconds": 1.0,
                "cpu_system_seconds": 0.25,
                "thread_count": 1,
                "rss_bytes": 1024,
                "pss_bytes": 768,
                "context_switches": 2,
                "fd_count": 3,
            }
            with patch.object(
                profiler,
                "sample_now",
                side_effect=AssertionError("forced heartbeat called global sampler"),
            ) as global_sample, patch.object(
                profiler,
                "_bounded_tree",
                return_value=((pid,), False),
            ), patch.object(
                profiler,
                "_proc_snapshot",
                return_value=snapshot,
            ), patch.object(
                profiler,
                "_cgroup_snapshot",
                side_effect=AssertionError("terminal sample touched cgroup"),
            ), patch.object(
                profiler,
                "_artifact_snapshot",
                side_effect=AssertionError("terminal sample walked artifacts"),
            ):
                profiler.heartbeat(
                    task_id="task-terminal",
                    actor_id="agent-terminal",
                    episode=0,
                    force=True,
                    pid=pid,
                    process_alive=True,
                )
                self.assertFalse(global_sample.called)

                # A pre-spawn failure has no PID to attribute.  It should
                # still emit the liveness heartbeat, but must not fall back to
                # an expensive run-wide sample.
                profiler.heartbeat(force=True, task_id="task-no-pid")

            # The follow-up unregister must be a metadata-only operation: the
            # terminal flag prevents a second /proc traversal.
            profiler.unregister_process(pid, status="exited")
            profiler.close()
            rows = self._rows(root / PROFILE_FILENAME)
            terminal_rows = [
                row
                for row in rows
                if row["event"] == "resource.process"
                and row.get("sample_kind") == "terminal"
            ]
            self.assertEqual(len(terminal_rows), 1)
            terminal = terminal_rows[0]
            self.assertEqual(terminal["task_id"], "task-terminal")
            self.assertEqual(terminal["actor_id"], "agent-terminal")
            self.assertEqual(terminal["process_count"], 1)
            self.assertEqual(terminal["process_tree_truncated"], False)
            self.assertTrue(terminal["process_alive"])
            unregister = next(
                row for row in rows if row["event"] == "resource.process.unregister"
            )
            self.assertEqual(unregister["sample_count"], 1)

    def test_forced_global_samples_rate_limit_artifact_walk_until_closeout(self) -> None:
        """Repeated force samples refresh artifacts once, closeout may refresh."""

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-artifact-rate",
            )
            profiler.start(root_pid=os.getpid())
            artifact = {
                "artifact_bytes": 10,
                "sqlite_bytes": 0,
                "wal_bytes": 0,
                "profile_bytes": 0,
                "disk_free_bytes": 1,
                "artifact_files_scanned": 1,
                "artifact_directories_scanned": 1,
                "artifact_scan_truncated": False,
            }
            with patch.object(
                profiler, "_artifact_snapshot", return_value=artifact
            ) as snapshot:
                profiler.sample_now(force=True)
                profiler.sample_now(force=True)
                self.assertEqual(snapshot.call_count, 1)
                profiler.close()
                self.assertEqual(snapshot.call_count, 2)
            rows = self._rows(root / PROFILE_FILENAME)
            aggregate = next(row for row in rows if row["event"] == "resource.sample")
            self.assertEqual(aggregate["artifact_files_scanned"], 1)
            self.assertEqual(aggregate["artifact_directories_scanned"], 1)
            self.assertFalse(aggregate["artifact_scan_truncated"])

    def test_terminal_tree_traversal_is_bounded(self) -> None:
        """A pathological child list cannot make one attempt unbounded."""

        pid = 100
        with patch.object(
            RunProfiler,
            "_children",
            side_effect=lambda current: tuple(range(current + 1, current + 500)),
        ):
            tree, truncated = RunProfiler._bounded_tree(pid)
        self.assertTrue(truncated)
        self.assertLessEqual(len(tree), 128)

    def test_aggregate_sample_always_includes_runner_with_registered_solver(self) -> None:
        """Run totals retain the wrapper root when only a solver is registered."""

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-aggregate-root",
            )
            profiler.start(root_pid=os.getpid())
            runner_pid = profiler._root_pid
            # Simulate a profiler that starts sampling after its runner
            # registration was removed, then observes a separately registered
            # solver process.  This is the case that previously dropped the
            # runner from ``resource.sample`` aggregation.
            profiler.unregister_process(runner_pid, status="test_removed")
            solver_pid = 424242
            profiler.register_process(
                solver_pid,
                task_id="task-aggregate",
                actor_id="agent-aggregate",
                role="solver",
                episode=0,
            )
            snapshots = {
                runner_pid: {
                    "pid": runner_pid,
                    "process_state": "R",
                    "cpu_user_seconds": 1.0,
                    "cpu_system_seconds": 0.5,
                    "thread_count": 2,
                    "rss_bytes": 100,
                    "pss_bytes": 80,
                    "context_switches": 3,
                    "fd_count": 4,
                },
                solver_pid: {
                    "pid": solver_pid,
                    "process_state": "S",
                    "cpu_user_seconds": 2.0,
                    "cpu_system_seconds": 1.0,
                    "thread_count": 1,
                    "rss_bytes": 200,
                    "pss_bytes": 150,
                    "context_switches": 5,
                    "fd_count": 6,
                },
            }

            def fake_tree(pid: int) -> tuple[int, ...]:
                return (pid,)

            with patch.object(profiler, "_tree", side_effect=fake_tree), patch.object(
                profiler, "_proc_snapshot", side_effect=lambda pid: snapshots.get(pid)
            ), patch.object(profiler, "_cgroup_snapshot", return_value={}):
                profiler.sample_now(force=True)
            profiler.close()

            rows = self._rows(root / PROFILE_FILENAME)
            aggregate = next(row for row in rows if row["event"] == "resource.sample")
            self.assertEqual(aggregate["process_tree_count"], 2)
            self.assertEqual(aggregate["rss_bytes"], 300)
            self.assertEqual(aggregate["pss_bytes"], 230)
            process_rows = [row for row in rows if row["event"] == "resource.process"]
            self.assertTrue(
                any(
                    row.get("pid") == runner_pid and row.get("role") == "runner"
                    for row in process_rows
                )
            )
            self.assertTrue(
                any(
                    row.get("pid") == solver_pid and row.get("role") == "solver"
                    for row in process_rows
                )
            )

    def test_trace_bridge_profile_reports_pages_materialization_and_reuse(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(
                root / "profile",
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-bridge",
            )
            profiler.start(root_pid=os.getpid())
            bridge = TraceProjectionBridge(
                profiler=profiler,
                synthetic_features={"task-a": {"actionability": 0.5}},
            )
            first = bridge.read(["task-a"])
            second = bridge.read((item for item in ["task-a"]))
            self.assertEqual(first.watermark, second.watermark)
            profiler.close()
            rows = self._rows(root / "profile" / PROFILE_FILENAME)
            pages = [row for row in rows if row["event"] == "trace.bridge.page"]
            summaries = [row for row in rows if row["event"] == "trace.bridge.summary"]
            self.assertGreaterEqual(len(pages), 2)
            self.assertTrue(all(row["page_count"] == 1 for row in pages))
            self.assertEqual(len(summaries), 2)
            self.assertEqual(summaries[-1]["reuse_count"], 1)
            self.assertGreater(int(summaries[0]["materialized_bytes"]), 0)

    def test_extended_sqlite_selection_and_cps_metrics_are_emitted_once(self) -> None:
        """One profiled selection search exposes the complete non-agent chain."""

        class SelectionConfig:
            selector_name = "recency"
            selector_version = "profiling-test"
            policy_params = {}
            visibility = "project_shared"
            trace_slot_limit = 4
            context_token_budget = 4096
            tokenizer = "utf8_bytes_ceil_div4_v1"
            seed = 0
            tie_break = "trace_id_asc"

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profile_root = root / "profile"
            profiler = RunProfiler(
                profile_root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-selection",
            )
            profiler.start(root_pid=os.getpid())
            cps = CPSStore(root / "cps.sqlite3", profiler=profiler)
            selection = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            cps.create_piece(
                task_id="task",
                author="agent",
                kind="handoff",
                title="one",
                body="alpha beta",
            )
            cps.create_piece(
                task_id="task",
                author="agent",
                kind="strategy",
                title="two",
                body="beta gamma",
            )
            cps.search(task_id="task", query="beta", limit=2)
            message = cps.send_message(
                task_id="task", sender="agent", recipient="agent-2", body="handoff"
            )
            cps.inbox(task_id="task", recipient="agent-2", limit=2)
            cps.digest(task_id="task", actor_id="agent-2", query="beta", limit=2)
            cps.ack_message(message["id"], "agent-2")
            # Exercise the independent CPS progress projection as part of the
            # same profiled run that exercises Selection.
            cps.progress_snapshot(["task"])
            runtime = SelectionRuntime(
                cps,
                selection,
                SelectionConfig(),
                run_id="run-selection",
                profiler=profiler,
            )
            result = runtime.search(
                "task", "agent", query="beta", request_key="profile-search"
            )
            self.assertEqual(result["eligible_candidate_count"], 2)

            rows = self._rows(profile_root / PROFILE_FILENAME)
            events = {str(row["event"]) for row in rows}
            for expected in (
                "cps.progress.query",
                "cps.progress.materialize",
                "cps.search.query",
                "cps.search.materialize",
                "cps.inbox.query",
                "cps.inbox.materialize",
                "cps.digest.start",
                "cps.digest.end",
                "cps.write.lock",
                "cps.write.commit",
                "cps.sqlite.connect",
                "selection.eligible.read",
                "selection.eligible.filter",
                "selection.eligible.query_terms",
                "selection.eligible.materialize",
                "trace.project.query",
                "trace.project.read",
                "trace.project.materialize",
                "selection.rank.summary",
                "selection.pack.summary",
                "selection.persist.payload",
                "selection.persist.lock",
                "selection.persist.end",
                "selection.persist.readback",
                "selection.sqlite.connect",
            ):
                self.assertIn(expected, events)
            persist = next(
                row
                for row in rows
                if row["event"] == "selection.persist.end"
                and row.get("operation") == "record_search"
            )
            self.assertGreaterEqual(int(persist.get("rows_written", 0)), 1)
            self.assertIn("lock_hold_seconds", persist)
            self.assertIn("wal_bytes_before", persist)
            self.assertIn("wal_bytes_after", persist)
            self.assertIn("wal_bytes_delta", persist)
            self.assertIn("prepare_seconds", persist)
            self.assertIn("prepare_hash_seconds", next(
                row for row in rows
                if row["event"] == "selection.persist.payload"
                and row.get("phase") == "pre_lock"
            ))
            payload = next(
                row for row in rows if row["event"] == "selection.persist.payload"
            )
            self.assertIn("serialization_seconds", payload)
            self.assertIn("hash_seconds", payload)
            projection = next(
                row for row in rows if row["event"] == "trace.project.summary"
            )
            self.assertEqual(projection["projection_call_index"], 1)
            self.assertEqual(projection["snapshot_hit"], False)
            # The runtime deliberately has no projection cache.  The
            # normalized identity counter still exposes repeated materializing
            # of the same trace set (including a reordered caller iterable).
            runtime._trace_stats(result["eligible_trace_ids"])
            runtime._trace_stats(tuple(reversed(result["eligible_trace_ids"])))
            profiler.sample_now(force=True)
            profiler.close()
            rows = self._rows(profile_root / PROFILE_FILENAME)
            self.assertTrue(
                any(
                    row["event"] == "trace.project.summary"
                    and row.get("reuse_count") == 1
                    for row in rows
                )
            )
            self.assertEqual(sum(row["event"] == "profile.end" for row in rows), 1)

            progress = next(row for row in rows if row["event"] == "cps.progress.query")
            self.assertEqual(progress["scan_mode"], "full_active_piece_scan")
            self.assertIn("fetch_seconds", progress)
            self.assertIn("read_scope_seconds", progress)

    def test_selection_writer_queue_and_lock_are_separately_observable(self) -> None:
        """Two concurrent writers expose local queue depth and SQLite wait."""

        with tempfile.TemporaryDirectory(
            prefix=".contextswarm-profile-", dir=str(ROOT)
        ) as temporary:
            root = Path(temporary)
            profile_root = root / "profile"
            profiler = RunProfiler(
                profile_root,
                enabled=True,
                heartbeat_interval_seconds=60,
                run_id="run-lock",
            )
            profiler.start(root_pid=os.getpid())
            selection = SelectionStore(root / "selection.sqlite3", profiler=profiler)
            entered = threading.Event()
            release = threading.Event()
            errors: list[BaseException] = []

            def holder() -> None:
                try:
                    with selection._write(
                        "holder",
                        metrics={"input_bytes": 1, "payload_bytes": 1},
                    ):
                        entered.set()
                        self.assertTrue(release.wait(5))
                except BaseException as exc:  # pragma: no cover - assertion aid
                    errors.append(exc)

            first = threading.Thread(target=holder, name="profile-lock-holder")
            first.start()
            self.assertTrue(entered.wait(5))
            # Give the second connection a deterministic chance to enqueue
            # behind BEGIN IMMEDIATE before releasing the holder.
            second_done = threading.Event()

            def waiter() -> None:
                try:
                    with selection._write("waiter"):
                        pass
                except BaseException as exc:  # pragma: no cover - assertion aid
                    errors.append(exc)
                finally:
                    second_done.set()

            second = threading.Thread(target=waiter, name="profile-lock-waiter")
            second.start()
            time.sleep(0.12)
            release.set()
            self.assertTrue(second_done.wait(5))
            first.join(timeout=5)
            second.join(timeout=5)
            self.assertFalse(errors, errors)
            profiler.close()

            rows = self._rows(profile_root / PROFILE_FILENAME)
            queue_rows = [row for row in rows if row["event"] == "selection.persist.queue"]
            waiter_lock = next(
                row
                for row in rows
                if row["event"] == "selection.persist.lock"
                and row.get("operation") == "waiter"
            )
            waiter_end = next(
                row
                for row in rows
                if row["event"] == "selection.persist.end"
                and row.get("operation") == "waiter"
            )
            self.assertGreaterEqual(len(queue_rows), 2)
            self.assertTrue(
                any(int(row.get("write_waiters", 0)) >= 1 for row in queue_rows)
            )
            self.assertGreaterEqual(int(waiter_lock.get("lock_queue_depth", 0)), 1)
            self.assertGreater(float(waiter_lock.get("queue_residence_seconds", 0.0)), 0.0)
            self.assertGreater(float(waiter_end.get("lock_wait_seconds", 0.0)), 0.0)
            self.assertGreaterEqual(int(waiter_end.get("write_ops_total", 0)), 2)

    def test_pi_agent_registers_and_unregisters_spawned_process(self) -> None:
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            fake = root / "fake-pi"
            fake.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys, time\n"
                "request = json.loads(sys.stdin.readline())\n"
                "print(json.dumps({'id': request['id'], 'type': 'response', 'command': 'prompt', 'success': True}), flush=True)\n"
                "print(json.dumps({'type': 'agent_settled'}), flush=True)\n"
                "time.sleep(0.25)\n",
                encoding="utf-8",
            )
            fake.chmod(0o755)
            workdir = root / "work"
            workdir.mkdir()
            config = replace(
                load_config("configs/smoke.toml", ROOT),
                pi_binary=str(fake),
                aisw_enabled=False,
                pi_timeout_seconds=5,
            )
            profile_root = root / "profile"
            profiler = RunProfiler(
                profile_root,
                enabled=True,
                heartbeat_interval_seconds=0.1,
                run_id="run-3",
            )
            profiler.start(root_pid=os.getpid())
            result = PiAgent(config, profiler=profiler).run(
                task_id="task-3",
                actor_id="agent-3",
                episode=4,
                prompt="mock prompt",
                workdir=workdir,
            )
            profiler.close()

            self.assertEqual(result.returncode, 0, result.error_tail)
            rows = self._rows(profile_root / PROFILE_FILENAME)
            registered = [row for row in rows if row["event"] == "resource.process.register"]
            unregistered = [row for row in rows if row["event"] == "resource.process.unregister"]
            self.assertTrue(registered)
            self.assertTrue(unregistered)
            self.assertEqual(registered[-1]["task_id"], "task-3")
            self.assertEqual(registered[-1]["actor_id"], "agent-3")
            self.assertEqual(unregistered[-1]["task_id"], "task-3")
            self.assertEqual(unregistered[-1]["actor_id"], "agent-3")
            self.assertEqual(registered[-1]["pid"], unregistered[-1]["pid"])
            self.assertTrue(
                any(
                    row["event"] == "resource.process"
                    and row.get("task_id") == "task-3"
                    and row.get("actor_id") == "agent-3"
                    for row in rows
                )
            )
            self.assertTrue(
                any(
                    row["event"] == "resource.process"
                    and row.get("sample_kind") == "terminal"
                    and row.get("task_id") == "task-3"
                    for row in rows
                )
            )

    def test_attempt_wrapper_span_keeps_task_and_actor_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix=".contextswarm-profile-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            profiler = RunProfiler(root, enabled=True, heartbeat_interval_seconds=60, run_id="run-4")
            logger = RunLogger(root, profiler=profiler, run_id="run-4")
            config = load_config("configs/smoke.toml", ROOT)

            def invoke(_recovery_attempt: int) -> AgentResult:
                return AgentResult(
                    agent_id="agent-4",
                    task_id="task-4",
                    episode=2,
                    returncode=0,
                    started_at="start",
                    finished_at="finish",
                    events=3,
                )

            result = _run_solver_with_recovery(
                config,
                logger,
                invoke,
                task_id="task-4",
                actor_id="agent-4",
                episode=2,
                deadline=time.monotonic() + 5,
                cancel_event=threading.Event(),
            )
            logger.close()

            self.assertEqual(result.returncode, 0)
            rows = self._rows(root / PROFILE_FILENAME)
            start = next(row for row in rows if row["event"] == "attempt.lifecycle.start")
            end = next(row for row in rows if row["event"] == "attempt.lifecycle.end")
            agent_start = next(row for row in rows if row["event"] == "attempt.agent.invoke.start")
            agent_end = next(row for row in rows if row["event"] == "attempt.agent.invoke.end")
            outcome = next(row for row in rows if row["event"] == "attempt.result")
            for row in (start, end, agent_start, agent_end, outcome):
                self.assertEqual(row["task_id"], "task-4")
                self.assertEqual(row["actor_id"], "agent-4")
                self.assertEqual(row["episode"], 2)
            self.assertEqual(outcome["events"], 3)

    def test_judge_receipt_profile_covers_success_and_failure(self) -> None:
        for status in ("PROVED", "VERIFY_FAIL"):
            with self.subTest(status=status), tempfile.TemporaryDirectory(
                prefix=".contextswarm-profile-", dir=str(ROOT)
            ) as temporary:
                root = Path(temporary)
                workdir = root / "worker"
                workdir.mkdir()
                candidate = workdir / "result.lean"
                candidate.write_text(
                    "import Mathlib\ntheorem task : True := by trivial\n",
                    encoding="utf-8",
                )
                profile_root = root / "profile"
                profiler = RunProfiler(
                    profile_root,
                    enabled=True,
                    heartbeat_interval_seconds=60,
                    run_id=f"run-{status.casefold()}",
                )
                profiler.start(root_pid=os.getpid())
                broker = JudgeBroker(
                    _ReceiptEvaluator(status),
                    # One local fake evaluator slot; no external service is
                    # contacted by this focused test.
                    threading.BoundedSemaphore(1),
                    audit_path=root / "audit.jsonl",
                    min_probe_interval_seconds=0,
                    profiler=profiler,
                ).start()
                try:
                    with broker.session(
                        actor_id="agent",
                        episode=9,
                        workdir=workdir,
                        candidates={"task": (_receipt_task(root), candidate)},
                        deadline_monotonic=time.monotonic() + 3,
                    ) as env:
                        result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                finally:
                    broker.close()
                    profiler.close()

                self.assertEqual(result["status"], status)
                rows = self._rows(profile_root / PROFILE_FILENAME)
                receipts = [row for row in rows if row["event"] == "judge.receipt"]
                self.assertEqual(len(receipts), 1)
                receipt = receipts[0]
                self.assertEqual(receipt["status"], status)
                self.assertEqual(receipt["task_id"], "task")
                self.assertEqual(receipt["actor_id"], "agent")
                self.assertEqual(receipt["episode"], 9)
                self.assertIn("gate_wait_seconds", receipt)
                self.assertIn("elapsed_seconds", receipt)
                self.assertIn("cache_reused", receipt)
                self.assertEqual(receipt["accepted"], True)
                execute_rows = [
                    row for row in rows if row["event"] in {
                        "judge.execute.start", "judge.execute.end"
                    }
                ]
                self.assertEqual(len(execute_rows), 2)
                for row in execute_rows:
                    self.assertEqual(row["task_id"], "task")
                    self.assertEqual(row["actor_id"], "agent")
                    self.assertEqual(row["episode"], 9)


if __name__ == "__main__":
    unittest.main()
