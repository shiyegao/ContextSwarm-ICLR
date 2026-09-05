from __future__ import annotations

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from contextswarm_mini.cps import CPSStore
from contextswarm_mini.evaluator import LeanEvaluator
from contextswarm_mini.judge_broker import (
    CandidateSnapshot,
    JudgeBroker,
    JudgeBrokerDrainError,
    _valid_judge_checkpoint,
)
from contextswarm_mini.models import Task, Verdict


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


def _task(root: Path, slug: str = "task") -> Task:
    return Task(
        slug=slug,
        root=root,
        problem_text="problem",
        baseline_code="import Mathlib\ntheorem task : True := by sorry\n",
        metadata={"problem_id": "Task", "theorem_name": "task"},
    )


def _coding_task(root: Path, slug: str = "task") -> Task:
    return Task(
        slug=slug,
        root=root,
        problem_text="coding problem",
        baseline_code="#include <bits/stdc++.h>\nint main() { return 0; }\n",
        metadata={
            "problem_id": "coding-task",
            "language": "cpp",
            "candidate_filename": "result.cpp",
        },
    )


class _RecordingEvaluator:
    def __init__(self) -> None:
        self.calls: list[tuple[Task, Path, float | None]] = []

    def expected_task_contract_sha256(self, _task: Task) -> str:
        return "a" * 64

    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        self.calls.append((task, candidate, deadline_monotonic))
        return Verdict(
            task.slug,
            "VERIFY_FAIL",
            0.0,
            0.25,
            {
                "probe_diagnostics": {
                    "items": [{"severity": "error", "data": "type mismatch", "line": 2, "column": 3}],
                    "truncated": False,
                }
            },
        )


class _SequenceEvaluator(_RecordingEvaluator):
    def __init__(self, statuses: list[str]) -> None:
        super().__init__()
        self.statuses = list(statuses)

    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        self.calls.append((task, candidate, deadline_monotonic))
        status = self.statuses.pop(0)
        return Verdict(
            task.slug,
            status,
            0.0,
            0.0,
            task_contract_sha256="a" * 64,
            judge_job_id=(
                None if status == "LOCAL_REJECTED" else f"job-{len(self.calls)}"
            ),
        )


class _CheckpointEvaluator(_RecordingEvaluator):
    """Return one deliberately controlled checkpoint provenance tuple."""

    def __init__(
        self,
        status: str,
        *,
        judge_job_id: object = None,
        task_contract_sha256: str = "a" * 64,
    ) -> None:
        super().__init__()
        self.status = status
        self.judge_job_id = judge_job_id
        self.task_contract_sha256 = task_contract_sha256

    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        self.calls.append((task, candidate, deadline_monotonic))
        return Verdict(
            task.slug,
            self.status,
            0.0,
            0.0,
            task_contract_sha256=self.task_contract_sha256,
            judge_job_id=self.judge_job_id,  # type: ignore[arg-type]
        )


class _BlockingEvaluator(_RecordingEvaluator):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def probe(self, task: Task, candidate: Path, *, deadline_monotonic: float | None) -> Verdict:
        self.started.set()
        self.release.wait(timeout=3)
        return super().probe(task, candidate, deadline_monotonic=deadline_monotonic)


class _CancelAwareEvaluator(_RecordingEvaluator):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.cancel_observed = threading.Event()

    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None = None,
    ) -> Verdict:
        del candidate, deadline_monotonic
        self.started.set()
        if cancel_event is not None and cancel_event.wait(timeout=3):
            self.cancel_observed.set()
        return Verdict(task.slug, "TASK_CANCELLED", 0.0, 0.0)


class _UnsettledCancellationEvaluator(_RecordingEvaluator):
    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        self.calls.append((task, candidate, deadline_monotonic))
        return Verdict(
            task.slug,
            "TASK_CANCELLED",
            0.0,
            0.0,
            {
                "judge_cancellation": {
                    "attempted": True,
                    "succeeded": False,
                    "settled": False,
                    "unconfirmed": True,
                    "failure_category": "cancel_settlement_unconfirmed",
                }
            },
            judge_job_id="unsettled-job",
        )


class _GlobalUnsettledEvaluator(_RecordingEvaluator):
    def __init__(self) -> None:
        super().__init__()
        self.remote_unsettled_jobs = 0

    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        self.calls.append((task, candidate, deadline_monotonic))
        self.remote_unsettled_jobs += 1
        return Verdict(
            task.slug,
            "NETWORK_ERROR",
            0.0,
            0.0,
            {"remote_settlement_unconfirmed": True},
        )


class _DeferredOnlyEvaluator(_RecordingEvaluator):
    """Expose a deferred receipt without the legacy unsettled counter."""

    def __init__(self) -> None:
        super().__init__()
        self.settlement_callback: object | None = None

    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
        settlement_callback: object | None = None,
    ) -> Verdict:
        self.calls.append((task, candidate, deadline_monotonic))
        self.settlement_callback = settlement_callback
        return Verdict(
            task.slug,
            "TASK_CANCELLED",
            0.0,
            0.0,
            {
                "settlement_error": "cancel_settlement_deferred",
                "judge_cancellation": {
                    "attempted": False,
                    "succeeded": False,
                    "settled": False,
                    "unconfirmed": False,
                    "deferred": True,
                },
            },
            judge_job_id="job-deferred",
        )


class _PendingSettlementEvaluator(_RecordingEvaluator):
    """Minimal evaluator surface for broker watcher-drain tests."""

    def __init__(self, watcher_timeout: float) -> None:
        super().__init__()
        self.deferred_settlement_timeout_seconds = watcher_timeout
        self.pending_settlement_watchers = 1


class _NestedRemoteCacheEvaluator(_RecordingEvaluator):
    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        self.calls.append((task, candidate, deadline_monotonic))
        return Verdict(
            task.slug,
            "VERIFY_FAIL",
            0.0,
            0.0,
            {"response": {"cache_reused": True}},
            task_contract_sha256="a" * 64,
            judge_job_id="remote-cache-job",
            cache_reused=True,
        )


class _SnapshotEvaluator:
    def __init__(self, candidate: Path) -> None:
        self.candidate = candidate
        self.sources: list[str] = []

    def expected_task_contract_sha256(self, _task: Task) -> str:
        return "a" * 64

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        self.sources.append(candidate_code)
        self.candidate.write_text("changed after broker snapshot", encoding="utf-8")
        return Verdict(task.slug, "VERIFY_FAIL", 0.0, 0.0)


class _AuthoritativeSnapshotEvaluator:
    def __init__(self, candidate: Path, *, valid_provenance: bool = True) -> None:
        self.candidate = candidate
        self.valid_provenance = valid_provenance

    def expected_task_contract_sha256(self, _task: Task) -> str:
        return "a" * 64

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        del deadline_monotonic
        digest = hashlib.sha256(candidate_code.encode("utf-8")).hexdigest()
        self.candidate.write_text("changed after authoritative snapshot", encoding="utf-8")
        return Verdict(
            task.slug,
            "PASSED",
            1.0,
            0.01,
            candidate_sha256=digest if self.valid_provenance else "0" * 64,
            task_contract_sha256="a" * 64,
            judge_job_id="judge-job-1",
        )


class _UnsafeEvaluator:
    def expected_task_contract_sha256(self, _task: Task) -> str:
        return "a" * 64

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        del candidate_code, deadline_monotonic
        return Verdict(
            task.slug,
            "VERIFY_FAIL",
            0.0,
            0.0,
            {
                "status": "failed",
                "formal_status": "VERIFY_FAIL",
                "error_message": (
                    "request https://judge.invalid/private failed with "
                    "token=test-secret-value at /home/test/private/file.lean"
                ),
                "job_id": "https://judge.invalid/private-job",
                "private": "must-not-escape",
                "probe_diagnostics": {
                    "items": [
                        {
                            "severity": "error",
                            "data": (
                                "Bearer test-bearer-value from "
                                "/tmp/private/source.lean"
                            ),
                            "line": 1,
                            "column": 2,
                        }
                    ]
                },
            },
            error=(
                "https://judge.invalid/raw Authorization: test-auth-value "
                "/run/private/socket sk-abcdefghijklmnopqrstuv"
            ),
            task_contract_sha256="https://judge.invalid/not-a-hash",
            judge_job_id="/tmp/private/job-id",
        )


class _OrderedEvaluator(_RecordingEvaluator):
    def __init__(self) -> None:
        super().__init__()
        self.order: list[str] = []
        self.lock = threading.Lock()

    def probe(
        self,
        task: Task,
        candidate: Path,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        with self.lock:
            self.order.append(task.slug)
        time.sleep(0.01)
        return super().probe(
            task, candidate, deadline_monotonic=deadline_monotonic
        )


class _OverloadVerdictEvaluator:
    def expected_task_contract_sha256(self, _task: Task) -> str:
        return "a" * 64

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None,
    ) -> Verdict:
        del candidate_code, deadline_monotonic
        return Verdict(
            task.slug,
            "REJECTED_OVERLOADED",
            0.0,
            1.0,
            {
                "evaluator_failure": {
                    "category": "judge_overloaded_deadline",
                    "http_status": 429,
                    "attempts": 6,
                    "retry_after_seconds": 12.5,
                }
            },
            error="Judge capacity remained unavailable.",
        )


def _wait_for_queue_depth(broker: JudgeBroker, depth: int) -> bool:
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        with broker._admission_condition:  # noqa: SLF001 - white-box concurrency test
            if len(broker._admission_queue) >= depth:  # noqa: SLF001
                return True
        time.sleep(0.005)
    return False


class JudgeBrokerTests(unittest.TestCase):
    def _run_checkpoint_gate_case(
        self, evaluator: _CheckpointEvaluator
    ) -> tuple[dict[str, object], dict[str, object]]:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof", encoding="utf-8")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="bound-agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3,
                    cps_store=store,
                    communication="blackboard",
                ) as env:
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    checkpoint = _post(url, "judge_check", {})
                    searched = _post(url, "cps_search", {})
                    return checkpoint, searched
            finally:
                broker.close()

    def test_checkpoint_gate_binds_raw_job_id_and_task_contract(self) -> None:
        cases = (
            (
                "malformed local job id",
                _CheckpointEvaluator(
                    "LOCAL_REJECTED",
                    judge_job_id="bad job id",
                ),
                False,
            ),
            (
                "wrong local task contract",
                _CheckpointEvaluator(
                    "LOCAL_REJECTED",
                    task_contract_sha256="b" * 64,
                ),
                False,
            ),
            (
                "valid local checkpoint",
                _CheckpointEvaluator("LOCAL_REJECTED"),
                True,
            ),
            (
                "valid remote checkpoint",
                _CheckpointEvaluator(
                    "VERIFY_FAIL",
                    judge_job_id="job-1",
                ),
                True,
            ),
        )
        for label, evaluator, allowed in cases:
            with self.subTest(case=label):
                checkpoint, searched = self._run_checkpoint_gate_case(evaluator)
                self.assertTrue(checkpoint["accepted"])
                self.assertEqual(checkpoint["status"], evaluator.status)
                if allowed:
                    self.assertTrue(searched["ok"])
                else:
                    self.assertEqual(searched["status"], "JUDGE_CHECK_REQUIRED")
                    self.assertFalse(searched["accepted"])

    def test_candidate_terminal_feedback_statuses_unlock_cps_with_job_provenance(self) -> None:
        for status in (
            "CHEATING",
            "RESOURCE_LIMIT",
            "EXECUTION_TIMEOUT",
        ):
            with self.subTest(status=status):
                checkpoint, searched = self._run_checkpoint_gate_case(
                    _CheckpointEvaluator(status, judge_job_id="job-feedback")
                )
                self.assertTrue(checkpoint["accepted"])
                self.assertEqual(checkpoint["status"], status)
                self.assertTrue(searched["ok"])

    def test_coding_terminal_feedback_statuses_unlock_cps_with_job_provenance(self) -> None:
        for status in ("WA", "PE", "CE", "MLE", "TLE", "RE"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                workdir = root / "worker"
                workdir.mkdir()
                candidate = workdir / "result.cpp"
                candidate.write_text("int main() { return 0; }\n", encoding="utf-8")
                store = CPSStore(root / "cps.sqlite3")
                broker = JudgeBroker(
                    _CheckpointEvaluator(status, judge_job_id="coding-job"),
                    threading.BoundedSemaphore(1),
                    audit_path=root / "audit.jsonl",
                    min_probe_interval_seconds=0,
                ).start()
                try:
                    with broker.session(
                        actor_id="coding-agent",
                        workdir=workdir,
                        candidates={"task": (_coding_task(root), candidate)},
                        deadline_monotonic=time.monotonic() + 3,
                        cps_store=store,
                        communication="blackboard",
                    ) as env:
                        checkpoint = _post(
                            env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {}
                        )
                        searched = _post(
                            env["CONTEXTSWARM_JUDGE_URL"], "cps_search", {}
                        )
                finally:
                    broker.close()

                self.assertTrue(checkpoint["accepted"])
                self.assertEqual(checkpoint["status"], status)
                self.assertTrue(searched["ok"])

    def test_local_rejected_checkpoint_requires_raw_job_id_to_be_absent(self) -> None:
        base = {
            "accepted": True,
            "status": "LOCAL_REJECTED",
            "candidate_sha256": "a" * 64,
            "task_contract_sha256": "b" * 64,
            "judge_job_id": None,
        }
        self.assertTrue(_valid_judge_checkpoint(base))
        for malformed in ("", "   ", 123, {}, "bad job id"):
            with self.subTest(judge_job_id=malformed):
                self.assertFalse(
                    _valid_judge_checkpoint(
                        {**base, "judge_job_id": malformed}
                    )
                )

    def test_cancel_path_falls_back_to_job_id_and_rejects_cross_origin_endpoint(self) -> None:
        evaluator = LeanEvaluator(
            "https://judge.invalid",
            lean_env_id="test-env",
        )
        terminal = {"job_id": "job-123", "status": "CANCELLED"}
        with patch.object(evaluator, "_request", return_value=terminal) as request:
            fallback = evaluator._cancel_submitted_job("job-123")  # noqa: SLF001
        request.assert_called_once_with(
            "DELETE",
            "/api/lean/jobs/job-123",
            timeout_seconds=2.0,
        )
        self.assertTrue(fallback["succeeded"])
        self.assertTrue(fallback["settled"])
        self.assertFalse(fallback["unconfirmed"])

        with patch.object(evaluator, "_request", return_value=terminal) as request:
            safe_fallback = evaluator._cancel_submitted_job(  # noqa: SLF001
                "job-123",
                cancel_endpoint="https://attacker.invalid/private-capability",
            )
        request.assert_called_once_with(
            "DELETE",
            "/api/lean/jobs/job-123",
            timeout_seconds=2.0,
        )
        self.assertEqual(
            safe_fallback,
            {
                "attempted": True,
                "succeeded": True,
                "settled": True,
                "unconfirmed": False,
                "failure_category": None,
            },
        )

    def test_probe_cancellation_interrupts_poll_and_uses_returned_cancel_endpoint(self) -> None:
        poll_started = threading.Event()
        deleted_paths: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                return

            def _send(self, payload: dict[str, object]) -> None:
                raw = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self) -> None:  # noqa: N802
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self._send(
                    {
                        "job_id": "job-123",
                        "status": "QUEUED",
                        "cancel_endpoint": "/cancel/capability?opaque=private-query",
                    }
                )

            def do_GET(self) -> None:  # noqa: N802
                poll_started.set()
                self._send({"job_id": "job-123", "status": "RUNNING"})

            def do_DELETE(self) -> None:  # noqa: N802
                deleted_paths.append(self.path)
                self._send({"job_id": "job-123", "status": "CANCELLED"})

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        cancelled = threading.Event()
        verdicts: list[Verdict] = []
        private_token = "operator-private-cancel-token"
        try:
            evaluator = LeanEvaluator(
                f"http://127.0.0.1:{server.server_port}",
                lean_env_id="test-env",
                timeout_seconds=10,
                poll_interval_seconds=10,
            )
            with patch.dict(os.environ, {"LEAN_AUTH_TOKEN": private_token}):
                worker = threading.Thread(
                    target=lambda: verdicts.append(
                        evaluator.probe_source(
                            _task(Path("/tmp")),
                            "import Mathlib\ntheorem task : True := by trivial\n",
                            deadline_monotonic=time.monotonic() + 5,
                            cancel_event=cancelled,
                        )
                    )
                )
                worker.start()
                self.assertTrue(poll_started.wait(timeout=2))
                cancelled.set()
                worker.join(timeout=1)
                self.assertFalse(worker.is_alive())
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=1)

        self.assertEqual(len(verdicts), 1)
        verdict = verdicts[0]
        self.assertEqual(verdict.status, "TASK_CANCELLED")
        self.assertEqual(
            deleted_paths,
            ["/cancel/capability?opaque=private-query"],
        )
        self.assertTrue(verdict.response["judge_cancellation"]["succeeded"])
        rendered = json.dumps(verdict.as_dict(), ensure_ascii=False)
        self.assertNotIn(private_token, rendered)
        self.assertNotIn("private-query", rendered)

    def test_probe_cancellation_interrupts_capacity_backoff_without_job(self) -> None:
        request_seen = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                return

            def do_POST(self) -> None:  # noqa: N802
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                request_seen.set()
                payload = json.dumps(
                    {
                        "error": "admission_capacity_exceeded",
                        "message": "HTTP ingress capacity is exhausted",
                    }
                ).encode("utf-8")
                self.send_response(429)
                self.send_header("Retry-After", "20")
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        cancelled = threading.Event()
        verdicts: list[Verdict] = []
        try:
            evaluator = LeanEvaluator(
                f"http://127.0.0.1:{server.server_port}",
                lean_env_id="test-env",
                timeout_seconds=30,
            )
            worker = threading.Thread(
                target=lambda: verdicts.append(
                    evaluator.probe_source(
                        _task(Path("/tmp")),
                        "import Mathlib\ntheorem task : True := by trivial\n",
                        deadline_monotonic=time.monotonic() + 25,
                        cancel_event=cancelled,
                    )
                )
            )
            worker.start()
            self.assertTrue(request_seen.wait(timeout=2))
            cancelled.set()
            worker.join(timeout=1)
            self.assertFalse(worker.is_alive())
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=1)

        self.assertEqual(verdicts[0].status, "TASK_CANCELLED")
        self.assertIsNone(verdicts[0].judge_job_id)

    def test_cached_probe_reuses_only_terminal_job_without_remote_activity(self) -> None:
        post_count = 0
        delete_count = 0

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                return

            def do_POST(self) -> None:  # noqa: N802
                nonlocal post_count
                post_count += 1
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                raw = json.dumps(
                    {
                        "job_id": "terminal-job",
                        "status": "FAILED",
                        "formal_status": "VERIFY_FAIL",
                        "cancel_endpoint": "/cancel/terminal-job",
                    }
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_DELETE(self) -> None:  # noqa: N802
                nonlocal delete_count
                delete_count += 1
                self.send_response(204)
                self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        try:
            evaluator = LeanEvaluator(
                f"http://127.0.0.1:{server.server_port}",
                lean_env_id="test-env",
                timeout_seconds=3,
            )
            task = _task(Path("/tmp"))
            source = "import Mathlib\ntheorem task : True := by trivial\n"
            first = evaluator.probe_source(task, source)
            second = evaluator.probe_source(task, source)
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=1)

        self.assertEqual(first.status, "VERIFY_FAIL")
        self.assertFalse(first.cache_reused)
        self.assertTrue(second.cache_reused)
        self.assertEqual(post_count, 1)
        self.assertEqual(delete_count, 0)

    def test_transport_failure_classification_is_recorded_in_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                _OverloadVerdictEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2.0,
                ) as env:
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
            finally:
                broker.close()
            audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(result["status"], "REJECTED_OVERLOADED")
            self.assertEqual(audit["failure_category"], "judge_overloaded_deadline")
            self.assertEqual(audit["failure_http_status"], 429)
            self.assertEqual(audit["failure_attempts"], 6)
            self.assertEqual(audit["failure_retry_after_seconds"], 12.5)

    def test_disabled_profiling_preserves_audit_timing_fields(self) -> None:
        """The profiling-off path must retain the historical timing audit."""

        class _DelayedEvaluator(_RecordingEvaluator):
            def probe(
                self,
                task: Task,
                candidate: Path,
                *,
                deadline_monotonic: float | None,
            ) -> Verdict:
                time.sleep(0.02)
                return super().probe(
                    task,
                    candidate,
                    deadline_monotonic=deadline_monotonic,
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text(
                "import Mathlib\ntheorem task : True := by trivial\n",
                encoding="utf-8",
            )
            gate = threading.BoundedSemaphore(1)
            # Hold the only evaluator slot long enough that the audit's
            # gate_wait_seconds cannot be confused with the default zero.
            gate.acquire()
            release = threading.Timer(0.08, gate.release)
            release.start()
            audit_path = root / "judge_checks.jsonl"
            broker = JudgeBroker(
                _DelayedEvaluator(),
                gate,
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3.0,
                ) as env:
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
            finally:
                release.join(timeout=1.0)
                broker.close()

            audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])

        self.assertEqual(result["status"], "VERIFY_FAIL")
        self.assertGreater(audit["gate_wait_seconds"], 0.03)
        self.assertGreater(audit["elapsed_seconds"], 0.03)

    def test_audit_preserves_nested_remote_cache_reuse_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text(
                "import Mathlib\ntheorem task : True := by trivial\n",
                encoding="utf-8",
            )
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                _NestedRemoteCacheEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2,
                ) as env:
                    result = _post(
                        env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {}
                    )
            finally:
                broker.close()
            audit = json.loads(
                audit_path.read_text(encoding="utf-8").splitlines()[0]
            )

        self.assertTrue(result["cache_reused"])
        self.assertTrue(audit["cache_reused"])
        self.assertFalse(audit["probe_cache_reused"])
        self.assertTrue(audit["remote_cache_reused"])

    def test_unexpected_broker_failure_is_stable_unaccepted_and_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=audit_path,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2.0,
                ) as env, patch.object(
                    broker,
                    "_judge_check",
                    side_effect=RuntimeError(
                        "token=test-secret https://judge.invalid /home/test/private"
                    ),
                ):
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
            finally:
                broker.close()
            rendered = json.dumps(result)
            audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(result["status"], "BROKER_ERROR")
            self.assertFalse(result["accepted"])
            self.assertFalse(audit["accepted"])
            self.assertEqual(audit["status"], "BROKER_ERROR")
            self.assertNotIn("test-secret", rendered)
            self.assertNotIn("judge.invalid", rendered)
            self.assertNotIn("/home/test", rendered)

    def test_session_injects_client_deadline_after_broker_evaluator_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
            ).start()
            before_epoch_ms = int(time.time() * 1_000)
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2.0,
                ) as env:
                    deadline_epoch_ms = int(
                        env["CONTEXTSWARM_BROKER_DEADLINE_EPOCH_MS"]
                    )
            finally:
                broker.close()
            self.assertGreaterEqual(deadline_epoch_ms, before_epoch_ms + 1_500)
            self.assertLessEqual(deadline_epoch_ms, int(time.time() * 1_000) + 2_500)

    def test_judge_capability_fixes_candidate_and_rejects_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            evaluator = _RecordingEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent-a",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=10**12,
                ) as env:
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    result = _post(url, "judge_check", {})
                    rejected = _post(
                        url,
                        "judge_check",
                        {"candidate_path": "/tmp/other.lean", "lean_env_id": "other"},
                    )
                    with self.assertRaises(HTTPError) as invalid:
                        _post(url.replace(url.rsplit("/", 1)[1], "wrong-token"), "judge_check", {})
                    invalid.exception.close()
            finally:
                broker.close()

            self.assertEqual(result["status"], "VERIFY_FAIL")
            self.assertEqual(rejected["status"], "INVALID_REQUEST")
            self.assertEqual(len(evaluator.calls), 1)
            self.assertEqual(evaluator.calls[0][1], candidate.resolve())
            audit = (root / "judge_checks.jsonl").read_text()
            self.assertIn('"actor_id": "agent-a"', audit)
            self.assertNotIn(candidate.read_text(), audit)
            self.assertNotIn("wrong-token", audit)

    def test_coding_capability_binds_result_cpp_and_immutable_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.cpp"
            original = "#include <iostream>\nint main() { return 0; }\n"
            candidate.write_text(original, encoding="utf-8")
            evaluator = _SnapshotEvaluator(candidate)
            audit_path = root / "judge_checks.jsonl"
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="coding-agent",
                    workdir=workdir,
                    candidates={"task": (_coding_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2,
                ) as env:
                    result = _post(
                        env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {}
                    )
            finally:
                broker.close()

            audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(result["status"], "VERIFY_FAIL")
            self.assertEqual(evaluator.sources, [original])
            self.assertEqual(
                result["candidate_sha256"],
                hashlib.sha256(original.encode("utf-8")).hexdigest(),
            )
            self.assertEqual(audit["candidate_sha256"], result["candidate_sha256"])
            self.assertNotEqual(candidate.read_text(encoding="utf-8"), original)

    def test_probe_and_audit_use_the_same_immutable_candidate_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            original = "import Mathlib\ntheorem task : True := by trivial\n"
            candidate.write_text(original, encoding="utf-8")
            evaluator = _SnapshotEvaluator(candidate)
            audit_path = root / "judge_checks.jsonl"
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent-a",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=10**12,
                ) as env:
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
            finally:
                broker.close()

            audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(result["status"], "VERIFY_FAIL")
            self.assertEqual(evaluator.sources, [original])
            self.assertEqual(
                audit["candidate_sha256"],
                hashlib.sha256(original.encode("utf-8")).hexdigest(),
            )
            self.assertNotEqual(candidate.read_text(encoding="utf-8"), original)

    def test_authoritative_callback_receives_exact_snapshot_before_response(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            original = "import Mathlib\ntheorem task : True := by trivial\n"
            candidate.write_text(original, encoding="utf-8")
            callbacks: list[tuple[Task, Verdict, CandidateSnapshot]] = []
            broker = JudgeBroker(
                _AuthoritativeSnapshotEvaluator(candidate),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2,
                    on_authoritative_verdict=lambda task, verdict, snapshot: callbacks.append(
                        (task, verdict, snapshot)
                    ),
                ) as env:
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
            finally:
                broker.close()

            self.assertEqual(result["status"], "PROVED")
            self.assertTrue(result["proved"])
            self.assertEqual(len(callbacks), 1)
            self.assertEqual(callbacks[0][0].slug, "task")
            self.assertEqual(callbacks[0][1].judge_job_id, "judge-job-1")
            self.assertEqual(callbacks[0][2].source, original)
            self.assertEqual(
                callbacks[0][2].sha256,
                hashlib.sha256(original.encode("utf-8")).hexdigest(),
            )
            self.assertNotEqual(candidate.read_text(encoding="utf-8"), original)

    def test_authoritative_callback_failure_and_bad_provenance_fail_closed(self) -> None:
        for invalid_provenance, callback_raises, expected in (
            (True, False, "PROVENANCE_INVALID"),
            (False, True, "BROKER_ERROR"),
        ):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                workdir = root / "worker"
                workdir.mkdir()
                candidate = workdir / "result.lean"
                candidate.write_text("theorem task : True := by trivial\n", encoding="utf-8")
                callback_calls = 0

                def callback(_task: Task, _verdict: Verdict, _snapshot: CandidateSnapshot) -> None:
                    nonlocal callback_calls
                    callback_calls += 1
                    if callback_raises:
                        raise RuntimeError("private callback detail")

                broker = JudgeBroker(
                    _AuthoritativeSnapshotEvaluator(
                        candidate,
                        valid_provenance=not invalid_provenance,
                    ),
                    threading.BoundedSemaphore(1),
                    audit_path=root / "audit.jsonl",
                    min_probe_interval_seconds=0,
                ).start()
                try:
                    with broker.session(
                        actor_id="agent",
                        workdir=workdir,
                        candidates={"task": (_task(root), candidate)},
                        deadline_monotonic=time.monotonic() + 2,
                        on_authoritative_verdict=callback,
                    ) as env:
                        result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                finally:
                    broker.close()

                self.assertEqual(result["status"], expected)
                self.assertFalse(result["proved"])
                self.assertEqual(callback_calls, 0 if invalid_provenance else 1)
                self.assertNotIn("private callback detail", json.dumps(result))

    def test_session_allows_only_one_inflight_probe(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            evaluator = _BlockingEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            first: list[dict[str, object]] = []
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=10**12,
                ) as env:
                    thread = threading.Thread(
                        target=lambda: first.append(
                            _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                        )
                    )
                    thread.start()
                    self.assertTrue(evaluator.started.wait(timeout=2))
                    second = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                    evaluator.release.set()
                    thread.join(timeout=3)
            finally:
                evaluator.release.set()
                broker.close()
            self.assertEqual(second["status"], "SESSION_PROBE_IN_FLIGHT")
            self.assertEqual(first[0]["status"], "VERIFY_FAIL")

    def test_task_cancellation_removes_waiting_probe_without_judge_work(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            gate = threading.BoundedSemaphore(1)
            self.assertTrue(gate.acquire(timeout=0))
            cancelled = threading.Event()
            evaluator = _RecordingEvaluator()
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                evaluator,
                gate,
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            results: list[dict[str, object]] = []
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3,
                    cancel_event=cancelled,
                ) as env:
                    worker = threading.Thread(
                        target=lambda: results.append(
                            _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                        )
                    )
                    worker.start()
                    self.assertTrue(_wait_for_queue_depth(broker, 1))
                    cancelled.set()
                    worker.join(timeout=1)
                    self.assertFalse(worker.is_alive())
            finally:
                gate.release()
                broker.close()

            self.assertEqual(results[0]["status"], "TASK_CANCELLED")
            self.assertFalse(results[0]["accepted"])
            self.assertEqual(evaluator.calls, [])
            audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(audit["status"], "TASK_CANCELLED")
            self.assertFalse(audit["accepted"])

    def test_close_revokes_fifo_and_returns_zeroed_drain_state_after_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            gate = threading.BoundedSemaphore(1)
            self.assertTrue(gate.acquire(timeout=0))
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                _RecordingEvaluator(),
                gate,
                audit_path=audit_path,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            session = broker.session(
                actor_id="agent",
                workdir=workdir,
                candidates={"task": (_task(root), candidate)},
                deadline_monotonic=time.monotonic() + 3,
            )
            env = session.__enter__()
            results: list[dict[str, object]] = []
            request_thread = threading.Thread(
                target=lambda: results.append(
                    _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                )
            )
            try:
                request_thread.start()
                self.assertTrue(_wait_for_queue_depth(broker, 1))
                state = broker.close()
                request_thread.join(timeout=1)
            finally:
                gate.release()
                session.__exit__(None, None, None)
                if request_thread.is_alive():
                    request_thread.join(timeout=1)

            self.assertFalse(request_thread.is_alive())
            self.assertEqual(results[0]["status"], "TASK_CANCELLED")
            self.assertEqual(
                state,
                {
                    "drained": True,
                    "active_handlers": 0,
                    "fifo_depth": 0,
                    "remote_unsettled_jobs": 0,
                },
            )
            self.assertEqual(
                broker.drain_state(),
                {
                    "active_handlers": 0,
                    "fifo_depth": 0,
                    "remote_unsettled_jobs": 0,
                },
            )
            self.assertEqual(broker.active_handlers, 0)
            self.assertEqual(broker.fifo_depth, 0)
            self.assertEqual(broker.remote_unsettled_jobs, 0)
            rows = audit_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(rows), 1)
            self.assertEqual(json.loads(rows[0])["status"], "TASK_CANCELLED")

    def test_drain_deadline_boundary_accepts_already_quiet_broker(self) -> None:
        """Do not fail closeout when the last handler exits at the deadline."""

        with tempfile.TemporaryDirectory() as temporary:
            evaluator = _RecordingEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=Path(temporary) / "audit.jsonl",
                min_probe_interval_seconds=0,
            )

            samples = iter(
                [
                    {
                        "active_handlers": 1,
                        "fifo_depth": 0,
                        "remote_unsettled_jobs": 0,
                    },
                    {
                        "active_handlers": 0,
                        "fifo_depth": 0,
                        "remote_unsettled_jobs": 0,
                    },
                ]
            )
            with patch.object(broker, "drain_state", side_effect=lambda: next(samples)):
                state = broker._wait_for_drain(time.monotonic() - 1.0)

            self.assertEqual(
                state,
                {
                    "drained": True,
                    "active_handlers": 0,
                    "fifo_depth": 0,
                    "remote_unsettled_jobs": 0,
                },
            )

    def test_close_cancels_active_evaluator_then_waits_for_handler_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            evaluator = _CancelAwareEvaluator()
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=audit_path,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            session = broker.session(
                actor_id="agent",
                workdir=workdir,
                candidates={"task": (_task(root), candidate)},
                deadline_monotonic=time.monotonic() + 3,
            )
            env = session.__enter__()
            results: list[dict[str, object]] = []
            request_thread = threading.Thread(
                target=lambda: results.append(
                    _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                )
            )
            try:
                request_thread.start()
                self.assertTrue(evaluator.started.wait(timeout=1))
                self.assertEqual(broker.active_handlers, 1)
                state = broker.close()
                request_thread.join(timeout=1)
            finally:
                session.__exit__(None, None, None)
                if request_thread.is_alive():
                    request_thread.join(timeout=1)

            self.assertTrue(evaluator.cancel_observed.is_set())
            self.assertFalse(request_thread.is_alive())
            self.assertEqual(results[0]["status"], "TASK_CANCELLED")
            self.assertEqual(state["active_handlers"], 0)
            self.assertEqual(state["fifo_depth"], 0)
            self.assertEqual(state["remote_unsettled_jobs"], 0)
            self.assertEqual(len(audit_path.read_text().splitlines()), 1)

    def test_unsettled_remote_cancellation_latches_gate_and_fails_closeout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            gate = threading.BoundedSemaphore(1)
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                _UnsettledCancellationEvaluator(),
                gate,
                audit_path=audit_path,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=0.2,
            ).start()
            with broker.session(
                actor_id="agent",
                workdir=workdir,
                candidates={"task": (_task(root), candidate)},
                deadline_monotonic=time.monotonic() + 2,
            ) as env:
                result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})

            self.assertEqual(result["status"], "REMOTE_SETTLEMENT_UNCONFIRMED")
            self.assertEqual(
                result["response"]["judge_cancellation"]["settled"],  # type: ignore[index]
                False,
            )
            self.assertEqual(broker.remote_unsettled_jobs, 1)
            self.assertFalse(gate.acquire(timeout=0))
            started = time.monotonic()
            with self.assertRaises(JudgeBrokerDrainError) as raised:
                broker.close()
            self.assertLess(time.monotonic() - started, 0.15)
            self.assertEqual(
                raised.exception.state,
                {
                    "drained": False,
                    "active_handlers": 0,
                    "fifo_depth": 0,
                    "remote_unsettled_jobs": 1,
                },
            )
            self.assertEqual(len(audit_path.read_text().splitlines()), 1)

    def test_deferred_settlement_retains_gate_until_callback_releases_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text(
                "import Mathlib\ntheorem task : True := by trivial\n"
            )
            evaluator = _DeferredOnlyEvaluator()
            gate = threading.BoundedSemaphore(1)
            broker = JudgeBroker(
                evaluator,
                gate,
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2,
                ) as env:
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                    self.assertEqual(result["status"], "TASK_CANCELLED")
                    self.assertFalse(gate.acquire(blocking=False))
                    callback = evaluator.settlement_callback
                    self.assertTrue(callable(callback))
                    callback()  # type: ignore[operator]
                    self.assertTrue(gate.acquire(timeout=1))
                    gate.release()
            finally:
                broker.close()

    def test_default_closeout_deadline_covers_deferred_watcher_horizon(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, patch(
            "contextswarm_mini.judge_broker._BROKER_DRAIN_TIMEOUT_SECONDS",
            0.02,
        ), patch(
            "contextswarm_mini.judge_broker._BROKER_DRAIN_SETTLEMENT_MARGIN_SECONDS",
            0.02,
        ):
            evaluator = _PendingSettlementEvaluator(0.15)
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=Path(temporary) / "audit.jsonl",
            )

            def settle() -> None:
                time.sleep(0.06)
                evaluator.pending_settlement_watchers = 0

            settlement = threading.Thread(target=settle)
            settlement.start()
            started = time.monotonic()
            state = broker.close()
            elapsed = time.monotonic() - started
            settlement.join(timeout=1)

            self.assertFalse(settlement.is_alive())
            self.assertAlmostEqual(broker.drain_timeout_seconds, 0.17)
            self.assertGreaterEqual(elapsed, 0.05)
            self.assertEqual(
                state,
                {
                    "drained": True,
                    "active_handlers": 0,
                    "fifo_depth": 0,
                    "remote_unsettled_jobs": 0,
                },
            )

    def test_explicit_close_timeout_reports_pending_settlement_watcher(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evaluator = _PendingSettlementEvaluator(300.0)
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=Path(temporary) / "audit.jsonl",
                drain_timeout_seconds=0.03,
            )

            with self.assertRaises(JudgeBrokerDrainError) as raised:
                broker.close()

            self.assertEqual(broker.drain_timeout_seconds, 0.03)
            self.assertEqual(
                raised.exception.state,
                {
                    "drained": False,
                    "active_handlers": 0,
                    "fifo_depth": 0,
                    "remote_unsettled_jobs": 0,
                    "pending_settlement_watchers": 1,
                },
            )

    def test_global_remote_latch_rejects_all_later_sessions_without_admission(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            evaluator = _GlobalUnsettledEvaluator()
            gate = threading.BoundedSemaphore(1)
            broker = JudgeBroker(
                evaluator,
                gate,
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
                drain_timeout_seconds=0.1,
            ).start()
            try:
                with broker.session(
                    actor_id="first",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2,
                ) as env:
                    first = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                with broker.session(
                    actor_id="second",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2,
                ) as env:
                    second = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
            finally:
                with self.assertRaises(JudgeBrokerDrainError):
                    broker.close()

            self.assertEqual(first["status"], "REMOTE_SETTLEMENT_UNCONFIRMED")
            self.assertTrue(first["accepted"])
            self.assertEqual(second["status"], "REMOTE_SETTLEMENT_UNCONFIRMED")
            self.assertFalse(second["accepted"])
            self.assertEqual(len(evaluator.calls), 1)
            self.assertEqual(broker.remote_unsettled_jobs, 1)
            self.assertFalse(gate.acquire(blocking=False))

    def test_evaluator_global_unsettled_count_fails_closeout_without_handler(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evaluator = _RecordingEvaluator()
            evaluator.remote_unsettled_jobs = 2
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=Path(temporary) / "audit.jsonl",
                drain_timeout_seconds=0.2,
            ).start()
            with self.assertRaises(JudgeBrokerDrainError) as raised:
                broker.close()
            self.assertEqual(raised.exception.state["remote_unsettled_jobs"], 2)
            self.assertEqual(raised.exception.state["active_handlers"], 0)
            self.assertEqual(raised.exception.state["fifo_depth"], 0)

    def test_close_timeout_raises_bounded_redacted_fatal_drain_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            evaluator = _BlockingEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
                drain_timeout_seconds=0.05,
            ).start()
            session = broker.session(
                actor_id="operator-private-agent",
                workdir=workdir,
                candidates={"task": (_task(root), candidate)},
                deadline_monotonic=time.monotonic() + 3,
            )
            env = session.__enter__()
            request_thread = threading.Thread(
                target=lambda: _post(
                    env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {}
                )
            )
            request_thread.start()
            self.assertTrue(evaluator.started.wait(timeout=1))
            started = time.monotonic()
            try:
                with self.assertRaises(JudgeBrokerDrainError) as raised:
                    broker.close()
                elapsed = time.monotonic() - started
            finally:
                evaluator.release.set()
                request_thread.join(timeout=1)
                session.__exit__(None, None, None)

            self.assertLess(elapsed, 0.5)
            self.assertFalse(raised.exception.state["drained"])
            self.assertGreaterEqual(raised.exception.state["active_handlers"], 1)
            self.assertEqual(raised.exception.state["fifo_depth"], 0)
            self.assertNotIn("operator-private-agent", str(raised.exception))
            self.assertEqual(
                broker.close(timeout_seconds=1),
                {
                    "drained": True,
                    "active_handlers": 0,
                    "fifo_depth": 0,
                    "remote_unsettled_jobs": 0,
                },
            )

    def test_close_joins_active_handler_before_returning(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            evaluator = _BlockingEvaluator()
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            session = broker.session(
                actor_id="agent",
                workdir=workdir,
                candidates={"task": (_task(root), candidate)},
                deadline_monotonic=time.monotonic() + 3,
            )
            env = session.__enter__()
            results: list[dict[str, object]] = []
            request_thread = threading.Thread(
                target=lambda: results.append(
                    _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                )
            )
            request_thread.start()
            self.assertTrue(evaluator.started.wait(timeout=1))
            session.__exit__(None, None, None)
            close_thread = threading.Thread(target=broker.close)
            close_thread.start()
            time.sleep(0.1)
            self.assertTrue(close_thread.is_alive())
            evaluator.release.set()
            request_thread.join(timeout=2)
            close_thread.join(timeout=2)

            self.assertFalse(request_thread.is_alive())
            self.assertFalse(close_thread.is_alive())
            self.assertEqual(results[0]["status"], "VERIFY_FAIL")
            self.assertEqual(len(audit_path.read_text(encoding="utf-8").splitlines()), 1)

    def test_admission_timeout_is_unaccepted_and_does_not_consume_quota(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            gate = threading.BoundedSemaphore(1)
            self.assertTrue(gate.acquire(timeout=0))
            audit_path = root / "audit.jsonl"
            evaluator = _RecordingEvaluator()
            broker = JudgeBroker(
                evaluator,
                gate,
                audit_path=audit_path,
                max_probe_calls_per_session=1,
                min_probe_interval_seconds=0,
                probe_admission_timeout_seconds=0.05,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3.0,
                ) as env:
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    timed_out = _post(url, "judge_check", {})
                    gate.release()
                    accepted = _post(url, "judge_check", {})
                    exhausted = _post(url, "judge_check", {})
            finally:
                broker.close()

            rows = [
                json.loads(line)
                for line in audit_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(timed_out["status"], "JUDGE_ADMISSION_TIMEOUT")
            self.assertFalse(timed_out["accepted"])
            self.assertTrue(timed_out["retryable"])
            self.assertTrue(accepted["accepted"])
            self.assertEqual(accepted["call_index"], 1)
            self.assertEqual(exhausted["status"], "SESSION_PROBE_BUDGET_EXHAUSTED")
            self.assertFalse(exhausted["accepted"])
            self.assertEqual(len(evaluator.calls), 1)
            self.assertEqual([row["accepted"] for row in rows], [False, True, False])
            self.assertEqual([row["call_index"] for row in rows], [None, 1, None])

    def test_default_admission_wait_uses_remaining_session_horizon(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            gate = threading.BoundedSemaphore(1)
            self.assertTrue(gate.acquire(timeout=0))
            audit_path = root / "audit.jsonl"
            broker = JudgeBroker(
                _RecordingEvaluator(),
                gate,
                audit_path=audit_path,
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 0.08,
                ) as env:
                    started = time.monotonic()
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                    waited = time.monotonic() - started
            finally:
                gate.release()
                broker.close()
            audit = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(result["status"], "OUT_OF_HORIZON")
            self.assertFalse(result["accepted"])
            self.assertFalse(result["retryable"])
            self.assertGreaterEqual(waited, 0.05)
            self.assertFalse(audit["accepted"])
            self.assertIsNone(audit["call_index"])

    def test_broker_admission_is_fifo_across_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evaluator = _OrderedEvaluator()
            gate = threading.BoundedSemaphore(1)
            self.assertTrue(gate.acquire(timeout=0))
            broker = JudgeBroker(
                evaluator,
                gate,
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            workdirs: list[Path] = []
            for slug in ("first", "second"):
                workdir = root / slug
                workdir.mkdir()
                (workdir / "result.lean").write_text(
                    f"import Mathlib\ntheorem {slug} : True := by trivial\n"
                )
                workdirs.append(workdir)
            results: list[dict[str, object]] = []
            deadline = time.monotonic() + 3.0
            try:
                with broker.session(
                    actor_id="first-agent",
                    workdir=workdirs[0],
                    candidates={
                        "first": (_task(root, "first"), workdirs[0] / "result.lean")
                    },
                    deadline_monotonic=deadline,
                ) as first_env, broker.session(
                    actor_id="second-agent",
                    workdir=workdirs[1],
                    candidates={
                        "second": (_task(root, "second"), workdirs[1] / "result.lean")
                    },
                    deadline_monotonic=deadline,
                ) as second_env:
                    first = threading.Thread(
                        target=lambda: results.append(
                            _post(first_env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                        )
                    )
                    second = threading.Thread(
                        target=lambda: results.append(
                            _post(second_env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                        )
                    )
                    first.start()
                    self.assertTrue(_wait_for_queue_depth(broker, 1))
                    second.start()
                    self.assertTrue(_wait_for_queue_depth(broker, 2))
                    gate.release()
                    first.join(timeout=2)
                    second.join(timeout=2)
            finally:
                broker.close()
            self.assertEqual(evaluator.order, ["first", "second"])
            self.assertEqual(len(results), 2)
            self.assertTrue(all(item["accepted"] for item in results))

    def test_worker_visible_verdict_is_bounded_and_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("import Mathlib\ntheorem task : True := by trivial\n")
            broker = JudgeBroker(
                _UnsafeEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 2.0,
                ) as env:
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
            finally:
                broker.close()
            rendered = json.dumps(result, ensure_ascii=False)
            for forbidden in (
                "judge.invalid",
                "test-secret-value",
                "test-bearer-value",
                "test-auth-value",
                "sk-abcdefghijklmnopqrstuv",
                "/home/test",
                "/tmp/private",
                "/run/private",
                "must-not-escape",
            ):
                self.assertNotIn(forbidden, rendered)
            self.assertIn("<redacted-", rendered)
            self.assertIsNone(result["judge_job_id"])
            self.assertIsNone(result["task_contract_sha256"])

    def test_cps_operations_use_bound_actor_and_task(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                _SequenceEvaluator(["VERIFY_FAIL"]),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
            ).start()
            try:
                with (
                    patch.object(store, "create_piece", wraps=store.create_piece) as create_piece,
                    patch.object(store, "send_message", wraps=store.send_message) as send_message,
                    patch.object(store, "ack_message", wraps=store.ack_message) as ack_message,
                    broker.session(
                        actor_id="bound-agent",
                        workdir=workdir,
                        candidates={"task": (_task(root), candidate)},
                        deadline_monotonic=10**12,
                        cps_store=store,
                        communication="blackboard",
                    ) as env,
                ):
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    checkpoint = _post(url, "judge_check", {})
                    self.assertEqual(checkpoint["status"], "VERIFY_FAIL")
                    published = _post(
                        url,
                        "cps_publish",
                        {"kind": "lemma", "title": "route", "body": "use induction"},
                    )
                    runner_kind = _post(
                        url,
                        "cps_publish",
                        {
                            "kind": "validation_result",
                            "title": "forged score",
                            "body": "PROVED",
                        },
                    )
                    sent = _post(url, "cps_send", {"body": "try route"})
                    inbox = _post(url, "cps_inbox", {})
                    message_id = str(inbox["messages"][0]["id"])  # type: ignore[index]
                    acked = _post(url, "cps_ack", {"message_id": message_id})
                    found = _post(url, "cps_search", {"query": "induction"})
                    expected_deadline = int(
                        env["CONTEXTSWARM_BROKER_DEADLINE_EPOCH_MS"]
                    )
            finally:
                broker.close()
            self.assertTrue(published["ok"])
            self.assertEqual(published["piece"]["author"], "bound-agent")  # type: ignore[index]
            self.assertEqual(published["piece"]["task_id"], "task")  # type: ignore[index]
            self.assertEqual(runner_kind["status"], "RUNNER_ONLY_CPS_KIND")
            self.assertTrue(sent["ok"])
            self.assertTrue(acked["acked"])
            self.assertEqual(len(found["items"]), 1)  # type: ignore[arg-type]
            for write in (create_piece, send_message, ack_message):
                self.assertEqual(
                    write.call_args.kwargs["deadline_epoch_ms"],
                    expected_deadline,
                )
                self.assertTrue(callable(write.call_args.kwargs["cancel_guard"]))

    def test_accepted_terminal_feedback_with_missing_provenance_does_not_unlock_cps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="bound-agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3,
                    cps_store=store,
                    communication="blackboard",
                ) as env:
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    feedback = _post(url, "judge_check", {})
                    self.assertEqual(feedback["status"], "VERIFY_FAIL")
                    self.assertTrue(feedback["accepted"])
                    self.assertIsNone(feedback["task_contract_sha256"])
                    blocked = _post(url, "cps_search", {})
            finally:
                broker.close()
            self.assertEqual(blocked["status"], "JUDGE_CHECK_REQUIRED")

    def test_cps_operations_require_a_terminal_judge_checkpoint_per_session(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            evaluator = _SequenceEvaluator(["EVALUATOR_ERROR", "VERIFY_FAIL"])
            gate = threading.BoundedSemaphore(1)
            self.assertTrue(gate.acquire(timeout=0))
            gate_held = True
            broker = JudgeBroker(
                evaluator,
                gate,
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
                probe_admission_timeout_seconds=0.02,
            ).start()
            blocked_calls = (
                ("cps_search", {}),
                ("cps_publish", {"title": "blocked", "body": "must not persist"}),
                ("cps_actors", {}),
                ("cps_send", {"body": "blocked"}),
                ("cps_inbox", {}),
                ("cps_ack", {"message_id": "missing"}),
            )
            try:
                with broker.session(
                    actor_id="bound-agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3,
                    cps_store=store,
                    communication="blackboard",
                ) as env:
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    for operation, payload in blocked_calls:
                        blocked = _post(url, operation, payload)
                        self.assertEqual(blocked["status"], "JUDGE_CHECK_REQUIRED")
                        self.assertFalse(blocked["accepted"])
                    control = _post(url, "judge_check", {})
                    self.assertEqual(control["status"], "JUDGE_ADMISSION_TIMEOUT")
                    self.assertFalse(control["accepted"])
                    gate.release()
                    gate_held = False
                    after_control = _post(url, "cps_search", {})
                    self.assertEqual(after_control["status"], "JUDGE_CHECK_REQUIRED")
                    first = _post(url, "judge_check", {})
                    self.assertEqual(first["status"], "EVALUATOR_ERROR")
                    self.assertTrue(first["accepted"])
                    still_blocked = _post(url, "cps_search", {})
                    self.assertEqual(still_blocked["status"], "JUDGE_CHECK_REQUIRED")
                    terminal = _post(url, "judge_check", {})
                    self.assertEqual(terminal["status"], "VERIFY_FAIL")
                    self.assertTrue(terminal["accepted"])
                    published = _post(
                        url,
                        "cps_publish",
                        {"title": "allowed", "body": "after checkpoint"},
                    )
                    self.assertTrue(published["ok"])
                with broker.session(
                    actor_id="new-agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3,
                    cps_store=store,
                    communication="blackboard",
                ) as env:
                    fresh_session = _post(
                        env["CONTEXTSWARM_JUDGE_URL"], "cps_search", {}
                    )
                    self.assertEqual(fresh_session["status"], "JUDGE_CHECK_REQUIRED")
            finally:
                if gate_held:
                    gate.release()
                broker.close()

            self.assertEqual(len(evaluator.calls), 2)
            self.assertEqual(len(store.search(task_id="task")), 1)

    def test_local_rejected_is_a_terminal_local_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                _SequenceEvaluator(["LOCAL_REJECTED"]),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="bound-agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3,
                    cps_store=store,
                    communication="blackboard",
                ) as env:
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    checkpoint = _post(url, "judge_check", {})
                    self.assertEqual(checkpoint["status"], "LOCAL_REJECTED")
                    self.assertTrue(checkpoint["accepted"])
                    searched = _post(url, "cps_search", {})
            finally:
                broker.close()
            self.assertTrue(searched["ok"])
            self.assertEqual(searched["items"], [])

    def test_non_cps_session_keeps_cps_unavailable_without_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                min_probe_interval_seconds=0,
            ).start()
            try:
                with broker.session(
                    actor_id="mono",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() + 3,
                ) as env:
                    result = _post(env["CONTEXTSWARM_JUDGE_URL"], "cps_search", {})
            finally:
                broker.close()
            self.assertEqual(result["status"], "CPS_UNAVAILABLE")

    def test_cps_operations_fail_closed_after_horizon(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
            ).start()
            try:
                with broker.session(
                    actor_id="bound-agent",
                    workdir=workdir,
                    candidates={"task": (_task(root), candidate)},
                    deadline_monotonic=time.monotonic() - 0.01,
                    cps_store=store,
                    communication="blackboard",
                ) as env:
                    url = env["CONTEXTSWARM_JUDGE_URL"]
                    searched = _post(url, "cps_search", {})
                    published = _post(
                        url,
                        "cps_publish",
                        {"title": "late", "body": "must not persist"},
                    )
            finally:
                broker.close()

            self.assertEqual(searched["status"], "OUT_OF_HORIZON")
            self.assertEqual(published["status"], "OUT_OF_HORIZON")
            self.assertEqual(store.search(task_id="task"), [])

    def test_cps_write_lock_wait_rechecks_horizon_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                _SequenceEvaluator(["VERIFY_FAIL"]),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
            ).start()
            blocker = sqlite3.connect(store.path, timeout=1, isolation_level=None)
            blocker.execute("BEGIN IMMEDIATE")
            entered_store = threading.Event()
            results: list[dict[str, object]] = []
            errors: list[BaseException] = []
            original_create_piece = store.create_piece

            def observed_create_piece(**kwargs: object) -> dict[str, object]:
                entered_store.set()
                return original_create_piece(**kwargs)  # type: ignore[arg-type]

            deadline = time.monotonic() + 0.1
            try:
                with (
                    patch.object(store, "create_piece", side_effect=observed_create_piece),
                    broker.session(
                        actor_id="bound-agent",
                        workdir=workdir,
                        candidates={"task": (_task(root), candidate)},
                        deadline_monotonic=deadline,
                        cps_store=store,
                        communication="blackboard",
                    ) as env,
                ):
                    checkpoint = _post(
                        env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {}
                    )
                    self.assertEqual(checkpoint["status"], "VERIFY_FAIL")

                    def publish() -> None:
                        try:
                            results.append(
                                _post(
                                    env["CONTEXTSWARM_JUDGE_URL"],
                                    "cps_publish",
                                    {"title": "late", "body": "must not persist"},
                                )
                            )
                        except BaseException as exc:  # pragma: no cover - asserted below
                            errors.append(exc)

                    worker = threading.Thread(target=publish)
                    worker.start()
                    self.assertTrue(entered_store.wait(timeout=1))
                    time.sleep(max(0.0, deadline - time.monotonic()) + 0.03)
                    blocker.execute("ROLLBACK")
                    worker.join(timeout=1)
                    self.assertFalse(worker.is_alive())
            finally:
                if blocker.in_transaction:
                    blocker.execute("ROLLBACK")
                blocker.close()
                broker.close()

            self.assertEqual(errors, [])
            self.assertEqual(results[0]["status"], "OUT_OF_HORIZON")
            self.assertEqual(store.search(task_id="task"), [])

    def test_cps_write_lock_wait_rechecks_task_cancellation_before_commit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                _SequenceEvaluator(["VERIFY_FAIL"]),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
            ).start()
            blocker = sqlite3.connect(store.path, timeout=1, isolation_level=None)
            blocker.execute("BEGIN IMMEDIATE")
            cancelled = threading.Event()
            entered_store = threading.Event()
            results: list[dict[str, object]] = []
            errors: list[BaseException] = []
            original_create_piece = store.create_piece

            def observed_create_piece(**kwargs: object) -> dict[str, object]:
                entered_store.set()
                return original_create_piece(**kwargs)  # type: ignore[arg-type]

            try:
                with (
                    patch.object(store, "create_piece", side_effect=observed_create_piece),
                    broker.session(
                        actor_id="bound-agent",
                        workdir=workdir,
                        candidates={"task": (_task(root), candidate)},
                        deadline_monotonic=time.monotonic() + 5,
                        cps_store=store,
                        communication="blackboard",
                        cancel_event=cancelled,
                    ) as env,
                ):
                    checkpoint = _post(
                        env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {}
                    )
                    self.assertEqual(checkpoint["status"], "VERIFY_FAIL")

                    def publish() -> None:
                        try:
                            results.append(
                                _post(
                                    env["CONTEXTSWARM_JUDGE_URL"],
                                    "cps_publish",
                                    {"title": "cancelled", "body": "must not persist"},
                                )
                            )
                        except BaseException as exc:  # pragma: no cover - asserted below
                            errors.append(exc)

                    worker = threading.Thread(target=publish)
                    worker.start()
                    self.assertTrue(entered_store.wait(timeout=1))
                    cancelled.set()
                    blocker.execute("ROLLBACK")
                    worker.join(timeout=1)
                    self.assertFalse(worker.is_alive())
            finally:
                if blocker.in_transaction:
                    blocker.execute("ROLLBACK")
                blocker.close()
                broker.close()

            self.assertEqual(errors, [])
            self.assertEqual(results[0]["status"], "TASK_CANCELLED")
            self.assertEqual(store.search(task_id="task"), [])

    def test_cps_request_with_resolved_claim_fails_after_session_revocation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            candidate = workdir / "result.lean"
            candidate.write_text("proof")
            store = CPSStore(root / "cps.sqlite3")
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
            ).start()
            entered_operation = threading.Event()
            release_operation = threading.Event()
            results: list[dict[str, object]] = []
            errors: list[BaseException] = []
            original_operation = broker._cps_operation  # noqa: SLF001
            session = broker.session(
                actor_id="bound-agent",
                workdir=workdir,
                candidates={"task": (_task(root), candidate)},
                deadline_monotonic=time.monotonic() + 5,
                cps_store=store,
                communication="blackboard",
            )
            env = session.__enter__()
            session_closed = False

            def delayed_operation(*args: object, **kwargs: object) -> dict[str, object]:
                entered_operation.set()
                release_operation.wait(timeout=1)
                return original_operation(*args, **kwargs)  # type: ignore[arg-type]

            def search() -> None:
                try:
                    results.append(
                        _post(env["CONTEXTSWARM_JUDGE_URL"], "cps_search", {})
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            worker = threading.Thread(target=search)
            try:
                with patch.object(broker, "_cps_operation", side_effect=delayed_operation):
                    worker.start()
                    self.assertTrue(entered_operation.wait(timeout=1))
                    session.__exit__(None, None, None)
                    session_closed = True
                    release_operation.set()
                    worker.join(timeout=1)
                    self.assertFalse(worker.is_alive())
            finally:
                release_operation.set()
                if worker.is_alive():
                    worker.join(timeout=1)
                if not session_closed:
                    session.__exit__(None, None, None)
                broker.close()

            self.assertEqual(errors, [])
            self.assertEqual(results[0]["status"], "TASK_CANCELLED")
            self.assertEqual(store.search(task_id="task"), [])

    def test_candidate_must_match_task_filename_inside_assigned_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workdir = root / "worker"
            workdir.mkdir()
            outside = root / "result.lean"
            outside.write_text("proof")
            wrong_formal = workdir / "result.cpp"
            wrong_formal.write_text("int main() {}\n")
            wrong_coding = workdir / "result.lean"
            wrong_coding.write_text("proof\n")
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
            ).start()
            try:
                with self.assertRaises(ValueError):
                    with broker.session(
                        actor_id="agent",
                        workdir=workdir,
                        candidates={"task": (_task(root), outside)},
                        deadline_monotonic=10**12,
                    ):
                        pass
                with self.assertRaises(ValueError):
                    with broker.session(
                        actor_id="agent",
                        workdir=workdir,
                        candidates={"task": (_task(root), wrong_formal)},
                        deadline_monotonic=10**12,
                    ):
                        pass
                with self.assertRaises(ValueError):
                    with broker.session(
                        actor_id="agent",
                        workdir=workdir,
                        candidates={"task": (_coding_task(root), wrong_coding)},
                        deadline_monotonic=10**12,
                    ):
                        pass
            finally:
                broker.close()

    def test_public_policy_declares_task_bound_candidate_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            broker = JudgeBroker(
                _RecordingEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=Path(temporary) / "audit.jsonl",
            )
            policy = broker.public_policy()

        self.assertEqual(
            policy["candidate_selection"],
            "runner_bound_task_candidate_filename",
        )
        self.assertEqual(
            policy["allowed_candidate_filenames"],
            ["result.cpp", "result.lean"],
        )


if __name__ == "__main__":
    unittest.main()
