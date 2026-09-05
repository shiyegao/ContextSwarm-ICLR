from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from dataclasses import replace
from pathlib import Path
import sqlite3
import stat
import subprocess
import tempfile
import unittest
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

from contextswarm_mini.config import load_config
from contextswarm_mini.formal_tools import (
    DeclarationIndex,
    FormalToolPolicy,
    ToolCapability,
    prepare_declaration_index,
    public_files_manifest,
    stage_worker_tools,
)
from contextswarm_mini.judge_broker import JudgeBroker, JudgeBrokerDrainError
from contextswarm_mini.models import Task, Verdict
from contextswarm_mini.pi_agent import PiAgent


ROOT = Path(__file__).resolve().parents[1]


class _DiagnosticEvaluator:
    """Small evaluator double with the same provenance surface as LeanEvaluator."""

    is_mock_evaluator = False

    def __init__(self, *, remote_unsettled: bool = False) -> None:
        self.calls = 0
        self.remote_unsettled = remote_unsettled

    def expected_task_contract_sha256(self, task: Task) -> str:
        digest = hashlib.sha256()
        for value in (task.slug, task.problem_id, task.theorem_name, task.baseline_code):
            digest.update(value.encode("utf-8"))
            digest.update(b"\0")
        return digest.hexdigest()

    def probe_source(
        self,
        task: Task,
        source: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: object | None = None,
    ) -> Verdict:
        del deadline_monotonic, cancel_event
        self.calls += 1
        contract = self.expected_task_contract_sha256(task)
        if self.remote_unsettled:
            return Verdict(
                task.slug,
                "REMOTE_SETTLEMENT_UNCONFIRMED",
                0.0,
                0.0,
                {"remote_settlement_unconfirmed": True},
                candidate_sha256=hashlib.sha256(source.encode()).hexdigest(),
                task_contract_sha256=contract,
            )
        return Verdict(
            task.slug,
            "PROVED",
            1.0,
            0.001,
            {
                "is_valid_with_sorry": True,
                "is_valid_no_sorry": "sorry" not in source,
                "probe_diagnostics": [],
            },
            candidate_sha256=hashlib.sha256(source.encode()).hexdigest(),
            task_contract_sha256=contract,
            judge_job_id=f"job-{self.calls}",
        )


def _task(root: Path, slug: str = "task") -> Task:
    source = root / slug
    (source / "baseline").mkdir(parents=True)
    baseline = "import Mathlib\ntheorem task : True := by\n  sorry\n"
    (source / "baseline" / "task.lean").write_text(baseline, encoding="utf-8")
    return Task(
        slug=slug,
        root=source,
        problem_text="Prove True.",
        baseline_code=baseline,
        metadata={"problem_id": slug, "theorem_name": "task"},
    )


def _stage(task: Task, workspace: Path, *, surface: str = "formal-test-v1") -> None:
    workspace.mkdir(parents=True)
    (workspace / "problem.md").write_text(task.problem_text, encoding="utf-8")
    (workspace / "metadata.json").write_text(json.dumps(task.metadata), encoding="utf-8")
    (workspace / "baseline").mkdir()
    (workspace / "baseline" / "task.lean").write_text(task.baseline_code, encoding="utf-8")
    (workspace / "result.lean").write_text(
        task.baseline_code.replace("sorry", "trivial"), encoding="utf-8"
    )
    stage_worker_tools(
        workspace,
        capability=ToolCapability(task_id=task.slug, surface_version=surface),
        baseline_names=["task.lean"],
    )


def _policy(index: DeclarationIndex | None = None, **overrides: int) -> FormalToolPolicy:
    values = {
        "evaluate_calls_per_task": 8,
        "evaluate_backend_jobs_per_task": 8,
        "query_calls_per_task": 8,
        "query_backend_probes_per_task": 8,
    }
    values.update(overrides)
    return FormalToolPolicy(
        enabled=True,
        surface_version="formal-test-v1",
        max_candidate_bytes=1024 * 1024,
        command_timeout_seconds=30,
        declaration_index=index or DeclarationIndex(None),
        **values,
    )


def _post(url: str, operation: str, payload: dict[str, object]) -> dict[str, object]:
    request = Request(
        f"{url}/{operation}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


class FormalToolBrokerTests(unittest.TestCase):
    def _fixture(
        self,
        root: Path,
        *,
        evaluator: _DiagnosticEvaluator | None = None,
        policy: FormalToolPolicy | None = None,
        tasks: list[Task] | None = None,
    ) -> tuple[JudgeBroker, _DiagnosticEvaluator, list[Task], Path]:
        evaluator = evaluator or _DiagnosticEvaluator()
        tasks = tasks or [_task(root)]
        workspace = root / "workspace"
        _stage(tasks[0], workspace)
        broker = JudgeBroker(
            evaluator,
            __import__("threading").BoundedSemaphore(1),
            audit_path=root / "judge_checks.jsonl",
            formal_policy=policy or _policy(),
            formal_audit_path=root / "formal_tool_calls.jsonl",
            drain_timeout_seconds=0.25,
        ).start()
        return broker, evaluator, tasks, workspace

    def test_helpers_are_real_and_diagnostic_only(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            broker, evaluator, tasks, workspace = self._fixture(root)
            callback_rows: list[object] = []
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workspace,
                    candidates={tasks[0].slug: (tasks[0], workspace / "result.lean")},
                    deadline_monotonic=10**9,
                    on_authoritative_verdict=lambda *args: callback_rows.append(args),
                ) as env:
                    child_env = dict(os.environ)
                    child_env.update(env)
                    child_env["PATH"] = "/usr/local/bin:/usr/bin:/bin"
                    child_env["PYTHONPATH"] = str(ROOT)
                    evaluated = subprocess.run(
                        ["python3", "evaluate.py"],
                        cwd=workspace,
                        env=child_env,
                        capture_output=True,
                        text=True,
                        timeout=5,
                        check=False,
                    )
                    self.assertEqual(evaluated.returncode, 0, evaluated.stderr)
                    feedback = json.loads(evaluated.stdout)
                    self.assertEqual(feedback["status"], "PROVED")
                    self.assertTrue(feedback["advisory_only"])
                    self.assertFalse(feedback["official_score_eligible"])
                    self.assertEqual(callback_rows, [])

                    scout = subprocess.run(
                        ["./formal_query", "check", "Nat.succ"],
                        cwd=workspace,
                        env=child_env,
                        capture_output=True,
                        text=True,
                        timeout=5,
                        check=False,
                    )
                    self.assertEqual(scout.returncode, 0, scout.stderr)
                    self.assertEqual(json.loads(scout.stdout)["status"], "elaborated")
                    official = _post(env["CONTEXTSWARM_JUDGE_URL"], "judge_check", {})
                    self.assertTrue(official["proved"])
                    self.assertEqual(len(callback_rows), 1)
                self.assertEqual(evaluator.calls, 3)
                self.assertTrue((root / "formal_tool_calls.jsonl").read_text())
                self.assertEqual((root / "judge_checks.jsonl").read_text().count("judge_check"), 1)
            finally:
                broker.close()

    def test_task_global_quota_and_cache_hits(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            policy = _policy(
                evaluate_calls_per_task=3,
                evaluate_backend_jobs_per_task=1,
                query_calls_per_task=3,
                query_backend_probes_per_task=1,
            )
            broker, evaluator, tasks, workspace = self._fixture(root, policy=policy)
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workspace,
                    candidates={tasks[0].slug: (tasks[0], workspace / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    first = _post(env["CONTEXTSWARM_JUDGE_URL"], "evaluate_local", {})
                    cached = _post(env["CONTEXTSWARM_JUDGE_URL"], "evaluate_local", {})
                    self.assertEqual(first["status"], "PROVED")
                    self.assertTrue(cached["cache_hit"])
                    (workspace / "result.lean").write_text(
                        tasks[0].baseline_code.replace("sorry", "exact True.intro"),
                        encoding="utf-8",
                    )
                    exhausted = _post(env["CONTEXTSWARM_JUDGE_URL"], "evaluate_local", {})
                    self.assertEqual(exhausted["status"], "BUDGET_EXHAUSTED")

                    query = {"command": "check", "query": ["Nat.succ"]}
                    first_query = _post(env["CONTEXTSWARM_JUDGE_URL"], "formal_query", query)
                    cached_query = _post(env["CONTEXTSWARM_JUDGE_URL"], "formal_query", query)
                    exhausted_query = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "formal_query",
                        {"command": "check", "query": ["Nat.pred"]},
                    )
                    self.assertEqual(first_query["status"], "elaborated")
                    self.assertTrue(cached_query["cache_hit_count"])
                    self.assertEqual(exhausted_query["status"], "probe_budget_exhausted")
                self.assertEqual(evaluator.calls, 2)
                summary = broker.formal_summary()["tasks"][tasks[0].slug]
                self.assertEqual(summary["evaluate_calls"], 3)
                self.assertEqual(summary["evaluate_backend_jobs"], 1)
                self.assertEqual(summary["query_backend_probes"], 1)
            finally:
                broker.close()

    def test_mono_requires_task_id_and_capability_revocation_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            first = _task(root, "one")
            second = _task(root, "two")
            workspace = root / "mono"
            _stage(first, workspace / "tasks" / first.slug)
            _stage(second, workspace / "tasks" / second.slug)
            evaluator = _DiagnosticEvaluator()
            broker = JudgeBroker(
                evaluator,
                __import__("threading").BoundedSemaphore(1),
                audit_path=root / "audit.jsonl",
                formal_policy=_policy(),
                drain_timeout_seconds=1,
            ).start()
            try:
                with broker.session(
                    actor_id="mono",
                    workdir=workspace,
                    candidates={
                        first.slug: (first, workspace / "tasks" / first.slug / "result.lean"),
                        second.slug: (second, workspace / "tasks" / second.slug / "result.lean"),
                    },
                    deadline_monotonic=10**9,
                ) as env:
                    missing = _post(env["CONTEXTSWARM_JUDGE_URL"], "evaluate_local", {})
                    self.assertEqual(missing["status"], "INVALID_TASK_SELECTION")
                    selected = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "evaluate_local",
                        {"task_id": first.slug},
                    )
                    self.assertEqual(selected["status"], "PROVED")
                    capability_url = env["CONTEXTSWARM_JUDGE_URL"]
                with self.assertRaises(HTTPError) as revoked:
                    _post(capability_url, "evaluate_local", {"task_id": first.slug})
                revoked.exception.close()
            finally:
                broker.close()

    def test_remote_unsettled_retains_gate_and_closeout_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            evaluator = _DiagnosticEvaluator(remote_unsettled=True)
            broker, _evaluator, tasks, workspace = self._fixture(root, evaluator=evaluator)
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workspace,
                    candidates={tasks[0].slug: (tasks[0], workspace / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    first = _post(env["CONTEXTSWARM_JUDGE_URL"], "evaluate_local", {})
                    self.assertEqual(first["status"], "REMOTE_SETTLEMENT_UNCONFIRMED")
                    second = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "formal_query",
                        {"command": "check", "query": ["Nat.succ"]},
                    )
                    self.assertEqual(second["status"], "REMOTE_SETTLEMENT_UNCONFIRMED")
                with self.assertRaises(JudgeBrokerDrainError):
                    broker.close()
            finally:
                if broker._server is not None:
                    try:
                        broker.close()
                    except JudgeBrokerDrainError:
                        pass


class DeclarationIndexTests(unittest.TestCase):
    def _index(self, root: Path, revision: str = "rev-1") -> tuple[Path, str]:
        path = root / "decls.sqlite3"
        connection = sqlite3.connect(path)
        try:
            connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            connection.execute(
                "CREATE TABLE decls (name TEXT, kind TEXT, file TEXT, line INTEGER, head TEXT, snippet TEXT)"
            )
            connection.executemany(
                "INSERT INTO meta VALUES (?, ?)",
                [("schema", "decl_index_v1"), ("mathlib_revision", revision), ("lean_toolchain", "v4.9")],
            )
            connection.executemany(
                "INSERT INTO decls VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("Nat.succ", "def", "Mathlib/Nat.lean", 1, "Nat.succ", "Nat.succ : Nat -> Nat"),
                    ("task", "theorem", "Answers/task.lean", 1, "task", "task : True"),
                    ("private_helper", "lemma", "private/answer.lean", 1, "private", "secret"),
                ],
            )
            connection.commit()
        finally:
            connection.close()
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def test_index_search_is_revision_bound_and_filters_guarded_records(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path, digest = self._index(Path(raw))
            index = DeclarationIndex(path, expected_sha256=digest, expected_revision="rev-1")
            self.assertTrue(index.info.compatible)
            self.assertEqual([row["name"] for row in index.search("Nat succ", limit=5, guarded_names={"task"})], ["Nat.succ"])
            self.assertEqual(index.search("private", limit=5), [])
            self.assertFalse(
                DeclarationIndex(path, expected_sha256=digest, expected_revision="rev-2").info.compatible
            )

    def test_snapshot_is_private_content_addressed_and_rejects_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source, digest = self._index(root)
            config = load_config("configs/smoke.toml", ROOT)
            config = replace(
                config,
                formal_tools_decl_index=str(source),
                formal_tools_decl_index_sha256=digest,
                formal_tools_mathlib_revision="rev-1",
            )
            snapshot = prepare_declaration_index(config, root / "private")
            self.assertEqual(snapshot.info.sha256, digest)
            self.assertEqual(stat.S_IMODE(snapshot.path.stat().st_mode), 0o400)
            self.assertNotEqual(snapshot.path, source)
            source.write_bytes(b"changed")
            self.assertEqual([row["name"] for row in snapshot.search("Nat.succ", limit=2)], ["Nat.succ"])

            link = root / "link.sqlite3"
            link.symlink_to(snapshot.path)
            linked = replace(config, formal_tools_decl_index=str(link))
            with self.assertRaises(OSError):
                prepare_declaration_index(linked, root / "private-link")


class PiEnvironmentTests(unittest.TestCase):
    def test_worker_environment_drops_ambient_path_and_pythonpath(self) -> None:
        config = load_config("configs/smoke.toml", ROOT)
        with tempfile.TemporaryDirectory() as raw, patch.dict(
            os.environ,
            {"PATH": "/operator/shadow", "PYTHONPATH": "/operator/imports", "LEAN_AUTH_TOKEN": "secret"},
            clear=False,
        ):
            env = PiAgent(config).environment(
                task_id="task", actor_id="actor", workdir=Path(raw)
            )
        self.assertEqual(env["PATH"], "/usr/local/bin:/usr/bin:/bin")
        self.assertEqual(env["PYTHONPATH"], str(ROOT))
        self.assertNotIn("LEAN_AUTH_TOKEN", env)

    def test_worker_environment_and_public_helper_follow_configured_timeout_cap(self) -> None:
        config = load_config("configs/smoke.toml", ROOT)
        config = replace(
            config,
            lean_timeout_seconds=600,
            formal_tools_command_timeout_seconds=720,
        )
        with tempfile.TemporaryDirectory() as raw:
            env = PiAgent(config).environment(
                task_id="task", actor_id="actor", workdir=Path(raw)
            )
        self.assertEqual(env["CONTEXTSWARM_AGENT_TIMEOUT_MAX_SECONDS"], "600")
        public = public_files_manifest(
            baseline_names=["task.lean"],
            agent_timeout_enabled=True,
            agent_timeout_cap_seconds=600,
        )
        self.assertIn("5–600 second range", public)
        self.assertNotIn("5-300", public)

    def test_staged_client_transport_ceiling_follows_configured_timeout_cap(self) -> None:
        class _Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, _limit):
                return b"{}"

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            stage_worker_tools(
                root,
                capability=ToolCapability(
                    task_id="task", surface_version="formal-test-v1"
                ),
                baseline_names=["task.lean"],
                agent_timeout_enabled=True,
                agent_timeout_cap_seconds=600,
            )
            module_path = root / "_contextswarm_tool_client.py"
            spec = importlib.util.spec_from_file_location(
                "contextswarm_generated_tool_client", module_path
            )
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            observed: dict[str, float] = {}

            def fake_urlopen(_request, *, timeout):
                observed["timeout"] = float(timeout)
                return _Response()

            token = "a" * 43
            with patch.object(module, "urlopen", fake_urlopen), patch.dict(
                os.environ,
                {
                    "CONTEXTSWARM_JUDGE_URL": f"http://127.0.0.1:12345/{token}",
                    "CONTEXTSWARM_AGENT_TIMEOUT_MAX_SECONDS": "600",
                    "CONTEXTSWARM_BROKER_DEADLINE_EPOCH_MS": str(
                        int((time.time() + 3600) * 1000)
                    ),
                },
                clear=False,
            ):
                self.assertEqual(
                    module.request(str(root / "evaluate.py"), "evaluate_local", {}),
                    {},
                )
        self.assertGreaterEqual(observed["timeout"], 719.9)
        self.assertLessEqual(observed["timeout"], 720.1)


if __name__ == "__main__":
    unittest.main()
