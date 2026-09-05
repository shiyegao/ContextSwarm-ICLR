from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

from contextswarm_mini.config import load_config
from contextswarm_mini.evaluator import LeanEvaluator, safe_worker_response
from contextswarm_mini.formal_tools import DeclarationIndex, FormalToolPolicy
from contextswarm_mini.judge_broker import JudgeBroker
from contextswarm_mini.models import Task, Verdict
from contextswarm_mini.prompts import build_task_prompt
from contextswarm_mini.profiling import RunProfiler
from contextswarm_mini.timeout_policy import agent_timeout_bounds, normalize_agent_timeout


ROOT = Path(__file__).resolve().parents[1]


def _post(url: str, operation: str, payload: dict[str, object]) -> dict[str, object]:
    request = Request(
        f"{url}/{operation}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        value = json.loads(response.read().decode("utf-8"))
    assert isinstance(value, dict)
    return value


class _TimeoutEvaluator:
    is_mock_evaluator = False
    timeout_seconds = 300

    def __init__(self) -> None:
        self.timeouts: list[int | None] = []

    def expected_task_contract_sha256(self, task: Task) -> str:
        return hashlib.sha256(task.slug.encode("utf-8")).hexdigest()

    def probe_source(
        self,
        task: Task,
        source: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: object | None = None,
        timeout_seconds: int | None = None,
    ) -> Verdict:
        del source, deadline_monotonic, cancel_event
        self.timeouts.append(timeout_seconds)
        return Verdict(
            task.slug,
            "VERIFY_FAIL",
            0.0,
            0.01,
            {},
            candidate_sha256="a" * 64,
            task_contract_sha256=self.expected_task_contract_sha256(task),
            judge_job_id=f"job-{len(self.timeouts)}",
        )


def _task(root: Path) -> Task:
    source = "import Mathlib\ntheorem task : True := by sorry\n"
    return Task(
        slug="task",
        root=root,
        problem_text="Prove True.",
        baseline_code=source,
        metadata={"problem_id": "task", "theorem_name": "task"},
    )


def _policy() -> FormalToolPolicy:
    return FormalToolPolicy(
        enabled=True,
        surface_version="adaptive-timeout-test-v1",
        evaluate_calls_per_task=8,
        evaluate_backend_jobs_per_task=8,
        query_calls_per_task=8,
        query_backend_probes_per_task=8,
        max_candidate_bytes=1024 * 1024,
        command_timeout_seconds=30,
        declaration_index=DeclarationIndex(None),
    )


class AdaptiveTimeoutTests(unittest.TestCase):
    def test_enabled_prompt_requires_deliberate_budget_choice_and_keeps_baseline_quiet(self) -> None:
        task = _task(ROOT)
        enabled = build_task_prompt(
            task,
            task_workspace="tasks/task",
            agent_id="worker-task-e1",
            episode=1,
            communication_enabled=False,
            formal_tools_enabled=True,
            agent_timeout_enabled=True,
        )
        disabled = build_task_prompt(
            task,
            task_workspace="tasks/task",
            agent_id="worker-task-e1",
            episode=1,
            communication_enabled=False,
            formal_tools_enabled=True,
        )
        self.assertIn("Agent-proposed validation budget", enabled)
        self.assertIn("timeout_seconds", enabled)
        self.assertIn("evaluate.py --timeout", enabled)
        self.assertIn("EXECUTION_TIMEOUT", enabled)
        self.assertNotIn("Agent-proposed validation budget", disabled)

    def test_prompt_uses_configured_timeout_cap_for_guidance(self) -> None:
        task = _task(ROOT)
        prompt = build_task_prompt(
            task,
            task_workspace="tasks/task",
            agent_id="worker-task-e1",
            episode=1,
            communication_enabled=False,
            formal_tools_enabled=True,
            agent_timeout_enabled=True,
            agent_timeout_cap_seconds=600,
        )
        self.assertIn("range 5–600", prompt)
        self.assertIn('"timeout_seconds": 120', prompt)
        self.assertIn("reserve the full 600 seconds", prompt)
        self.assertNotIn("reserve the full 300 seconds", prompt)

    def test_prompt_does_not_claim_unrounded_percentages_for_tiny_cap(self) -> None:
        task = _task(ROOT)
        prompt = build_task_prompt(
            task,
            task_workspace="tasks/task",
            agent_id="worker-task-e1",
            episode=1,
            communication_enabled=False,
            formal_tools_enabled=True,
            agent_timeout_enabled=True,
            agent_timeout_cap_seconds=3,
        )
        self.assertIn("range 3–3 seconds", prompt)
        self.assertIn("rounded for this configured cap", prompt)

    def test_prompt_omits_formal_helper_when_surface_is_disabled(self) -> None:
        task = _task(ROOT)
        prompt = build_task_prompt(
            task,
            task_workspace="tasks/task",
            agent_id="worker-task-e1",
            episode=1,
            communication_enabled=False,
            formal_tools_enabled=False,
            agent_timeout_enabled=True,
            agent_timeout_cap_seconds=600,
        )
        self.assertIn("Agent-proposed validation budget", prompt)
        self.assertNotIn("evaluate.py --timeout", prompt)

    def test_solver_schema_and_formal_guard_follow_capability_bit(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable")
        harness = r"""
import { pathToFileURL } from "node:url";
const [extensionPath, workdir] = process.argv.slice(1);
const listeners = new Map();
const definitions = {};
const pi = {
  on(name, callback) { listeners.set(name, callback); },
  registerTool(definition) { definitions[definition.name] = definition; },
};
const extension = await import(pathToFileURL(extensionPath).href);
extension.default(pi);
const guard = listeners.get("tool_call");
const check = async (command) => (await guard(
  { toolName: "bash", input: { command } },
  { cwd: workdir },
))?.block === true;
process.stdout.write(JSON.stringify({
  schema: definitions.judge_check.parameters.properties.timeout_seconds ?? null,
  enabled_command_blocked: await check("python3 evaluate.py --timeout 60"),
  malformed_command_blocked: await check("python3 evaluate.py --timeout nope"),
}));
"""
        with tempfile.TemporaryDirectory() as temporary:
            workdir = Path(temporary)
            (workdir / "evaluate.py").write_text("# staged helper\n", encoding="utf-8")
            base_env = os.environ | {
                "CONTEXTSWARM_WORKDIR": str(workdir),
                "CONTEXTSWARM_CANDIDATE_FILENAME": "result.lean",
                "PATH": "/usr/local/bin:/usr/bin:/bin",
            }
            enabled = subprocess.run(
                [
                    node,
                    "--input-type=module",
                    "--eval",
                    harness,
                    str(ROOT / "contextswarm_mini" / "pi_solver_tools.mjs"),
                    str(workdir),
                ],
                cwd=workdir,
                env=base_env
                | {
                    "CONTEXTSWARM_AGENT_TIMEOUT_ENABLED": "1",
                    "CONTEXTSWARM_AGENT_TIMEOUT_MAX_SECONDS": "120",
                },
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            disabled = subprocess.run(
                [
                    node,
                    "--input-type=module",
                    "--eval",
                    harness,
                    str(ROOT / "contextswarm_mini" / "pi_solver_tools.mjs"),
                    str(workdir),
                ],
                cwd=workdir,
                env=base_env | {"CONTEXTSWARM_AGENT_TIMEOUT_ENABLED": "0"},
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
        self.assertEqual(enabled.returncode, 0, enabled.stderr)
        enabled_value = json.loads(enabled.stdout)
        self.assertEqual(enabled_value["schema"]["minimum"], 5)
        # Leave the upper bound out of the client-side JSON schema so values
        # above it reach the broker and are auditable as soft-control clamps.
        self.assertNotIn("maximum", enabled_value["schema"])
        self.assertIn("5-120", enabled_value["schema"]["description"])
        self.assertFalse(enabled_value["enabled_command_blocked"])
        self.assertTrue(enabled_value["malformed_command_blocked"])
        self.assertEqual(disabled.returncode, 0, disabled.stderr)
        disabled_value = json.loads(disabled.stdout)
        self.assertIsNone(disabled_value["schema"])
        self.assertTrue(disabled_value["enabled_command_blocked"])

    def test_normalization_clamps_both_bounds_and_honors_evaluator_cap(self) -> None:
        self.assertEqual(normalize_agent_timeout(60).effective_seconds, 60)
        self.assertEqual(normalize_agent_timeout(999).effective_seconds, 300)
        self.assertTrue(normalize_agent_timeout(999).clamped)
        self.assertEqual(normalize_agent_timeout(1).effective_seconds, 5)
        self.assertEqual(
            normalize_agent_timeout(300, configured_timeout_seconds=30).effective_seconds,
            30,
        )
        self.assertEqual(
            normalize_agent_timeout(999, configured_timeout_seconds=600).effective_seconds,
            600,
        )
        self.assertEqual(
            normalize_agent_timeout(999, configured_timeout_seconds=3).effective_seconds,
            3,
        )
        self.assertEqual(agent_timeout_bounds(3).min_seconds, 3)
        self.assertEqual(agent_timeout_bounds(600).max_seconds, 600)
        with self.assertRaises(ValueError):
            normalize_agent_timeout(True)

    def test_broker_policy_uses_a_larger_manifest_cap(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")

            class ConflictingCapEvaluator(_TimeoutEvaluator):
                # The runner's manifest value must be authoritative even for
                # a narrow/mock adapter that exposes a stale or missing
                # evaluator timeout attribute.
                timeout_seconds = 300

            evaluator = ConflictingCapEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                agent_timeout_cap_seconds=600,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                self.assertEqual(
                    broker.public_policy()["agent_timeout"]["max_seconds"], 600
                )
                with broker.session(
                    actor_id="worker",
                    workdir=workdir,
                    candidates={task.slug: (task, workdir / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    result = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "judge_check",
                        {"timeout_seconds": 999},
                    )
                self.assertEqual(result["requested_timeout_seconds"], 999)
                self.assertEqual(result["effective_timeout_seconds"], 600)
                self.assertEqual(evaluator.timeouts, [600])
            finally:
                broker.close()

    def test_broker_budget_projection_cannot_be_overwritten_by_nested_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evaluator = _TimeoutEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                agent_timeout_cap_seconds=600,
                drain_timeout_seconds=1,
            )
            timeout = normalize_agent_timeout(
                600, configured_timeout_seconds=600
            )
            started = time.monotonic() - 30.0
            result = broker._attach_timeout(
                {
                    "accepted": True,
                    "response": {
                        "timeout_budget_mode": "forged",
                        "timeout_budget_seconds": 999_999,
                        "timeout_budget_elapsed_seconds": 999_999.0,
                        "timeout_budget_remaining_seconds": 999_999.0,
                    },
                },
                timeout,
                timeout_started=started,
                timeout_deadline=time.monotonic() + 570.0,
            )
        self.assertEqual(result["timeout_budget_mode"], "cumulative_total")
        self.assertEqual(result["timeout_budget_seconds"], 600)
        self.assertLessEqual(result["timeout_budget_elapsed_seconds"], 600.0)
        self.assertLessEqual(result["timeout_budget_remaining_seconds"], 600.0)

    def test_nested_timeout_budget_scalars_are_capped_for_worker_feedback(self) -> None:
        safe = safe_worker_response(
            {
                "timeout_budget_seconds": 999_999,
                "timeout_budget_elapsed_seconds": 999_999.0,
                "timeout_budget_remaining_seconds": 999_999.0,
            },
            timeout_max_seconds=600,
        )
        self.assertEqual(safe["timeout_budget_seconds"], 600.0)
        self.assertEqual(safe["timeout_budget_elapsed_seconds"], 600.0)
        self.assertEqual(safe["timeout_budget_remaining_seconds"], 600.0)

    def test_loaded_manifest_derives_helper_guard_for_a_larger_cap(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "larger-cap.toml"
            path.write_text(
                f'extends = ["{ROOT / "configs" / "smoke.toml"}"]\n'
                "\n[judge]\nagent_timeout_enabled = true\ntimeout_seconds = 600\n"
                "\n[formal_tools]\ncommand_timeout_seconds = 420\n",
                encoding="utf-8",
            )
            config = load_config(path, ROOT)
        self.assertEqual(config.lean_timeout_seconds, 600)
        # The Pi Bash guard gets a 120-second handoff margin, so the helper
        # cannot be killed before the configured Agent/Judge budget expires.
        self.assertEqual(config.formal_tools_command_timeout_seconds, 720)

    def test_treatment_config_advertises_capability_and_baseline_does_not(self) -> None:
        baseline = load_config("configs/formal_1h_cps32_profiled_clean.toml", ROOT)
        treatment = load_config(
            "configs/formal_1h_cps32_profiled_adaptive_timeout.toml", ROOT
        )
        self.assertFalse(baseline.judge_agent_timeout_enabled)
        self.assertTrue(treatment.judge_agent_timeout_enabled)
        self.assertFalse(baseline.public_dict()["judge_agent_timeout_enabled"])
        self.assertTrue(treatment.public_dict()["judge_agent_timeout_enabled"])
        self.assertEqual(baseline.lean_timeout_seconds, treatment.lean_timeout_seconds)
        self.assertEqual(baseline.max_parallel, treatment.max_parallel)
        self.assertEqual(baseline.time_limit_seconds, treatment.time_limit_seconds)

    def test_broker_clamps_and_audits_judge_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            evaluator = _TimeoutEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workdir,
                    candidates={task.slug: (task, workdir / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    normal = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "judge_check",
                        {"timeout_seconds": 60},
                    )
                    clamped = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "judge_check",
                        {"timeout_seconds": 999},
                    )
                self.assertEqual(normal["effective_timeout_seconds"], 60)
                self.assertFalse(normal["timeout_clamped"])
                self.assertEqual(clamped["requested_timeout_seconds"], 999)
                self.assertEqual(clamped["effective_timeout_seconds"], 300)
                self.assertTrue(clamped["timeout_clamped"])
                self.assertEqual(evaluator.timeouts, [60, 300])
                rows = [
                    json.loads(line)
                    for line in (root / "judge_checks.jsonl").read_text().splitlines()
                ]
                self.assertEqual(
                    [row["effective_timeout_seconds"] for row in rows], [60, 300]
                )
            finally:
                broker.close()

    def test_evaluate_local_uses_the_same_timeout_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            evaluator = _TimeoutEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_audit_path=root / "formal_tool_calls.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workdir,
                    candidates={task.slug: (task, workdir / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    result = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "evaluate_local",
                        {"timeout_seconds": 60},
                    )
                self.assertEqual(result["effective_timeout_seconds"], 60)
                self.assertEqual(evaluator.timeouts, [60])
                rows = [
                    json.loads(line)
                    for line in (root / "formal_tool_calls.jsonl").read_text().splitlines()
                ]
                self.assertEqual(rows[0]["effective_timeout_seconds"], 60)
            finally:
                broker.close()

    def test_evaluate_local_custom_budget_bypasses_legacy_cache(self) -> None:
        """A custom call must not return a cached legacy diagnostic."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            evaluator = _TimeoutEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_audit_path=root / "formal_tool_calls.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workdir,
                    candidates={task.slug: (task, workdir / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    legacy = _post(
                        env["CONTEXTSWARM_JUDGE_URL"], "evaluate_local", {}
                    )
                    custom = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "evaluate_local",
                        {"timeout_seconds": 60},
                    )
                self.assertFalse(legacy["cache_hit"])
                self.assertFalse(custom["cache_hit"])
                self.assertEqual(evaluator.timeouts, [None, 60])
                self.assertEqual(custom["timeout_budget_mode"], "cumulative_total")
                self.assertEqual(custom["judge_attempt_count"], 1)
            finally:
                broker.close()

    def test_profiling_allowlist_keeps_timeout_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            profiler = RunProfiler(root, enabled=True, run_id="run-1")
            profiler.emit(
                "judge.receipt",
                requested_timeout_seconds=999,
                effective_timeout_seconds=300,
                timeout_clamped=True,
                timeout_source="agent_requested",
                timeout_budget_mode="cumulative_total",
                timeout_budget_seconds=300,
                timeout_budget_elapsed_seconds=30.0,
                timeout_budget_remaining_seconds=270.0,
                timeout_budget_exhausted=False,
                judge_attempt_count=2,
                judge_retry_count=1,
            )
            profiler.close()
            row = json.loads((root / "profiling.jsonl").read_text().splitlines()[0])
        self.assertEqual(row["requested_timeout_seconds"], 999)
        self.assertEqual(row["effective_timeout_seconds"], 300)
        self.assertTrue(row["timeout_clamped"])
        self.assertEqual(row["timeout_source"], "agent_requested")
        self.assertEqual(row["timeout_budget_mode"], "cumulative_total")
        self.assertEqual(row["timeout_budget_seconds"], 300)
        self.assertEqual(row["timeout_budget_remaining_seconds"], 270.0)
        self.assertEqual(row["judge_attempt_count"], 2)
        self.assertEqual(row["judge_retry_count"], 1)
        self.assertNotIn("dropped_fields", row)

    def test_disabled_broker_rejects_timeout_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            broker = JudgeBroker(
                _TimeoutEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_policy=_policy(),
                drain_timeout_seconds=1,
            ).start()
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workdir,
                    candidates={task.slug: (task, workdir / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    result = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "judge_check",
                        {"timeout_seconds": 60},
                    )
                self.assertEqual(result["status"], "INVALID_REQUEST")
            finally:
                broker.close()

    def test_lean_payload_uses_one_backend_attempt_for_custom_budget(self) -> None:
        class RecordingLean(LeanEvaluator):
            def __init__(self) -> None:
                super().__init__("http://unused", lean_env_id="test")
                self.payloads: list[dict[str, object]] = []

            def _request(self, method, path, payload=None, *, timeout_seconds=None, cancel_event=None):  # type: ignore[no-untyped-def]
                del method, path, timeout_seconds, cancel_event
                self.payloads.append(dict(payload or {}))
                return {
                    "job_id": f"job-{len(self.payloads)}",
                    "status": "failed",
                    "formal_status": "VERIFY_FAIL",
                }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            evaluator = RecordingLean()
            evaluator.probe_source(task, "import Mathlib\ntheorem task : True := by sorry\n", timeout_seconds=60)
            evaluator.probe_source(task, "import Mathlib\ntheorem task : True := by sorry\n--2\n", timeout_seconds=999)
            evaluator.probe_source(task, "import Mathlib\ntheorem task : True := by sorry\n--3\n")
        self.assertEqual(evaluator.payloads[0]["timeout"], 60)
        self.assertEqual(evaluator.payloads[0]["max_retries"], 0)
        self.assertEqual(evaluator.payloads[1]["timeout"], 300)
        self.assertEqual(evaluator.payloads[1]["max_retries"], 0)
        self.assertEqual(evaluator.payloads[2]["timeout"], 300)
        self.assertEqual(evaluator.payloads[2]["max_retries"], 1)

    def test_custom_budget_is_shared_by_independent_retries(self) -> None:
        """A 300-second choice leaves 270 seconds after a 30-second failure."""

        class FakeClock:
            def __init__(self) -> None:
                self.value = 1_000.0

            def monotonic(self) -> float:
                return self.value

        class RetryingLean(LeanEvaluator):
            def __init__(self, clock: FakeClock) -> None:
                super().__init__(
                    "http://unused",
                    lean_env_id="test",
                    backend_max_retries=1,
                    terminal_overload_retries=0,
                )
                self.clock = clock
                self.timeouts: list[int] = []

            def _evaluate_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                del args
                self.timeouts.append(int(kwargs["timeout_seconds"]))
                if len(self.timeouts) == 1:
                    self.clock.value += 30.0
                    return Verdict(
                        "task",
                        "EVALUATOR_ERROR",
                        0.0,
                        30.0,
                        {"evaluator_failure": {"category": "runtime_exception"}},
                        error="transient evaluator failure",
                        judge_job_id="job-1",
                    )
                return Verdict(
                    "task",
                    "PROVED",
                    1.0,
                    0.0,
                    {},
                    judge_job_id="job-2",
                )

        clock = FakeClock()
        evaluator = RetryingLean(clock)
        task = _task(ROOT)
        with patch("contextswarm_mini.evaluator.time.monotonic", clock.monotonic):
            verdict = evaluator.probe_source(task, task.baseline_code, timeout_seconds=300)

        self.assertEqual(verdict.status, "PROVED")
        self.assertEqual(evaluator.timeouts, [300, 270])
        self.assertEqual(verdict.response["timeout_budget_mode"], "cumulative_total")
        self.assertEqual(verdict.response["timeout_budget_seconds"], 300)
        self.assertEqual(verdict.response["timeout_budget_elapsed_seconds"], 30.0)
        self.assertEqual(verdict.response["timeout_budget_remaining_seconds"], 270.0)
        self.assertFalse(verdict.response["timeout_budget_exhausted"])
        self.assertEqual(verdict.response["judge_attempt_count"], 2)
        self.assertEqual(verdict.response["judge_retry_count"], 1)
        self.assertEqual(verdict.response["judge_attempt_timeouts_seconds"], [300, 270])
        self.assertEqual(verdict.response["judge_retry_reasons"], ["execution"])

    def test_timeout_consuming_first_attempt_does_not_retry(self) -> None:
        class FakeClock:
            value = 2_000.0

            def monotonic(self) -> float:
                return self.value

        class TimedOutLean(LeanEvaluator):
            def __init__(self, clock: FakeClock) -> None:
                super().__init__(
                    "http://unused", lean_env_id="test", backend_max_retries=1
                )
                self.clock = clock
                self.calls = 0

            def _evaluate_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                del args
                self.calls += 1
                self.clock.value += 300.0
                return Verdict(
                    "task",
                    "EXECUTION_TIMEOUT",
                    0.0,
                    300.0,
                    {"terminal_reason": "execution_timeout"},
                    judge_job_id="job-timeout",
                )

        clock = FakeClock()
        evaluator = TimedOutLean(clock)
        task = _task(ROOT)
        with patch("contextswarm_mini.evaluator.time.monotonic", clock.monotonic):
            verdict = evaluator.probe_source(task, task.baseline_code, timeout_seconds=300)

        self.assertEqual(evaluator.calls, 1)
        self.assertEqual(verdict.status, "EXECUTION_TIMEOUT")
        self.assertTrue(verdict.response["timeout_budget_exhausted"])
        self.assertEqual(verdict.response["judge_attempt_count"], 1)
        self.assertEqual(verdict.response["judge_retry_count"], 0)
        self.assertEqual(verdict.response["timeout_budget_stop_reason"], "budget_exhausted")

    def test_late_terminal_result_is_not_success_after_evaluator_budget(self) -> None:
        """The evaluator itself must reject a proof observed during cleanup grace."""

        class FakeClock:
            value = 3_000.0

            def monotonic(self) -> float:
                return self.value

        class LateProofLean(LeanEvaluator):
            def __init__(self, clock: FakeClock) -> None:
                super().__init__(
                    "http://unused",
                    lean_env_id="test",
                    backend_max_retries=1,
                    terminal_overload_retries=0,
                )
                self.clock = clock

            def _evaluate_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                del args, kwargs
                self.clock.value += 6.0
                return Verdict("task", "PROVED", 1.0, 6.0, {}, judge_job_id="late")

        clock = FakeClock()
        evaluator = LateProofLean(clock)
        task = _task(ROOT)
        with patch("contextswarm_mini.evaluator.time.monotonic", clock.monotonic):
            verdict = evaluator.probe_source(task, task.baseline_code, timeout_seconds=5)

        self.assertEqual(verdict.status, "EVALUATOR_TIMEOUT")
        self.assertEqual(verdict.score, 0.0)
        self.assertTrue(verdict.response["timeout_budget_exhausted"])
        self.assertEqual(verdict.response["timeout_budget_remaining_seconds"], 0.0)

    def test_run_horizon_floor_does_not_consume_remaining_agent_budget(self) -> None:
        """A near-horizon no-attempt closeout is not an Agent timeout."""

        class FakeClock:
            value = 4_000.0

            def monotonic(self) -> float:
                return self.value

        clock = FakeClock()
        evaluator = LeanEvaluator("http://unused", lean_env_id="test")
        task = _task(ROOT)
        with patch("contextswarm_mini.evaluator.time.monotonic", clock.monotonic):
            verdict = evaluator.probe_source(
                task,
                task.baseline_code,
                deadline_monotonic=4_003.0,
                timeout_seconds=60,
            )

        self.assertEqual(verdict.status, "OUT_OF_HORIZON")
        self.assertEqual(verdict.response["timeout_budget_stop_reason"], "run_horizon")
        self.assertFalse(verdict.response["timeout_budget_exhausted"])
        self.assertEqual(verdict.response["judge_attempt_count"], 0)
        self.assertAlmostEqual(
            verdict.response["timeout_budget_remaining_seconds"], 60.0
        )

    def test_formal_backend_quota_blocks_a_fresh_retry_without_losing_attempt_count(self) -> None:
        """Each fresh evaluate retry consumes a task-global backend-job unit."""

        class RetryingLean(LeanEvaluator):
            def __init__(self) -> None:
                super().__init__(
                    "http://unused",
                    lean_env_id="test",
                    backend_max_retries=1,
                    terminal_overload_retries=0,
                )
                self.calls = 0

            def _evaluate_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                del args
                self.calls += 1
                if self.calls == 1:
                    return Verdict(
                        "task",
                        "EVALUATOR_ERROR",
                        0.0,
                        0.0,
                        {"evaluator_failure": {"category": "runtime_exception"}},
                        judge_job_id="job-1",
                    )
                return Verdict("task", "PROVED", 1.0, 0.0, {}, judge_job_id="job-2")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            policy = FormalToolPolicy(
                enabled=True,
                surface_version="adaptive-timeout-quota-v1",
                evaluate_calls_per_task=8,
                evaluate_backend_jobs_per_task=1,
                query_calls_per_task=8,
                query_backend_probes_per_task=8,
                max_candidate_bytes=1024 * 1024,
                command_timeout_seconds=30,
                declaration_index=DeclarationIndex(None),
            )
            evaluator = RetryingLean()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_audit_path=root / "formal_tool_calls.jsonl",
                formal_policy=policy,
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workdir,
                    candidates={task.slug: (task, workdir / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    result = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "evaluate_local",
                        {"timeout_seconds": 60},
                    )
                self.assertEqual(evaluator.calls, 1)
                self.assertEqual(result["status"], "EVALUATOR_ERROR")
                self.assertTrue(result["formal_backend_budget_exhausted"])
                self.assertEqual(result["retry_blocked_reason"], "formal_backend_job_quota")
                self.assertEqual(result["judge_attempt_count"], 1)
                self.assertEqual(result["judge_retry_count"], 0)
                self.assertEqual(result["backend_job_count"], 1)
                self.assertEqual(
                    broker.formal_summary()["tasks"][task.slug]["evaluate_backend_jobs"],
                    1,
                )
            finally:
                broker.close()

    def test_formal_backend_quota_admits_and_records_a_fresh_retry(self) -> None:
        class RetryingLean(LeanEvaluator):
            def __init__(self) -> None:
                super().__init__(
                    "http://unused",
                    lean_env_id="test",
                    backend_max_retries=1,
                    terminal_overload_retries=0,
                )
                self.calls = 0

            def _evaluate_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                del args, kwargs
                self.calls += 1
                if self.calls == 1:
                    return Verdict(
                        "task",
                        "EVALUATOR_ERROR",
                        0.0,
                        0.0,
                        {"evaluator_failure": {"category": "runtime_exception"}},
                        judge_job_id="job-1",
                    )
                return Verdict("task", "PROVED", 1.0, 0.0, {}, judge_job_id="job-2")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            policy = FormalToolPolicy(
                enabled=True,
                surface_version="adaptive-timeout-quota-v1",
                evaluate_calls_per_task=8,
                evaluate_backend_jobs_per_task=2,
                query_calls_per_task=8,
                query_backend_probes_per_task=8,
                max_candidate_bytes=1024 * 1024,
                command_timeout_seconds=30,
                declaration_index=DeclarationIndex(None),
            )
            evaluator = RetryingLean()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_audit_path=root / "formal_tool_calls.jsonl",
                formal_policy=policy,
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                with broker.session(
                    actor_id="worker",
                    workdir=workdir,
                    candidates={task.slug: (task, workdir / "result.lean")},
                    deadline_monotonic=10**9,
                ) as env:
                    result = _post(
                        env["CONTEXTSWARM_JUDGE_URL"],
                        "evaluate_local",
                        {"timeout_seconds": 60},
                    )
                self.assertEqual(evaluator.calls, 2)
                self.assertEqual(result["status"], "PROVED")
                self.assertEqual(result["judge_attempt_count"], 2)
                self.assertEqual(result["judge_retry_count"], 1)
                self.assertEqual(result["backend_job_count"], 2)
                self.assertEqual(result["backend_job_numbers"], [1, 2])
                self.assertEqual(
                    broker.formal_summary()["tasks"][task.slug]["evaluate_backend_jobs"],
                    2,
                )
            finally:
                broker.close()

    def test_evaluate_local_retry_receives_only_remaining_total_budget(self) -> None:
        """The formal helper shares the same absolute budget as judge_check."""

        class FakeClock:
            value = 5_000.0

            def monotonic(self) -> float:
                return self.value

        class RetryingLean(LeanEvaluator):
            def __init__(self, clock: FakeClock) -> None:
                super().__init__(
                    "http://unused",
                    lean_env_id="test",
                    backend_max_retries=1,
                    terminal_overload_retries=0,
                )
                self.clock = clock
                self.timeouts: list[int] = []

            def _evaluate_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                del args
                self.timeouts.append(int(kwargs["timeout_seconds"]))
                if len(self.timeouts) == 1:
                    self.clock.value += 30.0
                    return Verdict(
                        "task",
                        "EVALUATOR_ERROR",
                        0.0,
                        30.0,
                        {"evaluator_failure": {"category": "runtime_exception"}},
                        error="transient evaluator failure",
                        judge_job_id="job-1",
                    )
                return Verdict("task", "PROVED", 1.0, 0.0, {}, judge_job_id="job-2")

        clock = FakeClock()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            evaluator = RetryingLean(clock)
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_audit_path=root / "formal_tool_calls.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                with patch(
                    "contextswarm_mini.evaluator.time.monotonic",
                    clock.monotonic,
                ), patch(
                    "contextswarm_mini.judge_broker.time.monotonic",
                    clock.monotonic,
                ):
                    with broker.session(
                        actor_id="worker",
                        workdir=workdir,
                        candidates={task.slug: (task, workdir / "result.lean")},
                        deadline_monotonic=10**9,
                    ) as env:
                        result = _post(
                            env["CONTEXTSWARM_JUDGE_URL"],
                            "evaluate_local",
                            {"timeout_seconds": 60},
                        )
            finally:
                broker.close()

        self.assertEqual(evaluator.timeouts, [60, 30])
        self.assertEqual(result["status"], "PROVED")
        self.assertEqual(result["timeout_budget_mode"], "cumulative_total")
        self.assertEqual(result["judge_attempt_timeouts_seconds"], [60, 30])
        self.assertEqual(result["judge_attempt_count"], 2)
        self.assertEqual(result["judge_retry_count"], 1)
        self.assertEqual(result["backend_job_count"], 2)

    def test_confirmed_pre_admission_overload_uses_separate_retry_budget(self) -> None:
        class FakeClock:
            value = 4_000.0

            def monotonic(self) -> float:
                return self.value

        class OverloadThenProof(LeanEvaluator):
            def __init__(self, clock: FakeClock) -> None:
                super().__init__(
                    "http://unused",
                    lean_env_id="test",
                    backend_max_retries=0,
                    terminal_overload_retries=1,
                )
                self.clock = clock
                self.calls = 0

            def _evaluate_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                del args
                self.calls += 1
                if self.calls == 1:
                    self.clock.value += 2.0
                    return Verdict(
                        "task",
                        "EVALUATOR_ERROR",
                        0.0,
                        2.0,
                        {"evaluator_failure": {"category": "judge_overloaded"}},
                        error="confirmed admission overload",
                        judge_job_id=None,
                    )
                return Verdict("task", "PROVED", 1.0, 0.0, {}, judge_job_id="job-2")

        clock = FakeClock()
        evaluator = OverloadThenProof(clock)
        task = _task(ROOT)
        with patch("contextswarm_mini.evaluator.time.monotonic", clock.monotonic):
            verdict = evaluator.probe_source(task, task.baseline_code, timeout_seconds=60)

        self.assertEqual(verdict.status, "PROVED")
        self.assertEqual(evaluator.calls, 2)
        self.assertEqual(verdict.response["judge_retry_reasons"], ["overload"])
        self.assertEqual(verdict.response["judge_attempt_timeouts_seconds"], [60, 58])

    def test_broker_does_not_accept_proof_returned_after_total_budget(self) -> None:
        """Settlement grace must not turn a late proof into success."""

        class FakeClock:
            value = 10_000.0

            def monotonic(self) -> float:
                return self.value

        class LateProofEvaluator(_TimeoutEvaluator):
            def probe_source(
                self,
                task: Task,
                source: str,
                *,
                deadline_monotonic: float | None = None,
                cancel_event: object | None = None,
                timeout_seconds: int | None = None,
                timeout_deadline_monotonic: float | None = None,
            ) -> Verdict:
                del source, deadline_monotonic, cancel_event, timeout_seconds
                self.timeouts.append(timeout_deadline_monotonic)
                clock.value += 6.0
                return Verdict(
                    task.slug,
                    "PROVED",
                    1.0,
                    6.0,
                    {},
                    candidate_sha256="a" * 64,
                    task_contract_sha256=self.expected_task_contract_sha256(task),
                    judge_job_id="late-proof",
                )

        clock = FakeClock()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            broker = JudgeBroker(
                LateProofEvaluator(),
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                with patch("contextswarm_mini.judge_broker.time.monotonic", clock.monotonic):
                    with broker.session(
                        actor_id="worker",
                        workdir=workdir,
                        candidates={task.slug: (task, workdir / "result.lean")},
                        deadline_monotonic=10**9,
                    ) as env:
                        result = _post(
                            env["CONTEXTSWARM_JUDGE_URL"],
                            "judge_check",
                            {"timeout_seconds": 5},
                        )
            finally:
                broker.close()

        self.assertEqual(result["status"], "EVALUATOR_TIMEOUT")
        self.assertFalse(result["proved"])
        self.assertEqual(result["score"], 0.0)
        self.assertTrue(result["timeout_budget_exhausted"])

    def test_broker_fails_closed_if_narrow_adapter_floor_disappears_before_call(self) -> None:
        """A late admission race must not grant a fresh full narrow-adapter timeout."""

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task = _task(root)
            workdir = root / "worker"
            workdir.mkdir()
            (workdir / "result.lean").write_text(task.baseline_code, encoding="utf-8")
            evaluator = _TimeoutEvaluator()
            broker = JudgeBroker(
                evaluator,
                threading.BoundedSemaphore(1),
                audit_path=root / "judge_checks.jsonl",
                formal_policy=_policy(),
                agent_timeout_enabled=True,
                min_probe_interval_seconds=0,
                drain_timeout_seconds=1,
            ).start()
            try:
                # The first check is the post-admission floor check.  The
                # second is immediately before invoking this deliberately
                # narrow adapter, which cannot receive an absolute deadline.
                with patch(
                    "contextswarm_mini.judge_broker._remaining_agent_attempt_seconds",
                    side_effect=[5, None],
                ):
                    with broker.session(
                        actor_id="worker",
                        workdir=workdir,
                        candidates={task.slug: (task, workdir / "result.lean")},
                        deadline_monotonic=10**9,
                    ) as env:
                        result = _post(
                            env["CONTEXTSWARM_JUDGE_URL"],
                            "judge_check",
                            {"timeout_seconds": 5},
                        )
                self.assertEqual(result["status"], "EVALUATOR_TIMEOUT")
                self.assertEqual(evaluator.timeouts, [])
                self.assertTrue(broker.evaluator_gate.acquire(timeout=0))
                broker.evaluator_gate.release()
            finally:
                broker.close()


if __name__ == "__main__":
    unittest.main()
