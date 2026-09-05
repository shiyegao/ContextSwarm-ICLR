from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from contextswarm_mini.config import load_config
from contextswarm_mini.cps import CPSStore, make_policy
from contextswarm_mini.models import AgentResult, Verdict
from contextswarm_mini.runner import RunLogger, _run_elastic_cps, load_tasks


ROOT = Path(__file__).resolve().parents[1]


class _Broker:
    """Minimal capability context for the direct CPS runner harness."""

    def __init__(self) -> None:
        self.calls = 0

    @contextmanager
    def session(self, **_kwargs):
        self.calls += 1
        yield {
            "CONTEXTSWARM_JUDGE_URL": "http://127.0.0.1:1/test-token",
            "CONTEXTSWARM_BROKER_DEADLINE_EPOCH_MS": "9999999999999",
        }


class _FailTwiceThenSucceedPi:
    """Leave a visible partial candidate on failed processes."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def run(self, **kwargs):
        self.calls.append(dict(kwargs))
        attempt = len(self.calls)
        workdir = Path(kwargs["workdir"])
        (workdir / "result.lean").write_text(
            f"partial-{attempt}\n",
            encoding="utf-8",
        )
        now = "2026-01-01T00:00:00+00:00"
        return AgentResult(
            agent_id=str(kwargs["actor_id"]),
            task_id=str(kwargs["task_id"]),
            episode=int(kwargs["episode"]),
            returncode=0 if attempt >= 3 else 1,
            started_at=now,
            finished_at=now,
            error_tail="" if attempt >= 3 else "Coordinator response failed",
        )


class _FailOnceThenSucceedPi(_FailTwiceThenSucceedPi):
    def run(self, **kwargs):
        self.calls.append(dict(kwargs))
        attempt = len(self.calls)
        workdir = Path(kwargs["workdir"])
        (workdir / "result.lean").write_text(
            f"partial-{attempt}\n",
            encoding="utf-8",
        )
        now = "2026-01-01T00:00:00+00:00"
        return AgentResult(
            agent_id=str(kwargs["actor_id"]),
            task_id=str(kwargs["task_id"]),
            episode=int(kwargs["episode"]),
            returncode=0 if attempt >= 2 else 1,
            started_at=now,
            finished_at=now,
            error_tail="" if attempt >= 2 else "Coordinator response failed",
        )


class _StopThenSucceedPi:
    """Return one terminal stop, then succeed on a fresh CPS assignment."""

    def __init__(self, *, timed_out: bool = False, cancelled: bool = False) -> None:
        if timed_out == cancelled:
            raise ValueError("choose exactly one terminal stop kind")
        self.timed_out = timed_out
        self.cancelled = cancelled
        self.calls: list[dict[str, object]] = []

    def run(self, **kwargs):
        self.calls.append(dict(kwargs))
        attempt = len(self.calls)
        workdir = Path(kwargs["workdir"])
        (workdir / "result.lean").write_text(
            f"partial-{attempt}\n",
            encoding="utf-8",
        )
        now = "2026-01-01T00:00:00+00:00"
        first = attempt == 1
        return AgentResult(
            agent_id=str(kwargs["actor_id"]),
            task_id=str(kwargs["task_id"]),
            episode=int(kwargs["episode"]),
            returncode=(124 if self.timed_out else 130) if first else 0,
            started_at=now,
            finished_at=now,
            error_tail=("Pi RPC deadline elapsed" if self.timed_out else "Pi RPC was cancelled")
            if first
            else "",
            timed_out=self.timed_out and first,
            cancelled=self.cancelled and first,
        )


class _RecordingEvaluator:
    is_mock_evaluator = True

    def __init__(self) -> None:
        self.candidates: list[str] = []

    def expected_task_contract_sha256(self, _task) -> str:
        return "a" * 64

    def evaluate(
        self,
        task,
        candidate_path: Path,
        *,
        deadline_monotonic=None,
        cancel_event=None,
        settlement_callback=None,
    ) -> Verdict:
        del deadline_monotonic, cancel_event, settlement_callback
        self.candidates.append(candidate_path.read_text(encoding="utf-8"))
        # Keep this a candidate-attempt outcome, not a proof.  The important
        # assertion is that only the successful replacement reaches Judge;
        # the failed process's partial file must never be evaluated.
        return Verdict(
            task.slug,
            "MOCK_SKIPPED",
            0.0,
            0.0,
            {"mock": True},
            candidate_sha256="b" * 64,
            task_contract_sha256="a" * 64,
        )


class CpsRecoveryPartialCandidateTests(unittest.TestCase):
    def _run_terminal_stop_case(
        self,
        pi: _StopThenSucceedPi,
    ) -> tuple[list[tuple[AgentResult, Verdict]], list[dict[str, object]], _RecordingEvaluator, _Broker]:
        base = load_config("configs/smoke.toml", ROOT)
        config = replace(
            base,
            max_tasks=1,
            max_parallel=1,
            initial_agents_per_task=1,
            max_attempts_per_task=2,
            time_limit_seconds=2,
            pi_recovery_enabled=True,
            pi_recovery_max_restarts=1,
            pi_recovery_base_delay_ms=0,
        )
        task = load_tasks(config)[0]
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            logger = RunLogger(run_dir)
            store = CPSStore(run_dir / "cps.sqlite3")
            policy = make_policy(config.communication, store)
            evaluator = _RecordingEvaluator()
            broker = _Broker()
            scheduler_results: list[AgentResult] = []
            results = _run_elastic_cps(
                config,
                [task],
                run_dir,
                logger,
                evaluator,
                pi,
                policy,
                mock_agent=False,
                deadline=time.monotonic() + 2.0,
                evaluator_gate=threading.BoundedSemaphore(1),
                judge_broker=broker,
                scheduler_result_sink=scheduler_results,
            )
            events = [
                json.loads(line)
                for line in (run_dir / "events.jsonl").read_text().splitlines()
            ]
            # Copy only bounded, in-memory evidence before the temporary run
            # directory is removed.  No prompt/provider payload is retained.
            return results, events, evaluator, broker

    def test_timeout_does_not_recover_same_actor_but_cps_admits_fresh_assignment(self):
        pi = _StopThenSucceedPi(timed_out=True)
        results, events, evaluator, broker = self._run_terminal_stop_case(pi)

        self.assertEqual(len(pi.calls), 2)
        self.assertNotEqual(pi.calls[0]["actor_id"], pi.calls[1]["actor_id"])
        self.assertEqual([call["episode"] for call in pi.calls], [1, 2])
        self.assertEqual(evaluator.candidates, ["partial-2\n"])
        self.assertEqual(broker.calls, 2)
        self.assertFalse(
            any(
                row.get("event")
                in {
                    "agent_recovery_scheduled",
                    "agent_recovery_started",
                    "agent_refill_scheduled",
                    "agent_refill_started",
                }
                for row in events
            )
        )
        observed = next(
            row
            for row in events
            if row.get("event") == "agent_recovery_failure_observed"
        )
        self.assertTrue(observed["timed_out"])
        self.assertFalse(observed["recoverable"])
        exhausted = next(
            row for row in events if row.get("event") == "agent_recovery_exhausted"
        )
        self.assertEqual(exhausted["reason"], "task_timeout")
        assigned = [row for row in events if row.get("event") == "agent_assigned"]
        self.assertEqual(len(assigned), 2)
        self.assertEqual(
            [row.get("allocation_phase") for row in assigned],
            ["initial", "adaptive"],
        )
        self.assertEqual(
            sorted(verdict.status for _result, verdict in results),
            ["AGENT_FAILURE", "MOCK_SKIPPED"],
        )

    def test_intentional_cancel_does_not_recover_same_actor_but_cps_admits_fresh_assignment(self):
        pi = _StopThenSucceedPi(cancelled=True)
        results, events, evaluator, broker = self._run_terminal_stop_case(pi)

        self.assertEqual(len(pi.calls), 2)
        self.assertNotEqual(pi.calls[0]["actor_id"], pi.calls[1]["actor_id"])
        self.assertEqual([call["episode"] for call in pi.calls], [1, 2])
        self.assertEqual(evaluator.candidates, ["partial-2\n"])
        self.assertEqual(broker.calls, 2)
        self.assertFalse(
            any(
                row.get("event")
                in {
                    "agent_recovery_scheduled",
                    "agent_recovery_started",
                    "agent_refill_scheduled",
                    "agent_refill_started",
                }
                for row in events
            )
        )
        observed = next(
            row
            for row in events
            if row.get("event") == "agent_recovery_failure_observed"
        )
        self.assertTrue(observed["cancelled"])
        self.assertFalse(observed["recoverable"])
        exhausted = next(
            row for row in events if row.get("event") == "agent_recovery_exhausted"
        )
        self.assertEqual(exhausted["reason"], "intentional_cancel")
        assigned = [row for row in events if row.get("event") == "agent_assigned"]
        self.assertEqual(len(assigned), 2)
        self.assertEqual(
            [row.get("allocation_phase") for row in assigned],
            ["initial", "adaptive"],
        )
        self.assertEqual(
            sorted(verdict.status for _result, verdict in results),
            ["CANCELLED", "MOCK_SKIPPED"],
        )

    def test_exhausted_solver_recovery_skips_partial_candidate_and_refills_slot(self):
        base = load_config("configs/smoke.toml", ROOT)
        config = replace(
            base,
            max_tasks=1,
            max_parallel=1,
            initial_agents_per_task=1,
            max_attempts_per_task=2,
            time_limit_seconds=2,
            pi_recovery_enabled=True,
            pi_recovery_max_restarts=1,
            pi_recovery_base_delay_ms=0,
        )
        task = load_tasks(config)[0]

        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            logger = RunLogger(run_dir)
            store = CPSStore(run_dir / "cps.sqlite3")
            policy = make_policy(config.communication, store)
            pi = _FailTwiceThenSucceedPi()
            evaluator = _RecordingEvaluator()
            broker = _Broker()
            scheduler_results: list[AgentResult] = []

            results = _run_elastic_cps(
                config,
                [task],
                run_dir,
                logger,
                evaluator,
                pi,
                policy,
                mock_agent=False,
                deadline=time.monotonic() + 2.0,
                evaluator_gate=threading.BoundedSemaphore(1),
                judge_broker=broker,
                scheduler_result_sink=scheduler_results,
            )

            # The first logical agent gets one in-session recovery, then the
            # released CPS slot admits a fresh assignment.  Its partial file
            # must not be sent to the Judge before that refill.
            self.assertEqual(len(pi.calls), 3)
            self.assertEqual(
                pi.calls[0]["actor_id"],
                pi.calls[1]["actor_id"],
            )
            self.assertNotEqual(
                pi.calls[0]["actor_id"],
                pi.calls[2]["actor_id"],
            )
            self.assertEqual(len(evaluator.candidates), 1)
            self.assertEqual(evaluator.candidates, ["partial-3\n"])
            self.assertEqual(broker.calls, 2)

            self.assertEqual(
                sorted(verdict.status for _result, verdict in results),
                ["AGENT_FAILURE", "MOCK_SKIPPED"],
            )
            failed = next(
                result for result, verdict in results if verdict.status == "AGENT_FAILURE"
            )
            self.assertNotEqual(failed.returncode, 0)

            events = [
                json.loads(line)
                for line in (run_dir / "events.jsonl").read_text().splitlines()
            ]
            failure_rows = [
                row
                for row in events
                if row.get("event") == "evaluation_finished"
                and row.get("source") == "agent_failure"
            ]
            self.assertEqual(len(failure_rows), 1)
            self.assertEqual(failure_rows[0]["status"], "AGENT_FAILURE")
            self.assertTrue(
                any(row.get("event") == "agent_recovery_exhausted" for row in events)
            )
            self.assertFalse(
                any(
                    row.get("event") == "best_candidate_promoted"
                    and row.get("agent_id") == failed.agent_id
                    for row in events
                )
            )

            scheduler_state = json.loads(
                (run_dir / "elastic_scheduler_state.json").read_text()
            )
            self.assertEqual(scheduler_state["active_slots"], 0)
            task_state = scheduler_state["tasks"][task.slug]
            self.assertTrue(task_state["retired"])
            self.assertEqual(task_state["retired_reason"], "attempt_budget_exhausted")

    def test_refill_still_runs_when_inner_backoff_no_longer_fits(self):
        base = load_config("configs/smoke.toml", ROOT)
        config = replace(
            base,
            max_tasks=1,
            max_parallel=1,
            initial_agents_per_task=1,
            max_attempts_per_task=2,
            time_limit_seconds=2,
            pi_recovery_enabled=True,
            pi_recovery_max_restarts=1,
            # Deliberately longer than the remaining test horizon.  The
            # recovery helper must decline its in-session backoff without
            # falsely marking the global horizon as reached; CPS then gets a
            # chance to refill the released slot immediately.
            pi_recovery_base_delay_ms=10_000,
        )
        task = load_tasks(config)[0]

        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            logger = RunLogger(run_dir)
            store = CPSStore(run_dir / "cps.sqlite3")
            policy = make_policy(config.communication, store)
            pi = _FailOnceThenSucceedPi()
            evaluator = _RecordingEvaluator()
            broker = _Broker()
            scheduler_results: list[AgentResult] = []

            results = _run_elastic_cps(
                config,
                [task],
                run_dir,
                logger,
                evaluator,
                pi,
                policy,
                mock_agent=False,
                deadline=time.monotonic() + 0.5,
                evaluator_gate=threading.BoundedSemaphore(1),
                judge_broker=broker,
                scheduler_result_sink=scheduler_results,
            )

            self.assertEqual(len(pi.calls), 2)
            self.assertEqual(len(evaluator.candidates), 1)
            self.assertEqual(evaluator.candidates, ["partial-2\n"])
            self.assertEqual(broker.calls, 2)
            self.assertIn("AGENT_FAILURE", [verdict.status for _result, verdict in results])
            self.assertIn("MOCK_SKIPPED", [verdict.status for _result, verdict in results])
            events = [
                json.loads(line)
                for line in (run_dir / "events.jsonl").read_text().splitlines()
            ]
            self.assertTrue(
                any(
                    row.get("event") == "agent_recovery_exhausted"
                    and row.get("reason") == "insufficient_horizon_for_backoff"
                    for row in events
                )
            )
            # This is a scheduler-level refill after the inner backoff was
            # declined, so it is recorded as a second adaptive assignment
            # rather than an in-session ``agent_refill_scheduled`` event.
            self.assertEqual(
                sum(row.get("event") == "agent_assigned" for row in events),
                2,
            )
            self.assertTrue(
                any(
                    row.get("event") == "agent_assigned"
                    and row.get("allocation_phase") == "adaptive"
                    for row in events
                )
            )


if __name__ == "__main__":
    unittest.main()
