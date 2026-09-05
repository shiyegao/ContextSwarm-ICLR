from __future__ import annotations

import threading
import subprocess
import time
import unittest
from pathlib import Path

from contextswarm_mini.config import load_config
from contextswarm_mini.agent_recovery import (
    is_recoverable_agent_failure,
    run_with_recovery,
)
from contextswarm_mini.models import AgentResult


ROOT = Path(__file__).resolve().parents[1]


def _result(
    returncode: int,
    *,
    timed_out: bool = False,
    cancelled: bool = False,
    run_horizon_reached: bool = False,
    error_tail: str = "",
) -> AgentResult:
    return AgentResult(
        agent_id="worker-task-e1",
        task_id="task",
        episode=1,
        returncode=returncode,
        started_at="2026-08-23T00:00:00+00:00",
        finished_at="2026-08-23T00:00:01+00:00",
        error_tail=error_tail,
        timed_out=timed_out,
        cancelled=cancelled,
        run_horizon_reached=run_horizon_reached,
    )


class _CancelDuringBackoff:
    def __init__(self) -> None:
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return False

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        return True


class _ReasonedCancel:
    def __init__(self, reason: str) -> None:
        self.reason = reason
        self._set = False

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True

    def cancellation_reason(self) -> str:
        return self.reason


class AgentRecoveryTests(unittest.TestCase):
    def test_manifest_exposes_shared_outer_recovery_defaults(self) -> None:
        config = load_config("configs/parallel.toml", ROOT)
        self.assertTrue(config.pi_recovery_enabled)
        self.assertEqual(config.pi_recovery_max_restarts, 1)
        self.assertEqual(config.pi_recovery_base_delay_ms, 1_000)
        public = config.public_dict()
        self.assertEqual(public["pi_recovery_max_restarts"], 1)

    def test_coordinator_session_failure_restarts_same_logical_invocation(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []
        workspace = {"candidate": "best-so-far"}
        recovered = _result(0)

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            self.assertEqual(workspace["candidate"], "best-so-far")
            if recovery_attempt == 0:
                workspace["checkpoint"] = "persisted"
                return _result(
                    1,
                    error_tail="Pi RPC agent settled with an error: "
                    '{"error":"Coordinator response failed"}',
                )
            self.assertEqual(workspace["checkpoint"], "persisted")
            return recovered

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertIs(result, recovered)
        self.assertEqual(attempts, [0, 1])
        self.assertEqual(
            [name for name, _payload in events],
            [
                "agent_recovery_failure_observed",
                "agent_recovery_scheduled",
                "agent_recovery_started",
                "agent_recovery_succeeded",
            ],
        )
        for _name, payload in events:
            self.assertEqual(payload["task_id"], "task")
            self.assertEqual(payload["agent_id"], "worker-task-e1")
            self.assertEqual(payload["episode"], 1)
        self.assertTrue(events[0][1]["recoverable"])
        self.assertEqual(events[1][1]["recovery_attempt"], 1)

    def test_transport_timeout_without_task_timeout_remains_recoverable(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            if recovery_attempt == 0:
                # This is a provider/coordinator diagnostic timeout, not the
                # task's bounded Pi deadline.  It remains eligible for the
                # abnormal-process recovery path because ``timed_out`` is
                # false.
                return _result(1, error_tail="coordinator transport timed out")
            return _result(0)

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(attempts, [0, 1])
        observed = next(
            payload
            for name, payload in events
            if name == "agent_recovery_failure_observed"
        )
        self.assertTrue(observed["recoverable"])
        self.assertEqual(observed["failure_category"], "timeout")

    def test_invocation_exception_restarts_without_leaking_exception_message(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []
        secret = "private-token-that-must-not-appear"

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            if recovery_attempt == 0:
                raise RuntimeError(secret)
            return _result(0)

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(attempts, [0, 1])
        observed = next(
            payload
            for name, payload in events
            if name == "agent_recovery_failure_observed"
        )
        self.assertEqual(observed["failure_source"], "invoke_exception")
        self.assertEqual(observed["exception_type"], "RuntimeError")
        self.assertNotIn(secret, result.error_tail)

    def test_invocation_exception_exhaustion_returns_bounded_failed_result(self) -> None:
        attempts: list[int] = []

        def invoke(_recovery_attempt: int) -> AgentResult:
            attempts.append(_recovery_attempt)
            raise OSError("private endpoint and bearer token")

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(attempts, [0, 1])
        self.assertEqual(result.error_tail, "Pi solver invocation raised OSError")

    def test_timeout_invocation_exception_is_terminal_without_relaunch(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            raise TimeoutError("inner timeout must not be retried")

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.timed_out)
        self.assertEqual(attempts, [0])
        observed = next(
            payload
            for name, payload in events
            if name == "agent_recovery_failure_observed"
        )
        self.assertFalse(observed["recoverable"])
        self.assertEqual(observed["failure_source"], "invoke_exception")
        exhausted = next(
            payload
            for name, payload in events
            if name == "agent_recovery_exhausted"
        )
        self.assertEqual(exhausted["reason"], "task_timeout")

    def test_subprocess_timeout_exception_is_terminal_without_relaunch(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            raise subprocess.TimeoutExpired(cmd="pi", timeout=1)

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.timed_out)
        self.assertEqual(attempts, [0])
        observed = next(
            payload
            for name, payload in events
            if name == "agent_recovery_failure_observed"
        )
        self.assertFalse(observed["recoverable"])
        self.assertEqual(observed["exception_type"], "TimeoutExpired")
        exhausted = next(
            payload
            for name, payload in events
            if name == "agent_recovery_exhausted"
        )
        self.assertEqual(exhausted["reason"], "task_timeout")

    def test_terminal_stop_flags_override_zero_returncode(self) -> None:
        cases = (
            ({"timed_out": True}, 124, "task_timeout"),
            ({"cancelled": True}, 130, "intentional_cancel"),
            ({"run_horizon_reached": True}, 124, "horizon"),
        )
        for flags, expected_returncode, expected_reason in cases:
            with self.subTest(flags=flags):
                attempts: list[int] = []
                events: list[tuple[str, dict[str, object]]] = []

                def invoke(recovery_attempt: int) -> AgentResult:
                    attempts.append(recovery_attempt)
                    return _result(0, **flags)

                result = run_with_recovery(
                    invoke,
                    task_id="task",
                    actor_id="worker-task-e1",
                    episode=1,
                    deadline_monotonic=time.monotonic() + 5.0,
                    max_restarts=1,
                    base_delay_seconds=0.0,
                    on_event=lambda name, payload: events.append((name, payload)),
                )

                self.assertEqual(result.returncode, expected_returncode)
                self.assertEqual(attempts, [0])
                self.assertNotIn(
                    "agent_recovery_succeeded",
                    [name for name, _payload in events],
                )
                exhausted = next(
                    payload
                    for name, payload in events
                    if name == "agent_recovery_exhausted"
                )
                self.assertEqual(exhausted["reason"], expected_reason)

    def test_invocation_exception_after_deadline_is_normal_horizon_closeout(self) -> None:
        attempts: list[int] = []
        deadline = time.monotonic() + 0.01

        def invoke(_recovery_attempt: int) -> AgentResult:
            attempts.append(_recovery_attempt)
            time.sleep(0.02)
            raise RuntimeError("late process failure")

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=deadline,
            max_restarts=1,
            base_delay_seconds=0.0,
        )

        self.assertTrue(result.run_horizon_reached)
        self.assertEqual(attempts, [0])

    def test_base_exception_is_not_swallowed_by_recovery_boundary(self) -> None:
        def invoke(_recovery_attempt: int) -> AgentResult:
            raise KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            run_with_recovery(
                invoke,
                task_id="task",
                actor_id="worker-task-e1",
                episode=1,
                deadline_monotonic=time.monotonic() + 5.0,
                max_restarts=1,
                base_delay_seconds=0.0,
            )

    def test_inner_pi_timeout_is_terminal_while_horizon_remains(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            return _result(124, timed_out=True)

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 124)
        self.assertTrue(result.timed_out)
        self.assertEqual(attempts, [0])
        self.assertNotIn(
            "agent_recovery_scheduled",
            [name for name, _payload in events],
        )
        observed = next(
            payload
            for name, payload in events
            if name == "agent_recovery_failure_observed"
        )
        self.assertFalse(observed["recoverable"])
        self.assertEqual(observed["failure_category"], "timeout")
        exhausted = next(
            payload
            for name, payload in events
            if name == "agent_recovery_exhausted"
        )
        self.assertEqual(exhausted["reason"], "task_timeout")

    def test_timeout_returned_at_global_deadline_is_marked_horizon_and_not_retried(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []
        deadline = time.monotonic() + 0.01

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            time.sleep(0.02)
            return _result(124, timed_out=True)

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=deadline,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertTrue(result.run_horizon_reached)
        self.assertEqual(attempts, [0])
        exhausted = next(
            payload
            for name, payload in events
            if name == "agent_recovery_exhausted"
        )
        self.assertEqual(exhausted["reason"], "horizon")

    def test_expired_horizon_is_guarded_before_initial_invocation(self) -> None:
        attempts: list[int] = []
        result = run_with_recovery(
            lambda recovery_attempt: (
                attempts.append(recovery_attempt) or _result(0)
            ),
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() - 0.001,
            max_restarts=1,
            base_delay_seconds=0.0,
        )

        self.assertEqual(attempts, [])
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(result.run_horizon_reached)
        self.assertTrue(result.timed_out)

    def test_cancelled_slot_is_guarded_before_recovery_relaunch(self) -> None:
        attempts: list[int] = []
        cancel_event = threading.Event()
        cancel_event.set()
        result = run_with_recovery(
            lambda recovery_attempt: (
                attempts.append(recovery_attempt) or _result(0)
            ),
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            cancel_event=cancel_event,
            max_restarts=1,
            base_delay_seconds=0.0,
        )

        self.assertEqual(attempts, [])
        self.assertEqual(result.returncode, 130)
        self.assertTrue(result.cancelled)
        self.assertFalse(result.run_horizon_reached)

    def test_nonzero_process_exit_after_global_deadline_is_normal_horizon_closeout(self) -> None:
        attempts: list[int] = []
        deadline = time.monotonic() + 0.01

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            time.sleep(0.02)
            return _result(1, error_tail="Coordinator response failed")

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=deadline,
            max_restarts=1,
            base_delay_seconds=0.0,
        )

        self.assertTrue(result.run_horizon_reached)
        self.assertEqual(attempts, [0])

    def test_classifier_excludes_success_cancellation_and_fixed_horizon(self) -> None:
        deadline = 100.0
        cancelled = threading.Event()
        cancelled.set()
        cases = (
            ("success", _result(0), 10.0, None, False),
            ("cancelled result", _result(130, cancelled=True), 10.0, None, False),
            (
                "run horizon marker",
                _result(124, timed_out=True, run_horizon_reached=True),
                10.0,
                None,
                False,
            ),
            ("deadline elapsed", _result(1), deadline, None, False),
            ("external cancellation", _result(1), 10.0, cancelled, False),
            ("inner timeout", _result(124, timed_out=True), 10.0, None, False),
            ("process failure", _result(1), 10.0, None, True),
        )
        for label, result, now, cancel_event, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(
                    is_recoverable_agent_failure(
                        result,
                        deadline_monotonic=deadline,
                        now_monotonic=now,
                        cancel_event=cancel_event,
                    ),
                    expected,
                )

    def test_cancellation_reason_is_reported_without_relaunch(self) -> None:
        cases = (
            ("runner_failure", "runner_failure"),
            ("remote_settlement_unconfirmed", "remote_settlement_unconfirmed"),
            ("full_score", "intentional_cancel"),
            ("task_solved_by_peer", "intentional_cancel"),
        )
        for reason, expected_reason in cases:
            with self.subTest(reason=reason):
                attempts: list[int] = []
                events: list[tuple[str, dict[str, object]]] = []
                cancel_event = _ReasonedCancel(reason)

                def invoke(recovery_attempt: int) -> AgentResult:
                    attempts.append(recovery_attempt)
                    cancel_event.set()
                    return _result(1)

                result = run_with_recovery(
                    invoke,
                    task_id="task",
                    actor_id="worker-task-e1",
                    episode=1,
                    deadline_monotonic=time.monotonic() + 5.0,
                    cancel_event=cancel_event,
                    max_restarts=1,
                    base_delay_seconds=0.0,
                    on_event=lambda name, payload: events.append((name, payload)),
                )

                self.assertEqual(result.returncode, 1)
                self.assertEqual(attempts, [0])
                observed = next(
                    payload
                    for name, payload in events
                    if name == "agent_recovery_failure_observed"
                )
                self.assertFalse(observed["recoverable"])
                self.assertEqual(observed["failure_source"], "agent_result")
                exhausted = next(
                    payload
                    for name, payload in events
                    if name == "agent_recovery_exhausted"
                )
                self.assertEqual(exhausted["reason"], expected_reason)

    def test_restart_limit_returns_last_failure_and_emits_exhausted(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            return _result(1, error_tail="Coordinator response failed")

        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(attempts, [0, 1])
        self.assertEqual(
            [name for name, _payload in events].count(
                "agent_recovery_failure_observed"
            ),
            2,
        )
        exhausted = [
            payload
            for name, payload in events
            if name == "agent_recovery_exhausted"
        ]
        self.assertEqual(len(exhausted), 1)
        self.assertEqual(exhausted[0]["reason"], "restart_limit")
        self.assertEqual(exhausted[0]["recovery_attempt"], 1)

    def test_backoff_never_crosses_fixed_horizon(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []

        result = run_with_recovery(
            lambda recovery_attempt: (
                attempts.append(recovery_attempt) or _result(1)
            ),
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 0.05,
            max_restarts=3,
            base_delay_seconds=1.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(attempts, [0])
        self.assertNotIn(
            "agent_recovery_scheduled",
            [name for name, _payload in events],
        )
        exhausted = next(
            payload
            for name, payload in events
            if name == "agent_recovery_exhausted"
        )
        self.assertEqual(exhausted["reason"], "insufficient_horizon_for_backoff")
        # A backoff that no longer fits is not the same thing as the global
        # experiment horizon being reached; task-level slot refill may still
        # run while there is remaining time.
        self.assertFalse(result.run_horizon_reached)

    def test_cancellation_during_backoff_prevents_relaunch(self) -> None:
        attempts: list[int] = []
        events: list[tuple[str, dict[str, object]]] = []
        cancel_event = _CancelDuringBackoff()

        result = run_with_recovery(
            lambda recovery_attempt: (
                attempts.append(recovery_attempt) or _result(1)
            ),
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            cancel_event=cancel_event,
            max_restarts=1,
            base_delay_seconds=0.25,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(attempts, [0])
        self.assertEqual(cancel_event.waits, [0.25])
        exhausted = [
            payload
            for name, payload in events
            if name == "agent_recovery_exhausted"
        ]
        self.assertEqual(len(exhausted), 1)
        self.assertEqual(exhausted[0]["reason"], "cancelled_during_backoff")

    def test_zero_delay_rechecks_cancellation_before_relaunch(self) -> None:
        attempts: list[int] = []
        cancel_event = threading.Event()

        def invoke(recovery_attempt: int) -> AgentResult:
            attempts.append(recovery_attempt)
            if recovery_attempt == 0:
                cancel_event.set()
                return _result(1)
            return _result(0)

        events: list[tuple[str, dict[str, object]]] = []
        result = run_with_recovery(
            invoke,
            task_id="task",
            actor_id="worker-task-e1",
            episode=1,
            deadline_monotonic=time.monotonic() + 5.0,
            cancel_event=cancel_event,
            max_restarts=1,
            base_delay_seconds=0.0,
            on_event=lambda name, payload: events.append((name, payload)),
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(attempts, [0])
        exhausted = [
            payload for name, payload in events if name == "agent_recovery_exhausted"
        ]
        self.assertEqual(len(exhausted), 1)
        self.assertIn(
            exhausted[0]["reason"],
            {"intentional_cancel", "cancelled_before_relaunch"},
        )


if __name__ == "__main__":
    unittest.main()
