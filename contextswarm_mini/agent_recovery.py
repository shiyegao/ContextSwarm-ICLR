"""Bounded recovery for solver process/session failures.

The Pi runtime already retries provider requests inside a live session.  This
module owns the narrower outer boundary: when that whole RPC process/session
exits abnormally, restart the same logical actor against its persisted session
and workspace, without extending the experiment horizon.  A task timeout or
an intentional cancellation is terminal at this boundary; only an abnormal
non-timeout, non-cancelled failure may be restarted.
"""

from __future__ import annotations

import datetime as dt
import math
import subprocess
import time
from typing import Any, Callable

from .models import AgentResult


RecoveryEventSink = Callable[[str, dict[str, Any]], None]
RecoveryInvocation = Callable[[int], AgentResult]


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _exception_label(exc: BaseException) -> str:
    """Return a bounded exception class label without exposing its message."""

    raw = type(exc).__name__ or "Exception"
    label = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in raw
    )
    return label[:80] or "Exception"


def _event_is_set(event: Any | None) -> bool:
    if event is None:
        return False
    try:
        return bool(event.is_set())
    except Exception:
        # A broken cancellation adapter must not turn a solver failure into an
        # unbounded retry loop.  Treat it as cancellation (fail closed).
        return True


def _exception_result(
    exc: Exception,
    *,
    task_id: str,
    actor_id: str,
    episode: int,
    deadline_monotonic: float,
    cancel_event: Any | None,
) -> tuple[AgentResult, str]:
    """Convert an invocation exception into a safe logical failed attempt."""

    label = _exception_label(exc)
    now = time.monotonic()
    # ``subprocess.TimeoutExpired`` is not a subclass of the built-in
    # ``TimeoutError`` on all supported Python versions.  Normalize both
    # forms at this boundary so an adapter that lets the subprocess exception
    # escape cannot accidentally enter the abnormal-process retry path.
    timed_out = isinstance(exc, (TimeoutError, subprocess.TimeoutExpired))
    cancelled = _event_is_set(cancel_event)
    horizon_reached = now >= float(deadline_monotonic)
    return (
        AgentResult(
            agent_id=actor_id,
            task_id=task_id,
            episode=episode,
            returncode=1,
            started_at=_utc_now(),
            finished_at=_utc_now(),
            error_tail=f"Pi solver invocation raised {label}",
            timed_out=timed_out,
            cancelled=cancelled,
            run_horizon_reached=horizon_reached,
        ),
        label,
    )


def _guard_result(
    *,
    task_id: str,
    actor_id: str,
    episode: int,
    cancelled: bool = False,
) -> AgentResult:
    """Build a terminal result when admission races closeout/cancellation.

    The guard runs before calling the solver adapter.  Returning a normal
    ``AgentResult`` keeps runner closeout/accounting uniform while ensuring
    that a replacement slot never launches a Pi process after the fixed
    horizon (or after the runner has revoked the slot).
    """

    now = _utc_now()
    return AgentResult(
        agent_id=actor_id,
        task_id=task_id,
        episode=episode,
        returncode=130 if cancelled else 124,
        started_at=now,
        finished_at=now,
        error_tail=(
            "Pi solver invocation cancelled before start"
            if cancelled
            else "Pi solver horizon elapsed before start"
        ),
        timed_out=not cancelled,
        cancelled=cancelled,
        run_horizon_reached=not cancelled,
    )


def _failure_category(result: AgentResult) -> str:
    """Classify a bounded Pi failure without copying diagnostic text."""

    text = f"{result.error_tail}\n{result.output_tail}".lower()
    if result.timed_out or "timeout" in text or "timed out" in text:
        return "timeout"
    if any(
        token in text
        for token in ("coordinator", "websocket", "network", "connection", "transport")
    ):
        return "transport"
    if result.returncode == 137 or "out of memory" in text or "oom" in text:
        return "resource"
    if "provider" in text or "oauth" in text or "429" in text or "5xx" in text:
        return "provider"
    return "process"


def recovery_settings(config: Any) -> tuple[int, float]:
    """Return the manifest-owned outer restart count and backoff in seconds."""

    if not bool(getattr(config, "pi_recovery_enabled", True)):
        return 0, 0.0
    return (
        max(0, int(getattr(config, "pi_recovery_max_restarts", 1))),
        max(0, int(getattr(config, "pi_recovery_base_delay_ms", 1_000)))
        / 1_000.0,
    )


def is_recoverable_agent_failure(
    result: AgentResult,
    *,
    deadline_monotonic: float,
    now_monotonic: float | None = None,
    cancel_event: Any | None = None,
) -> bool:
    """Return whether an outer solver restart is safe and still in budget.

    Candidate quality is deliberately absent from this classifier.  PE, WA,
    verification failures, and other Judge verdicts are candidate-attempt
    outcomes, not process/session failures.  A task timeout (including an
    inner Pi timeout) and an intentional cancellation are always terminal.
    Only a non-timeout, non-cancelled abnormal process/invocation failure is
    recoverable while the fixed run deadline still has time; reaching the run
    horizon itself is terminal.
    """

    now = time.monotonic() if now_monotonic is None else float(now_monotonic)
    return bool(
        result.returncode != 0
        and not result.timed_out
        and not result.cancelled
        and not result.run_horizon_reached
        and now < float(deadline_monotonic)
        and not _event_is_set(cancel_event)
    )


_INTENTIONAL_CANCEL_REASONS = frozenset(
    {
        "active_cancel",
        "cancelled",
        "full_score",
        "operator_stop",
        "task_solved",
        "task_solved_by_peer",
    }
)
_FAIL_CLOSED_CANCEL_REASONS = frozenset(
    {
        "remote_settlement_unconfirmed",
        "runner_failure",
    }
)


def _stable_cancellation_reason(cancel_event: Any | None) -> str:
    """Project an Event-compatible stop source onto a bounded vocabulary.

    Runner cancellation adapters may expose ``cancellation_reason`` (for
    example ``_AnyCancelEvent``).  Do not copy arbitrary adapter text into an
    artifact: known task/operator stops are grouped as ``intentional_cancel``
    while fail-closed runner/infrastructure latches retain their stable source
    labels.  A plain Event or an unknown source is conservatively intentional.
    """

    if cancel_event is None:
        return "intentional_cancel"
    reason_getter = getattr(cancel_event, "cancellation_reason", None)
    if callable(reason_getter):
        try:
            raw_reason = reason_getter()
        except Exception:
            raw_reason = None
        if isinstance(raw_reason, str):
            reason = raw_reason.strip().casefold()
            if reason in _FAIL_CLOSED_CANCEL_REASONS:
                return reason
            if reason in _INTENTIONAL_CANCEL_REASONS:
                return "intentional_cancel"
    return "intentional_cancel"


def _recovery_exhaustion_reason(
    result: AgentResult,
    *,
    recoverable: bool,
    cancel_event: Any | None,
) -> str:
    """Return a bounded reason for a failed attempt that will not relaunch.

    The reason is intentionally a small stable vocabulary for comparison
    artifacts.  ``restart_limit`` remains distinct from the stop classification
    because it means an otherwise recoverable abnormal failure consumed the
    configured retry budget.
    """

    if result.run_horizon_reached:
        return "horizon"
    if result.timed_out:
        return "task_timeout"
    if result.cancelled or _event_is_set(cancel_event):
        return _stable_cancellation_reason(cancel_event)
    if recoverable:
        return "restart_limit"
    return "abnormal"


def _normalize_terminal_flags(result: AgentResult) -> None:
    """Make terminal stop flags authoritative even when an adapter reports rc=0.

    ``AgentResult`` is an adapter boundary and a few test/provider shims have
    historically returned a successful process code while setting a timeout
    or cancellation marker.  Letting that combination fall through the
    success branch would both emit a false success and permit a same-actor
    refill.  Preserve nonzero codes, but synthesize the conventional timeout
    or cancellation code for a malformed zero-code terminal result.
    """

    if result.returncode != 0:
        return
    if result.run_horizon_reached or result.timed_out:
        result.returncode = 124
    elif result.cancelled:
        result.returncode = 130


def run_with_recovery(
    invoke: RecoveryInvocation,
    *,
    task_id: str,
    actor_id: str,
    episode: int,
    deadline_monotonic: float,
    cancel_event: Any | None = None,
    max_restarts: int = 1,
    base_delay_seconds: float = 1.0,
    on_event: RecoveryEventSink | None = None,
) -> AgentResult:
    """Run one logical solver actor and recover abnormal session exits.

    ``invoke`` receives the zero-based recovery attempt.  Callers must keep
    actor/task/episode, workspace, prompt, and deadline fixed across calls.
    Pi derives its session identity from actor/episode and therefore resumes
    the same persisted conversation; the mutable candidate workspace is also
    retained.  Timeout and intentional-cancellation results are terminal and
    never relaunch.  If an invocation raises an ordinary ``Exception``, it is
    converted to a bounded failed attempt and follows the same retry policy
    (except for a timeout exception); ``BaseException`` subclasses such as
    ``KeyboardInterrupt`` still escape.  Backoff time counts against
    ``deadline_monotonic``.
    """

    if isinstance(max_restarts, bool) or int(max_restarts) < 0:
        raise ValueError("max_restarts must be a non-negative integer")
    restart_limit = int(max_restarts)
    delay_base = float(base_delay_seconds)
    if not math.isfinite(delay_base) or delay_base < 0:
        raise ValueError("base_delay_seconds must be a finite non-negative number")
    if not math.isfinite(float(deadline_monotonic)):
        raise ValueError("deadline_monotonic must be finite")

    def emit(event: str, **payload: Any) -> None:
        if on_event is not None:
            on_event(
                event,
                {
                    "task_id": task_id,
                    "agent_id": actor_id,
                    "episode": episode,
                    "resume_scope": "same_session_and_workspace",
                    **payload,
                },
            )

    recovery_attempt = 0
    while True:
        # A replacement can be queued while the prior attempt is draining or
        # while a broker callback is revoking the slot.  Check the fixed
        # lifecycle boundary before invoking the adapter as well as after it
        # returns; otherwise a zero-delay refill could launch a fresh Pi
        # process after normal arm closeout has already begun.
        cancelled_before_start = _event_is_set(cancel_event)
        horizon_before_start = time.monotonic() >= float(deadline_monotonic)
        if cancelled_before_start or horizon_before_start:
            result = _guard_result(
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                cancelled=cancelled_before_start,
            )
            emit(
                "agent_recovery_exhausted",
                recovery_attempt=recovery_attempt,
                max_restarts=restart_limit,
                returncode=result.returncode,
                recoverable=False,
                reason=(
                    "cancelled_before_start"
                    if cancelled_before_start
                    else "horizon_elapsed_before_start"
                ),
            )
            return result
        if recovery_attempt:
            emit(
                "agent_recovery_started",
                recovery_attempt=recovery_attempt,
                max_restarts=restart_limit,
            )
        invocation_exception_type: str | None = None
        try:
            result = invoke(recovery_attempt)
        except Exception as exc:
            # PiAgent normally converts transport/process errors into an
            # AgentResult.  Keep the generic boundary defensive for adapters
            # or test/runtime shims that raise instead: an abnormal exception
            # may retry the logical actor, while a timeout exception remains
            # terminal; never copy an exception message into artifacts.
            result, invocation_exception_type = _exception_result(
                exc,
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
        if not isinstance(result, AgentResult):
            raise TypeError("solver recovery invocation must return AgentResult")
        # PiAgent uses the same deadline for its bounded RPC wait.  A drain
        # after that wait can return just after the monotonic boundary, so the
        # caller must not mistake this ordinary arm closeout for a recoverable
        # inner timeout.  Mark it before classification and before emitting
        # the failure event; the AgentResult object is intentionally mutable.
        if (
            result.returncode != 0
            and not result.run_horizon_reached
            and time.monotonic() >= float(deadline_monotonic)
        ):
            result.run_horizon_reached = True
        _normalize_terminal_flags(result)
        if result.returncode == 0:
            if recovery_attempt:
                emit(
                    "agent_recovery_succeeded",
                    recovery_attempt=recovery_attempt,
                    max_restarts=restart_limit,
                    returncode=result.returncode,
                    timed_out=result.timed_out,
                )
            return result

        recoverable = is_recoverable_agent_failure(
            result,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        emit(
            "agent_recovery_failure_observed",
            recovery_attempt=recovery_attempt,
            max_restarts=restart_limit,
            returncode=result.returncode,
            timed_out=result.timed_out,
            cancelled=result.cancelled,
            run_horizon_reached=result.run_horizon_reached,
            recoverable=recoverable,
            failure_category=_failure_category(result),
            failure_source=(
                "invoke_exception" if invocation_exception_type else "agent_result"
            ),
            exception_type=invocation_exception_type,
        )
        if not recoverable or recovery_attempt >= restart_limit:
            emit(
                "agent_recovery_exhausted",
                recovery_attempt=recovery_attempt,
                max_restarts=restart_limit,
                returncode=result.returncode,
                recoverable=recoverable,
                reason=(
                    "restart_limit"
                    if recoverable
                    else _recovery_exhaustion_reason(
                        result,
                        recoverable=recoverable,
                        cancel_event=cancel_event,
                    )
                ),
            )
            return result

        delay = delay_base * (2**recovery_attempt)
        remaining = float(deadline_monotonic) - time.monotonic()
        if remaining <= delay:
            # The configured backoff no longer fits, but the fixed horizon
            # may still have time left.  This is recovery exhaustion, not a
            # natural horizon truncation: callers must be able to release the
            # failed slot and perform their bounded task-level refill while
            # ``is_recoverable_agent_failure`` still sees time remaining.
            horizon_elapsed = time.monotonic() >= float(deadline_monotonic)
            if horizon_elapsed:
                result.run_horizon_reached = True
            emit(
                "agent_recovery_exhausted",
                recovery_attempt=recovery_attempt,
                max_restarts=restart_limit,
                returncode=result.returncode,
                recoverable=False,
                reason=(
                    "horizon_elapsed_before_backoff"
                    if horizon_elapsed
                    else "insufficient_horizon_for_backoff"
                ),
            )
            return result
        next_attempt = recovery_attempt + 1
        emit(
            "agent_recovery_scheduled",
            recovery_attempt=next_attempt,
            max_restarts=restart_limit,
            delay_seconds=delay,
        )
        if delay > 0 and cancel_event is not None:
            if bool(cancel_event.wait(delay)):
                emit(
                    "agent_recovery_exhausted",
                    recovery_attempt=recovery_attempt,
                    max_restarts=restart_limit,
                    returncode=result.returncode,
                    recoverable=False,
                    reason="cancelled_during_backoff",
                )
                return result
        elif delay > 0:
            time.sleep(delay)

        # The wait/sleep itself is part of the fixed horizon.  Check both
        # runner cancellation and the monotonic deadline again immediately
        # before relaunching; otherwise a zero-delay retry (or scheduling
        # overhead after a short backoff) could start a new Pi process after
        # the arm has already entered normal closeout.
        if _event_is_set(cancel_event):
            emit(
                "agent_recovery_exhausted",
                recovery_attempt=recovery_attempt,
                max_restarts=restart_limit,
                returncode=result.returncode,
                recoverable=False,
                reason="cancelled_before_relaunch",
            )
            return result
        if time.monotonic() >= float(deadline_monotonic):
            result.run_horizon_reached = True
            emit(
                "agent_recovery_exhausted",
                recovery_attempt=recovery_attempt,
                max_restarts=restart_limit,
                returncode=result.returncode,
                recoverable=False,
                reason="horizon_elapsed_before_relaunch",
            )
            return result
        recovery_attempt = next_attempt
