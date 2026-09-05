"""Run-local capability broker for solver Judge probes and CPS operations.

The solver process receives only an unguessable loopback URL.  The broker owns
the real evaluator, task/baseline binding, candidate path, CPS identity, global
admission semaphore, and experiment deadline.  Worker-supplied payloads cannot
override any of those values.
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
import datetime as dt
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import inspect
import json
import math
from pathlib import Path
import re
import secrets
import threading
import time
from typing import Any, Callable, Iterator, Mapping

from .cps import CPSStore
from .evaluator import (
    LEAN_PROBE_RESPONSE_PROFILE,
    safe_worker_response,
    sanitize_worker_identifier,
    sanitize_worker_text,
)
from .formal_tools import FormalToolPolicy, sanitize_public_text
from .models import Task, Verdict
from .secure_io import DEFAULT_MAX_CANDIDATE_BYTES, read_regular_bytes
from .selection_store import CANONICAL_FEEDBACK_KINDS, SelectionStore
from .timeout_policy import (
    AGENT_TIMEOUT_MIN_SECONDS,
    AgentTimeout,
    agent_timeout_bounds,
    normalize_agent_timeout,
    timeout_fields,
)


_MAX_REQUEST_BYTES = 32 * 1024
# Keep a per-session guard, but leave enough room for a bounded CPS solver to
# work through a hard task.  The old value (32) was routinely reached by
# normal formal agents before the one-hour horizon, turning an otherwise
# valid candidate-attempt stream into a fail-closed infrastructure error.
# 128 remains finite and, together with the one-second admission interval and
# the fixed horizon, bounds one session to a small, auditable share of Judge
# capacity.  This is a broker-wide runtime guard and is identical for every
# allocation arm; it is not a policy-specific tuning knob.
_MAX_PROBE_CALLS_PER_SESSION = 128
_MIN_PROBE_INTERVAL_SECONDS = 1.0
_PROBE_ADMISSION_TIMEOUT_SECONDS: float | None = None
_AGENT_TIMEOUT_BOUNDARY_EPSILON_SECONDS = 0.01
# Closeout is outside the solver horizon.  A five-second drain was sufficient
# for a single canary, but it races legitimate Judge handlers when several
# formal arms revoke their sessions together: queued cancellation/receipt
# reconciliation can take tens of seconds even after the remote service is
# healthy.  Keep this bounded (and fail closed if it is genuinely stuck), but
# leave enough time for the fixed Judge lifecycle to settle.  The default is
# extended below when the evaluator exposes its deferred-settlement horizon;
# the extra margin covers the final poll/callback handoff.
_BROKER_DRAIN_TIMEOUT_SECONDS = 120.0
_BROKER_DRAIN_SETTLEMENT_MARGIN_SECONDS = 60.0
_RUNNER_ONLY_CPS_KINDS = frozenset({"validation_result"})
_ALLOWED_CANDIDATE_FILENAMES = frozenset({"result.lean", "result.cpp"})
_LEAN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_'.₀-₉ⁿ¹²³@]*$")
_IMPORT_LINE = re.compile(r"(?m)^\s*import\s+[^\n]+\s*$")
_DEFAULT_TACTICS = (
    "exact?",
    "simp",
    "simp_all",
    "omega",
    "norm_num",
    "positivity",
    "linarith",
    "ring",
    "aesop",
    "decide",
)
_FORMAL_NONCACHEABLE_STATUSES = frozenset(
    {
        "BUDGET_EXHAUSTED",
        "BROKER_ERROR",
        "EVALUATOR_ERROR",
        "EVALUATOR_TIMEOUT",
        "INFRASTRUCTURE_ERROR",
        "NETWORK_ERROR",
        "OUT_OF_HORIZON",
        "REMOTE_SETTLEMENT_UNCONFIRMED",
        "REJECTED_OVERLOADED",
        "TASK_CANCELLED",
    }
)
# CPS workers must first receive a real terminal Judge/local-contract result.
# These statuses are candidate-attempt feedback, including bounded resource or
# execution failures; they do not confer score authority. Admission, transport,
# provenance, and other control failures do not satisfy this ordering contract.
_JUDGE_CHECKPOINT_TERMINAL_STATUSES = frozenset(
    {
        "PROVED",
        "COMPILES_WITH_SORRY",
        "VERIFY_FAIL",
        "LOCAL_REJECTED",
        "CHEATING",
        "RESOURCE_LIMIT",
        "EXECUTION_TIMEOUT",
        # Coding Judge candidate-attempt outcomes.  These unlock CPS only
        # after the same candidate/task/job provenance checks as formal
        # terminal feedback below; a status label by itself is insufficient.
        "WA",
        "PE",
        "CE",
        "MLE",
        "TLE",
        "RE",
    }
)
_CHECKPOINT_VALUE_UNSET = object()

# Cumulative Agent-budget fields are copied to the bounded broker/audit
# surface.  Keep the vocabulary explicit: these values are accounting
# metadata, not arbitrary evaluator response passthrough.
_TIMEOUT_BUDGET_TEXT_FIELDS = frozenset(
    {
        "timeout_budget_mode",
        "timeout_budget_stop_reason",
    }
)
_TIMEOUT_BUDGET_NUMBER_FIELDS = frozenset(
    {
        "timeout_budget_seconds",
        "timeout_budget_elapsed_seconds",
        "timeout_budget_remaining_seconds",
        "judge_attempt_count",
        "judge_retry_count",
    }
)
_TIMEOUT_BUDGET_BOOL_FIELDS = frozenset({"timeout_budget_exhausted"})
_TIMEOUT_BUDGET_ARRAY_FIELDS = frozenset(
    {
        "judge_attempt_timeouts_seconds",
        "judge_attempt_elapsed_seconds",
        "judge_retry_reasons",
        "judge_attempt_ids",
    }
)

# Formal helper retry admission is a separate task-global quota.  Only these
# bounded, typed markers may cross the worker/audit projection; never forward
# arbitrary evaluator response keys.
_FORMAL_QUOTA_TEXT_FIELDS = frozenset({"retry_blocked_reason"})
_FORMAL_QUOTA_NUMBER_FIELDS = frozenset({"backend_job_count"})
_FORMAL_QUOTA_BOOL_FIELDS = frozenset({"formal_backend_budget_exhausted"})


def _default_drain_timeout_seconds(evaluator: Any) -> float:
    """Choose a closeout deadline that covers deferred Judge settlement.

    ``LeanEvaluator`` may retain a semaphore permit while a known cancelled
    job is reconciled by a background watcher.  The broker must not give up
    before that watcher has reached its own bounded deadline: doing so turns a
    recoverable cancellation into a spurious ``JudgeBrokerDrainError``.  Keep
    the historical 120-second floor for adapters without this optional
    surface, and add a fixed handoff margin for evaluators that expose it.
    """

    try:
        watcher_timeout = float(
            getattr(evaluator, "deferred_settlement_timeout_seconds", 0.0)
        )
    except (TypeError, ValueError, OverflowError):
        watcher_timeout = 0.0
    if not math.isfinite(watcher_timeout) or watcher_timeout <= 0:
        watcher_timeout = 0.0
    return max(
        _BROKER_DRAIN_TIMEOUT_SECONDS,
        watcher_timeout + _BROKER_DRAIN_SETTLEMENT_MARGIN_SECONDS,
    )


class JudgeBrokerDrainError(RuntimeError):
    """The broker could not finish handler audit and FIFO drain in time."""

    def __init__(self, state: Mapping[str, Any]):
        super().__init__("Judge broker closeout did not drain before its deadline.")
        self.state = {
            "drained": False,
            "active_handlers": max(0, int(state.get("active_handlers", 0))),
            "fifo_depth": max(0, int(state.get("fifo_depth", 0))),
            "remote_unsettled_jobs": max(
                0, int(state.get("remote_unsettled_jobs", 0))
            ),
        }
        pending = _nonnegative_count(state.get("pending_settlement_watchers", 0))
        if pending:
            self.state["pending_settlement_watchers"] = pending


@dataclass
class _CandidateBinding:
    task: Task
    path: Path
    expected_task_contract_sha256: str


@dataclass(frozen=True)
class CandidateSnapshot:
    """Exact UTF-8 candidate bytes frozen before Judge admission."""

    source: str
    sha256: str


@dataclass
class _SessionClaim:
    broker: Any
    actor_id: str
    workdir: Path
    candidates: dict[str, _CandidateBinding]
    deadline_monotonic: float
    deadline_epoch_ms: int
    # Logical solver episode (not a process attempt/restart counter).  Keeping
    # it on the capability claim lets every broker-side Judge/receipt event
    # join the runner's task/actor/episode attribution tuple.
    episode: int = 0
    cps_store: CPSStore | None = None
    communication: str = "none"
    direct_messages_allowed: bool = True
    selection_store: SelectionStore | None = None
    selection_enabled: bool = False
    selection_search: Callable[[Any, str, int], Mapping[str, Any]] | None = None
    roster_path: Path | None = None
    on_authoritative_verdict: (
        Callable[[Task, Verdict, CandidateSnapshot], None] | None
    ) = None
    cancel_event: threading.Event | None = None
    revoked_event: threading.Event = field(default_factory=threading.Event, repr=False)
    probe_calls: int = 0
    last_probe_started: float = 0.0
    probe_active: bool = False
    judge_checkpoint_reached: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    cps_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class _ClaimCancelEvent:
    """Event-compatible view over session revocation and task cancellation."""

    def __init__(self, claim: _SessionClaim):
        self.claim = claim

    def is_set(self) -> bool:
        return _claim_cancelled(self.claim)

    def wait(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        while not self.is_set():
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                delay = min(remaining, 0.02)
            else:
                delay = 0.02
            self.claim.revoked_event.wait(timeout=delay)
        return True

    def cancellation_reason(self) -> str | None:
        if self.claim.revoked_event.is_set():
            return "broker_revoked"
        event = self.claim.cancel_event
        getter = getattr(event, "cancellation_reason", None)
        if callable(getter):
            try:
                reason = getter()
            except Exception:
                reason = None
            if isinstance(reason, str) and reason:
                return reason
        return None


class _BrokerHTTPServer(ThreadingHTTPServer):
    # JudgeBroker performs its own observable, deadline-bounded drain.  Daemon
    # request threads ensure a broken evaluator cannot wedge process exit after
    # that deadline has already been reported as a fatal closeout error.
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = False

    def process_request(self, request: Any, client_address: Any) -> None:
        self._broker._handler_started()
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._broker._handler_finished()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            # BaseHTTPRequestHandler returns only after the operation response;
            # judge_check appends its audit row before writing that response.
            self._broker._handler_finished()


class JudgeBroker:
    """A loopback-only, token-bound broker shared by all solver sessions."""

    def __init__(
        self,
        evaluator: Any,
        evaluator_gate: threading.BoundedSemaphore,
        *,
        audit_path: Path,
        max_probe_calls_per_session: int = _MAX_PROBE_CALLS_PER_SESSION,
        min_probe_interval_seconds: float = _MIN_PROBE_INTERVAL_SECONDS,
        probe_admission_timeout_seconds: float | None = _PROBE_ADMISSION_TIMEOUT_SECONDS,
        drain_timeout_seconds: float | None = None,
        formal_policy: FormalToolPolicy | None = None,
        formal_audit_path: Path | None = None,
        direct_messages_allowed: bool | None = None,
        selection_store: SelectionStore | None = None,
        selection_enabled: bool = False,
        selection_search: Callable[[Any, str, int], Mapping[str, Any]] | None = None,
        profiler: Any | None = None,
        agent_timeout_enabled: bool = False,
        agent_timeout_cap_seconds: int | float | None = None,
    ):
        self.evaluator = evaluator
        self.evaluator_gate = evaluator_gate
        self.audit_path = Path(audit_path)
        self.max_probe_calls_per_session = max(1, int(max_probe_calls_per_session))
        self.min_probe_interval_seconds = max(0.0, float(min_probe_interval_seconds))
        self.probe_admission_timeout_seconds = (
            None
            if probe_admission_timeout_seconds is None
            else max(0.01, float(probe_admission_timeout_seconds))
        )
        if drain_timeout_seconds is None:
            normalized_drain_timeout = _default_drain_timeout_seconds(evaluator)
        else:
            normalized_drain_timeout = float(drain_timeout_seconds)
        if not math.isfinite(normalized_drain_timeout) or normalized_drain_timeout <= 0:
            raise ValueError("broker drain timeout must be finite and positive")
        self.drain_timeout_seconds = normalized_drain_timeout
        self.formal_policy = formal_policy
        if direct_messages_allowed is not None and not isinstance(
            direct_messages_allowed, bool
        ):
            raise ValueError("direct_messages_allowed must be a boolean or None")
        # ``None`` preserves the historical broker surface: every CPS-enabled
        # session can use both shared and direct operations.  Callers can set a
        # broker-wide default while a session remains able to narrow it.
        self.direct_messages_allowed = direct_messages_allowed
        if not isinstance(selection_enabled, bool):
            raise ValueError("selection_enabled must be a boolean")
        if selection_enabled and selection_store is None:
            raise ValueError("selection_enabled requires a selection store")
        self.selection_store = selection_store
        self.selection_enabled = selection_enabled
        if selection_search is not None and not callable(selection_search):
            raise ValueError("selection_search must be callable or None")
        self.selection_search = selection_search
        # Optional diagnostic sink.  It is deliberately duck-typed so the
        # broker remains usable by narrow test/evaluator adapters without
        # importing or depending on the profiling implementation.
        self.profiler = profiler
        if not isinstance(agent_timeout_enabled, bool):
            raise ValueError("agent_timeout_enabled must be a boolean")
        self.agent_timeout_enabled = agent_timeout_enabled
        configured_agent_timeout = (
            agent_timeout_cap_seconds
            if agent_timeout_cap_seconds is not None
            else getattr(evaluator, "timeout_seconds", None)
        )
        try:
            self.agent_timeout_bounds = agent_timeout_bounds(
                configured_agent_timeout if agent_timeout_enabled else None
            )
        except ValueError:
            if agent_timeout_cap_seconds is not None:
                raise
            # A narrow test/evaluator adapter may not expose a usable timeout;
            # retain the historical default rather than making broker startup
            # depend on an optional attribute.
            self.agent_timeout_bounds = agent_timeout_bounds()
        self.agent_timeout_cap_seconds = self.agent_timeout_bounds.max_seconds
        try:
            self._profiling_enabled = bool(
                profiler is not None and getattr(profiler, "enabled", False)
            )
        except BaseException:
            self._profiling_enabled = False
        self.formal_audit_path = Path(
            formal_audit_path
            if formal_audit_path is not None
            else self.audit_path.with_name("formal_tool_calls.jsonl")
        )
        self._formal_lock = threading.RLock()
        self._formal_counts: dict[tuple[str, str], int] = {}
        self._formal_serials: dict[tuple[str, str], int] = {}
        self._formal_evaluate_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._formal_query_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._claims: dict[str, _SessionClaim] = {}
        self._claims_lock = threading.RLock()
        self._audit_lock = threading.Lock()
        self._admission_condition = threading.Condition()
        self._admission_queue: deque[object] = deque()
        self._handler_condition = threading.Condition()
        self._active_handlers = 0
        self._remote_unsettled_jobs = 0
        self._server: _BrokerHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def _profile_event(
        self,
        event: str,
        *,
        claim: _SessionClaim | None = None,
        task_id: str | None = None,
        episode: int | None = None,
        **fields: Any,
    ) -> None:
        if not self._profiling_enabled:
            return
        profiler = self.profiler
        actor_id = claim.actor_id if claim is not None else None
        if episode is None and claim is not None:
            episode = claim.episode
        try:
            profiler.emit(
                event,
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                **fields,
            )
        except BaseException:
            # A diagnostic sink must never change broker admission, settlement,
            # or cancellation behavior.
            return

    def _profile_settlement_state(self, event: str, *, state: Mapping[str, Any] | None = None) -> None:
        """Emit a bounded evaluator watcher snapshot for drain diagnosis."""

        if not self._profiling_enabled:
            return
        values: dict[str, Any] = {}
        snapshot = getattr(self.evaluator, "settlement_snapshot", None)
        if callable(snapshot):
            try:
                candidate = snapshot()
                if isinstance(candidate, Mapping):
                    values.update(candidate)
            except BaseException:
                pass
        if state:
            for key in (
                "active_handlers",
                "fifo_depth",
                "remote_unsettled_jobs",
                "pending_settlement_watchers",
            ):
                if key in state:
                    values[key] = state[key]
        self._profile_event(event, **values)

    def start(self) -> "JudgeBroker":
        if self._server is not None:
            return self
        broker = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "ContextSwarmBroker/1"
            sys_version = ""

            def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                broker._handle_http(self)

            def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
                broker._send_json(
                    self,
                    405,
                    {"ok": False, "status": "METHOD_NOT_ALLOWED"},
                )

            def log_message(self, _format: str, *_args: Any) -> None:
                return

        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        self.audit_path.touch(exist_ok=True)
        if self.formal_policy is not None and self.formal_policy.enabled:
            self.formal_audit_path.parent.mkdir(parents=True, exist_ok=True)
            self.formal_audit_path.touch(exist_ok=True)
        self._server = _BrokerHTTPServer(("127.0.0.1", 0), Handler)
        self._server._broker = self
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            name="contextswarm-judge-broker",
            daemon=True,
        )
        self._thread.start()
        self._profile_event("judge.broker.start")
        return self

    def close(self, *, timeout_seconds: float | None = None) -> dict[str, Any]:
        """Revoke capabilities and drain audited handlers within one deadline."""

        timeout = (
            self.drain_timeout_seconds
            if timeout_seconds is None
            else float(timeout_seconds)
        )
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("broker close timeout must be finite and positive")
        self._profile_event(
            "drain.start",
            timeout_seconds=timeout,
            active_handlers=self.active_handlers,
            fifo_depth=self.fifo_depth,
            pending_settlement_watchers=_nonnegative_count(
                getattr(self.evaluator, "pending_settlement_watchers", 0)
            ),
            remote_unsettled_jobs=self.remote_unsettled_jobs,
        )
        self._profile_settlement_state("drain.sample")
        deadline = time.monotonic() + timeout
        server = self._server
        thread = self._thread
        self._server = None
        self._thread = None
        with self._claims_lock:
            claims = list(self._claims.values())
            for claim in claims:
                claim.revoked_event.set()
            self._claims.clear()
        with self._admission_condition:
            self._admission_condition.notify_all()
        if server is not None:
            server.shutdown()
        state = self._wait_for_drain(deadline)
        if server is not None:
            server.server_close()
        if thread is not None:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if not state["drained"]:
            self._profile_event("drain.timeout", **state)
            raise JudgeBrokerDrainError(state)
        # A successful close is stronger than a point-in-time observation:
        # serve_forever is stopped, claims are revoked, and no handler or FIFO
        # waiter remains that could re-populate either count.
        final_state = self.drain_state()
        if any(final_state.values()):
            self._profile_event("drain.timeout", **final_state)
            raise JudgeBrokerDrainError(final_state)
        self._profile_event("drain.end", **final_state, drained=True)
        return {"drained": True, **final_state}

    def drain_state(self) -> dict[str, int]:
        """Return the closeout counters consumed by runner health checks."""

        with self._handler_condition:
            active_handlers = self._active_handlers
            broker_unsettled_jobs = self._remote_unsettled_jobs
        evaluator_unsettled_jobs = _nonnegative_count(
            getattr(self.evaluator, "remote_unsettled_jobs", 0)
        )
        pending_watchers = _nonnegative_count(
            getattr(self.evaluator, "pending_settlement_watchers", 0)
        )
        with self._admission_condition:
            fifo_depth = len(self._admission_queue)
        state = {
            "active_handlers": max(0, int(active_handlers)),
            "fifo_depth": max(0, int(fifo_depth)),
            "remote_unsettled_jobs": (
                max(0, int(broker_unsettled_jobs)) + evaluator_unsettled_jobs
            ),
        }
        if pending_watchers:
            state["pending_settlement_watchers"] = pending_watchers
        return state

    @property
    def active_handlers(self) -> int:
        return self.drain_state()["active_handlers"]

    @property
    def fifo_depth(self) -> int:
        return self.drain_state()["fifo_depth"]

    @property
    def remote_unsettled_jobs(self) -> int:
        return self.drain_state()["remote_unsettled_jobs"]

    def _handler_started(self) -> None:
        with self._handler_condition:
            self._active_handlers += 1

    def _handler_finished(self) -> None:
        with self._handler_condition:
            self._active_handlers -= 1
            self._handler_condition.notify_all()

    def _mark_remote_unsettled(self) -> None:
        """Latch one unconfirmed remote job without retaining its identity."""

        # LeanEvaluator latches every unconfirmed cancellation, including
        # direct runner evaluations outside a broker handler.  Narrow test or
        # alternate evaluator adapters may expose only the safe response; use
        # the broker-local counter as their fail-closed fallback.
        if _nonnegative_count(
            getattr(self.evaluator, "remote_unsettled_jobs", 0)
        ) > 0:
            return
        with self._handler_condition:
            self._remote_unsettled_jobs += 1
            self._handler_condition.notify_all()

    def _remote_settlement_unconfirmed(self) -> bool:
        """Return whether any run-local Judge work lacks terminal proof."""

        with self._handler_condition:
            broker_unsettled_jobs = self._remote_unsettled_jobs
        return broker_unsettled_jobs > 0 or _nonnegative_count(
            getattr(self.evaluator, "remote_unsettled_jobs", 0)
        ) > 0

    def _wait_for_drain(self, deadline_monotonic: float) -> dict[str, Any]:
        last_sample_at = 0.0
        last_sample_state: tuple[int, int, int, int] | None = None
        while True:
            state = self.drain_state()
            settlement = getattr(self.evaluator, "settlement_snapshot", None)
            watcher_state: Mapping[str, Any] = {}
            now = 0.0
            if self._profiling_enabled:
                now = time.monotonic()
            if self._profiling_enabled and callable(settlement):
                try:
                    candidate = settlement()
                    if isinstance(candidate, Mapping):
                        watcher_state = candidate
                except BaseException:
                    watcher_state = {}
            sample_key = (
                int(state.get("active_handlers", 0)),
                int(state.get("fifo_depth", 0)),
                int(state.get("remote_unsettled_jobs", 0)),
                int(watcher_state.get("pending_settlement_watchers", 0) or 0),
            )
            if self._profiling_enabled and (
                now - last_sample_at >= 1.0
                or sample_key != last_sample_state
            ):
                sample_fields = dict(state)
                sample_fields.update(dict(watcher_state))
                self._profile_event("drain.sample", **sample_fields)
                last_sample_at = now
                last_sample_state = sample_key
            if (
                state["active_handlers"] == 0
                and state["fifo_depth"] == 0
                and _nonnegative_count(
                    getattr(self.evaluator, "pending_settlement_watchers", 0)
                ) == 0
            ):
                return {
                    "drained": state["remote_unsettled_jobs"] == 0,
                    **state,
                }
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                # A handler can finish on the same tick that the deadline is
                # reached.  The state sampled above is then stale, and
                # returning an unconditional failure turns an already quiet
                # broker into a spurious closeout error.  Re-sample once at
                # the boundary and accept the close if every accounting
                # domain is actually empty.
                final_state = self.drain_state()
                pending_watchers = _nonnegative_count(
                    getattr(self.evaluator, "pending_settlement_watchers", 0)
                )
                drained = (
                    final_state["active_handlers"] == 0
                    and final_state["fifo_depth"] == 0
                    and pending_watchers == 0
                    and final_state["remote_unsettled_jobs"] == 0
                )
                if pending_watchers:
                    final_state["pending_settlement_watchers"] = pending_watchers
                return {"drained": drained, **final_state}
            with self._handler_condition:
                self._handler_condition.wait(timeout=min(remaining, 0.05))

    def public_policy(self) -> dict[str, Any]:
        policy: dict[str, Any] = {
            "transport": "runner_owned_loopback_capability",
            "response_profile": LEAN_PROBE_RESPONSE_PROFILE,
            "max_probe_calls_per_session": self.max_probe_calls_per_session,
            "min_probe_interval_seconds": self.min_probe_interval_seconds,
            "probe_admission_deadline": "session_remaining_horizon",
            "probe_admission_timeout_seconds": self.probe_admission_timeout_seconds,
            "candidate_selection": "runner_bound_task_candidate_filename",
            "allowed_candidate_filenames": sorted(_ALLOWED_CANDIDATE_FILENAMES),
            "candidate_submission": "immutable_source_snapshot",
            "shares_final_evaluator_gate": True,
            "submitted_job_cancellation": "delete_on_probe_cancel_or_deadline",
            "closeout_requires_active_handlers": 0,
            "closeout_requires_fifo_depth": 0,
            "closeout_requires_remote_unsettled_jobs": 0,
            "drain_timeout_seconds": self.drain_timeout_seconds,
            "agent_timeout": {
                "enabled": self.agent_timeout_enabled,
                "field": "timeout_seconds",
                "min_seconds": self.agent_timeout_bounds.min_seconds,
                "max_seconds": self.agent_timeout_bounds.max_seconds,
                "configured_evaluator_cap_seconds": self.agent_timeout_cap_seconds,
                "semantics": "cumulative_total_validation_budget_across_retries",
                "retry_scope": "same_broker_handler_and_evaluator_gate",
                "custom_request_backend_max_retries": max(
                    0, int(getattr(self.evaluator, "backend_max_retries", 1))
                ),
                "custom_request_terminal_overload_retries": max(
                    0,
                    int(getattr(self.evaluator, "terminal_overload_retries", 1)),
                ),
                "custom_request_max_retries": 0,
                "custom_request_retry_policy": (
                    "outer_logical_loop_with_remaining_absolute_budget"
                ),
                "custom_request_backend_quota": (
                    "each_fresh_retry_reserves_one_task_backend_job_unit"
                ),
                "cleanup_grace": "bounded_settlement_grace_may_extend_return_after_budget",
                "omitted_request": "configured_evaluator_timeout_and_legacy_retry_policy",
            },
            "direct_messages_allowed": (
                self.direct_messages_allowed
                if self.direct_messages_allowed is not None
                else "legacy"
            ),
            "selection_feedback": {
                "enabled": self.selection_enabled,
                "origin": "worker_explicit",
                "feedback_kinds": sorted(CANONICAL_FEEDBACK_KINDS),
            },
            "selection_search": self.selection_search is not None,
        }
        formal = self.formal_policy
        policy["formal_tools"] = (
            {
                "enabled": True,
                "surface_version": formal.surface_version,
                "transport": "same_session_loopback_capability",
                "evaluate_authority": "diagnostic_only",
                "quota_scope": "task_across_all_sessions",
                "evaluate_calls_per_task": formal.evaluate_calls_per_task,
                "evaluate_backend_jobs_per_task": formal.evaluate_backend_jobs_per_task,
                "query_calls_per_task": formal.query_calls_per_task,
                "query_backend_probes_per_task": formal.query_backend_probes_per_task,
                "max_candidate_bytes": formal.max_candidate_bytes,
                "declaration_index": formal.declaration_index.info.public_dict(),
            }
            if formal is not None and formal.enabled
            else {"enabled": False}
        )
        return policy

    def _parse_agent_timeout(
        self, payload: Mapping[str, Any]
    ) -> tuple[AgentTimeout | None, str | None]:
        """Parse the optional worker budget at the capability boundary."""

        if "timeout_seconds" not in payload:
            return None, None
        if not self.agent_timeout_enabled:
            return None, "timeout_seconds is not enabled for this run"
        try:
            timeout = normalize_agent_timeout(
                payload.get("timeout_seconds"),
                configured_timeout_seconds=self.agent_timeout_cap_seconds,
            )
        except ValueError as exc:
            return None, str(exc)
        return timeout, None

    def _attach_timeout(
        self,
        result: Mapping[str, Any],
        timeout: AgentTimeout | None,
        *,
        timeout_started: float | None = None,
        timeout_deadline: float | None = None,
    ) -> dict[str, Any]:
        normalized = dict(result)
        # Keep the historical disabled tool response byte-compatible.  The
        # enabled arm records both explicit suggestions and omitted (legacy)
        # calls so treatment adoption and fallback behavior remain auditable.
        if timeout is not None or self.agent_timeout_enabled:
            normalized.update(timeout_fields(timeout))
        # Evaluators place detailed cumulative accounting inside their bounded
        # response.  Promote the same fields to the broker result so clients,
        # formal-tool records and audit rows do not need to understand a
        # particular evaluator response nesting shape.  Control outcomes that
        # never reached an evaluator still get the requested budget and a
        # zero-attempt marker below.
        response = normalized.get("response")
        response_fields = _safe_timeout_budget_fields(
            response, max_timeout_seconds=self.agent_timeout_cap_seconds
        )
        normalized.update(response_fields)
        normalized.update(_safe_formal_quota_fields(response))
        normalized.update(
            _safe_timeout_budget_fields(
                normalized, max_timeout_seconds=self.agent_timeout_cap_seconds
            )
        )
        normalized.update(_safe_formal_quota_fields(normalized))
        if timeout is not None:
            # The broker owns the logical budget.  Do not let a nested
            # evaluator diagnostic overwrite the authoritative cap or the
            # requested/effective metadata exposed to the worker.
            normalized["timeout_budget_mode"] = "cumulative_total"
            normalized["timeout_budget_seconds"] = int(timeout.effective_seconds)
            invoked = bool(
                normalized.get("accepted") is True
                or normalized.get("call_index") is not None
                or normalized.get("backend_job_number") is not None
            )
            normalized.setdefault("judge_attempt_count", 1 if invoked else 0)
            normalized.setdefault("judge_retry_count", 0)
            normalized.setdefault("timeout_budget_exhausted", False)
            if timeout_started is not None and timeout_deadline is not None:
                now = time.monotonic()
                elapsed = max(
                    0.0,
                    min(
                        float(timeout.effective_seconds),
                        now - float(timeout_started),
                    ),
                )
                remaining = max(0.0, float(timeout_deadline) - now)
                normalized["timeout_budget_elapsed_seconds"] = round(elapsed, 6)
                normalized["timeout_budget_remaining_seconds"] = round(
                    min(float(timeout.effective_seconds), remaining), 6
                )
                if remaining <= 0:
                    normalized["timeout_budget_exhausted"] = True
                    normalized["timeout_budget_remaining_seconds"] = 0.0
            else:
                # When the caller did not provide timestamps, preserve
                # evaluator accounting only within the broker's configured
                # cap.  This keeps a malformed nested value from advertising
                # more budget than the hard boundary actually allows.
                for key in (
                    "timeout_budget_elapsed_seconds",
                    "timeout_budget_remaining_seconds",
                ):
                    raw_value = normalized.get(key)
                    if isinstance(raw_value, (int, float)) and not isinstance(
                        raw_value, bool
                    ):
                        normalized[key] = round(
                            max(
                                0.0,
                                min(float(timeout.effective_seconds), float(raw_value)),
                            ),
                            6,
                        )
        return normalized

    def formal_summary(self) -> dict[str, Any]:
        """Return public run-global counters after broker capabilities are silent."""

        with self._formal_lock:
            counts: dict[str, dict[str, int]] = {}
            for (task_id, counter), value in sorted(self._formal_counts.items()):
                counts.setdefault(task_id, {})[counter] = max(0, int(value))
        return {
            "enabled": bool(self.formal_policy and self.formal_policy.enabled),
            "quota_scope": "task_across_all_sessions",
            "tasks": counts,
        }

    @contextmanager
    def session(
        self,
        *,
        actor_id: str,
        episode: int = 0,
        workdir: Path,
        candidates: Mapping[str, tuple[Task, Path]],
        deadline_monotonic: float,
        cps_store: CPSStore | None = None,
        communication: str = "none",
        direct_messages_allowed: bool | None = None,
        selection_store: SelectionStore | None = None,
        selection_enabled: bool | None = None,
        selection_search: Callable[[Any, str, int], Mapping[str, Any]] | None = None,
        roster_path: Path | None = None,
        on_authoritative_verdict: (
            Callable[[Task, Verdict, CandidateSnapshot], None] | None
        ) = None,
        cancel_event: threading.Event | None = None,
    ) -> Iterator[dict[str, str]]:
        """Issue and later revoke a capability bound to exact candidates."""

        if self._server is None:
            raise RuntimeError("Judge broker has not been started")
        if isinstance(episode, bool) or not isinstance(episode, int) or episode < 0:
            raise ValueError("broker session episode must be a non-negative integer")
        resolved_workdir = Path(workdir).resolve()
        bindings: dict[str, _CandidateBinding] = {}
        expected_contract = getattr(
            self.evaluator,
            "expected_task_contract_sha256",
            None,
        )
        if not callable(expected_contract):
            raise ValueError(
                "evaluator must expose expected_task_contract_sha256(task)"
            )
        for task_id, (task, candidate_path) in candidates.items():
            candidate_filename = str(task.candidate_filename)
            if candidate_filename not in _ALLOWED_CANDIDATE_FILENAMES:
                raise ValueError("task declares an unsupported candidate filename")
            candidate = Path(candidate_path).resolve()
            if candidate.name != candidate_filename or not _is_relative_to(
                candidate, resolved_workdir
            ):
                raise ValueError(
                    "broker candidate must match the task-declared filename "
                    "inside the assigned workdir"
                )
            if str(task_id) != task.slug:
                raise ValueError("broker task key must match the bound task slug")
            contract_sha256 = _safe_hash(expected_contract(task))
            if contract_sha256 is None:
                raise ValueError("evaluator returned an invalid task-contract identity")
            bindings[task.slug] = _CandidateBinding(
                task=task,
                path=candidate,
                expected_task_contract_sha256=contract_sha256,
            )
        if not bindings:
            raise ValueError("broker session requires at least one task candidate")

        requested_communication = str(communication or "none").strip().lower()
        normalized_communication = requested_communication
        if normalized_communication == "simple":
            normalized_communication = "blackboard"
        if normalized_communication not in {"none", "blackboard", "direct", "hybrid"}:
            raise ValueError("unsupported broker communication policy")
        if normalized_communication != "none" and (cps_store is None or len(bindings) != 1):
            raise ValueError("CPS broker sessions require one task and a CPS store")
        if direct_messages_allowed is not None and not isinstance(
            direct_messages_allowed, bool
        ):
            raise ValueError("direct_messages_allowed must be a boolean or None")
        # Either scope may narrow the capability, but a session cannot widen a
        # broker-wide denial.  With neither scope specified this remains True,
        # exactly preserving the pre-gate CPS behavior.
        effective_direct_messages_allowed = not (
            self.direct_messages_allowed is False
            or direct_messages_allowed is False
        )
        if selection_enabled is not None and not isinstance(selection_enabled, bool):
            raise ValueError("selection_enabled must be a boolean or None")
        effective_selection_enabled = (
            self.selection_enabled
            if selection_enabled is None
            else selection_enabled
        )
        effective_selection_store = (
            selection_store if selection_store is not None else self.selection_store
        )
        if selection_search is not None and not callable(selection_search):
            raise ValueError("selection_search must be callable or None")
        effective_selection_search = (
            selection_search if selection_search is not None else self.selection_search
        )
        if effective_selection_enabled and effective_selection_store is None:
            raise ValueError("selection_enabled requires a selection store")
        if effective_selection_enabled:
            # Selection experiments compare ranking only.  Accepting the
            # direct/hybrid surfaces here would reintroduce a second treatment
            # path even if the caller forgot to register the matching tool
            # gate.  Check the requested spelling so the legacy ``simple``
            # alias cannot silently enter a registered selection arm either.
            if requested_communication != "blackboard":
                raise ValueError(
                    "selection-enabled broker sessions require "
                    "communication = blackboard"
                )
            # Server-side authorization is authoritative.  A stale or
            # contradictory caller flag must never widen a selection arm's
            # direct-message capability.
            effective_direct_messages_allowed = False

        normalized_deadline = float(deadline_monotonic)
        if not math.isfinite(normalized_deadline):
            raise ValueError("broker session deadline must be finite")
        remaining_seconds = max(0.0, normalized_deadline - time.monotonic())
        deadline_epoch_ms = int((time.time() + remaining_seconds) * 1_000)
        token = secrets.token_urlsafe(32)
        claim = _SessionClaim(
            broker=self,
            actor_id=str(actor_id),
            episode=episode,
            workdir=resolved_workdir,
            candidates=bindings,
            deadline_monotonic=normalized_deadline,
            deadline_epoch_ms=deadline_epoch_ms,
            cps_store=cps_store,
            communication=normalized_communication,
            direct_messages_allowed=effective_direct_messages_allowed,
            selection_store=effective_selection_store,
            selection_enabled=effective_selection_enabled,
            selection_search=effective_selection_search,
            roster_path=Path(roster_path).resolve() if roster_path is not None else None,
            on_authoritative_verdict=on_authoritative_verdict,
            cancel_event=cancel_event,
        )
        with self._claims_lock:
            self._claims[token] = claim
        self._profile_event(
            "judge.session.start",
            claim=claim,
            candidate_count=len(bindings),
            communication=normalized_communication,
            selection_enabled=effective_selection_enabled,
        )
        host, port = self._server.server_address[:2]
        try:
            yield {
                "CONTEXTSWARM_JUDGE_URL": f"http://{host}:{port}/{token}",
                "CONTEXTSWARM_BROKER_DEADLINE_EPOCH_MS": str(deadline_epoch_ms),
            }
        finally:
            self._profile_event(
                "judge.session.end",
                claim=claim,
                candidate_count=len(bindings),
                probe_calls=claim.probe_calls,
            )
            claim.revoked_event.set()
            with self._claims_lock:
                self._claims.pop(token, None)
            with self._admission_condition:
                self._admission_condition.notify_all()
            # A handler may already have resolved the claim before it was
            # removed from the token map.  Drain its serialized CPS operation
            # so this context cannot return while a stale capability can still
            # commit runner-owned communication state.
            with claim.cps_lock:
                pass

    def _handle_http(self, handler: BaseHTTPRequestHandler) -> None:
        parts = [part for part in handler.path.split("?")[0].split("/") if part]
        if len(parts) != 2:
            self._send_json(handler, 404, {"ok": False, "status": "NOT_FOUND"})
            return
        token, operation = parts
        with self._claims_lock:
            claim = self._claims.get(token)
        if claim is None:
            self._send_json(handler, 403, {"ok": False, "status": "INVALID_CAPABILITY"})
            return
        try:
            length = int(handler.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length < 0 or length > _MAX_REQUEST_BYTES:
            self._send_json(handler, 413, {"ok": False, "status": "INVALID_REQUEST"})
            return
        try:
            raw = handler.rfile.read(length)
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeError, json.JSONDecodeError):
            self._send_json(handler, 400, {"ok": False, "status": "INVALID_JSON"})
            return
        if not isinstance(payload, Mapping):
            self._send_json(handler, 400, {"ok": False, "status": "INVALID_REQUEST"})
            return

        try:
            if operation == "judge_check":
                result = self._judge_check(claim, payload)
            elif operation == "evaluate_local":
                result = self._evaluate_local(claim, payload)
            elif operation == "formal_query":
                result = self._formal_query(claim, payload)
            elif operation.startswith("cps_"):
                result = self._cps_operation(claim, operation, payload)
            else:
                self._send_json(handler, 404, {"ok": False, "status": "NOT_FOUND"})
                return
        except Exception:
            # This is the process boundary for a solver capability.  Keep the
            # failure stable and auditable without reflecting exception text,
            # which may contain a host path or private transport detail.
            result = _control_result(
                "BROKER_ERROR",
                "The controlled experiment broker failed this capability call.",
                retryable=False,
            )
            if operation == "judge_check":
                audit_task = (
                    next(iter(claim.candidates))
                    if len(claim.candidates) == 1
                    else "__invalid__"
                )
                self._audit(claim, audit_task, result, accepted=False)
            elif operation in {"evaluate_local", "formal_query"}:
                self._formal_audit(
                    claim,
                    operation,
                    "__invalid__",
                    result,
                    accepted=False,
                )
        self._send_json(handler, 200, result)

    def _judge_check(
        self, claim: _SessionClaim, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        # Start the capability clock before validation so even a rejected
        # request gets a terminal ``judge.receipt`` row.  This is intentionally
        # separate from the evaluator execution clock below: validation,
        # admission, execution and audit are distinct stages in the profile.
        timeout_request, timeout_error = self._parse_agent_timeout(payload)
        # Capture the capability-call start only when the explicit budget is
        # enabled (or profiling already needs the clock).  This lets the
        # budget cover candidate snapshot and evaluator-gate admission, rather
        # than granting a fresh full budget after a long queue wait.
        request_started = (
            time.monotonic()
            if self._profiling_enabled or timeout_request is not None
            else 0.0
        )
        timeout_deadline = (
            request_started + timeout_request.effective_seconds
            if timeout_request is not None
            else None
        )

        def finish(
            task_id: str,
            result: Mapping[str, Any],
            **kwargs: Any,
        ) -> dict[str, Any]:
            return self._finish_judge_check(
                claim,
                task_id,
                self._attach_timeout(
                    result,
                    timeout_request,
                    timeout_started=request_started,
                    timeout_deadline=timeout_deadline,
                ),
                **kwargs,
            )

        allowed_fields = {"task_id"}
        if self.agent_timeout_enabled:
            allowed_fields.add("timeout_seconds")
        if timeout_error or set(payload) - allowed_fields:
            result = _control_result(
                "INVALID_REQUEST",
                timeout_error
                or "judge_check accepts only the runner-bound task selection and optional timeout_seconds.",
                retryable=False,
            )
            audit_task = (
                next(iter(claim.candidates))
                if len(claim.candidates) == 1
                else "__invalid__"
            )
            return finish(
                audit_task,
                result,
                accepted=False,
                started=request_started,
                gate_wait_started=request_started,
            )
        try:
            task_id = _select_task_id(claim, payload.get("task_id"))
        except ValueError as exc:
            result = _control_result(
                "INVALID_TASK_SELECTION",
                _safe_error(exc),
                retryable=False,
            )
            return finish(
                "__invalid__",
                result,
                accepted=False,
                started=request_started,
                gate_wait_started=request_started,
            )
        now = time.monotonic()
        with claim.lock:
            if claim.probe_active:
                result = _control_result(
                    "SESSION_PROBE_IN_FLIGHT",
                    "Only one judge_check may be in flight for this solver session.",
                    retryable=True,
                )
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=request_started,
                    gate_wait_started=request_started,
                )
            if claim.probe_calls >= self.max_probe_calls_per_session:
                result = _control_result(
                    "SESSION_PROBE_BUDGET_EXHAUSTED",
                    "The controlled Judge-call budget for this solver session is exhausted.",
                    retryable=False,
                )
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=request_started,
                    gate_wait_started=request_started,
                )
            cooldown = self.min_probe_interval_seconds - (now - claim.last_probe_started)
            if claim.last_probe_started and cooldown > 0:
                result = _control_result(
                    "SESSION_PROBE_COOLDOWN",
                    "Wait before submitting another candidate to judge_check.",
                    retryable=True,
                    retry_after_seconds=round(cooldown, 3),
                )
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=request_started,
                    gate_wait_started=request_started,
                )
            if now >= claim.deadline_monotonic:
                result = _control_result(
                    "OUT_OF_HORIZON",
                    "The experiment horizon has elapsed.",
                    retryable=False,
                )
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=request_started,
                    gate_wait_started=request_started,
                )
            if _claim_cancelled(claim):
                result = _control_result(
                    "TASK_CANCELLED",
                    "This solver task no longer accepts Judge work.",
                    retryable=False,
                )
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=request_started,
                    gate_wait_started=request_started,
                )
            if self._remote_settlement_unconfirmed():
                result = _remote_settlement_control_result()
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=request_started,
                    gate_wait_started=request_started,
                )
            claim.probe_active = True

        started = time.monotonic()
        gate_wait_started = started
        snapshot_seconds = 0.0
        evaluator_seconds = 0.0
        audit_seconds = 0.0
        acquired = False
        retain_evaluator_gate = False
        accepted = False
        call_index: int | None = None
        result: dict[str, Any]
        binding = claim.candidates[task_id]
        snapshot: CandidateSnapshot | None = None
        authoritative_verdict: Verdict | None = None
        evaluator_unsettled_before = 0
        evaluator_call_started = False
        # Keep the evaluator's raw provenance beside the sanitized public
        # result.  In particular, ``sanitize_worker_identifier`` deliberately
        # maps malformed values to ``None``; that must not turn a malformed
        # remote id into a valid LOCAL_REJECTED checkpoint.
        raw_judge_job_id: Any = _CHECKPOINT_VALUE_UNSET
        try:
            # Freeze exactly what this capability call submits before waiting
            # for the shared Judge gate.  The worker may continue editing its
            # candidate file while queued, but neither the request nor its audit
            # hash can then drift to a different file state.
            snapshot_started = (
                time.monotonic() if self._profiling_enabled else 0.0
            )
            if self._profiling_enabled:
                # Candidate freezing is a first-class broker stage.  Emit its
                # start before touching the filesystem so audit tooling can
                # always pair the terminal event, including snapshot errors.
                self._profile_event(
                    "judge.snapshot.start",
                    claim=claim,
                    task_id=task_id,
                    operation="candidate_snapshot",
                    phase="snapshot",
                )
            try:
                snapshot = _candidate_snapshot(
                    binding.path,
                    trusted_root=claim.workdir,
                    max_bytes=(
                        self.formal_policy.max_candidate_bytes
                        if self.formal_policy is not None
                        else DEFAULT_MAX_CANDIDATE_BYTES
                    ),
                )
            except (OSError, UnicodeError):
                result = _control_result(
                    "CANDIDATE_SNAPSHOT_ERROR",
                    "The runner could not freeze the task candidate for Judge submission.",
                    retryable=True,
                )
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=started,
                    gate_wait_started=gate_wait_started,
                    clear_probe_active=True,
                )
            finally:
                if self._profiling_enabled:
                    snapshot_seconds = max(
                        0.0, time.monotonic() - snapshot_started
                    )
                    self._profile_event(
                        "judge.snapshot.end",
                        claim=claim,
                        task_id=task_id,
                        operation="candidate_snapshot",
                        phase="snapshot",
                        elapsed_seconds=snapshot_seconds,
                        status="ok" if snapshot is not None else "error",
                    )

            self._profile_event(
                "judge.submitted",
                claim=claim,
                task_id=task_id,
                candidate_sha256=snapshot.sha256,
            )

            admission_deadline = claim.deadline_monotonic
            capped_by_agent_timeout = False
            if timeout_deadline is not None:
                if timeout_deadline <= admission_deadline:
                    capped_by_agent_timeout = True
                    admission_deadline = timeout_deadline
            capped_admission = False
            if self.probe_admission_timeout_seconds is not None:
                admission_deadline = min(
                    admission_deadline,
                    gate_wait_started + self.probe_admission_timeout_seconds,
                )
                capped_admission = admission_deadline < claim.deadline_monotonic
            try:
                acquired = self._acquire_evaluator_gate(
                    admission_deadline,
                    claim=claim,
                    task_id=task_id,
                )
            except Exception:
                result = _control_result(
                    "JUDGE_ADMISSION_ERROR",
                    "The controlled Judge admission queue failed.",
                    retryable=True,
                )
                return finish(
                    task_id,
                    result,
                    accepted=False,
                    started=started,
                    gate_wait_started=gate_wait_started,
                    candidate_sha256=snapshot.sha256,
                    clear_probe_active=True,
                )
            gate_wait = time.monotonic() - gate_wait_started
            if not acquired:
                if self._remote_settlement_unconfirmed():
                    result = _remote_settlement_control_result()
                elif _claim_cancelled(claim):
                    result = _control_result(
                        "TASK_CANCELLED",
                        "This solver task was cancelled before Judge admission.",
                        retryable=False,
                    )
                elif capped_by_agent_timeout and timeout_deadline is not None and time.monotonic() >= timeout_deadline:
                    result = _control_result(
                        "EVALUATOR_TIMEOUT",
                        "The Agent validation budget elapsed while waiting for Judge admission.",
                        retryable=False,
                    )
                elif capped_admission and time.monotonic() < claim.deadline_monotonic:
                    result = _control_result(
                        "JUDGE_ADMISSION_TIMEOUT",
                        "The controlled Judge remained busy until the admission deadline.",
                        retryable=True,
                        retry_after_seconds=max(
                            1.0, self.min_probe_interval_seconds
                        ),
                    )
                else:
                    result = _control_result(
                        "OUT_OF_HORIZON",
                        "The experiment horizon elapsed while waiting for Judge admission.",
                        retryable=False,
                    )
            elif self._remote_settlement_unconfirmed():
                result = _remote_settlement_control_result()
            elif _claim_cancelled(claim):
                result = _control_result(
                    "TASK_CANCELLED",
                    "This solver task was cancelled during Judge admission.",
                    retryable=False,
                )
            elif (
                timeout_deadline is not None
                and _remaining_agent_attempt_seconds(
                    timeout_request,
                    timeout_deadline,
                )
                is None
            ):
                result = _control_result(
                    "EVALUATOR_TIMEOUT",
                    "The Agent validation budget elapsed before Judge execution.",
                    retryable=False,
                )
            elif time.monotonic() >= claim.deadline_monotonic:
                result = _control_result(
                    "OUT_OF_HORIZON",
                    "The experiment horizon elapsed during Judge admission.",
                    retryable=False,
                )
            else:
                admitted_at = time.monotonic()
                with claim.lock:
                    # The session permits only one in-flight probe, but keep
                    # the budget check adjacent to the actual admission so the
                    # quota accounting contract remains self-contained.
                    if claim.probe_calls >= self.max_probe_calls_per_session:
                        result = _control_result(
                            "SESSION_PROBE_BUDGET_EXHAUSTED",
                            "The controlled Judge-call budget for this solver session is exhausted.",
                            retryable=False,
                        )
                    else:
                        claim.probe_calls += 1
                        claim.last_probe_started = admitted_at
                        call_index = claim.probe_calls
                        accepted = True
                if not accepted:
                    return finish(
                        task_id,
                        result,
                        accepted=False,
                        started=started,
                        gate_wait_started=gate_wait_started,
                        gate_wait_seconds=gate_wait,
                        candidate_sha256=snapshot.sha256,
                        clear_probe_active=True,
                    )
                probe_source = getattr(self.evaluator, "probe_source", None)
                probe = getattr(self.evaluator, "probe", None)
                if callable(probe_source):
                    evaluator_call = probe_source
                    candidate_argument: Any = snapshot.source
                elif callable(probe):
                    evaluator_call = probe
                    candidate_argument = binding.path
                else:
                    evaluator_call = self.evaluator.evaluate
                    candidate_argument = binding.path
                evaluator_unsettled_before = _nonnegative_count(
                    getattr(self.evaluator, "remote_unsettled_jobs", 0)
                )
                evaluator_kwargs: dict[str, Any] = {
                    "deadline_monotonic": claim.deadline_monotonic,
                }
                # A known peer-cancelled job may outlive the foreground grace
                # period.  Keep this gate permit retained and release it only
                # after the evaluator's bounded watcher receives a terminal
                # receipt.  Evaluators without this optional hook retain the
                # legacy fail-closed behavior.
                if _accepts_settlement_callback(evaluator_call):
                    evaluator_kwargs["settlement_callback"] = self._release_evaluator_gate
                if _accepts_cancel_event(evaluator_call):
                    evaluator_kwargs["cancel_event"] = _ClaimCancelEvent(claim)
                if timeout_request is not None:
                    if not _accepts_timeout(evaluator_call):
                        raise TypeError(
                            "the configured evaluator does not support timeout_seconds"
                        )
                    remaining_attempt_seconds = _remaining_agent_attempt_seconds(
                        timeout_request,
                        timeout_deadline,
                    )
                    if remaining_attempt_seconds is None:
                        # The admission check above and the evaluator call are
                        # separate scheduling points.  If the last few seconds
                        # disappear while entering this block, do not fall back
                        # to a fresh full timeout for a narrow adapter that
                        # cannot receive the absolute deadline keyword.  The
                        # hard Agent budget wins and the already-held gate is
                        # released before the normal audited receipt path.
                        self._release_evaluator_gate()
                        acquired = False
                        return finish(
                            task_id,
                            _control_result(
                                "EVALUATOR_TIMEOUT",
                                "The Agent validation budget elapsed before Judge execution.",
                                retryable=False,
                            ),
                            accepted=True,
                            started=started,
                            gate_wait_started=gate_wait_started,
                            gate_wait_seconds=gate_wait,
                            candidate_sha256=snapshot.sha256,
                            clear_probe_active=True,
                        )
                    evaluator_kwargs["timeout_seconds"] = remaining_attempt_seconds
                    if _accepts_timeout_deadline(evaluator_call):
                        evaluator_kwargs["timeout_deadline_monotonic"] = timeout_deadline
                evaluator_call_started = True
                evaluator_started_at = time.monotonic()
                self._profile_event(
                    "judge.execute.start",
                    claim=claim,
                    task_id=task_id,
                    call_index=call_index,
                    **timeout_fields(timeout_request),
                )
                try:
                    verdict: Verdict = evaluator_call(
                        binding.task,
                        candidate_argument,
                        **evaluator_kwargs,
                    )
                finally:
                    evaluator_seconds = max(
                        0.0, time.monotonic() - evaluator_started_at
                    )
                    self._profile_event(
                        "judge.execute.end",
                        claim=claim,
                        task_id=task_id,
                        call_index=call_index,
                        elapsed_seconds=evaluator_seconds,
                        **timeout_fields(timeout_request),
                    )
                raw_judge_job_id = verdict.judge_job_id
                verdict_status = _safe_verdict_status(verdict.status)
                safe_job_id = sanitize_worker_identifier(verdict.judge_job_id)
                safe_response = safe_worker_response(
                    verdict.response,
                    timeout_max_seconds=self.agent_timeout_cap_seconds,
                )
                evaluator_unsettled_after = _nonnegative_count(
                    getattr(self.evaluator, "remote_unsettled_jobs", 0)
                )
                call_unsettled = (
                    evaluator_unsettled_after > 0
                    or _has_unsettled_remote_work(
                        verdict_status,
                        safe_response,
                    )
                )
                deferred_remote = _has_deferred_remote_work(
                    verdict_status, safe_response
                )
                if call_unsettled or deferred_remote:
                    # Both unresolved classes retain the permit.  A deferred
                    # known-job cancellation is released by its watcher;
                    # an unknown job remains permanently fail-closed.
                    retain_evaluator_gate = True
                if call_unsettled and not deferred_remote:
                    # The evaluator has attempted cancellation but cannot
                    # prove the remote job terminal.  Permanently consume this
                    # process-local permit and latch a non-sensitive count;
                    # releasing it could exceed the experiment's Judge
                    # concurrency contract while the remote job still runs.
                    retain_evaluator_gate = True
                    self._mark_remote_unsettled()
                result = {
                    "ok": verdict_status
                    not in {
                        "EVALUATOR_ERROR",
                        "EVALUATOR_TIMEOUT",
                        "REJECTED_OVERLOADED",
                        "NETWORK_ERROR",
                        "CANCELLED",
                        "TASK_CANCELLED",
                    },
                    "accepted": True,
                    "call_index": call_index,
                    "task_id": task_id,
                    "status": verdict_status,
                    "proved": _safe_score(verdict.score) >= 1.0,
                    "score": _safe_score(verdict.score),
                    "elapsed_seconds": round(
                        max(0.0, _safe_finite_float(verdict.elapsed_seconds)), 6
                    ),
                    "response": safe_response,
                    "error": (
                        sanitize_worker_text(verdict.error) if verdict.error else None
                    ),
                    "candidate_sha256": snapshot.sha256,
                    "task_contract_sha256": _safe_hash(
                        verdict.task_contract_sha256
                    ),
                    "judge_job_id": safe_job_id,
                    "cache_reused": verdict.cache_reused is True,
                    "retryable": verdict_status
                    in {
                        "EVALUATOR_ERROR",
                        "EVALUATOR_TIMEOUT",
                        "REJECTED_OVERLOADED",
                        "NETWORK_ERROR",
                    },
                }
                if call_unsettled and not deferred_remote:
                    result.update(
                        {
                            "ok": False,
                            "status": "REMOTE_SETTLEMENT_UNCONFIRMED",
                            "proved": False,
                            "score": 0.0,
                            "retryable": False,
                        }
                    )
                    safe_response["remote_settlement_unconfirmed"] = True
                elif deferred_remote:
                    result.update(
                        {
                            "ok": False,
                            "status": "TASK_CANCELLED",
                            "proved": False,
                            "score": 0.0,
                            "retryable": False,
                        }
                    )
                    safe_response["settlement_deferred"] = True
                proof_claimed = verdict_status == "PROVED" or _safe_score(verdict.score) >= 1.0
                # The evaluator has a bounded settlement grace so it can
                # reconcile a known Judge job after the logical deadline.  A
                # terminal receipt observed during that grace is still too
                # late to become worker-visible validation feedback: otherwise
                # a slow adapter could return a proof after the Agent's total
                # budget and accidentally establish authoritative success.
                # Keep remote-settlement and explicit cancellation outcomes
                # ahead of this check; they are safety states, not scores.
                budget_expired = (
                    timeout_deadline is not None
                    and time.monotonic() >= timeout_deadline
                    and not _claim_cancelled(claim)
                )
                if call_unsettled or deferred_remote:
                    authoritative_verdict = None
                elif time.monotonic() >= claim.deadline_monotonic:
                    result.update(
                        {
                            "ok": False,
                            "status": "OUT_OF_HORIZON",
                            "proved": False,
                            "score": 0.0,
                            "error": None,
                            "retryable": False,
                        }
                    )
                    authoritative_verdict = None
                elif budget_expired:
                    result.update(
                        {
                            "ok": False,
                            "status": "EVALUATOR_TIMEOUT",
                            "proved": False,
                            "score": 0.0,
                            "error": "The Agent validation budget elapsed during Judge evaluation.",
                            "retryable": False,
                        }
                    )
                    authoritative_verdict = None
                elif proof_claimed and _is_authoritative_proof(
                    task=binding.task,
                    verdict=verdict,
                    snapshot=snapshot,
                    expected_task_contract_sha256=(
                        binding.expected_task_contract_sha256
                    ),
                    allow_mock_provenance=(
                        getattr(self.evaluator, "is_mock_evaluator", False) is True
                    ),
                ):
                    authoritative_verdict = Verdict(
                        task_id=binding.task.slug,
                        status="PROVED",
                        score=_safe_score(verdict.score),
                        elapsed_seconds=max(
                            0.0, _safe_finite_float(verdict.elapsed_seconds)
                        ),
                        response=safe_response,
                        error=(
                            sanitize_worker_text(verdict.error)
                            if verdict.error
                            else None
                        ),
                        candidate_sha256=snapshot.sha256,
                        task_contract_sha256=_safe_hash(
                            verdict.task_contract_sha256
                        ),
                        judge_job_id=safe_job_id,
                        cache_reused=verdict.cache_reused is True,
                    )
                elif proof_claimed:
                    result.update(
                        {
                            "ok": False,
                            "status": "PROVENANCE_INVALID",
                            "proved": False,
                            "score": 0.0,
                            "response": {},
                            "error": "The Judge result was not bound to the submitted candidate and task contract.",
                            "retryable": False,
                        }
                    )
        except Exception as exc:  # keep one failed probe from killing the broker thread
            evaluator_unsettled_after = _nonnegative_count(
                getattr(self.evaluator, "remote_unsettled_jobs", 0)
            )
            if (
                evaluator_call_started
                and evaluator_unsettled_after > evaluator_unsettled_before
            ):
                retain_evaluator_gate = True
                self._mark_remote_unsettled()
                result = _remote_settlement_control_result()
                result.update(
                    {
                        "accepted": accepted,
                        "call_index": call_index,
                        "task_id": task_id,
                    }
                )
            else:
                result = {
                    "ok": False,
                    "accepted": accepted,
                    "call_index": call_index,
                    "task_id": task_id,
                    "status": "EVALUATOR_ERROR",
                    "proved": False,
                    "score": 0.0,
                    "response": {},
                    "error": sanitize_worker_text(exc),
                    "retryable": True,
                }
        finally:
            if acquired and not retain_evaluator_gate:
                self._release_evaluator_gate()
            with claim.lock:
                claim.probe_active = False

        # The callback runs only after the evaluator gate and session lock are
        # released.  A proof is not exposed to the solver unless the runner
        # has first durably admitted that exact frozen snapshot.
        callback = claim.on_authoritative_verdict
        if (
            callback is not None
            and authoritative_verdict is not None
            and snapshot is not None
        ):
            try:
                callback(binding.task, authoritative_verdict, snapshot)
            except Exception:
                result = {
                    "ok": False,
                    "accepted": accepted,
                    "call_index": call_index,
                    "task_id": task_id,
                    "status": "BROKER_ERROR",
                    "proved": False,
                    "score": 0.0,
                    "response": {},
                    "error": "The runner could not admit the authoritative Judge result.",
                    "retryable": False,
                }

        result = self._attach_timeout(
            result,
            timeout_request,
            timeout_started=request_started,
            timeout_deadline=timeout_deadline,
        )
        audit_gate_wait_seconds = (
            time.monotonic() - gate_wait_started
            if not acquired
            else gate_wait
        )
        audit_elapsed_seconds = time.monotonic() - started
        if self._profiling_enabled:
            audit_started = time.monotonic()
            try:
                self._audit(
                    claim,
                    task_id,
                    result,
                    accepted=accepted,
                    call_index=call_index,
                    gate_wait_seconds=audit_gate_wait_seconds,
                    elapsed_seconds=audit_elapsed_seconds,
                    candidate_sha256=(
                        snapshot.sha256 if snapshot is not None else None
                    ),
                    task_contract_sha256=result.get("task_contract_sha256"),
                    judge_job_id=result.get("judge_job_id"),
                    cache_reused=result.get("cache_reused") is True,
                )
            finally:
                audit_seconds = max(0.0, time.monotonic() - audit_started)
                self._profile_event(
                    "judge.audit.end",
                    claim=claim,
                    task_id=task_id,
                    elapsed_seconds=audit_seconds,
                    accepted=accepted,
                )
        else:
            self._audit(
                claim,
                task_id,
                result,
                accepted=accepted,
                call_index=call_index,
                gate_wait_seconds=audit_gate_wait_seconds,
                elapsed_seconds=audit_elapsed_seconds,
                candidate_sha256=snapshot.sha256 if snapshot is not None else None,
                task_contract_sha256=result.get("task_contract_sha256"),
                judge_job_id=result.get("judge_job_id"),
                cache_reused=result.get("cache_reused") is True,
            )
        # The normal evaluator path reaches this point after the durable audit;
        # expose the same bounded receipt profile as pre-admission failures.
        # ``result`` is already sanitized above, and no response/error payload
        # is forwarded to the profiling sink.
        budget_profile_fields = _timeout_profile_fields(
            result, max_timeout_seconds=self.agent_timeout_cap_seconds
        )
        self._profile_event(
            "judge.receipt",
            claim=claim,
            task_id=task_id,
            accepted=accepted,
            status=result.get("status"),
            proved=result.get("proved") is True,
            judge_job_id=result.get("judge_job_id"),
            score=result.get("score"),
            retryable=result.get("retryable") is True,
            gate_wait_seconds=audit_gate_wait_seconds,
            elapsed_seconds=audit_elapsed_seconds,
            snapshot_seconds=snapshot_seconds,
            evaluator_seconds=evaluator_seconds,
            audit_seconds=audit_seconds,
            cache_reused=result.get("cache_reused") is True,
            candidate_sha256=snapshot.sha256 if snapshot is not None else None,
            **timeout_fields(timeout_request),
            **budget_profile_fields,
        )
        if _valid_judge_checkpoint(
            result,
            raw_judge_job_id=raw_judge_job_id,
            expected_task_contract_sha256=binding.expected_task_contract_sha256,
        ):
            with claim.lock:
                claim.judge_checkpoint_reached = True
        return result

    def _acquire_evaluator_gate(
        self,
        deadline_monotonic: float,
        *,
        claim: _SessionClaim,
        task_id: str | None = None,
    ) -> bool:
        """Acquire the shared evaluator gate FIFO among broker callers."""

        waiter = object()
        acquired = False
        queued_at = time.monotonic()
        with self._admission_condition:
            self._admission_queue.append(waiter)
            self._profile_event(
                "judge.queued",
                claim=claim,
                task_id=task_id,
                queue_depth=len(self._admission_queue),
            )
        try:
            while True:
                if self._remote_settlement_unconfirmed():
                    return False
                if _claim_cancelled(claim):
                    return False
                with self._admission_condition:
                    remaining = deadline_monotonic - time.monotonic()
                    if remaining <= 0:
                        return False
                    if not self._admission_queue or self._admission_queue[0] is not waiter:
                        self._admission_condition.wait(timeout=min(remaining, 0.1))
                        continue
                # Only the FIFO head competes for the underlying semaphore.
                # Final evaluator calls share that semaphore and may release it
                # without notifying this condition, so block on the semaphore
                # itself once this waiter reaches the head.
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    return False
                acquired = self.evaluator_gate.acquire(timeout=min(remaining, 0.1))
                if acquired:
                    if self._remote_settlement_unconfirmed():
                        self._release_evaluator_gate()
                        acquired = False
                        return False
                    self._profile_event(
                        "judge.running",
                        claim=claim,
                        task_id=task_id,
                        queue_depth=len(self._admission_queue),
                        wait_seconds=time.monotonic() - queued_at,
                    )
                    return True
        finally:
            with self._admission_condition:
                try:
                    self._admission_queue.remove(waiter)
                except ValueError:
                    pass
                self._admission_condition.notify_all()

    def _release_evaluator_gate(self) -> None:
        self.evaluator_gate.release()
        with self._admission_condition:
            self._admission_condition.notify_all()
        with self._handler_condition:
            self._handler_condition.notify_all()

    def _finish_judge_check(
        self,
        claim: _SessionClaim,
        task_id: str,
        result: Mapping[str, Any],
        *,
        accepted: bool,
        started: float,
        gate_wait_started: float,
        gate_wait_seconds: float | None = None,
        candidate_sha256: str | None = None,
        clear_probe_active: bool = False,
    ) -> dict[str, Any]:
        """Close and audit a pre-admission control result."""

        # The initial validation/quota branches call this helper while holding
        # ``claim.lock``.  They do not own the active probe marker (and must
        # leave a concurrent request's marker untouched), so only callers that
        # have set ``probe_active`` for this request ask us to clear it.  This
        # explicit ownership bit avoids re-entering the non-reentrant lock and
        # preserves the in-flight guard under concurrent capability calls.
        if clear_probe_active:
            with claim.lock:
                claim.probe_active = False
        normalized = dict(result)
        normalized["accepted"] = accepted
        if not self._profiling_enabled:
            # Keep the disabled path equivalent to the historical audit call:
            # no profiling clocks, receipt construction, or sink interaction.
            self._audit(
                claim,
                task_id,
                normalized,
                accepted=accepted,
                gate_wait_seconds=(
                    time.monotonic() - gate_wait_started
                    if gate_wait_seconds is None
                    else gate_wait_seconds
                ),
                elapsed_seconds=time.monotonic() - started,
                candidate_sha256=candidate_sha256,
            )
            return normalized

        effective_gate_wait = (
            time.monotonic() - gate_wait_started
            if gate_wait_seconds is None
            else gate_wait_seconds
        )
        audit_started = time.monotonic()
        budget_profile_fields = _timeout_profile_fields(
            normalized, max_timeout_seconds=self.agent_timeout_cap_seconds
        )
        try:
            self._audit(
                claim,
                task_id,
                normalized,
                accepted=accepted,
                gate_wait_seconds=effective_gate_wait,
                elapsed_seconds=time.monotonic() - started,
                candidate_sha256=candidate_sha256,
            )
        finally:
            self._profile_event(
                "judge.audit.end",
                claim=claim,
                task_id=task_id,
                elapsed_seconds=max(0.0, time.monotonic() - audit_started),
                accepted=accepted,
                requested_timeout_seconds=normalized.get("requested_timeout_seconds"),
                effective_timeout_seconds=normalized.get("effective_timeout_seconds"),
                timeout_clamped=normalized.get("timeout_clamped") is True,
                timeout_source=normalized.get("timeout_source"),
                **budget_profile_fields,
            )
        self._profile_event(
            "judge.receipt",
            claim=claim,
            task_id=task_id,
            accepted=accepted,
            status=normalized.get("status"),
            proved=normalized.get("proved") is True,
            judge_job_id=normalized.get("judge_job_id"),
            gate_wait_seconds=effective_gate_wait,
            elapsed_seconds=time.monotonic() - started,
            candidate_sha256=candidate_sha256,
            requested_timeout_seconds=normalized.get("requested_timeout_seconds"),
            effective_timeout_seconds=normalized.get("effective_timeout_seconds"),
            timeout_clamped=normalized.get("timeout_clamped") is True,
            timeout_source=normalized.get("timeout_source"),
            **budget_profile_fields,
        )
        return normalized

    def _evaluate_local(
        self,
        claim: _SessionClaim,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Evaluate exact runner-bound bytes without creating proof authority."""

        started = time.monotonic()
        if self.formal_policy is None or not self.formal_policy.enabled:
            return _control_result(
                "FORMAL_TOOLS_DISABLED",
                "The manifest does not enable the bounded formal tool surface.",
                retryable=False,
            )
        timeout_request, timeout_error = self._parse_agent_timeout(payload)
        timeout_deadline = (
            started + timeout_request.effective_seconds
            if timeout_request is not None
            else None
        )

        def attach_timeout(result: Mapping[str, Any]) -> dict[str, Any]:
            return self._attach_timeout(
                result,
                timeout_request,
                timeout_started=started,
                timeout_deadline=timeout_deadline,
            )

        allowed_fields = {"task_id"}
        if self.agent_timeout_enabled:
            allowed_fields.add("timeout_seconds")
        if timeout_error or set(payload) - allowed_fields:
            result = _control_result(
                "INVALID_REQUEST",
                timeout_error
                or "evaluate_local accepts only the runner-bound task selection and optional timeout_seconds.",
                retryable=False,
            )
            result = attach_timeout(result)
            self._formal_audit(claim, "evaluate_local", "__invalid__", result, accepted=False)
            return result
        try:
            task_id = _select_task_id(claim, payload.get("task_id"))
        except ValueError as exc:
            result = _control_result(
                "INVALID_TASK_SELECTION",
                _safe_error(exc),
                retryable=False,
            )
            result = attach_timeout(result)
            self._formal_audit(claim, "evaluate_local", "__invalid__", result, accepted=False)
            return result
        call_number = self._formal_increment(task_id, "evaluate_calls")
        if call_number > self.formal_policy.evaluate_calls_per_task:
            result = {
                **_control_result(
                    "BUDGET_EXHAUSTED",
                    "The task-global evaluate.py call budget is exhausted.",
                    retryable=False,
                ),
                "call_number": call_number,
                "advisory_only": True,
                "official_score_eligible": False,
            }
            result = attach_timeout(result)
            self._formal_audit(claim, "evaluate_local", task_id, result, accepted=False)
            return result
        capability_failure = _formal_capability_failure(claim)
        if capability_failure is not None:
            result = {
                **capability_failure,
                "call_number": call_number,
                "advisory_only": True,
                "official_score_eligible": False,
            }
            result = attach_timeout(result)
            self._formal_audit(claim, "evaluate_local", task_id, result, accepted=False)
            return result

        binding = claim.candidates[task_id]
        try:
            source_bytes = read_regular_bytes(
                binding.path,
                trusted_root=claim.workdir,
                max_bytes=self.formal_policy.max_candidate_bytes,
            )
            source = source_bytes.decode("utf-8")
        except (OSError, UnicodeError):
            result = {
                **_control_result(
                    "CANDIDATE_SNAPSHOT_ERROR",
                    "The runner could not freeze the task candidate for diagnostic evaluation.",
                    retryable=True,
                ),
                "call_number": call_number,
                "advisory_only": True,
                "official_score_eligible": False,
            }
            result = attach_timeout(result)
            self._formal_audit(claim, "evaluate_local", task_id, result, accepted=False)
            return result
        source_sha256 = hashlib.sha256(source_bytes).hexdigest()
        if timeout_deadline is not None and time.monotonic() >= timeout_deadline:
            result = {
                **_control_result(
                    "EVALUATOR_TIMEOUT",
                    "The Agent validation budget elapsed while freezing the local candidate.",
                    retryable=False,
                ),
                "call_number": call_number,
                "candidate_sha256": source_sha256,
                "advisory_only": True,
                "official_score_eligible": False,
            }
            result = attach_timeout(result)
            result["timeout_budget_exhausted"] = True
            result["timeout_budget_remaining_seconds"] = 0.0
            self._formal_audit(claim, "evaluate_local", task_id, result, accepted=False)
            return result
        cache_key = (
            task_id,
            binding.expected_task_contract_sha256,
            source_sha256,
        )
        cached_result = None
        # A custom total budget must exercise the evaluator under that
        # contract.  Reusing a legacy cached diagnostic could both hide a
        # retry and return a result produced with a different timeout.
        if timeout_request is None:
            with self._formal_lock:
                cached = self._formal_evaluate_cache.get(cache_key)
                cached_result = _json_clone(cached) if cached is not None else None
        if cached_result is not None:
            cached_result.update(
                {
                    "call_number": call_number,
                    "cache_hit": True,
                    "backend_job_number": None,
                    "accepted": True,
                }
            )
            cached_result = attach_timeout(cached_result)
            self._formal_audit(
                claim,
                "evaluate_local",
                task_id,
                cached_result,
                accepted=True,
                candidate_sha256=source_sha256,
                elapsed_seconds=time.monotonic() - started,
            )
            return cached_result

        backend_number = self._formal_reserve(
            task_id,
            "evaluate_backend_jobs",
            self.formal_policy.evaluate_backend_jobs_per_task,
        )
        if backend_number is None:
            result = {
                **_control_result(
                    "BUDGET_EXHAUSTED",
                    "The task-global evaluate.py backend-job budget is exhausted.",
                    retryable=False,
                ),
                "call_number": call_number,
                "candidate_sha256": source_sha256,
                "advisory_only": True,
                "official_score_eligible": False,
            }
            result = attach_timeout(result)
            self._formal_audit(claim, "evaluate_local", task_id, result, accepted=False)
            return result

        retry_backend_numbers: list[int] = []

        def reserve_retry_backend_job() -> bool:
            """Reserve one additional task-global slot for a fresh retry."""

            serial = self._formal_reserve(
                task_id,
                "evaluate_backend_jobs",
                self.formal_policy.evaluate_backend_jobs_per_task,
            )
            if serial is None:
                return False
            retry_backend_numbers.append(serial)
            return True

        gate_started = time.monotonic()
        acquired = False
        retain_evaluator_gate = False
        evaluator_call_started = False
        evaluator_unsettled_before = 0
        verdict: Verdict | None = None
        try:
            admission_deadline = claim.deadline_monotonic
            if timeout_deadline is not None:
                admission_deadline = min(admission_deadline, timeout_deadline)
            acquired = self._acquire_evaluator_gate(
                admission_deadline,
                claim=claim,
                task_id=task_id,
            )
            if not acquired:
                self._formal_release(task_id, "evaluate_backend_jobs", backend_number)
                backend_number = None
                if (
                    timeout_deadline is not None
                    and time.monotonic() >= timeout_deadline
                    and not self._remote_settlement_unconfirmed()
                    and not _claim_cancelled(claim)
                ):
                    result = _control_result(
                        "EVALUATOR_TIMEOUT",
                        "The Agent validation budget elapsed while waiting for local evaluation admission.",
                        retryable=False,
                    )
                else:
                    result = _formal_gate_failure(
                        claim, self._remote_settlement_unconfirmed()
                    )
            else:
                if (
                    timeout_deadline is not None
                    and time.monotonic() >= timeout_deadline
                ):
                    self._release_evaluator_gate()
                    acquired = False
                    self._formal_release(task_id, "evaluate_backend_jobs", backend_number)
                    backend_number = None
                    result = _control_result(
                        "EVALUATOR_TIMEOUT",
                        "The Agent validation budget elapsed before local evaluation execution.",
                        retryable=False,
                    )
                    # Skip evaluator invocation; the final accounting below
                    # still records this as a zero-attempt budget exhaustion.
                    probe_source = None
                else:
                    probe_source = getattr(self.evaluator, "probe_source", None)
                if acquired and not callable(probe_source):
                    self._formal_release(task_id, "evaluate_backend_jobs", backend_number)
                    backend_number = None
                    result = _control_result(
                        "EVALUATOR_ERROR",
                        "The evaluator lacks immutable diagnostic-source support.",
                        retryable=False,
                    )
                elif acquired:
                    remaining_attempt_seconds = _remaining_agent_attempt_seconds(
                        timeout_request,
                        timeout_deadline,
                    )
                    if timeout_request is not None and remaining_attempt_seconds is None:
                        self._release_evaluator_gate()
                        acquired = False
                        self._formal_release(
                            task_id,
                            "evaluate_backend_jobs",
                            backend_number,
                        )
                        backend_number = None
                        result = _control_result(
                            "EVALUATOR_TIMEOUT",
                            "The Agent validation budget elapsed before local evaluation execution.",
                            retryable=False,
                        )
                    else:
                        options: dict[str, Any] = {
                            "deadline_monotonic": claim.deadline_monotonic,
                        }
                        if _accepts_settlement_callback(probe_source):
                            options["settlement_callback"] = self._release_evaluator_gate
                        if _accepts_cancel_event(probe_source):
                            options["cancel_event"] = _ClaimCancelEvent(claim)
                        if timeout_request is not None:
                            if not _accepts_timeout(probe_source):
                                raise TypeError(
                                    "the configured evaluator does not support timeout_seconds"
                                )
                            options["timeout_seconds"] = (
                                remaining_attempt_seconds
                                or timeout_request.effective_seconds
                            )
                            if _accepts_timeout_deadline(probe_source):
                                options["timeout_deadline_monotonic"] = timeout_deadline
                            if _accepts_retry_admission(probe_source):
                                options["retry_admission_callback"] = reserve_retry_backend_job
                        evaluator_unsettled_before = _nonnegative_count(
                            getattr(self.evaluator, "remote_unsettled_jobs", 0)
                        )
                        evaluator_call_started = True
                        verdict = probe_source(binding.task, source, **options)
                        evaluator_unsettled_after = _nonnegative_count(
                            getattr(self.evaluator, "remote_unsettled_jobs", 0)
                        )
                        call_unsettled = (
                            evaluator_unsettled_after > evaluator_unsettled_before
                            or _has_unsettled_remote_work(
                                _safe_verdict_status(verdict.status),
                                verdict.response,
                            )
                        )
                        deferred_remote = _has_deferred_remote_work(
                            _safe_verdict_status(verdict.status), verdict.response
                        )
                        if call_unsettled or deferred_remote:
                            retain_evaluator_gate = True
                        if call_unsettled and not deferred_remote:
                            self._mark_remote_unsettled()
                            result = _remote_settlement_control_result()
                        elif (
                            timeout_deadline is not None
                            and time.monotonic() >= timeout_deadline
                        ):
                            # A narrow/custom adapter may not understand the
                            # optional absolute-deadline keyword.  Do not let it
                            # turn a late result into apparently valid diagnostic
                            # feedback; the broker's hard boundary wins.
                            result = _control_result(
                                "EVALUATOR_TIMEOUT",
                                "The Agent validation budget elapsed during local evaluation.",
                                retryable=False,
                            )
                        else:
                            result = _formal_worker_verdict(
                                verdict,
                                max_timeout_seconds=self.agent_timeout_cap_seconds,
                            )
        except Exception:
            evaluator_unsettled_after = _nonnegative_count(
                getattr(self.evaluator, "remote_unsettled_jobs", 0)
            )
            if (
                evaluator_call_started
                and evaluator_unsettled_after > evaluator_unsettled_before
            ):
                retain_evaluator_gate = True
                self._mark_remote_unsettled()
                result = _remote_settlement_control_result()
            else:
                result = _control_result(
                    "EVALUATOR_ERROR",
                    "The controlled diagnostic evaluation failed.",
                    retryable=False,
                )
        finally:
            if acquired and not retain_evaluator_gate:
                self._release_evaluator_gate()

        if not retain_evaluator_gate and verdict is not None and (
            verdict.cache_reused or _verdict_proves_no_backend_job(verdict)
        ) and backend_number is not None:
            self._formal_release(task_id, "evaluate_backend_jobs", backend_number)
            backend_number = None
        result.update(
            {
                "accepted": verdict is not None,
                "call_number": call_number,
                "backend_job_number": backend_number,
                "candidate_sha256": source_sha256,
                "cache_hit": bool(
                    verdict and verdict.cache_reused and not retain_evaluator_gate
                ),
                "advisory_only": True,
                "official_score_eligible": False,
                "note": (
                    "Agent-local feedback never selects a candidate or writes the score; "
                    "outer closeout submits frozen bytes independently."
                ),
            }
        )
        backend_numbers = []
        if backend_number is not None:
            backend_numbers.append(int(backend_number))
        backend_numbers.extend(int(value) for value in retry_backend_numbers)
        if backend_numbers:
            # Keep the historical singular field above for consumers that only
            # understand the first reservation, while exposing the bounded
            # complete lineage for quota/audit analysis.
            result["backend_job_numbers"] = backend_numbers[:16]
            result["backend_job_count"] = len(backend_numbers)
        result = attach_timeout(result)
        if (
            timeout_request is None
            and not retain_evaluator_gate
            and verdict is not None
            and _safe_verdict_status(verdict.status) not in _FORMAL_NONCACHEABLE_STATUSES
        ):
            with self._formal_lock:
                self._formal_evaluate_cache[cache_key] = _json_clone(result)
        self._formal_audit(
            claim,
            "evaluate_local",
            task_id,
            result,
            accepted=verdict is not None,
            candidate_sha256=source_sha256,
            gate_wait_seconds=max(0.0, time.monotonic() - gate_started),
            elapsed_seconds=time.monotonic() - started,
            backend_number=backend_number,
        )
        return result

    def _formal_query(
        self,
        claim: _SessionClaim,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Serve one bounded search/elaboration query under task-global quotas."""

        started = time.monotonic()
        formal = self.formal_policy
        if formal is None or not formal.enabled:
            return _control_result(
                "FORMAL_TOOLS_DISABLED",
                "The manifest does not enable the bounded formal tool surface.",
                retryable=False,
            )
        allowed_fields = {
            "task_id",
            "command",
            "query",
            "limit",
            "snippet",
            "tactics",
            "tactic",
            "timeout",
        }
        if set(payload) - allowed_fields:
            result = _control_result(
                "INVALID_REQUEST",
                "formal_query received unsupported fields.",
                retryable=False,
            )
            self._formal_audit(claim, "formal_query", "__invalid__", result, accepted=False)
            return result
        try:
            task_id = _select_task_id(claim, payload.get("task_id"))
        except ValueError as exc:
            result = _control_result(
                "INVALID_TASK_SELECTION",
                _safe_error(exc),
                retryable=False,
            )
            self._formal_audit(claim, "formal_query", "__invalid__", result, accepted=False)
            return result
        command = str(payload.get("command") or "").strip().lower()
        if command not in {"search", "decl", "check", "type", "axioms", "deps"}:
            result = _control_result(
                "INVALID_REQUEST",
                "Unsupported formal_query command.",
                retryable=False,
            )
            self._formal_audit(claim, "formal_query", task_id, result, accepted=False)
            return result
        call_number = self._formal_increment(task_id, "query_calls")
        if call_number > formal.query_calls_per_task:
            result = {
                "ok": False,
                "status": "scout_call_budget_exhausted",
                "call_number": call_number,
                "advisory_only": True,
                "final_verify_required": True,
            }
            self._formal_audit(claim, "formal_query", task_id, result, accepted=False)
            return result
        capability_failure = _formal_capability_failure(claim)
        if capability_failure is not None:
            result = {
                **capability_failure,
                "call_number": call_number,
                "advisory_only": True,
                "final_verify_required": True,
            }
            self._formal_audit(claim, "formal_query", task_id, result, accepted=False)
            return result

        raw_query = payload.get("query")
        query_values = raw_query if isinstance(raw_query, list) else []
        query = [sanitize_public_text(str(value), limit=400) for value in query_values[:12]]
        query_text = " ".join(query).strip()
        limit = _bounded_limit(payload.get("limit"), default=12, maximum=24)
        timeout = _bounded_limit(payload.get("timeout"), default=30, maximum=120)
        binding = claim.candidates[task_id]
        guarded = {binding.task.theorem_name, binding.task.problem_id, binding.task.slug}
        backend_probes = 0
        cache_hits = 0

        if command == "search":
            public = self._formal_search_public(binding.path.parent, query, limit=limit)
            matches = formal.declaration_index.search(
                query_text,
                limit=limit,
                guarded_names=guarded,
            )
            result: dict[str, Any] = {
                "status": "ok" if formal.declaration_index.info.compatible else "index_unavailable",
                "query_kind": "search",
                "public_results": public,
                "mathlib_matches": matches,
                "search_corpus_revision": formal.declaration_index.info.mathlib_revision,
                "index_contract": formal.declaration_index.info.public_dict(),
                "hint": "Index names are advisory; verify a name with ./formal_query check <name>.",
            }
        elif command == "decl":
            matches = formal.declaration_index.search(
                query_text,
                limit=limit,
                guarded_names=guarded,
            )
            result = {
                "status": "searched" if formal.declaration_index.info.compatible else "index_unavailable",
                "query_kind": "decl",
                "matches": matches,
                "result_count": len(matches),
                "search_corpus_revision": formal.declaration_index.info.mathlib_revision,
                "index_contract": formal.declaration_index.info.public_dict(),
                "hint": "Verify candidate names with ./formal_query check <name>.",
            }
        elif command == "deps":
            exact = formal.declaration_index.search(
                query_text,
                limit=6,
                guarded_names=guarded,
            )
            related = formal.declaration_index.search(
                query_text.replace(".", " ").replace("_", " "),
                limit=10,
                guarded_names=guarded,
            )
            result = {
                "status": "searched" if formal.declaration_index.info.compatible else "index_unavailable",
                "query_kind": "deps",
                "query": query_text,
                "exact_matches": exact,
                "related_declarations": related,
                "semantics": "index_related_premises_not_dependency_graph",
                "hint": "Verify related declarations individually with check.",
            }
        else:
            result, backend_probes, cache_hits = self._formal_kernel_query(
                claim,
                binding,
                command,
                query,
                payload,
                timeout=timeout,
                guarded=guarded,
            )
        result.update(
            {
                "advisory_only": True,
                "final_verify_required": True,
                "call_number": call_number,
                "backend_probe_count": backend_probes,
                "cache_hit_count": cache_hits,
                "surface_version": formal.surface_version,
            }
        )
        self._formal_audit(
            claim,
            "formal_query",
            task_id,
            result,
            accepted=True,
            elapsed_seconds=time.monotonic() - started,
            backend_number=backend_probes,
            command=command,
        )
        return result

    def _formal_kernel_query(
        self,
        claim: _SessionClaim,
        binding: _CandidateBinding,
        command: str,
        query: list[str],
        request: Mapping[str, Any],
        *,
        timeout: int,
        guarded: set[str],
    ) -> tuple[dict[str, Any], int, int]:
        task = binding.task
        query_text = " ".join(query).strip()
        snippet = request.get("snippet")
        tactics = request.get("tactics")
        if any(name and _contains_guarded(query_text, name) for name in guarded):
            return {"status": "guarded_declaration_refused", "query_kind": command}, 0, 0
        imports = "\n".join(
            match.group(0).strip() for match in _IMPORT_LINE.finditer(task.baseline_code)
        )
        if command == "check" and isinstance(snippet, str) and snippet.strip():
            code = snippet.strip()[:8_000]
            if any(name and _contains_guarded(code, name) for name in guarded):
                return {"status": "guarded_declaration_refused", "query_kind": "check_snippet"}, 0, 0
            probe, consumed, cache_hit = self._formal_kernel_probe(
                claim,
                binding,
                f"{imports}\n\n{code}\n",
                timeout=timeout,
            )
            return (
                {
                    **probe,
                    "query_kind": "check_snippet",
                    "contains_sorry": bool(
                        re.search(r"(?<![A-Za-z0-9_])sorry(?![A-Za-z0-9_])", code)
                    ),
                },
                int(consumed),
                int(cache_hit),
            )
        if command == "check" and isinstance(tactics, str) and tactics.strip():
            header = tactics.strip()[:4_000]
            if any(name and _contains_guarded(header, name) for name in guarded):
                return {"status": "guarded_declaration_refused", "query_kind": "check_tactics"}, 0, 0
            raw_tactics = request.get("tactic")
            portfolio = (
                [str(value).strip()[:500] for value in raw_tactics[:12] if str(value).strip()]
                if isinstance(raw_tactics, list)
                else []
            ) or list(_DEFAULT_TACTICS)
            attempts: list[dict[str, Any]] = []
            closing: list[str] = []
            remote_unsettled = False
            probes = 0
            hits = 0
            for tactic in portfolio:
                if re.search(
                    r"(?<![A-Za-z0-9_])(?:sorry|admit)(?![A-Za-z0-9_])",
                    tactic,
                ):
                    attempts.append(
                        {
                            "tactic": tactic,
                            "outcome": "placeholder_refused",
                            "diagnostics": [],
                            "cache_hit": False,
                        }
                    )
                    continue
                probe, consumed, cache_hit = self._formal_kernel_probe(
                    claim,
                    binding,
                    f"{imports}\n\n{header} := by\n  {tactic}\n",
                    timeout=timeout,
                )
                probes += int(consumed)
                hits += int(cache_hit)
                closed = (
                    probe.get("status") == "elaborated"
                    and probe.get("is_valid_no_sorry") is True
                )
                attempts.append(
                    {
                        "tactic": tactic,
                        "outcome": "closed" if closed else str(probe.get("status") or "failed"),
                        "diagnostics": list(probe.get("diagnostics") or [])[:4],
                        "elapsed_ms": probe.get("elapsed_ms"),
                        "cache_hit": cache_hit,
                    }
                )
                if closed:
                    closing.append(tactic)
                    break
                if probe.get("status") == "probe_budget_exhausted":
                    break
                if probe.get("status") in {
                    "REMOTE_SETTLEMENT_UNCONFIRMED",
                    "probe_remote_settlement_unconfirmed",
                }:
                    remote_unsettled = True
                    break
            return (
                {
                    "status": (
                        "REMOTE_SETTLEMENT_UNCONFIRMED"
                        if remote_unsettled
                        else "closed" if closing else "not_closed"
                    ),
                    "retryable": False if remote_unsettled else None,
                    "query_kind": "check_tactics",
                    "closing_tactics": closing,
                    "attempts": attempts,
                    "note": "Each uncached tactic attempt consumes one task-global backend-probe unit.",
                },
                probes,
                hits,
            )
        if command == "axioms":
            name = query[0].strip() if query else ""
            if not _LEAN_NAME.fullmatch(name):
                return {"status": "invalid_query", "query_kind": "axioms"}, 0, 0
            try:
                candidate = read_regular_bytes(
                    binding.path,
                    trusted_root=claim.workdir,
                    max_bytes=self.formal_policy.max_candidate_bytes,
                ).decode("utf-8")
            except (OSError, UnicodeError):
                return {"status": "candidate_unavailable", "query_kind": "axioms"}, 0, 0
            probe, consumed, cache_hit = self._formal_kernel_probe(
                claim,
                binding,
                f"{candidate}\n\n#print axioms {name}\n",
                timeout=timeout,
            )
            probe.update(
                {
                    "query_kind": "axioms",
                    "query": name,
                    "candidate_context_included": True,
                }
            )
            return probe, int(consumed), int(cache_hit)
        if not query_text:
            return {"status": "empty_query", "query_kind": command}, 0, 0
        if command == "check" and all(_LEAN_NAME.fullmatch(name) for name in query[:8]):
            code = "\n".join(f"#check {name}" for name in query[:8])
        else:
            code = f"#check {query_text}"
        probe, consumed, cache_hit = self._formal_kernel_probe(
            claim,
            binding,
            f"{imports}\n\n{code}\n",
            timeout=timeout,
        )
        probe.update({"query_kind": command, "query": query_text})
        return probe, int(consumed), int(cache_hit)

    def _formal_kernel_probe(
        self,
        claim: _SessionClaim,
        binding: _CandidateBinding,
        source: str,
        *,
        timeout: int,
    ) -> tuple[dict[str, Any], bool, bool]:
        digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
        cache_key = (
            binding.task.slug,
            binding.expected_task_contract_sha256,
            digest,
        )
        with self._formal_lock:
            cached = self._formal_query_cache.get(cache_key)
            cached_result = _json_clone(cached) if cached is not None else None
        if cached_result is not None:
            cached_result["cache_hit"] = True
            return cached_result, False, True
        backend_number = self._formal_reserve(
            binding.task.slug,
            "query_backend_probes",
            self.formal_policy.query_backend_probes_per_task,
        )
        if backend_number is None:
            return {"status": "probe_budget_exhausted", "cache_hit": False}, False, False

        deadline = min(
            claim.deadline_monotonic,
            time.monotonic() + max(10, int(timeout)) + 30.0,
        )
        acquired = False
        retain_evaluator_gate = False
        evaluator_call_started = False
        evaluator_unsettled_before = 0
        verdict: Verdict | None = None
        try:
            acquired = self._acquire_evaluator_gate(
                deadline,
                claim=claim,
                task_id=binding.task.slug,
            )
            if not acquired:
                self._formal_release(
                    binding.task.slug,
                    "query_backend_probes",
                    backend_number,
                )
                return {
                    "status": (
                        "REMOTE_SETTLEMENT_UNCONFIRMED"
                        if self._remote_settlement_unconfirmed()
                        else "probe_admission_closed"
                    ),
                    "probe_status": (
                        "probe_remote_settlement_unconfirmed"
                        if self._remote_settlement_unconfirmed()
                        else "probe_admission_closed"
                    ),
                    "retryable": False,
                    "cache_hit": False,
                }, False, False
            probe_source = getattr(self.evaluator, "probe_source", None)
            if not callable(probe_source):
                self._formal_release(
                    binding.task.slug,
                    "query_backend_probes",
                    backend_number,
                )
                return {"status": "probe_transport_error", "cache_hit": False}, False, False
            metadata = dict(binding.task.metadata)
            metadata["theorem_name"] = ""
            import_contract = "\n".join(
                match.group(0).strip() for match in _IMPORT_LINE.finditer(source)
            )
            probe_task = Task(
                slug=binding.task.slug,
                root=binding.task.root,
                problem_text="",
                baseline_code=f"{import_contract}\n" if import_contract else "",
                metadata=metadata,
            )
            options: dict[str, Any] = {"deadline_monotonic": deadline}
            if _accepts_settlement_callback(probe_source):
                options["settlement_callback"] = self._release_evaluator_gate
            if _accepts_cancel_event(probe_source):
                options["cancel_event"] = _ClaimCancelEvent(claim)
            evaluator_unsettled_before = _nonnegative_count(
                getattr(self.evaluator, "remote_unsettled_jobs", 0)
            )
            evaluator_call_started = True
            verdict = probe_source(probe_task, source, **options)
            evaluator_unsettled_after = _nonnegative_count(
                getattr(self.evaluator, "remote_unsettled_jobs", 0)
            )
            call_unsettled = (
                evaluator_unsettled_after > evaluator_unsettled_before
                or _has_unsettled_remote_work(
                    _safe_verdict_status(verdict.status),
                    verdict.response,
                )
            )
            deferred_remote = _has_deferred_remote_work(
                _safe_verdict_status(verdict.status), verdict.response
            )
            if call_unsettled or deferred_remote:
                retain_evaluator_gate = True
            if call_unsettled and not deferred_remote:
                self._mark_remote_unsettled()
                result = {
                    "status": "REMOTE_SETTLEMENT_UNCONFIRMED",
                    "probe_status": "probe_remote_settlement_unconfirmed",
                    "retryable": False,
                    "cache_hit": False,
                }
            else:
                result = _formal_probe_result(
                    verdict,
                    max_timeout_seconds=self.agent_timeout_cap_seconds,
                )
        except Exception:
            evaluator_unsettled_after = _nonnegative_count(
                getattr(self.evaluator, "remote_unsettled_jobs", 0)
            )
            if (
                evaluator_call_started
                and evaluator_unsettled_after > evaluator_unsettled_before
            ):
                retain_evaluator_gate = True
                self._mark_remote_unsettled()
                result = {
                    "status": "REMOTE_SETTLEMENT_UNCONFIRMED",
                    "probe_status": "probe_remote_settlement_unconfirmed",
                    "retryable": False,
                    "cache_hit": False,
                }
            else:
                result = {"status": "probe_transport_error", "cache_hit": False}
        finally:
            if acquired and not retain_evaluator_gate:
                self._release_evaluator_gate()

        consumed = True
        if not retain_evaluator_gate and verdict is not None and (
            verdict.cache_reused or _verdict_proves_no_backend_job(verdict)
        ):
            self._formal_release(
                binding.task.slug,
                "query_backend_probes",
                backend_number,
            )
            consumed = False
        result.update(
            {
                "cache_hit": bool(
                    verdict and verdict.cache_reused and not retain_evaluator_gate
                ),
                "probe_number": backend_number if consumed else None,
            }
        )
        if (
            not retain_evaluator_gate
            and
            verdict is not None
            and _safe_verdict_status(verdict.status) not in _FORMAL_NONCACHEABLE_STATUSES
        ):
            with self._formal_lock:
                self._formal_query_cache[cache_key] = _json_clone(result)
        return result, consumed, bool(verdict and verdict.cache_reused)

    def _formal_search_public(
        self,
        workspace: Path,
        terms: list[str],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        candidates = [workspace / "problem.md", workspace / "result.lean"]
        candidates.extend(sorted((workspace / "baseline").glob("*.lean")))
        lowered_terms = [term.lower() for term in terms if term]
        rows: list[dict[str, Any]] = []
        for path in candidates:
            try:
                text = read_regular_bytes(
                    path,
                    trusted_root=workspace,
                    max_bytes=self.formal_policy.max_candidate_bytes,
                ).decode("utf-8", errors="replace")
            except OSError:
                continue
            for line_number, line in enumerate(text.splitlines(), start=1):
                lowered = line.lower()
                if lowered_terms and not all(term in lowered for term in lowered_terms):
                    continue
                rows.append(
                    {
                        "file": str(path.relative_to(workspace)),
                        "line": line_number,
                        "text": sanitize_public_text(line.strip(), limit=260),
                    }
                )
                if len(rows) >= limit:
                    return rows
        return rows

    def _formal_increment(self, task_id: str, counter: str) -> int:
        with self._formal_lock:
            key = (task_id, counter)
            value = self._formal_counts.get(key, 0) + 1
            self._formal_counts[key] = value
            return value

    def _formal_reserve(self, task_id: str, counter: str, limit: int) -> int | None:
        with self._formal_lock:
            key = (task_id, counter)
            current = self._formal_counts.get(key, 0)
            if current >= max(0, int(limit)):
                return None
            serial = self._formal_serials.get(key, 0) + 1
            self._formal_serials[key] = serial
            self._formal_counts[key] = current + 1
            return serial

    def _formal_release(self, task_id: str, counter: str, serial: int) -> None:
        del serial
        with self._formal_lock:
            key = (task_id, counter)
            self._formal_counts[key] = max(0, self._formal_counts.get(key, 0) - 1)

    def _formal_audit(
        self,
        claim: _SessionClaim,
        operation: str,
        task_id: str,
        result: Mapping[str, Any],
        *,
        accepted: bool,
        candidate_sha256: str | None = None,
        gate_wait_seconds: float = 0.0,
        elapsed_seconds: float = 0.0,
        backend_number: int | None = None,
        command: str | None = None,
    ) -> None:
        if self.formal_policy is None or not self.formal_policy.enabled:
            return
        row = {
            "at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": operation,
            "actor_id": claim.actor_id,
            "episode": claim.episode,
            "task_id": task_id,
            "command": command,
            "accepted": bool(accepted),
            "status": str(result.get("status") or "UNKNOWN")[:120],
            "call_number": result.get("call_number"),
            "backend_number": backend_number,
            "backend_job_numbers": _safe_backend_job_numbers(
                result.get("backend_job_numbers")
            ),
            "backend_job_count": _safe_backend_job_count(
                result.get("backend_job_count")
            ),
            "backend_probe_count": result.get("backend_probe_count"),
            "cache_hit": result.get("cache_hit") is True,
            "cache_hit_count": result.get("cache_hit_count"),
            "candidate_sha256": _safe_hash(candidate_sha256),
            "gate_wait_seconds": round(max(0.0, gate_wait_seconds), 6),
            "elapsed_seconds": round(max(0.0, elapsed_seconds), 6),
            "requested_timeout_seconds": result.get("requested_timeout_seconds"),
            "effective_timeout_seconds": result.get("effective_timeout_seconds"),
            "timeout_clamped": result.get("timeout_clamped") is True,
            "timeout_source": str(result.get("timeout_source") or "configured_legacy")[:32],
            "advisory_only": True,
            "official_score_eligible": False,
        }
        row.update(
            _safe_timeout_budget_fields(
                result, max_timeout_seconds=self.agent_timeout_cap_seconds
            )
        )
        row.update(_safe_formal_quota_fields(result))
        with self._audit_lock:
            with self.formal_audit_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    def _cps_operation(
        self,
        claim: _SessionClaim,
        operation: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        # Serialize each session's CPS calls with capability revocation.  The
        # store rechecks the same cancellation signal after acquiring its
        # SQLite write lock, closing the cancellation-during-lock-wait race.
        with claim.cps_lock:
            failure = _cps_capability_failure(claim)
            if failure is not None:
                return failure
            # Mono/Parallel sessions do not carry a CPS store or communication
            # capability. Preserve their historical CPS_UNAVAILABLE response
            # rather than imposing a checkpoint requirement on an endpoint
            # their solver cannot access.
            if claim.cps_store is None or claim.communication == "none":
                return self._cps_operation_locked(claim, operation, payload)
            if operation in {"cps_actors", "cps_send", "cps_inbox", "cps_ack"} and not (
                claim.direct_messages_allowed
            ):
                return _control_result(
                    "CPS_CAPABILITY_DENIED",
                    "This solver session has no direct-message capability.",
                    retryable=False,
                )
            if operation == "cps_feedback" and not claim.selection_enabled:
                return _control_result(
                    "CPS_CAPABILITY_DENIED",
                    "This solver session has no selection-feedback capability.",
                    retryable=False,
                )
            with claim.lock:
                checkpoint_reached = claim.judge_checkpoint_reached
            if not checkpoint_reached:
                return _control_result(
                    "JUDGE_CHECK_REQUIRED",
                    "Complete a terminal judge_check before using CPS communication.",
                    retryable=False,
                )
            try:
                return self._cps_operation_locked(claim, operation, payload)
            except RuntimeError:
                # CPSStore intentionally raises a plain RuntimeError when the
                # horizon or cancellation guard closes after a lock wait.  Map
                # only a now-observable capability closure to a stable worker
                # response; unrelated store failures remain BROKER_ERROR.
                failure = _cps_capability_failure(claim)
                if failure is not None:
                    return failure
                raise

    def _cps_operation_locked(
        self,
        claim: _SessionClaim,
        operation: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        store = claim.cps_store
        if store is None or claim.communication == "none":
            return _control_result(
                "CPS_UNAVAILABLE",
                "This solver session has no communication capability.",
                retryable=False,
            )
        task_id = next(iter(claim.candidates))
        allowed_fields = {
            "cps_search": {"query", "limit"},
            "cps_publish": {"kind", "title", "body", "tags", "scope"},
            "cps_actors": {"query"},
            "cps_send": {"recipient", "body", "scope"},
            "cps_inbox": {"limit"},
            "cps_ack": {"message_id"},
            "cps_feedback": {
                "request_key",
                "exposure_item_id",
                "trace_id",
                "feedback_kind",
                "value",
                "note",
            },
        }.get(operation)
        if allowed_fields is None:
            return _control_result(
                "UNKNOWN_CPS_OPERATION", "Unknown CPS operation.", retryable=False
            )
        if set(payload) - allowed_fields:
            return _control_result(
                "INVALID_REQUEST",
                "The CPS tool accepts only its declared runner-controlled fields.",
                retryable=False,
            )
        if operation == "cps_search":
            query = _bounded_string(payload.get("query"), 500)
            limit = _bounded_limit(payload.get("limit"), default=8, maximum=8)
            if claim.selection_enabled and claim.selection_search is not None:
                try:
                    selected = claim.selection_search(claim, query, limit)
                except Exception:
                    return _control_result(
                        "BROKER_ERROR",
                        "The controlled selection search failed this capability call.",
                        retryable=False,
                    )
                return _safe_selection_search_response(selected, limit=limit)
            return {
                "ok": True,
                "items": [
                    _safe_piece(item)
                    for item in store.search(
                        task_id=task_id,
                        query=query,
                        limit=limit,
                        include_global=claim.communication == "hybrid",
                    )
                ],
            }
        if operation == "cps_publish":
            title = _required_string(payload.get("title"), "title", 300)
            body = _required_string(payload.get("body"), "body", 8_000)
            kind = _bounded_string(payload.get("kind"), 64) or "handoff"
            if kind.casefold() in _RUNNER_ONLY_CPS_KINDS:
                return _control_result(
                    "RUNNER_ONLY_CPS_KIND",
                    "This CPS piece kind is reserved for runner-owned evaluator feedback.",
                    retryable=False,
                )
            tags_raw = payload.get("tags", [])
            if not isinstance(tags_raw, list):
                raise ValueError("tags must be an array")
            tags = [_bounded_string(item, 64) for item in tags_raw[:8]]
            scope = _scope(payload.get("scope"), claim.communication)
            item = store.create_piece(
                task_id="__global__" if scope == "global" else task_id,
                author=claim.actor_id,
                kind=kind,
                title=title,
                body=body,
                tags=[tag for tag in tags if tag],
                deadline_epoch_ms=claim.deadline_epoch_ms,
                cancel_guard=lambda: _claim_cancelled(claim),
            )
            return {"ok": True, "piece": _safe_piece(item)}
        if operation == "cps_actors":
            query = _bounded_string(payload.get("query"), 300).lower()
            actors = _safe_roster(claim.roster_path)
            if query:
                actors = [
                    item
                    for item in actors
                    if query in json.dumps(item, ensure_ascii=False).lower()
                ]
            return {"ok": True, "actors": actors[:100]}
        if operation == "cps_send":
            body = _required_string(payload.get("body"), "body", 8_000)
            recipient = _bounded_string(payload.get("recipient"), 256) or None
            scope = _scope(payload.get("scope"), claim.communication)
            item = store.send_message(
                task_id="__global__" if scope == "global" else task_id,
                sender=claim.actor_id,
                recipient=recipient,
                body=body,
                deadline_epoch_ms=claim.deadline_epoch_ms,
                cancel_guard=lambda: _claim_cancelled(claim),
            )
            return {"ok": True, "message": _safe_message(item)}
        if operation == "cps_inbox":
            limit = _bounded_limit(payload.get("limit"), default=8, maximum=8)
            return {
                "ok": True,
                "messages": [
                    _safe_message(item)
                    for item in store.inbox(
                        task_id=task_id,
                        recipient=claim.actor_id,
                        limit=limit,
                        include_global=claim.communication == "hybrid",
                    )
                ],
            }
        if operation == "cps_ack":
            message_id = _required_string(payload.get("message_id"), "message_id", 64)
            visible = store.inbox(
                task_id=task_id,
                recipient=claim.actor_id,
                limit=50,
                include_global=claim.communication == "hybrid",
            )
            if not any(str(item.get("id")) == message_id for item in visible):
                return {"ok": False, "status": "MESSAGE_NOT_VISIBLE", "acked": False}
            return {
                "ok": True,
                "acked": store.ack_message(
                    message_id,
                    claim.actor_id,
                    deadline_epoch_ms=claim.deadline_epoch_ms,
                    cancel_guard=lambda: _claim_cancelled(claim),
                ),
            }
        if operation == "cps_feedback":
            selection_store = claim.selection_store
            if not claim.selection_enabled or selection_store is None:
                return _control_result(
                    "CPS_CAPABILITY_DENIED",
                    "This solver session has no selection-feedback capability.",
                    retryable=False,
                )
            request_key = _required_string(payload.get("request_key"), "request_key", 512)
            exposure_item_id = _required_string(
                payload.get("exposure_item_id"), "exposure_item_id", 512
            )
            trace_id = _required_string(payload.get("trace_id"), "trace_id", 512)
            feedback_kind = _required_string(
                payload.get("feedback_kind"), "feedback_kind", 64
            )
            if feedback_kind not in CANONICAL_FEEDBACK_KINDS:
                return _control_result(
                    "INVALID_REQUEST",
                    "feedback_kind is not part of the registered feedback contract.",
                    retryable=False,
                )
            feedback_payload: dict[str, Any] = {}
            if "value" in payload:
                value = payload.get("value")
                if not _is_bounded_feedback_value(value):
                    return _control_result(
                        "INVALID_REQUEST",
                        "value must be a bounded JSON scalar.",
                        retryable=False,
                    )
                feedback_payload["value"] = value
            if "note" in payload:
                feedback_payload["note"] = _required_string(
                    payload.get("note"), "note", 2_000
                )
            try:
                recorded = selection_store.record_feedback(
                    request_key=request_key,
                    exposure_item_id=exposure_item_id,
                    actor_id=claim.actor_id,
                    trace_id=trace_id,
                    feedback_kind=feedback_kind,
                    origin="worker_explicit",
                    terminal=True,
                    payload=feedback_payload,
                )
            except ValueError:
                return _control_result(
                    "INVALID_FEEDBACK",
                    "Feedback could not be bound to this solver's selected exposure.",
                    retryable=False,
                )
            return {
                "ok": True,
                "status": str(recorded.get("status") or "RECORDED")[:64],
                "feedback_event_id": _bounded_string(
                    recorded.get("feedback_event_id"), 512
                ),
                "effective": recorded.get("effective") is True,
                "idempotent": recorded.get("idempotent") is True,
                "conflicts_with_feedback_event_id": _bounded_string(
                    recorded.get("conflicts_with_feedback_event_id"), 512
                )
                or None,
            }
        return _control_result("UNKNOWN_CPS_OPERATION", "Unknown CPS operation.", retryable=False)

    def _audit(
        self,
        claim: _SessionClaim,
        task_id: str,
        result: Mapping[str, Any],
        *,
        accepted: bool,
        call_index: int | None = None,
        gate_wait_seconds: float = 0.0,
        elapsed_seconds: float = 0.0,
        candidate_sha256: str | None = None,
        task_contract_sha256: Any = None,
        judge_job_id: Any = None,
        cache_reused: bool = False,
    ) -> None:
        failure_fields = _audit_failure_fields(result.get("response"))
        response = result.get("response")
        safe_response = response if isinstance(response, Mapping) else {}
        row = {
            "at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "event": "judge_check",
            "actor_id": claim.actor_id,
            "task_id": task_id,
            "accepted": accepted,
            "call_index": call_index,
            "status": str(result.get("status") or "UNKNOWN")[:120],
            "proved": result.get("proved") is True,
            "retryable": result.get("retryable") is True,
            "gate_wait_seconds": round(max(0.0, gate_wait_seconds), 6),
            "elapsed_seconds": round(max(0.0, elapsed_seconds), 6),
            "requested_timeout_seconds": result.get("requested_timeout_seconds"),
            "effective_timeout_seconds": result.get("effective_timeout_seconds"),
            "timeout_clamped": result.get("timeout_clamped") is True,
            "timeout_source": str(result.get("timeout_source") or "configured_legacy")[:32],
            "candidate_sha256": candidate_sha256,
            "task_contract_sha256": _safe_hash(task_contract_sha256),
            "judge_job_id": _safe_identifier(judge_job_id),
            "cache_reused": bool(cache_reused),
            "probe_cache_reused": _nested_response_bool(
                safe_response, "probe_cache_reused"
            ),
            "remote_cache_reused": _nested_response_bool(
                safe_response, "cache_reused"
            ),
            "response_profile": LEAN_PROBE_RESPONSE_PROFILE,
            **failure_fields,
        }
        row.update(
            _safe_timeout_budget_fields(
                result, max_timeout_seconds=self.agent_timeout_cap_seconds
            )
        )
        with self._audit_lock:
            with self.audit_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    @staticmethod
    def _send_json(
        handler: BaseHTTPRequestHandler, status: int, payload: Mapping[str, Any]
    ) -> None:
        raw = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(raw)))
        handler.send_header("Cache-Control", "no-store")
        handler.end_headers()
        handler.wfile.write(raw)


def _select_task_id(claim: _SessionClaim, raw: Any) -> str:
    requested = _bounded_string(raw, 256)
    if requested:
        if requested not in claim.candidates:
            raise ValueError("task_id is not part of this solver session")
        return requested
    if len(claim.candidates) != 1:
        raise ValueError("task_id is required for a multi-task solver session")
    return next(iter(claim.candidates))


def _claim_cancelled(claim: _SessionClaim) -> bool:
    return claim.revoked_event.is_set() or (
        claim.cancel_event is not None and claim.cancel_event.is_set()
    )


def _cps_capability_failure(claim: _SessionClaim) -> dict[str, Any] | None:
    """Return a stable fail-closed result for an expired CPS capability."""

    if (
        time.monotonic() >= claim.deadline_monotonic
        or int(time.time() * 1_000) >= claim.deadline_epoch_ms
    ):
        return _control_result(
            "OUT_OF_HORIZON",
            "The CPS communication horizon has elapsed.",
            retryable=False,
        )
    if _claim_cancelled(claim):
        return _control_result(
            "TASK_CANCELLED",
            "This solver task no longer accepts CPS work.",
            retryable=False,
        )
    return None


def _has_unsettled_remote_cancellation(response: Mapping[str, Any]) -> bool:
    cancellation = response.get("judge_cancellation")
    return (
        isinstance(cancellation, Mapping)
        and cancellation.get("attempted") is True
        and cancellation.get("settled") is not True
    )


def _nested_response_bool(response: Mapping[str, Any], name: str) -> bool:
    """Read one boolean through the bounded Judge ``response`` envelope chain."""

    current: Any = response
    for _depth in range(4):
        if not isinstance(current, Mapping):
            return False
        if current.get(name) is True:
            return True
        current = current.get("response")
    return False


def _has_unsettled_remote_work(
    verdict_status: str,
    response: Mapping[str, Any],
) -> bool:
    return (
        verdict_status == "REMOTE_SETTLEMENT_UNCONFIRMED"
        or response.get("remote_settlement_unconfirmed") is True
        or response.get("settlement_error") == "cancel_settlement_unconfirmed"
        or _has_unsettled_remote_cancellation(response)
    )


def _has_deferred_remote_work(
    verdict_status: str,
    response: Mapping[str, Any],
) -> bool:
    cancellation = response.get("judge_cancellation")
    return (
        response.get("settlement_error") == "cancel_settlement_deferred"
        or (
            isinstance(cancellation, Mapping)
            and cancellation.get("deferred") is True
        )
    )


def _accepts_settlement_callback(function: Callable[..., Any]) -> bool:
    try:
        parameters = inspect.signature(function).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "settlement_callback"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _remote_settlement_control_result() -> dict[str, Any]:
    return _control_result(
        "REMOTE_SETTLEMENT_UNCONFIRMED",
        "Remote Judge work did not provide a job-bound terminal receipt; further admission is disabled.",
        retryable=False,
    )


def _nonnegative_count(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return max(0, value)


def _accepts_cancel_event(function: Callable[..., Any]) -> bool:
    """Preserve compatibility with narrow test/custom evaluator adapters."""

    try:
        parameters = inspect.signature(function).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "cancel_event"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _accepts_timeout(function: Callable[..., Any]) -> bool:
    """Return whether an evaluator adapter can honor a worker timeout."""

    try:
        parameters = inspect.signature(function).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "timeout_seconds"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _accepts_timeout_deadline(function: Callable[..., Any]) -> bool:
    """Return whether an evaluator can receive the absolute budget deadline.

    The deadline keyword is an additive capability.  Narrow test adapters and
    older third-party evaluators may implement only ``timeout_seconds``; the
    broker still enforces the deadline while waiting for its own admission
    gate and falls back to a budget that starts at evaluator invocation for
    such adapters.  Built-in evaluators accept the keyword and therefore
    include snapshot/gate time in the same logical budget.
    """

    try:
        parameters = inspect.signature(function).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "timeout_deadline_monotonic"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _accepts_retry_admission(function: Callable[..., Any]) -> bool:
    """Return whether an evaluator can reserve quota for a fresh retry."""

    try:
        parameters = inspect.signature(function).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.name == "retry_admission_callback"
        or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def _remaining_agent_attempt_seconds(
    timeout: AgentTimeout | None,
    deadline_monotonic: float | None,
) -> int | None:
    """Return an integer per-attempt cap without extending a total budget.

    The absolute deadline is authoritative for built-in evaluators.  Narrow
    adapters may only understand ``timeout_seconds``; passing the remaining
    integer budget to them still prevents queue/snapshot time from silently
    granting a second full attempt.  ``None`` means that the configured
    minimum attempt floor can no longer be honored.
    """

    if timeout is None:
        return None
    minimum_attempt_seconds = min(
        AGENT_TIMEOUT_MIN_SECONDS, int(timeout.effective_seconds)
    )
    if deadline_monotonic is None:
        return int(timeout.effective_seconds)
    try:
        remaining = float(deadline_monotonic) - time.monotonic()
    except (TypeError, ValueError, OverflowError):
        return None
    remaining_ceiling = (
        int(math.ceil(max(0.0, remaining))) if math.isfinite(remaining) else 0
    )
    if (
        not math.isfinite(remaining)
        or remaining
        < minimum_attempt_seconds - _AGENT_TIMEOUT_BOUNDARY_EPSILON_SECONDS
    ):
        return None
    return max(
        minimum_attempt_seconds,
        min(int(timeout.effective_seconds), remaining_ceiling),
    )


def _scope(raw: Any, communication: str) -> str:
    scope = _bounded_string(raw, 16).lower() or "task"
    if scope not in {"task", "global"}:
        raise ValueError("scope must be task or global")
    if scope == "global" and communication != "hybrid":
        raise ValueError("global CPS scope requires hybrid communication")
    return scope


def _bounded_string(value: Any, maximum: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("text fields must be strings")
    text = value.strip()
    if len(text) > maximum:
        raise ValueError(f"text field exceeds {maximum} characters")
    return text


def _required_string(value: Any, name: str, maximum: int) -> str:
    text = _bounded_string(value, maximum)
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _is_bounded_feedback_value(value: Any) -> bool:
    if value is None or isinstance(value, (bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return isinstance(value, str) and len(value) <= 2_000


def _safe_selection_search_response(raw: Any, *, limit: int) -> dict[str, Any]:
    """Bound the narrow ``selection_search(claim, query, limit)`` response."""

    if not isinstance(raw, Mapping):
        return _control_result(
            "BROKER_ERROR",
            "The controlled selection search returned an invalid response.",
            retryable=False,
        )
    items = raw.get("items", [])
    if not isinstance(items, (list, tuple)):
        return _control_result(
            "BROKER_ERROR",
            "The controlled selection search returned an invalid response.",
            retryable=False,
        )
    safe_items = [_bounded_json(item) for item in items[:limit]]
    result: dict[str, Any] = {"ok": True, "items": safe_items}
    for key in ("search_event_id", "exposure_id", "request_key"):
        value = raw.get(key)
        if isinstance(value, str) and len(value) <= 512:
            result[key] = value
    return result


def _bounded_json(value: Any, *, _depth: int = 0) -> Any:
    """Return a JSON-safe bounded value for callback-owned selection rows."""

    if _depth >= 4:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value[:2_000]
    if isinstance(value, Mapping):
        return {
            str(key)[:128]: _bounded_json(item, _depth=_depth + 1)
            for key, item in list(value.items())[:64]
            if isinstance(key, (str, int, float, bool))
        }
    if isinstance(value, (list, tuple)):
        return [_bounded_json(item, _depth=_depth + 1) for item in value[:100]]
    return None


def _bounded_limit(value: Any, *, default: int, maximum: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError("limit must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    return max(1, min(parsed, maximum))


def _control_result(
    status: str,
    message: str,
    *,
    retryable: bool,
    retry_after_seconds: float | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": False,
        "accepted": False,
        "status": status,
        "message": message,
        "retryable": retryable,
    }
    if retry_after_seconds is not None:
        result["retry_after_seconds"] = retry_after_seconds
    return result


def _safe_timeout_budget_fields(
    value: Any,
    *,
    max_timeout_seconds: int | float | None = None,
) -> dict[str, Any]:
    """Extract bounded cumulative-budget metadata from a result/response.

    This helper is deliberately stricter than the generic JSON sanitizer.  A
    custom evaluator is allowed to return arbitrary diagnostic keys, but only
    the small, typed timeout accounting vocabulary may cross the broker audit
    boundary.  Arrays are capped because one logical call can contain several
    fresh backend attempts.
    """

    if not isinstance(value, Mapping):
        return {}
    try:
        timeout_cap = agent_timeout_bounds(max_timeout_seconds).max_seconds
    except ValueError:
        timeout_cap = agent_timeout_bounds().max_seconds
    result: dict[str, Any] = {}
    for key in _TIMEOUT_BUDGET_TEXT_FIELDS:
        raw = value.get(key)
        if isinstance(raw, str) and raw.strip():
            result[key] = sanitize_worker_text(raw.strip(), 64)
    for key in _TIMEOUT_BUDGET_NUMBER_FIELDS:
        raw = value.get(key)
        if isinstance(raw, bool):
            continue
        if key in {"judge_attempt_count", "judge_retry_count"}:
            if isinstance(raw, int):
                result[key] = max(0, min(int(raw), 1_000_000))
            continue
        if key == "timeout_budget_seconds":
            if isinstance(raw, int):
                result[key] = max(0, min(int(raw), int(timeout_cap)))
            continue
        try:
            parsed = float(raw)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(parsed) and parsed >= 0:
            result[key] = round(min(parsed, float(timeout_cap)), 6)
    for key in _TIMEOUT_BUDGET_BOOL_FIELDS:
        if isinstance(value.get(key), bool):
            result[key] = value[key]
    raw_timeouts = value.get("judge_attempt_timeouts_seconds")
    if isinstance(raw_timeouts, (list, tuple)):
        result["judge_attempt_timeouts_seconds"] = [
            max(0, min(int(item), timeout_cap))
            for item in raw_timeouts[:16]
            if isinstance(item, int) and not isinstance(item, bool)
        ]
    raw_elapsed = value.get("judge_attempt_elapsed_seconds")
    if isinstance(raw_elapsed, (list, tuple)):
        elapsed: list[float] = []
        for item in raw_elapsed[:16]:
            if isinstance(item, bool):
                continue
            try:
                parsed = float(item)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(parsed) and parsed >= 0:
                elapsed.append(round(parsed, 6))
        result["judge_attempt_elapsed_seconds"] = elapsed
    raw_reasons = value.get("judge_retry_reasons")
    if isinstance(raw_reasons, (list, tuple)):
        result["judge_retry_reasons"] = [
            sanitize_worker_text(item.strip(), 64)
            for item in raw_reasons[:16]
            if isinstance(item, str) and item.strip()
        ]
    raw_ids = value.get("judge_attempt_ids")
    if isinstance(raw_ids, (list, tuple)):
        result["judge_attempt_ids"] = [
            identifier
            for item in raw_ids[:16]
            if (identifier := sanitize_worker_identifier(item)) is not None
        ]
    return result


def _safe_formal_quota_fields(value: Any) -> dict[str, Any]:
    """Extract bounded markers for a formal backend-job retry admission."""

    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for key in _FORMAL_QUOTA_TEXT_FIELDS:
        raw = value.get(key)
        if isinstance(raw, str) and raw.strip():
            result[key] = sanitize_worker_text(raw.strip(), 64)
    for key in _FORMAL_QUOTA_BOOL_FIELDS:
        if isinstance(value.get(key), bool):
            result[key] = value[key]
    for key in _FORMAL_QUOTA_NUMBER_FIELDS:
        raw = value.get(key)
        if isinstance(raw, int) and not isinstance(raw, bool):
            result[key] = max(0, min(int(raw), 1_000_000))
    raw_numbers = value.get("backend_job_numbers")
    if isinstance(raw_numbers, (list, tuple)):
        result["backend_job_numbers"] = [
            max(0, min(int(item), 1_000_000))
            for item in raw_numbers[:16]
            if isinstance(item, int) and not isinstance(item, bool)
        ]
    return result


def _safe_backend_job_numbers(value: Any) -> list[int]:
    """Keep formal retry reservation serials bounded and non-sensitive."""

    if not isinstance(value, (list, tuple)):
        return []
    return [
        max(0, min(int(item), 1_000_000))
        for item in value[:16]
        if isinstance(item, int) and not isinstance(item, bool)
    ]


def _safe_backend_job_count(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return max(0, min(int(value), 1_000_000))
    return None


def _timeout_profile_fields(
    value: Any,
    *,
    max_timeout_seconds: int | float | None = None,
) -> dict[str, Any]:
    """Return only scalar/text timeout fields accepted by RunProfiler."""

    return {
        key: item
        for key, item in _safe_timeout_budget_fields(
            value, max_timeout_seconds=max_timeout_seconds
        ).items()
        if key not in _TIMEOUT_BUDGET_ARRAY_FIELDS
    }


def _safe_roster(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    result: list[dict[str, Any]] = []
    for raw in payload[:500]:
        if not isinstance(raw, Mapping):
            continue
        item = {
            key: raw[key]
            for key in ("actor_id", "task_id", "episode")
            if isinstance(raw.get(key), (str, int)) and not isinstance(raw.get(key), bool)
        }
        if item:
            result.append(item)
    return result


def _safe_piece(raw: Any) -> dict[str, Any]:
    """Return only bounded CPS fields that are safe for a solver response."""

    if not isinstance(raw, Mapping):
        return {}
    result = {
        key: _clipped_text(raw.get(key), maximum)
        for key, maximum in (
            ("id", 64),
            ("task_id", 256),
            ("author", 256),
            ("kind", 64),
            ("title", 300),
            ("body", 2_000),
            ("created_at", 64),
        )
    }
    tags = raw.get("tags")
    result["tags"] = (
        [_clipped_text(tag, 64) for tag in tags[:8] if isinstance(tag, str)]
        if isinstance(tags, list)
        else []
    )
    return result


def _safe_message(raw: Any) -> dict[str, Any]:
    """Return only bounded direct-message fields visible to a solver."""

    if not isinstance(raw, Mapping):
        return {}
    return {
        key: _clipped_text(raw.get(key), maximum)
        for key, maximum in (
            ("id", 64),
            ("task_id", 256),
            ("sender", 256),
            ("recipient", 256),
            ("body", 2_000),
            ("created_at", 64),
            ("acked_at", 64),
        )
    }


def _clipped_text(value: Any, maximum: int) -> str:
    return value[:maximum] if isinstance(value, str) else ""


def _safe_error(value: Any) -> str:
    return sanitize_worker_text(value)


def _safe_hash(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text if re.fullmatch(r"[0-9a-f]{64}", text) else None


def _safe_identifier(value: Any) -> str | None:
    return sanitize_worker_identifier(value)


def _safe_verdict_status(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text or len(text.encode("utf-8")) > 120:
        return "EVALUATOR_ERROR"
    if not re.fullmatch(r"[A-Z][A-Z0-9_:-]*", text):
        return "EVALUATOR_ERROR"
    return {"PASS": "PROVED", "AC": "PROVED", "PASSED": "PROVED"}.get(text, text)


def _valid_judge_checkpoint(
    result: Mapping[str, Any],
    *,
    raw_judge_job_id: Any = _CHECKPOINT_VALUE_UNSET,
    expected_task_contract_sha256: str | None = None,
) -> bool:
    """Recognize valid feedback without treating admission as verification."""

    if result.get("accepted") is not True:
        return False
    status = _safe_verdict_status(result.get("status"))
    if status not in _JUDGE_CHECKPOINT_TERMINAL_STATUSES:
        return False
    if _safe_hash(result.get("candidate_sha256")) is None:
        return False
    contract_hash = _safe_hash(result.get("task_contract_sha256"))
    if contract_hash is None:
        return False
    if expected_task_contract_sha256 is not None:
        expected_hash = _safe_hash(expected_task_contract_sha256)
        if expected_hash is None or contract_hash != expected_hash:
            return False
    if raw_judge_job_id is _CHECKPOINT_VALUE_UNSET:
        # Preserve the helper's standalone behavior for callers/tests that
        # already hold a complete result mapping.  The broker call site passes
        # the unsanitized Verdict field explicitly.
        raw_judge_job_id = result.get("judge_job_id")
    if status == "LOCAL_REJECTED":
        # LOCAL_REJECTED is a supervisor-local result and must carry no remote
        # identity at all.  Checking the raw field is intentional: an invalid
        # or empty non-None value must not disappear during sanitization and
        # accidentally satisfy the local-only contract.
        return raw_judge_job_id is None
    return sanitize_worker_identifier(raw_judge_job_id) is not None


def _safe_finite_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _safe_score(value: Any) -> float:
    return max(0.0, min(1.0, _safe_finite_float(value)))


def _audit_failure_fields(value: Any) -> dict[str, Any]:
    failure = value.get("evaluator_failure") if isinstance(value, Mapping) else None
    if not isinstance(failure, Mapping):
        return {
            "failure_category": None,
            "failure_http_status": None,
            "failure_attempts": None,
            "failure_retry_after_seconds": None,
        }
    category = sanitize_worker_identifier(failure.get("category"))
    http_status = failure.get("http_status")
    attempts = failure.get("attempts")
    retry_after = failure.get("retry_after_seconds")
    safe_retry_after: float | None = None
    if isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool):
        try:
            parsed_retry_after = float(retry_after)
        except (ValueError, OverflowError):
            pass
        else:
            if math.isfinite(parsed_retry_after) and parsed_retry_after >= 0:
                safe_retry_after = round(parsed_retry_after, 3)
    return {
        "failure_category": category,
        "failure_http_status": (
            http_status
            if isinstance(http_status, int)
            and not isinstance(http_status, bool)
            and 100 <= http_status <= 599
            else None
        ),
        "failure_attempts": (
            max(0, min(attempts, 1_000_000))
            if isinstance(attempts, int) and not isinstance(attempts, bool)
            else None
        ),
        "failure_retry_after_seconds": safe_retry_after,
    }


def _is_authoritative_proof(
    *,
    task: Task,
    verdict: Verdict,
    snapshot: CandidateSnapshot,
    expected_task_contract_sha256: str,
    allow_mock_provenance: bool,
) -> bool:
    """Accept only a complete Judge proof for the exact frozen submission."""

    candidate_hash = _safe_hash(verdict.candidate_sha256)
    contract_hash = _safe_hash(verdict.task_contract_sha256)
    job_id = sanitize_worker_identifier(verdict.judge_job_id)
    mock_provenance = (
        allow_mock_provenance and verdict.response.get("mock") is True
    )
    return bool(
        verdict.task_id == task.slug
        and _safe_verdict_status(verdict.status) == "PROVED"
        and _safe_score(verdict.score) >= 1.0
        and candidate_hash == snapshot.sha256
        and contract_hash == expected_task_contract_sha256
        and (job_id or mock_provenance)
    )


def _formal_capability_failure(claim: _SessionClaim) -> dict[str, Any] | None:
    # A failed cancellation/unknown remote terminal is run-global. Do not let
    # cache-only search or declaration lookup continue while a remote job may
    # still be consuming the shared Judge slot.
    broker = getattr(claim, "broker", None)
    if broker is not None and broker._remote_settlement_unconfirmed():
        return _remote_settlement_control_result()
    if time.monotonic() >= claim.deadline_monotonic:
        return _control_result(
            "OUT_OF_HORIZON",
            "The experiment horizon has elapsed.",
            retryable=False,
        )
    if _claim_cancelled(claim):
        return _control_result(
            "TASK_CANCELLED",
            "This solver task no longer accepts formal-tool work.",
            retryable=False,
        )
    return None


def _formal_gate_failure(
    claim: _SessionClaim,
    remote_unsettled: bool,
) -> dict[str, Any]:
    if remote_unsettled:
        return _remote_settlement_control_result()
    failure = _formal_capability_failure(claim)
    if failure is not None:
        return failure
    return _control_result(
        "JUDGE_ADMISSION_TIMEOUT",
        "The controlled Judge remained busy until the formal-tool deadline.",
        retryable=True,
    )


def _json_clone(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    return json.loads(json.dumps(dict(value), ensure_ascii=False))


def _recursive_value(payload: Any, name: str, *, depth: int = 0) -> Any:
    if not isinstance(payload, Mapping) or depth > 4:
        return None
    if payload.get(name) is not None:
        return payload.get(name)
    for nested_name in ("response", "canonical_verdict", "lean_environment"):
        nested = payload.get(nested_name)
        found = _recursive_value(nested, name, depth=depth + 1)
        if found is not None:
            return found
    return None


def _formal_diagnostics(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = _recursive_value(response, "probe_diagnostics")
    items = raw.get("items") if isinstance(raw, Mapping) else raw
    if not isinstance(items, list):
        return []
    result: list[dict[str, Any]] = []
    for item in items[:24]:
        if not isinstance(item, Mapping):
            continue
        result.append(
            {
                "severity": sanitize_public_text(str(item.get("severity") or "info"), limit=32),
                "message": sanitize_public_text(
                    str(item.get("data") or item.get("message") or ""),
                    limit=1_024,
                ),
                "line": item.get("line") if isinstance(item.get("line"), int) else 0,
                "column": item.get("column") if isinstance(item.get("column"), int) else 0,
            }
        )
    return result


def _formal_worker_verdict(
    verdict: Verdict,
    *,
    max_timeout_seconds: int | float | None = None,
) -> dict[str, Any]:
    response = safe_worker_response(
        verdict.response, timeout_max_seconds=max_timeout_seconds
    )
    errors: list[str] = []
    nested_error = _recursive_value(response, "error_message")
    if isinstance(nested_error, str) and nested_error.strip():
        errors.append(sanitize_public_text(nested_error, limit=1_024))
    if verdict.error:
        errors.append(sanitize_public_text(verdict.error, limit=1_024))
    result = {
        "ok": _safe_verdict_status(verdict.status) not in _FORMAL_NONCACHEABLE_STATUSES,
        "status": _safe_verdict_status(verdict.status),
        "score": 0.0,
        "diagnostics": _formal_diagnostics(response),
        "error_messages": errors[:24],
        "reason": sanitize_public_text(
            str(_recursive_value(response, "reason") or verdict.error or ""),
            limit=1_024,
        ),
        "elapsed_ms": int(max(0.0, verdict.elapsed_seconds) * 1_000),
    }
    # Preserve the cumulative timeout accounting even though formal helper
    # responses intentionally expose only a small diagnostics projection.
    result.update(
        _safe_timeout_budget_fields(
            response, max_timeout_seconds=max_timeout_seconds
        )
    )
    # A retry can be denied by the task-global backend-job quota after the
    # first attempt has already produced feedback.  Keep that bounded reason
    # visible to the Agent instead of silently returning the earlier transient
    # status as if the evaluator had simply stopped retrying.
    result.update(_safe_formal_quota_fields(response))
    return result


def _formal_probe_result(
    verdict: Verdict,
    *,
    max_timeout_seconds: int | float | None = None,
) -> dict[str, Any]:
    response = safe_worker_response(
        verdict.response, timeout_max_seconds=max_timeout_seconds
    )
    normalized = _safe_verdict_status(verdict.status)
    if normalized in {"PROVED", "COMPILES_WITH_SORRY", "ELABORATED"}:
        status = "elaborated"
    elif normalized in {"VERIFY_FAIL", "LOCAL_REJECTED", "ELAB_FAILED"}:
        status = "elab_failed"
    elif normalized == "REJECTED_OVERLOADED":
        status = "probe_admission_closed"
    elif normalized in {"OUT_OF_HORIZON", "TASK_CANCELLED"}:
        status = "probe_admission_closed"
    elif normalized == "REMOTE_SETTLEMENT_UNCONFIRMED":
        status = "probe_remote_settlement_unconfirmed"
    else:
        status = "probe_transport_error" if normalized == "EVALUATOR_ERROR" else normalized.lower()
    errors: list[str] = []
    nested_error = _recursive_value(response, "error_message")
    if isinstance(nested_error, str) and nested_error.strip():
        errors.append(sanitize_public_text(nested_error, limit=1_024))
    if verdict.error:
        errors.append(sanitize_public_text(verdict.error, limit=1_024))
    result: dict[str, Any] = {
        "status": status,
        "is_valid_with_sorry": _recursive_value(response, "is_valid_with_sorry") is True,
        "is_valid_no_sorry": _recursive_value(response, "is_valid_no_sorry") is True,
        "diagnostics": _formal_diagnostics(response),
        "error_messages": errors[:24],
        "elapsed_ms": int(max(0.0, verdict.elapsed_seconds) * 1_000),
    }
    for key in ("mathlib_revision", "lean_version"):
        value = _recursive_value(response, key)
        if isinstance(value, str) and value.strip():
            result[key] = sanitize_public_text(value, limit=256)
    environment = _recursive_value(response, "lean_environment")
    if isinstance(environment, Mapping):
        safe_environment: dict[str, str] = {}
        for key in ("mathlib_revision", "lean_version"):
            value = environment.get(key)
            if isinstance(value, str) and value.strip():
                safe_environment[key] = sanitize_public_text(value, limit=256)
        if safe_environment:
            result["lean_environment"] = safe_environment
    result.update(
        _safe_timeout_budget_fields(
            response, max_timeout_seconds=max_timeout_seconds
        )
    )
    return result


def _verdict_proves_no_backend_job(verdict: Verdict) -> bool:
    if sanitize_worker_identifier(verdict.judge_job_id) is not None:
        return False
    if _has_unsettled_remote_work(_safe_verdict_status(verdict.status), verdict.response):
        return False
    status = _safe_verdict_status(verdict.status)
    if status in {"LOCAL_REJECTED", "OUT_OF_HORIZON", "TASK_CANCELLED"}:
        return True
    return status == "REJECTED_OVERLOADED" and _nested_response_bool(
        verdict.response,
        "retryable",
    )


def _contains_guarded(text: str, name: str) -> bool:
    return bool(
        re.search(
            rf"(?<![A-Za-z0-9_']){re.escape(name)}(?![A-Za-z0-9_'])",
            text,
        )
    )


def _candidate_snapshot(
    path: Path,
    *,
    trusted_root: Path,
    max_bytes: int = DEFAULT_MAX_CANDIDATE_BYTES,
) -> CandidateSnapshot:
    raw = read_regular_bytes(
        path,
        trusted_root=trusted_root,
        max_bytes=max_bytes,
    )
    return CandidateSnapshot(
        source=raw.decode("utf-8"),
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


__all__ = ["CandidateSnapshot", "JudgeBroker"]
