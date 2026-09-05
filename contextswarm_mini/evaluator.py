"""Small HTTP client for the ContextSwarmJudge Lean router."""

from __future__ import annotations

import copy
from contextlib import contextmanager
import datetime as dt
from email.utils import parsedate_to_datetime
import hashlib
from http.client import HTTPException, InvalidURL
import json
import math
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Iterable, Mapping
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from .models import Task, Verdict
from .timeout_policy import (
    AGENT_TIMEOUT_MIN_SECONDS,
    AgentTimeout,
    agent_timeout_bounds,
    normalize_agent_timeout,
)


LEAN_PROBE_RESPONSE_PROFILE = "lean_probe_v1"
_MAX_PROBE_DIAGNOSTICS = 24
_MAX_PROBE_DATA_BYTES = 1_024
_MAX_PROBE_SEVERITY_BYTES = 32
_MAX_DIAGNOSTIC_POSITION = 2_147_483_647
_MIN_HTTP_BACKOFF_SECONDS = 0.5
_MAX_HTTP_BACKOFF_SECONDS = 30.0
_JUDGE_CANCEL_TIMEOUT_SECONDS = 2.0
_CANCEL_AWARE_LONG_POLL_MS = 250
_CANCEL_AWARE_HTTP_TIMEOUT_SECONDS = 1.0
_MAX_SETTLEMENT_POLL_PATHS = 32
_AGENT_TIMEOUT_BOUNDARY_EPSILON_SECONDS = 0.01
_MAX_WORKER_ERROR_BYTES = 1_200
_MAX_WORKER_STATUS_BYTES = 120
_MAX_WORKER_IDENTIFIER_BYTES = 256
# This header is deliberately out-of-band from the candidate/evaluator JSON.
# Newer Judge dispatch layers may use it to route a job through an independent
# (completed-result and singleflight bypass) path.  The legacy coding endpoint
# does not consume this hint, so the process-level health gate remains the
# authoritative cache-disabled contract for the current deployment.
_DISPATCH_CACHE_MODE_HEADER = "X-ContextSwarmJudge-Dispatch-Cache-Mode"
_CACHE_MODE_DISABLED = "disabled"
_ENDPOINT_RE = re.compile(r"https?://[^\s\])}>\"']+", re.IGNORECASE)
_CREDENTIAL_RE = re.compile(
    r"(?i)\b(authorization|bearer|access[_-]?token|api[_-]?key|token)\b"
    r"(?:\s*[:=]\s*|\s+)([^\s,;]+)"
)
_OPAQUE_SECRET_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9])(?:"
    r"(?:sk|tok|nur|aisw)[_-][A-Za-z0-9_-]{12,}"
    r"|eyJ[A-Za-z0-9_-]{10,}(?:\.[A-Za-z0-9_-]{10,}){2}"
    r"|[A-Za-z0-9_-]{48,}"
    r")(?![A-Za-z0-9])"
)
_UNIX_ABSOLUTE_PATH_RE = re.compile(
    r"(?<![A-Za-z0-9_.-])/(?!/)(?:[^/\s:;,\])}>]+/)+[^/\s,;\])}>]+"
)
_WINDOWS_ABSOLUTE_PATH_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9_.-])[A-Z]:\\(?:[^\\\s:;,\])}>]+\\)*[^\\\s,;\])}>]+"
)
_NON_CACHEABLE_PROBE_STATUSES = {
    "EVALUATOR_ERROR",
    "EVALUATOR_TIMEOUT",
    "NETWORK_ERROR",
    "INFRASTRUCTURE_ERROR",
    "REMOTE_SETTLEMENT_UNCONFIRMED",
    "REJECTED_OVERLOADED",
    "OUT_OF_HORIZON",
    "RUNNING",
    "QUEUED",
    "PENDING",
    "CANCELLED",
    "TASK_CANCELLED",
}
# Values copied into the optional profile stream are deliberately bounded.  A
# Judge may expose implementation-specific status strings, but those strings
# are not useful dimensions for a resource report and can have unbounded
# cardinality.  Keep only the stable lifecycle labels and collapse the rest.
_PROFILE_STATUS_VALUES = frozenset(
    {
        "ok",
        "queued",
        "running",
        "accepted",
        "submitted",
        "cancel_requested",
        "cancelling",
        "succeeded",
        "failed",
        "cancelled",
        "ac",
        "wa",
        "pe",
        "ce",
        "mle",
        "tle",
        "re",
        "verify_fail",
        "proved",
        "local_rejected",
        "evaluator_error",
        "evaluator_timeout",
        "network_error",
        "out_of_horizon",
        "timeout",
        "overloaded",
        "malformed",
        "invalid_request",
        "http_error",
        "unsettled",
        "other",
    }
)

_PROFILE_STATUS_ALIASES = {
    "complete": "succeeded",
    "completed": "succeeded",
    "pass": "proved",
    "passed": "proved",
    "cancelled_by_client": "cancelled",
    "canceled": "cancelled",
    "cancel_requested": "cancel_requested",
    "request_cancelled": "cancelled",
    "request_deadline_elapsed": "timeout",
    "judge_overloaded": "overloaded",
    "judge_overloaded_deadline": "overloaded",
    "malformed_response": "malformed",
    "invalid_request_configuration": "invalid_request",
    "http_error": "http_error",
    "remote_settlement_unconfirmed": "unsettled",
    "cancel_settlement_unconfirmed": "unsettled",
    "timeouterror": "timeout",
    "timeout_error": "timeout",
    "connectionerror": "network_error",
    "urlerror": "network_error",
    "httpexception": "network_error",
    "oserror": "network_error",
}


def _profile_status(value: Any) -> str:
    text = str(value or "").strip().casefold().replace("-", "_")
    text = _PROFILE_STATUS_ALIASES.get(text, text)
    return text if text in _PROFILE_STATUS_VALUES else "other"


def _profile_clock() -> float:
    """Read a diagnostic clock without allowing a broken clock to escape."""

    try:
        return time.monotonic()
    except BaseException:
        return 0.0


def _profile_elapsed(started: float) -> float:
    if not started:
        return 0.0
    return max(0.0, _profile_clock() - started)


class EvaluatorError(RuntimeError):
    """A classified, bounded transport or malformed-verdict failure."""

    def __init__(
        self,
        message: str,
        *,
        category: str = "evaluator_error",
        http_status: int | None = None,
        attempts: int = 0,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(sanitize_worker_text(message, _MAX_WORKER_ERROR_BYTES))
        self.category = _safe_category(category)
        self.http_status = (
            int(http_status)
            if isinstance(http_status, int) and not isinstance(http_status, bool)
            else None
        )
        self.attempts = max(0, int(attempts))
        self.retry_after_seconds = _safe_nonnegative_number(retry_after_seconds)

    def public_details(self) -> dict[str, Any]:
        details: dict[str, Any] = {
            "category": self.category,
            "attempts": self.attempts,
        }
        if self.http_status is not None:
            details["http_status"] = self.http_status
        if self.retry_after_seconds is not None:
            details["retry_after_seconds"] = round(self.retry_after_seconds, 3)
        return details


def sanitize_worker_text(
    value: Any,
    maximum_bytes: int = _MAX_WORKER_ERROR_BYTES,
    *,
    sensitive_values: Iterable[Any] = (),
    tail: bool = False,
) -> str:
    """Bound and redact text before it can reach a solver or run artifact."""

    text = str(value or "").replace("\x00", "")
    exact_values = {
        str(item)
        for item in sensitive_values
        if item is not None and len(str(item)) >= 4
    }
    for private_value in sorted(exact_values, key=len, reverse=True):
        text = text.replace(private_value, "<redacted-secret>")
    text = _ENDPOINT_RE.sub("<redacted-endpoint>", text)
    text = _CREDENTIAL_RE.sub(
        lambda match: f"{match.group(1)}=<redacted-secret>", text
    )
    text = _OPAQUE_SECRET_RE.sub("<redacted-secret>", text)
    text = _WINDOWS_ABSOLUTE_PATH_RE.sub("<redacted-path>", text)
    text = _UNIX_ABSOLUTE_PATH_RE.sub("<redacted-path>", text)
    text = text.strip()
    maximum = max(0, int(maximum_bytes))
    if maximum == 0:
        return ""
    if tail and len(text.encode("utf-8")) > maximum:
        return text.encode("utf-8")[-maximum:].decode("utf-8", errors="ignore")
    bounded, _ = _bounded_utf8_text(text, maximum)
    return bounded


def sanitize_worker_identifier(value: Any) -> str | None:
    """Return a bounded opaque identifier, rejecting sensitive shapes."""

    text = str(value or "").strip()
    if not text or len(text.encode("utf-8")) > _MAX_WORKER_IDENTIFIER_BYTES:
        return None
    if (
        _ENDPOINT_RE.search(text)
        or _CREDENTIAL_RE.search(text)
        or _OPAQUE_SECRET_RE.search(text)
    ):
        return None
    if _UNIX_ABSOLUTE_PATH_RE.search(text) or _WINDOWS_ABSOLUTE_PATH_RE.search(text):
        return None
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", text):
        return None
    return text


class EvaluatorOverloadedError(EvaluatorError):
    """A definitive pre-admission rejection which is safe to retry."""

    def __init__(
        self,
        message: str,
        *,
        response: Mapping[str, Any] | None = None,
        **details: Any,
    ) -> None:
        super().__init__(message, **details)
        self.response = dict(response or {})


class RemoteSettlementUnconfirmedError(EvaluatorError):
    """A submission may have created work whose identity was not returned."""

    def __init__(
        self,
        message: str,
        *,
        submission_response: Mapping[str, Any] | None = None,
        **details: Any,
    ):
        super().__init__(message, **details)
        self.submission_response = dict(submission_response or {})


class _CombinedCancelEvent:
    """Event-compatible view over caller cancellation and the global latch."""

    def __init__(self, caller_event: Any, remote_event: threading.Event):
        self._caller_event = caller_event
        self._remote_event = remote_event

    def is_set(self) -> bool:
        return bool(
            self._remote_event.is_set()
            or (
                self._caller_event is not None
                and self._caller_event.is_set()
            )
        )

    def wait(self, timeout: float | None = None) -> bool:
        deadline = (
            None
            if timeout is None
            else time.perf_counter() + max(0.0, float(timeout))
        )
        while not self.is_set():
            if deadline is None:
                delay = 0.02
            else:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    return False
                delay = min(0.02, remaining)
            # The process-global event wakes this wait immediately. Caller
            # events are polled at a small bounded interval because Python has
            # no native wait-any primitive for Event objects.
            self._remote_event.wait(delay)
        return True

    def cancellation_reason(self) -> str | None:
        """Preserve caller cancellation provenance across the global latch.

        The process-wide settlement latch takes precedence over a task-local
        peer cancellation.  This prevents a run failure from being treated as
        an innocuous delayed cancellation merely because both events became
        set at nearly the same instant.
        """

        if self._remote_event.is_set():
            return "remote_settlement_unconfirmed"
        nested = getattr(self._caller_event, "cancellation_reason", None)
        if callable(nested):
            try:
                reason = nested()
            except Exception:
                reason = None
            if isinstance(reason, str) and reason:
                return reason
        return None

    def settlement_callback(self) -> Any | None:
        callback = getattr(self._caller_event, "settlement_callback", None)
        if callable(callback):
            try:
                return callback()
            except Exception:
                return None
        return None


_NONTERMINAL_STATUSES = {
    "QUEUED",
    "PENDING",
    "RUNNING",
    "IN_PROGRESS",
    "STARTED",
    "CANCEL_REQUESTED",
}

_PROVED_STATUSES = {"PROVED", "AC", "PASS", "PASSED"}
_RAW_FAILURE_STATUSES = {
    "FAILED",
    "TIMED_OUT",
    "ERROR",
    "CANCELLED",
    "CANCELED",
    "REJECTED_OVERLOADED",
}


def normalize_base_url(raw: str) -> str:
    value = str(raw or "").strip().rstrip("/")
    for suffix in (
        "/api/lean/jobs",
        "/api/judge/jobs",
        "/api/judge/evaluate",
        "/api/lean/verify",
        "/verify",
        "/healthz",
    ):
        if value.endswith(suffix):
            value = value[: -len(suffix)]
    return value.rstrip("/")


def _read_candidate(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None

def candidate_sha256(code: str) -> str:
    """Hash the exact UTF-8 source submitted to the Judge."""

    return hashlib.sha256(code.encode("utf-8")).hexdigest()


def task_contract_sha256(
    task: Task,
    *,
    lean_env_id: str,
    verification_profile: str,
    judge_mode: str,
) -> str:
    """Hash every immutable field that changes the meaning of a verdict."""

    digest = hashlib.sha256()
    for value in (
        task.slug,
        task.problem_id,
        task.theorem_name,
        task.baseline_code,
        lean_env_id,
        verification_profile,
        judge_mode,
    ):
        digest.update(value.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _retry_after_seconds(raw: str | None, *, default: float) -> float:
    delay = float(default)
    value = str(raw or "").strip()
    if value:
        try:
            delay = float(value)
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                delay = (parsed - dt.datetime.now(dt.timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                delay = float(default)
    if not math.isfinite(delay):
        return max(0.0, float(default))
    return max(0.0, delay)


def _cancel_requested(cancel_event: Any | None) -> bool:
    return cancel_event is not None and cancel_event.is_set()


def _cancel_reason(cancel_event: Any | None) -> str | None:
    """Read bounded cancellation provenance from runner/broker event views."""

    if cancel_event is None:
        return None
    getter = getattr(cancel_event, "cancellation_reason", None)
    if callable(getter):
        try:
            reason = getter()
        except Exception:
            reason = None
        if isinstance(reason, str) and reason:
            return reason
    return None


def _evaluation_cancel_reason(
    cancel_event: Any | None,
    *,
    deadline_monotonic: float | None,
) -> str | None:
    """Preserve cancellation provenance when the evaluation horizon wins.

    A formal evaluation can leave its polling loop because the fixed run
    horizon elapsed without a task-local Event being set.  In that case the
    submitted Judge job is still a known, runner-owned cancellation: DELETE
    was intentional and the router may need a little time to publish its
    terminal receipt.  Treat the elapsed deadline as provenance rather than
    allowing the short foreground cancel grace to turn it into an unknown
    remote settlement failure.
    """

    reason = _cancel_reason(cancel_event)
    if reason:
        return reason
    if (
        deadline_monotonic is not None
        and time.monotonic() >= deadline_monotonic
    ):
        return "horizon_elapsed"
    return None


def _settlement_callback(cancel_event: Any | None) -> Any | None:
    callback = getattr(cancel_event, "settlement_callback", None)
    if callable(callback):
        try:
            candidate = callback()
        except Exception:
            return None
        return candidate if callable(candidate) else None
    return None


def _wait_for_cancel(
    cancel_event: Any | None,
    delay_seconds: float,
) -> bool:
    """Wait for backoff/poll time while allowing broker revocation to wake it."""

    delay = max(0.0, float(delay_seconds))
    if cancel_event is None:
        if delay:
            time.sleep(delay)
        return False
    return bool(cancel_event.wait(delay))

class LeanEvaluator:
    is_mock_evaluator = False

    def __init__(
        self,
        base_url: str,
        *,
        lean_env_id: str,
        timeout_seconds: int = 300,
        verification_profile: str = "formal_proof",
        judge_mode: str = "fast",
        poll_interval_seconds: float = 1.0,
        settlement_grace_seconds: float = 30.0,
        cancel_grace_seconds: float = 5.0,
        admission_retry_seconds: float = 30.0,
        max_lifecycle_seconds: float | None = None,
        backend_max_retries: int = 1,
        terminal_overload_retries: int = 1,
        profiler: Any | None = None,
    ):
        self.base_url = normalize_base_url(base_url)
        self.lean_env_id = lean_env_id
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.verification_profile = verification_profile
        self.judge_mode = judge_mode
        self.poll_interval_seconds = max(0.05, float(poll_interval_seconds))
        self.settlement_grace_seconds = max(0.1, float(settlement_grace_seconds))
        self.cancel_grace_seconds = max(0.1, float(cancel_grace_seconds))
        self.admission_retry_seconds = max(0.1, float(admission_retry_seconds))
        lifecycle_cap = (
            max(3_600.0, (8.0 * self.timeout_seconds) + 120.0)
            if max_lifecycle_seconds is None
            else float(max_lifecycle_seconds)
        )
        if not math.isfinite(lifecycle_cap) or lifecycle_cap <= 0:
            raise ValueError("max_lifecycle_seconds must be finite and positive")
        self.max_lifecycle_seconds = lifecycle_cap
        # This is the established Judge execution-retry policy.  It is kept
        # independent from an Agent-proposed timeout: explicit budgets use the
        # same number of logical retries, but divide one absolute budget over
        # fresh attempts instead of multiplying a per-attempt timeout.
        self.backend_max_retries = max(0, int(backend_max_retries))
        self.terminal_overload_retries = max(0, int(terminal_overload_retries))
        self._probe_cache: dict[str, Verdict] = {}
        self._probe_cache_lock = threading.Lock()
        self._remote_unsettled_lock = threading.Lock()
        self._remote_unsettled_jobs = 0
        self._remote_settlement_event = threading.Event()
        # A known cancellation (a peer solved the task or the broker revoked
        # the claim) may cancel a submitted Judge job whose worker takes
        # longer than the short foreground grace window to reset.
        # Such a job remains accounted for, but is reconciled asynchronously so
        # it does not poison unrelated CPS admissions while the Judge is doing
        # a known, bounded cancellation.  Unknown identities still use the
        # fail-closed process latch above.
        self._deferred_settlement_lock = threading.RLock()
        self._deferred_settlements: dict[str, dict[str, Any]] = {}
        # Per-thread dispatch state lets explicit-budget calls request a
        # completed-result/singleflight cache bypass without changing the
        # legacy arm or racing with another broker handler using this evaluator.
        self._dispatch_context = threading.local()
        self._settlement_poll_count = 0
        self._settlement_poll_seconds = 0.0
        self._settlement_receipt_count = 0
        self._settlement_cancel_count = 0
        self.deferred_settlement_timeout_seconds = 300.0
        # Profiling is an observational side channel.  Keep the sink optional
        # and duck-typed so preflight/test adapters do not need to know about
        # it, and make every call fail-open at this boundary.
        self.profiler = profiler
        try:
            self._profiling_enabled = bool(
                profiler is not None and getattr(profiler, "enabled", False)
            )
        except BaseException:
            self._profiling_enabled = False

    def _normalize_agent_timeout(
        self, timeout_seconds: int | None
    ) -> AgentTimeout | None:
        """Apply the evaluator-side hard ceiling to a worker suggestion."""

        if timeout_seconds is None:
            return None
        return normalize_agent_timeout(
            timeout_seconds,
            configured_timeout_seconds=self.timeout_seconds,
        )

    @contextmanager
    def _dispatch_cache_mode(self, mode: str):
        """Set a request-local Judge dispatch/cache mode.

        ``LeanEvaluator`` is shared by concurrent broker handlers.  A mutable
        evaluator-wide flag would let one explicit retry accidentally disable
        (or enable) cache for another request, so keep the state in a thread
        local and restore it even when a transport call raises.
        """

        previous = getattr(self._dispatch_context, "cache_mode", None)
        self._dispatch_context.cache_mode = str(mode)
        try:
            yield
        finally:
            if previous is None:
                try:
                    del self._dispatch_context.cache_mode
                except AttributeError:
                    pass
            else:
                self._dispatch_context.cache_mode = previous

    def _profile_event(
        self,
        event: str,
        *,
        task: Task | None = None,
        **fields: Any,
    ) -> None:
        if not self._profiling_enabled:
            return
        try:
            self.profiler.emit(
                event,
                task_id=task.slug if task is not None else None,
                **fields,
            )
        except BaseException:
            # A diagnostic sink must never affect Judge lifecycle semantics.
            return

    @contextmanager
    def _profile_span(self, name: str, *, task: Task | None = None, **fields: Any):
        """Use an injected span without allowing it to mask evaluator errors."""

        if not self._profiling_enabled:
            yield
            return
        try:
            context = self.profiler.span(
                name,
                task_id=task.slug if task is not None else None,
                **fields,
            )
        except BaseException:
            context = None
        if context is None:
            yield
            return
        try:
            context.__enter__()
        except BaseException:
            yield
            return
        try:
            yield
        except BaseException:
            try:
                context.__exit__(*__import__("sys").exc_info())
            except BaseException:
                pass
            raise
        else:
            try:
                context.__exit__(None, None, None)
            except BaseException:
                pass

    def _observed_request(
        self,
        operation: str,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
        *,
        task: Task | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Call the concrete transport and emit one bounded stage timing row."""

        if not self._profiling_enabled:
            if payload is None:
                return self._request(method, path, **kwargs)
            return self._request(method, path, payload, **kwargs)
        started = _profile_clock()
        status = "ok"
        attempts = 1
        self._profile_event(
            "judge.http.start",
            task=task,
            operation=operation,
            phase=operation,
            method=str(method).upper(),
        )
        try:
            if payload is None:
                response = self._request(method, path, **kwargs)
            else:
                response = self._request(method, path, payload, **kwargs)
            if isinstance(response, Mapping):
                raw_status = response.get("status") or response.get("job_status")
                if isinstance(raw_status, str) and raw_status:
                    # Keep only a low-cardinality lifecycle label; never copy a
                    # response body or endpoint into the profile stream.
                    status = _profile_status(raw_status)
            return response
        except EvaluatorError as exc:
            status = _profile_status(exc.category or "error")
            attempts = max(1, int(getattr(exc, "attempts", 1) or 1))
            raise
        except BaseException as exc:
            status = _profile_status(type(exc).__name__)
            raise
        finally:
            self._profile_event(
                "judge.http.end",
                task=task,
                operation=operation,
                phase=operation,
                method=str(method).upper(),
                status=status,
                attempt_count=attempts,
                elapsed_seconds=_profile_elapsed(started),
            )

    @property
    def remote_unsettled_jobs(self) -> int:
        """Return the run-global count of remote jobs lacking terminal proof."""

        with self._remote_unsettled_lock:
            return self._remote_unsettled_jobs

    @property
    def remote_settlement_event(self) -> threading.Event:
        """Expose the one-way process latch for admission/cancel coordination."""

        return self._remote_settlement_event

    def _mark_remote_unsettled(self) -> None:
        with self._remote_unsettled_lock:
            self._remote_unsettled_jobs += 1
            self._remote_settlement_event.set()

    @property
    def pending_settlement_watchers(self) -> int:
        """Number of known remote cancellations still awaiting a receipt."""

        with self._deferred_settlement_lock:
            return len(self._deferred_settlements)

    def settlement_snapshot(self) -> dict[str, int | float]:
        """Return bounded watcher counters for broker closeout profiling.

        Job IDs, URLs, response bodies, and paths intentionally stay inside the
        evaluator.  The broker only needs queue depth, age, and monotonic poll
        counters to explain a long drain interval.
        """

        # This method is a diagnostics-only surface.  Keep the bounded counts
        # available to callers even when profiling is disabled, but do not take
        # an instrumentation clock (or derive watcher ages) on the hot/off
        # path.
        now = _profile_clock() if self._profiling_enabled else 0.0
        with self._deferred_settlement_lock:
            ages: list[float] = []
            if self._profiling_enabled:
                for record in self._deferred_settlements.values():
                    try:
                        started_at = float(record.get("started_at", now))
                    except (TypeError, ValueError, OverflowError):
                        started_at = now
                    ages.append(max(0.0, now - started_at))
            pending = len(ages)
            if not self._profiling_enabled:
                pending = len(self._deferred_settlements)
            poll_count = self._settlement_poll_count
            poll_seconds = self._settlement_poll_seconds
            receipt_count = self._settlement_receipt_count
            cancel_count = self._settlement_cancel_count
        return {
            "pending_settlement_watchers": pending,
            "oldest_watcher_age_seconds": max(ages, default=0.0),
            "poll_count": max(0, int(poll_count)),
            "settlement_poll_seconds": max(0.0, float(poll_seconds)),
            "settled_job_count": max(0, int(receipt_count)),
            "cancel_job_count": max(0, int(cancel_count)),
        }

    def _profile_settlement_transition(
        self,
        state: str,
        *,
        elapsed_seconds: float | None = None,
    ) -> None:
        """Publish one value-free watcher transition and its aggregate state."""

        if not self._profiling_enabled:
            return
        fields: dict[str, Any] = dict(self.settlement_snapshot())
        fields["watcher_state"] = state
        if elapsed_seconds is not None:
            fields["elapsed_seconds"] = max(0.0, float(elapsed_seconds))
        self._profile_event("judge.settlement.watcher", **fields)

    def _start_settlement_watcher(
        self,
        job_id: Any,
        response: Mapping[str, Any],
        *,
        cancel_endpoint: Any = None,
        on_settled: Any | None = None,
    ) -> bool:
        """Keep a known cancelled job's gate accounted for until terminal.

        The watcher never invents a terminal receipt and never releases a
        caller-owned permit on timeout.  A timeout therefore transitions to
        the same global fail-closed latch as any other unresolved remote job.
        """

        normalized = sanitize_worker_identifier(job_id)
        if normalized is None:
            return False
        if self._watcher_receipt_identity(response, normalized) != "matching":
            return False
        paths = self._settlement_poll_paths(
            normalized,
            response,
            cancel_endpoint=cancel_endpoint,
        )[:_MAX_SETTLEMENT_POLL_PATHS]
        if not paths:
            return False
        callback = on_settled if callable(on_settled) else None
        with self._deferred_settlement_lock:
            existing = self._deferred_settlements.get(normalized)
            if existing is not None:
                if callback is not None:
                    existing.setdefault("callbacks", []).append(callback)
                return True
            record: dict[str, Any] = {
                "job_id": normalized,
                "response": dict(response),
                "cancel_endpoint": cancel_endpoint,
                "paths": tuple(paths),
                "callbacks": [callback] if callback is not None else [],
                "started_at": _profile_clock() if self._profiling_enabled else 0.0,
            }
            self._deferred_settlements[normalized] = record
            if self._profiling_enabled:
                self._settlement_cancel_count += 1
        self._profile_settlement_transition("started")
        thread = threading.Thread(
            target=self._settlement_watcher_loop,
            args=(record,),
            name=f"judge-settlement-{normalized}",
            daemon=True,
        )
        record["thread"] = thread
        thread.start()
        return True

    def _settlement_watcher_loop(self, record: Mapping[str, Any]) -> None:
        """Run a watcher with a terminal fail-closed boundary.

        Polling is deliberately best-effort, but an unexpected adapter/sink
        exception must not leave the watcher entry (and its evaluator permit)
        permanently invisible to broker drain.  Convert such failures into the
        same run-global unsettled latch used by a timeout.
        """

        try:
            self._settlement_watcher_loop_impl(record)
        except BaseException:
            try:
                job_id = str(record.get("job_id") or "")
            except BaseException:
                job_id = ""
            self._mark_remote_unsettled()
            if job_id:
                with self._deferred_settlement_lock:
                    self._deferred_settlements.pop(job_id, None)
            self._profile_settlement_transition("error")

    def _settlement_watcher_loop_impl(self, record: Mapping[str, Any]) -> None:
        job_id = str(record["job_id"])
        paths = list(record.get("paths") or ())
        path_index = 0
        cancel_endpoint = record.get("cancel_endpoint")
        tainted_paths: set[str] = set()
        deadline = time.monotonic() + max(
            0.1, float(self.deferred_settlement_timeout_seconds)
        )
        watcher_started = _profile_clock() if self._profiling_enabled else 0.0
        settled = False
        while paths and time.monotonic() < deadline:
            active_paths = [path for path in paths if path not in tainted_paths]
            if not active_paths:
                break
            remaining = deadline - time.monotonic()
            wait_ms = max(1, min(1_000, int(remaining * 1_000)))
            path = active_paths[path_index % len(active_paths)]
            separator = "&" if "?" in path else "?"
            try:
                poll_started = _profile_clock() if self._profiling_enabled else 0.0
                if self._profiling_enabled:
                    with self._deferred_settlement_lock:
                        self._settlement_poll_count += 1
                try:
                    current = self._observed_request(
                        "settlement_poll",
                        "GET",
                        f"{path}{separator}wait_ms={wait_ms}",
                        timeout_seconds=max(0.1, min(2.0, remaining)),
                    )
                finally:
                    if self._profiling_enabled:
                        with self._deferred_settlement_lock:
                            self._settlement_poll_seconds += _profile_elapsed(
                                poll_started
                            )
            except BaseException:
                current = None
            if isinstance(current, Mapping):
                identity = self._watcher_receipt_identity(current, job_id)
                if identity == "invalid":
                    # A capability which ever contradicts or malforms the job
                    # identity is permanently unusable by this watcher.  In
                    # particular, do not accept an id-less receipt from an
                    # endpoint advertised by that response on a later poll.
                    tainted_paths.add(path)
                    bound = None
                else:
                    bound = self._bind_watcher_receipt(
                        current,
                        job_id,
                        allow_idless=identity == "missing",
                    )
                if bound is not None:
                    # Judge capabilities can rotate during cancellation, but
                    # only an identity-valid receipt may delegate another
                    # same-origin capability.  Bound the set so a degraded or
                    # malicious Judge cannot grow watcher state indefinitely.
                    advertised_cancel_endpoint = _nested_value(
                        bound, "cancel_endpoint"
                    )
                    if advertised_cancel_endpoint is not None:
                        cancel_endpoint = advertised_cancel_endpoint
                    for candidate_path in self._settlement_poll_paths(
                        job_id,
                        bound,
                        cancel_endpoint=cancel_endpoint,
                    ):
                        if candidate_path in paths:
                            continue
                        if len(paths) >= _MAX_SETTLEMENT_POLL_PATHS:
                            break
                        paths.append(candidate_path)
                if bound is not None and self._authoritative_terminal_receipt(
                    bound, job_id
                ):
                    if self._profiling_enabled:
                        with self._deferred_settlement_lock:
                            self._settlement_receipt_count += 1
                    settled = True
                    break
            path_index += 1
            time.sleep(min(self.poll_interval_seconds, max(0.0, deadline - time.monotonic())))

        # Publish an unresolved timeout before dropping the pending watcher.
        # This prevents closeout from observing a transient all-zero state
        # between the two accounting domains.
        if not settled:
            self._mark_remote_unsettled()

        if not settled:
            # The identity was known, but the bounded watcher still could not
            # prove termination.  At this point fail closed exactly as for an
            # unknown/transport-ambiguous submission.
            with self._deferred_settlement_lock:
                current_record = self._deferred_settlements.pop(job_id, None)
            self._profile_settlement_transition(
                "timed_out",
                elapsed_seconds=_profile_elapsed(watcher_started),
            )
            return

        # Keep the record visible while callbacks run.  A broker closeout may
        # otherwise observe ``pending_settlement_watchers == 0`` and finish
        # while the callback still owns the evaluator permit.  Drain callback
        # batches outside the lock, then re-check the record so a concurrent
        # duplicate cancellation can append another permit-release callback.
        callback_failed = False
        while True:
            with self._deferred_settlement_lock:
                current_record = self._deferred_settlements.get(job_id)
                if current_record is None:
                    callbacks = []
                else:
                    callbacks = list(current_record.get("callbacks") or ())
                    current_record["callbacks"] = []
            if not callbacks:
                break
            for callback in callbacks:
                try:
                    callback()
                except BaseException:
                    # A permit-release callback is orchestration bookkeeping;
                    # if it fails, retain the fail-closed latch so a leaked
                    # permit cannot be mistaken for a drained run.
                    callback_failed = True

        if callback_failed:
            self._mark_remote_unsettled()
        with self._deferred_settlement_lock:
            current_record = self._deferred_settlements.get(job_id)
            if current_record is record:
                self._deferred_settlements.pop(job_id, None)
        self._profile_settlement_transition(
            "settled" if not callback_failed else "callback_failed",
            elapsed_seconds=_profile_elapsed(watcher_started),
        )

    @staticmethod
    def _bind_watcher_receipt(
        response: Mapping[str, Any],
        job_id: Any,
        *,
        allow_idless: bool,
    ) -> dict[str, Any] | None:
        """Bind id-less responses from a job-scoped status capability.

        The watcher only polls Judge-provided same-origin capabilities or the
        canonical `/api/lean/jobs/<id>` fallback.  An omitted id is therefore
        safely bound to the already authenticated job identity; a contradictory
        id is rejected and can never be scored as this job.
        """

        identity = LeanEvaluator._watcher_receipt_identity(response, job_id)
        if identity == "invalid" or (identity == "missing" and not allow_idless):
            return None
        expected = sanitize_worker_identifier(job_id)
        if expected is None:
            return None
        bound = dict(response)
        if identity == "missing":
            bound["job_id"] = expected
        return bound

    @staticmethod
    def _watcher_receipt_identity(
        response: Mapping[str, Any],
        job_id: Any,
    ) -> str:
        """Classify every explicit job identity in a watcher receipt.

        ``missing`` means that the receipt truly omitted both supported job-id
        fields.  An explicitly present but unsanitizable value is ``invalid``;
        it must never be rewritten into an apparently authoritative receipt.
        Wrapped receipt identities are checked together so an outer matching
        id cannot conceal a contradictory nested one.
        """

        expected = sanitize_worker_identifier(job_id)
        if expected is None:
            return "invalid"
        pending: list[Mapping[str, Any]] = [response]
        visited: set[int] = set()
        found = False
        while pending and len(visited) < 16:
            current = pending.pop()
            marker = id(current)
            if marker in visited:
                continue
            visited.add(marker)
            for key in ("job_id", "id"):
                if key not in current:
                    continue
                found = True
                observed = sanitize_worker_identifier(current.get(key))
                if observed is None or observed != expected:
                    return "invalid"
            for key in ("response", "canonical_verdict"):
                nested = current.get(key)
                if isinstance(nested, Mapping):
                    pending.append(nested)
        if pending:
            # JSON receipts should never need this much envelope depth.  Do
            # not accept a matching outer id while leaving deeper identities
            # unchecked merely because an adversarial response exhausted the
            # traversal bound.
            return "invalid"
        return "matching" if found else "missing"

    def _combined_cancel_event(self, cancel_event: Any | None) -> Any:
        if cancel_event is None or cancel_event is self._remote_settlement_event:
            return self._remote_settlement_event
        return _CombinedCancelEvent(cancel_event, self._remote_settlement_event)

    def _remote_submission_error(
        self,
        message: str,
        *,
        category: str,
        attempts: int,
        http_status: int | None = None,
        submission_response: Mapping[str, Any] | None = None,
    ) -> RemoteSettlementUnconfirmedError:
        """Latch exactly one submission whose remote identity is unknown."""

        self._mark_remote_unsettled()
        return RemoteSettlementUnconfirmedError(
            message,
            category=category,
            http_status=http_status,
            attempts=attempts,
            submission_response=submission_response,
        )

    def _remote_settlement_gate_verdict(
        self,
        task: Task,
        *,
        started: float,
        candidate_code: str | None = None,
        provenance: Mapping[str, Any] | None = None,
    ) -> Verdict:
        """Return the stable result used to block all later admissions."""

        bound_provenance = dict(provenance or {})
        bound_provenance.setdefault(
            "candidate_sha256",
            candidate_sha256(candidate_code) if candidate_code is not None else None,
        )
        bound_provenance.setdefault(
            "task_contract_sha256", self.expected_task_contract_sha256(task)
        )
        return Verdict(
            task_id=task.slug,
            status="REMOTE_SETTLEMENT_UNCONFIRMED",
            score=0.0,
            elapsed_seconds=max(0.0, time.monotonic() - started),
            response={"reason": "remote_settlement_gate_latched"},
            **bound_provenance,
        )

    def expected_task_contract_sha256(self, task: Task) -> str:
        """Return the exact contract identity this evaluator will submit."""

        return task_contract_sha256(
            task,
            lean_env_id=self.lean_env_id,
            verification_profile=self.verification_profile,
            judge_mode=self.judge_mode,
        )

    def _request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        is_job_submission = (
            method.upper() == "POST" and path == "/api/lean/jobs"
        )
        request_payload = dict(payload) if payload is not None else None
        headers = {"Accept": "application/json"}
        if request_payload is not None:
            headers["Content-Type"] = "application/json"
        if (
            is_job_submission
            and getattr(self._dispatch_context, "cache_mode", None)
            == _CACHE_MODE_DISABLED
        ):
            # Explicit cumulative-budget attempts are independent retries, not
            # cache waiters.  The header is understood by the current Judge
            # dispatch layer; older routers safely ignore it and still receive
            # a distinct per-attempt timeout payload.
            headers[_DISPATCH_CACHE_MODE_HEADER] = _CACHE_MODE_DISABLED
        token = str(__import__("os").environ.get("LEAN_AUTH_TOKEN") or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        total_timeout = (
            float(self.timeout_seconds)
            if timeout_seconds is None
            else float(timeout_seconds)
        )
        if not math.isfinite(total_timeout) or total_timeout <= 0:
            raise EvaluatorError(
                "The Judge request deadline elapsed before transport admission.",
                category="request_deadline_elapsed",
            )
        request_deadline = time.monotonic() + total_timeout
        capacity_deadline = request_deadline
        if is_job_submission:
            capacity_deadline = min(
                capacity_deadline,
                time.monotonic() + self.admission_retry_seconds,
            )
        raw = ""
        attempt = 0
        while True:
            if _cancel_requested(cancel_event):
                raise EvaluatorError(
                    "The Judge request was cancelled.",
                    category="request_cancelled",
                    attempts=attempt,
                )
            remaining = request_deadline - time.monotonic()
            if remaining <= 0:
                raise EvaluatorError(
                    "The Judge request deadline elapsed.",
                    category="request_deadline_elapsed",
                    attempts=attempt,
                )
            attempt += 1
            data = None
            if request_payload is not None:
                attempt_payload = dict(request_payload)
                raw_execution_timeout = attempt_payload.get("timeout")
                if (
                    method.upper() == "POST"
                    and isinstance(raw_execution_timeout, int)
                    and not isinstance(raw_execution_timeout, bool)
                ):
                    if remaining < 1.0:
                        raise EvaluatorError(
                            "The Judge request deadline left no execution budget.",
                            category="request_deadline_elapsed",
                            attempts=attempt - 1,
                        )
                    attempt_payload["timeout"] = min(
                        raw_execution_timeout,
                        max(1, int(remaining)),
                    )
                data = json.dumps(attempt_payload, ensure_ascii=False).encode("utf-8")
            try:
                request = Request(url, data=data, headers=headers, method=method)
                request_timeout = min(remaining, 30.0)
                with urlopen(request, timeout=request_timeout) as response:
                    raw = response.read().decode("utf-8")
                break
            except HTTPError as exc:
                http_status = int(exc.code)
                retry_after = (
                    exc.headers.get("Retry-After")
                    if exc.headers is not None
                    else None
                )
                try:
                    error_payload = json.loads(exc.read(64 * 1024).decode("utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError):
                    error_payload = None
                backoff = min(
                    _MAX_HTTP_BACKOFF_SECONDS,
                    _MIN_HTTP_BACKOFF_SECONDS * (2 ** min(attempt - 1, 16)),
                )
                delay = _retry_after_seconds(retry_after, default=backoff)
                exc.close()
                confirmed_overload = (
                    is_job_submission
                    and http_status in {429, 503}
                    and isinstance(error_payload, Mapping)
                    and _confirmed_pre_admission_rejection(error_payload)
                )
                if confirmed_overload:
                    explicit_retry_delay = _retry_after_seconds(
                        retry_after,
                        default=0.0,
                    )
                    raise EvaluatorOverloadedError(
                        "Judge admission was definitively overloaded.",
                        category="judge_overloaded",
                        http_status=http_status,
                        attempts=attempt,
                        retry_after_seconds=explicit_retry_delay,
                        response=error_payload,
                    ) from None
                # Generic 429/503 responses are safe to replay for read-only
                # health/poll requests, but not for job creation: without an
                # idempotency key they may be lost responses after admission.
                # Only the explicit pre-admission receipt above may retry POST.
                retryable_capacity = (
                    http_status in {429, 503} and not is_job_submission
                )
                if retryable_capacity:
                    delay = max(delay, backoff)
                    remaining_capacity = capacity_deadline - time.monotonic()
                    if _cancel_requested(cancel_event):
                        raise EvaluatorError(
                            "The Judge request was cancelled.",
                            category="request_cancelled",
                            http_status=http_status,
                            attempts=attempt,
                            retry_after_seconds=delay,
                        ) from None
                    if remaining_capacity > 0 and delay < remaining_capacity:
                        if _wait_for_cancel(cancel_event, delay):
                            raise EvaluatorError(
                                "The Judge request was cancelled.",
                                category="request_cancelled",
                                http_status=http_status,
                                attempts=attempt,
                                retry_after_seconds=delay,
                            ) from None
                        continue
                    raise EvaluatorError(
                        "Judge capacity remained unavailable until the request deadline.",
                        category="judge_overloaded_deadline",
                        http_status=http_status,
                        attempts=attempt,
                        retry_after_seconds=delay,
                    ) from None
                if (
                    is_job_submission
                    and (http_status == 429 or http_status >= 500)
                    and not _bindable_terminal_job_receipt(error_payload)
                ):
                    raise self._remote_submission_error(
                        "The Judge submission outcome could not be settled.",
                        category="http_error",
                        http_status=http_status,
                        attempts=attempt,
                        submission_response=(
                            error_payload
                            if isinstance(error_payload, Mapping)
                            else None
                        ),
                    ) from None
                raise EvaluatorError(
                    "The Judge rejected the HTTP request.",
                    category="http_error",
                    http_status=http_status,
                    attempts=attempt,
                ) from None
            except (InvalidURL, ValueError, TypeError, OverflowError):
                raise EvaluatorError(
                    "The Judge endpoint or request configuration is invalid.",
                    category="invalid_request_configuration",
                    attempts=attempt,
                ) from None
            except UnicodeError:
                if is_job_submission:
                    raise self._remote_submission_error(
                        "The Judge submission response was malformed.",
                        category="malformed_response",
                        attempts=attempt,
                    ) from None
                raise EvaluatorError(
                    "The Judge returned a non-UTF-8 response.",
                    category="malformed_response",
                    attempts=attempt,
                ) from None
            except (URLError, TimeoutError, OSError, HTTPException):
                if is_job_submission:
                    raise self._remote_submission_error(
                        "The Judge submission transport outcome is unknown.",
                        category="network_error",
                        attempts=attempt,
                    ) from None
                raise EvaluatorError(
                    "The Judge transport failed.",
                    category="network_error",
                    attempts=attempt,
                ) from None
        if is_job_submission and not raw:
            raise self._remote_submission_error(
                "The Judge returned an empty submission response.",
                category="malformed_response",
                attempts=attempt,
            )
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            if is_job_submission:
                raise self._remote_submission_error(
                    "The Judge returned a malformed submission response.",
                    category="malformed_response",
                    attempts=attempt,
                ) from None
            raise EvaluatorError(
                "The Judge returned a non-JSON response.",
                category="malformed_response",
                attempts=attempt,
            ) from None
        if not isinstance(parsed, dict):
            if is_job_submission:
                raise self._remote_submission_error(
                    "The Judge returned a non-object submission response.",
                    category="malformed_response",
                    attempts=attempt,
                )
            raise EvaluatorError(
                "The Judge returned a non-object response.",
                category="malformed_response",
                attempts=attempt,
            )
        if (
            is_job_submission
            and not _confirmed_pre_admission_rejection(parsed)
            and _submission_job_identifier(parsed) is None
        ):
            raise self._remote_submission_error(
                "The Judge submission response lacked a bindable job id.",
                category="missing_job_identifier",
                attempts=attempt,
                submission_response=parsed,
            )
        return parsed

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/healthz")

    def evaluate(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: Any | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        """Evaluate a candidate, reusing an exact in-process probe when present."""

        return self._evaluate_candidate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
            reuse_probe_cache=True,
        )

    def evaluate_fresh(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: Any | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        """Evaluate through a fresh Judge submission, bypassing probe cache.

        Frozen closeout candidates use this path so their observed receipt has
        independent remote lineage.  Ordinary in-horizon evaluation retains
        the exact-candidate probe-cache behavior exposed by :meth:`evaluate`.
        """

        return self._evaluate_candidate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
            reuse_probe_cache=False,
        )

    def _evaluate_candidate(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None,
        cancel_event: Any | None,
        reuse_probe_cache: bool,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        started = time.monotonic()
        code = _read_candidate(candidate_path)
        if self.remote_unsettled_jobs > 0:
            return self._remote_settlement_gate_verdict(
                task,
                started=started,
                candidate_code=code,
            )
        combined_cancel_event = self._combined_cancel_event(cancel_event)
        if _cancel_requested(combined_cancel_event):
            return self._cancelled_verdict(
                task,
                started=started,
                provenance={
                    "candidate_sha256": (
                        candidate_sha256(code) if code is not None else None
                    ),
                    "task_contract_sha256": self.expected_task_contract_sha256(task),
                },
                job_id=None,
                cancellation=None,
            )
        cache_key = self._probe_cache_key(task, code) if code is not None else None
        # A caller-supplied timeout is part of the validation contract, not
        # merely a presentation hint.  Do not return an older result which was
        # produced under a different timeout (or under the legacy retry
        # policy).  The Judge-side dispatch cache is bypassed separately by the
        # request-local mode in ``_evaluate_with_total_budget``.
        if timeout_seconds is None and reuse_probe_cache and cache_key is not None:
            cached = self._cached_verdict(cache_key)
            if cached is not None:
                return cached
        verdict = self._evaluate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            started=started,
            terminal_overload_retries=self.terminal_overload_retries,
            response_profile=None,
            candidate_code=code,
            cancel_event=combined_cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )
        verdict.cache_reused = bool(
            verdict.cache_reused
            or _nested_value(verdict.response, "cache_reused") is True
        )
        return verdict

    def probe(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        """Run the canonical evaluator with bounded worker-facing diagnostics."""

        return self._probe_source(
            task,
            candidate_path,
            _read_candidate(candidate_path),
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        """Probe a broker-owned immutable source snapshot."""

        if not isinstance(candidate_code, str):
            raise TypeError("candidate_code must be a string")
        return self._probe_source(
            task,
            Path("<broker-candidate-snapshot>"),
            candidate_code,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )

    def _probe_source(
        self,
        task: Task,
        candidate_path: Path,
        code: str | None,
        *,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        started = time.monotonic()
        if self.remote_unsettled_jobs > 0:
            return self._remote_settlement_gate_verdict(
                task,
                started=started,
                candidate_code=code,
            )
        combined_cancel_event = self._combined_cancel_event(cancel_event)
        cache_key = self._probe_cache_key(task, code) if code is not None else None
        if (
            timeout_seconds is None
            and not _cancel_requested(combined_cancel_event)
            and cache_key is not None
        ):
            cached = self._cached_verdict(cache_key)
            if cached is not None:
                return cached
        verdict = self._evaluate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            started=started,
            terminal_overload_retries=self.terminal_overload_retries,
            response_profile=LEAN_PROBE_RESPONSE_PROFILE,
            candidate_code=code,
            cancel_event=combined_cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )
        verdict.cache_reused = bool(
            verdict.cache_reused
            or _nested_value(verdict.response, "cache_reused") is True
        )
        if (
            timeout_seconds is None
            and cache_key is not None
            and verdict.status not in _NON_CACHEABLE_PROBE_STATUSES
        ):
            with self._probe_cache_lock:
                self._probe_cache[cache_key] = copy.deepcopy(verdict)
        return verdict

    def _cached_verdict(self, cache_key: str) -> Verdict | None:
        with self._probe_cache_lock:
            cached = self._probe_cache.get(cache_key)
        if cached is None:
            return None
        response = copy.deepcopy(cached.response)
        response["probe_cache_reused"] = True
        return Verdict(
            task_id=cached.task_id,
            status=cached.status,
            score=cached.score,
            elapsed_seconds=0.0,
            response=response,
            error=cached.error,
            candidate_sha256=cached.candidate_sha256,
            task_contract_sha256=cached.task_contract_sha256,
            judge_job_id=cached.judge_job_id,
            cache_reused=True,
        )

    def _evaluate(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None,
        started: float,
        terminal_overload_retries: int,
        response_profile: str | None,
        candidate_code: str | None,
        cancel_event: threading.Event | None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        """Evaluate one logical request under the selected timeout contract.

        The historical path (no ``timeout_seconds``) is intentionally kept in
        :meth:`_evaluate_once`: its ``timeout`` and ``max_retries`` fields are
        the legacy per-backend-attempt contract.  An explicit Agent value is a
        different contract.  It is an absolute budget for this logical broker
        call; retries are performed with fresh backend jobs and receive only
        the remaining budget.  Keeping this dispatch at the evaluator layer
        means the broker semaphore/handler remains owned by one call and no
        allocator transition is needed.
        """

        if timeout_seconds is None:
            return self._evaluate_once(
                task,
                candidate_path,
                deadline_monotonic=deadline_monotonic,
                started=started,
                terminal_overload_retries=terminal_overload_retries,
                response_profile=response_profile,
                candidate_code=candidate_code,
                cancel_event=cancel_event,
                settlement_callback=settlement_callback,
                timeout_seconds=None,
            )
        return self._evaluate_with_total_budget(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            started=started,
            terminal_overload_retries=terminal_overload_retries,
            response_profile=response_profile,
            candidate_code=candidate_code,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )

    def _evaluate_with_total_budget(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None,
        started: float,
        terminal_overload_retries: int,
        response_profile: str | None,
        candidate_code: str | None,
        cancel_event: threading.Event | None,
        settlement_callback: Any | None = None,
        timeout_seconds: int,
        timeout_deadline_monotonic: float | None = None,
        attempt_runner: Callable[[float, int], Verdict] | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        """Run an explicit Agent budget across all safe evaluator retries.

        The Judge API currently exposes a per-attempt ``timeout`` and a
        per-job ``max_retries``.  Sending ``max_retries=0`` for each fresh
        attempt and owning the retry loop here avoids multiplying a nominal
        Agent budget (for example 60 s) into a 120 s backend tail.  The loop
        still permits the configured number of retries for *abnormal,
        candidate-independent* terminal failures.  A timeout, cancellation,
        deterministic verification result, or an unsettled remote identity is
        never retried.

        ``deadline_monotonic`` remains the run horizon.  The earlier of the
        horizon and the Agent budget is passed to the attempt evaluator so
        admission, polling and cancellation observe one logical deadline.  A
        short remote settlement grace may still be needed after that deadline;
        the returned metadata distinguishes this bounded lifecycle grace from
        actual validation budget consumption.
        """

        agent_timeout = self._normalize_agent_timeout(timeout_seconds)
        if agent_timeout is None:  # defensive; the public dispatcher filters it
            return self._evaluate_once(
                task,
                candidate_path,
                deadline_monotonic=deadline_monotonic,
                started=started,
                terminal_overload_retries=terminal_overload_retries,
                response_profile=response_profile,
                candidate_code=candidate_code,
                cancel_event=cancel_event,
                settlement_callback=settlement_callback,
                timeout_seconds=None,
            )

        budget_seconds = int(agent_timeout.effective_seconds)
        budget_deadline = float(started) + float(budget_seconds)
        if timeout_deadline_monotonic is not None:
            try:
                supplied_deadline = float(timeout_deadline_monotonic)
            except (TypeError, ValueError, OverflowError):
                supplied_deadline = budget_deadline
            if math.isfinite(supplied_deadline):
                budget_deadline = min(budget_deadline, supplied_deadline)
        logical_deadline = budget_deadline
        if deadline_monotonic is not None:
            logical_deadline = min(logical_deadline, float(deadline_monotonic))

        # Keep execution retry count independent from overload/admission retry
        # count.  Both consume the same logical time budget, but a sustained
        # overload should not silently consume a worker-failure retry slot (or
        # vice versa).  ``backend_max_retries`` defaults to the historical one
        # and is intentionally not coupled to the Agent's requested seconds.
        execution_retries_left = max(0, int(self.backend_max_retries))
        overload_retries_left = max(0, int(terminal_overload_retries))
        attempt_index = 0
        attempt_job_ids: list[str] = []
        attempt_timeouts: list[int] = []
        attempt_elapsed: list[float] = []
        retry_reasons: list[str] = []
        pending_retry_reason: str | None = None
        last_verdict: Verdict | None = None

        while True:
            now = time.monotonic()
            remaining = logical_deadline - now
            # The default public Agent contract has a five-second floor.  A
            # manifest may deliberately choose a smaller positive cap, in
            # which case that cap is also the smallest executable attempt.
            minimum_attempt_seconds = min(
                AGENT_TIMEOUT_MIN_SECONDS, budget_seconds
            )
            if (
                remaining
                < minimum_attempt_seconds - _AGENT_TIMEOUT_BOUNDARY_EPSILON_SECONDS
            ):
                if last_verdict is None:
                    return self._total_budget_exhausted_verdict(
                        task,
                        started=started,
                        candidate_code=candidate_code,
                        budget_seconds=budget_seconds,
                        budget_deadline=budget_deadline,
                        logical_deadline=logical_deadline,
                        attempt_index=attempt_index,
                        attempt_job_ids=attempt_job_ids,
                        attempt_timeouts=attempt_timeouts,
                        attempt_elapsed=attempt_elapsed,
                        retry_reasons=retry_reasons,
                        timeout=agent_timeout,
                        run_horizon=deadline_monotonic,
                    )
                return self._annotate_total_budget_verdict(
                    last_verdict,
                    budget_seconds=budget_seconds,
                    budget_deadline=budget_deadline,
                    logical_deadline=logical_deadline,
                    attempt_index=attempt_index,
                    attempt_job_ids=attempt_job_ids,
                    attempt_timeouts=attempt_timeouts,
                    attempt_elapsed=attempt_elapsed,
                    retry_reasons=retry_reasons,
                    exhausted=True,
                    stop_reason=(
                        "run_horizon"
                        if deadline_monotonic is not None
                        and time.monotonic() >= float(deadline_monotonic)
                        else "budget_exhausted_before_retry"
                    ),
                    timeout=agent_timeout,
                )

            # A broker-owned formal quota may count each fresh backend job,
            # not merely the logical helper call.  Reserve the next slot only
            # after the remaining-budget check, so a retry that cannot honor
            # the configured minimum floor does not consume quota speculatively.
            if attempt_index > 0 and retry_admission_callback is not None:
                try:
                    retry_admitted = bool(retry_admission_callback())
                except Exception:
                    retry_admitted = False
                if not retry_admitted:
                    blocked_response = dict(last_verdict.response or {}) if last_verdict else {}
                    blocked_response.update(
                        {
                            "formal_backend_budget_exhausted": True,
                            "retry_blocked_reason": "formal_backend_job_quota",
                        }
                    )
                    blocked_verdict = last_verdict or Verdict(
                        task_id=task.slug,
                        status="EVALUATOR_ERROR",
                        score=0.0,
                        elapsed_seconds=0.0,
                        response=blocked_response,
                        candidate_sha256=(
                            candidate_sha256(candidate_code)
                            if candidate_code is not None
                            else None
                        ),
                        task_contract_sha256=self.expected_task_contract_sha256(task),
                    )
                    blocked_verdict = Verdict(
                        task_id=blocked_verdict.task_id,
                        status=blocked_verdict.status,
                        score=blocked_verdict.score,
                        elapsed_seconds=blocked_verdict.elapsed_seconds,
                        response=blocked_response,
                        error=blocked_verdict.error,
                        candidate_sha256=blocked_verdict.candidate_sha256,
                        task_contract_sha256=blocked_verdict.task_contract_sha256,
                        judge_job_id=blocked_verdict.judge_job_id,
                        cache_reused=blocked_verdict.cache_reused,
                    )
                    return self._annotate_total_budget_verdict(
                        blocked_verdict,
                        budget_seconds=budget_seconds,
                        budget_deadline=budget_deadline,
                        logical_deadline=logical_deadline,
                        # ``attempt_index`` counts completed attempts.  The
                        # callback is consulted before starting the next one,
                        # so this remains the actual attempt count even when
                        # quota denies the pending retry.
                        attempt_index=attempt_index,
                        attempt_job_ids=attempt_job_ids,
                        attempt_timeouts=attempt_timeouts,
                        attempt_elapsed=attempt_elapsed,
                        # ``retry_reasons`` contains only retries that have
                        # actually started.  The pending classified reason is
                        # intentionally omitted because the quota denied the
                        # next fresh attempt.
                        retry_reasons=retry_reasons,
                        exhausted=False,
                        stop_reason="formal_backend_job_quota",
                        timeout=agent_timeout,
                    )

            # The retry reason becomes observable only when the fresh attempt
            # is actually admitted.  This keeps ``judge_retry_count`` from
            # claiming a retry that was blocked by quota or fell below the
            # configured minimum floor while waiting for the next loop turn.
            if pending_retry_reason is not None:
                retry_reasons.append(pending_retry_reason)
                pending_retry_reason = None

            # Round up the integer sent to Judge so normal sub-second adapter
            # overhead does not turn a requested 60-second first attempt into
            # 59 seconds.  The absolute monotonic deadline below remains the
            # hard boundary, so the rounding cannot create another full
            # timeout tail.
            attempt_timeout = max(
                minimum_attempt_seconds,
                min(budget_seconds, int(math.ceil(remaining))),
            )
            attempt_index += 1
            attempt_timeouts.append(attempt_timeout)
            attempt_started = time.monotonic()
            # A custom attempt must not receive the legacy in-job retry.  The
            # fresh-attempt loop below is the single owner of retry accounting.
            # Disable completed-result/singleflight cache for the whole custom
            # call so an earlier timeout cannot be returned as a false retry.
            with self._dispatch_cache_mode(_CACHE_MODE_DISABLED):
                if attempt_runner is not None:
                    verdict = attempt_runner(logical_deadline, attempt_timeout)
                else:
                    verdict = self._evaluate_once(
                        task,
                        candidate_path,
                        deadline_monotonic=logical_deadline,
                        started=started,
                        terminal_overload_retries=0,
                        response_profile=response_profile,
                        candidate_code=candidate_code,
                        cancel_event=cancel_event,
                        settlement_callback=settlement_callback,
                        timeout_seconds=attempt_timeout,
                    )
            verdict = _relabel_agent_budget_timeout(
                verdict,
                budget_deadline=budget_deadline,
                run_horizon_deadline=deadline_monotonic,
            )
            attempt_elapsed.append(max(0.0, time.monotonic() - attempt_started))
            last_verdict = verdict
            if verdict.judge_job_id:
                normalized_job_id = sanitize_worker_identifier(verdict.judge_job_id)
                if normalized_job_id is not None:
                    attempt_job_ids.append(normalized_job_id)

            retry_class = _custom_retry_class(
                verdict,
                remaining_budget_seconds=max(
                    0.0, min(budget_deadline, logical_deadline) - time.monotonic()
                ),
                budget_seconds=budget_seconds,
                attempt_elapsed_seconds=attempt_elapsed[-1],
            )
            if retry_class is None:
                now = time.monotonic()
                if deadline_monotonic is not None and now >= float(deadline_monotonic):
                    stop_reason = "run_horizon"
                elif now >= budget_deadline:
                    stop_reason = "budget_exhausted"
                elif verdict.status in _CUSTOM_DETERMINISTIC_STATUSES:
                    stop_reason = "terminal_verdict"
                else:
                    stop_reason = "not_retryable"
                return self._annotate_total_budget_verdict(
                    verdict,
                    budget_seconds=budget_seconds,
                    budget_deadline=budget_deadline,
                    logical_deadline=logical_deadline,
                    attempt_index=attempt_index,
                    attempt_job_ids=attempt_job_ids,
                    attempt_timeouts=attempt_timeouts,
                    attempt_elapsed=attempt_elapsed,
                    retry_reasons=retry_reasons,
                    exhausted=(budget_deadline - now) <= 0,
                    stop_reason=stop_reason,
                    timeout=agent_timeout,
                )
            if retry_class == "overload":
                if overload_retries_left <= 0:
                    return self._annotate_total_budget_verdict(
                        verdict,
                        budget_seconds=budget_seconds,
                        budget_deadline=budget_deadline,
                        logical_deadline=logical_deadline,
                        attempt_index=attempt_index,
                        attempt_job_ids=attempt_job_ids,
                        attempt_timeouts=attempt_timeouts,
                        attempt_elapsed=attempt_elapsed,
                        retry_reasons=retry_reasons,
                        exhausted=False,
                        stop_reason="retry_limit_overload",
                        timeout=agent_timeout,
                    )
                overload_retries_left -= 1
            else:
                if execution_retries_left <= 0:
                    return self._annotate_total_budget_verdict(
                        verdict,
                        budget_seconds=budget_seconds,
                        budget_deadline=budget_deadline,
                        logical_deadline=logical_deadline,
                        attempt_index=attempt_index,
                        attempt_job_ids=attempt_job_ids,
                        attempt_timeouts=attempt_timeouts,
                        attempt_elapsed=attempt_elapsed,
                        retry_reasons=retry_reasons,
                        exhausted=False,
                        stop_reason="retry_limit_execution",
                        timeout=agent_timeout,
                    )
                execution_retries_left -= 1
            pending_retry_reason = retry_class

            # A retry is useful only if the next backend attempt can honor the
            # advertised floor.  The next loop computes the exact remaining
            # integer and will return the last terminal result otherwise.

    def _total_budget_exhausted_verdict(
        self,
        task: Task,
        *,
        started: float,
        candidate_code: str | None,
        budget_seconds: int,
        budget_deadline: float,
        logical_deadline: float,
        attempt_index: int,
        attempt_job_ids: list[str],
        attempt_timeouts: list[int],
        attempt_elapsed: list[float],
        retry_reasons: list[str],
        timeout: AgentTimeout,
        run_horizon: float | None,
    ) -> Verdict:
        now = time.monotonic()
        run_horizon_value: float | None = None
        if run_horizon is not None:
            try:
                candidate_horizon = float(run_horizon)
            except (TypeError, ValueError, OverflowError):
                candidate_horizon = float("nan")
            if math.isfinite(candidate_horizon):
                run_horizon_value = candidate_horizon

        # ``logical_deadline`` is the earlier of the Agent budget and the
        # outer run horizon.  Reaching the configured minimum remaining-attempt
        # floor before that logical deadline is not the same as exhausting the
        # Agent budget:
        # near the fixed run horizon there may still be 45/60 seconds in the
        # Agent budget, but no safe time left to start another backend job.
        # Classify that case as OUT_OF_HORIZON and keep the independent Agent
        # budget remaining/exhausted fields truthful.
        horizon_limited = (
            run_horizon_value is not None
            and run_horizon_value <= float(budget_deadline)
            and float(logical_deadline) <= run_horizon_value
        )
        run_horizon_elapsed = (
            run_horizon_value is not None and now >= run_horizon_value
        )
        budget_expired = now >= float(budget_deadline)
        horizon_stop = horizon_limited or run_horizon_elapsed
        status = "OUT_OF_HORIZON" if horizon_stop else "EVALUATOR_TIMEOUT"
        verdict = Verdict(
            task_id=task.slug,
            status=status,
            score=0.0,
            elapsed_seconds=max(0.0, now - started),
            response={
                "reason": (
                    "run_horizon_before_retry"
                    if horizon_limited and not run_horizon_elapsed
                    else "run_horizon_elapsed_before_retry"
                    if run_horizon_elapsed
                    else "agent_total_timeout_exhausted_before_retry"
                ),
            },
            candidate_sha256=(
                candidate_sha256(candidate_code)
                if candidate_code is not None
                else None
            ),
            task_contract_sha256=self.expected_task_contract_sha256(task),
        )
        return self._annotate_total_budget_verdict(
            verdict,
            budget_seconds=budget_seconds,
            budget_deadline=budget_deadline,
            logical_deadline=logical_deadline,
            attempt_index=attempt_index,
            attempt_job_ids=attempt_job_ids,
            attempt_timeouts=attempt_timeouts,
            attempt_elapsed=attempt_elapsed,
            retry_reasons=retry_reasons,
            exhausted=budget_expired,
            stop_reason=("run_horizon" if horizon_stop else "budget_exhausted"),
            timeout=timeout,
        )

    def _annotate_total_budget_verdict(
        self,
        verdict: Verdict,
        *,
        budget_seconds: int,
        budget_deadline: float,
        logical_deadline: float,
        attempt_index: int,
        attempt_job_ids: list[str],
        attempt_timeouts: list[int],
        attempt_elapsed: list[float],
        retry_reasons: list[str],
        exhausted: bool,
        stop_reason: str | None = None,
        timeout: AgentTimeout,
    ) -> Verdict:
        now = time.monotonic()
        response = dict(verdict.response or {})
        budget_started = float(budget_deadline) - float(budget_seconds)
        budget_elapsed = max(0.0, now - budget_started)
        budget_remaining = max(0.0, float(budget_deadline) - now)
        retry_count = len(retry_reasons)
        response.update(
            {
                "timeout_budget_mode": "cumulative_total",
                "requested_timeout_seconds": int(timeout.requested_seconds),
                "effective_timeout_seconds": int(timeout.effective_seconds),
                "timeout_clamped": bool(timeout.clamped),
                "timeout_source": "agent_requested",
                "timeout_budget_seconds": int(budget_seconds),
                "timeout_budget_elapsed_seconds": round(
                    max(0.0, min(float(budget_seconds), budget_elapsed)),
                    6,
                ),
                "timeout_budget_remaining_seconds": round(
                    budget_remaining, 6
                ),
                "timeout_budget_exhausted": bool(exhausted),
                "timeout_budget_stop_reason": (
                    str(stop_reason)[:64] if stop_reason else None
                ),
                "judge_attempt_count": max(0, int(attempt_index)),
                "judge_retry_count": max(0, int(retry_count)),
                "judge_attempt_timeouts_seconds": [
                    max(0, int(value)) for value in attempt_timeouts[:16]
                ],
                "judge_attempt_elapsed_seconds": [
                    round(max(0.0, float(value)), 6)
                    for value in attempt_elapsed[:16]
                ],
                "judge_retry_reasons": [str(value)[:64] for value in retry_reasons[:16]],
                "judge_attempt_ids": list(attempt_job_ids[:16]),
            }
        )
        return Verdict(
            task_id=verdict.task_id,
            status=verdict.status,
            score=verdict.score,
            elapsed_seconds=verdict.elapsed_seconds,
            response=response,
            error=verdict.error,
            candidate_sha256=verdict.candidate_sha256,
            task_contract_sha256=verdict.task_contract_sha256,
            judge_job_id=verdict.judge_job_id,
            cache_reused=verdict.cache_reused,
        )

    def _evaluate_once(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None,
        started: float,
        terminal_overload_retries: int,
        response_profile: str | None,
        candidate_code: str | None,
        cancel_event: threading.Event | None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
    ) -> Verdict:
        contract_sha256 = task_contract_sha256(
            task,
            lean_env_id=self.lean_env_id,
            verification_profile=self.verification_profile,
            judge_mode=self.judge_mode,
        )
        source_sha256 = (
            candidate_sha256(candidate_code) if candidate_code is not None else None
        )
        provenance = {
            "candidate_sha256": source_sha256,
            "task_contract_sha256": contract_sha256,
        }
        if self.remote_unsettled_jobs > 0:
            return self._remote_settlement_gate_verdict(
                task,
                started=started,
                candidate_code=candidate_code,
                provenance=provenance,
            )
        if _cancel_requested(cancel_event):
            return self._cancelled_verdict(
                task,
                started=started,
                provenance=provenance,
                job_id=None,
                cancellation=None,
            )
        if (
            deadline_monotonic is not None
            and time.monotonic() >= deadline_monotonic
        ):
            return Verdict(
                task.slug,
                "OUT_OF_HORIZON",
                0.0,
                0.0,
                {"reason": "run_horizon_elapsed"},
                **provenance,
            )
        job_id: Any = None
        cancel_endpoint: Any = None
        response: dict[str, Any] = {}
        last_poll_error: str | None = None
        try:
            code = (
                candidate_code
                if candidate_code is not None
                else candidate_path.read_text(encoding="utf-8")
            )
            if source_sha256 is None:
                source_sha256 = candidate_sha256(code)
                provenance["candidate_sha256"] = source_sha256
            target = task.baseline_code
            local_error = _local_contract_error(task, code, target)
            if local_error:
                return Verdict(
                    task_id=task.slug,
                    status="LOCAL_REJECTED",
                    score=0.0,
                    elapsed_seconds=time.monotonic() - started,
                    response={"reason": local_error},
                    **provenance,
                )
            remaining_horizon = (
                deadline_monotonic - time.monotonic()
                if deadline_monotonic is not None
                else None
            )
            if remaining_horizon is not None and remaining_horizon < 1.0:
                return Verdict(
                    task.slug,
                    "OUT_OF_HORIZON",
                    0.0,
                    time.monotonic() - started,
                    {"reason": "run_horizon_elapsed_before_submission"},
                    **provenance,
                )
            agent_timeout = self._normalize_agent_timeout(timeout_seconds)
            execution_timeout = (
                agent_timeout.effective_seconds
                if agent_timeout is not None
                else self.timeout_seconds
            )
            if remaining_horizon is not None:
                # Preserve the integer budget at the boundary.  The polling
                # loop still enforces the absolute monotonic deadline; floor
                # rounding here would unnecessarily turn (for example)
                # 60.0 seconds into a 59-second Judge request before any work
                # has actually consumed the budget.
                horizon_seconds = (
                    int(math.ceil(remaining_horizon))
                    if agent_timeout is not None
                    else int(remaining_horizon)
                )
                execution_timeout = min(
                    execution_timeout,
                    max(1, horizon_seconds),
                )
            # ``_evaluate_with_total_budget`` owns retries for explicit Agent
            # budgets.  This method is also used by the legacy path, where the
            # configured per-job retry policy remains intact.
            backend_max_retries = (
                0 if agent_timeout is not None else self.backend_max_retries
            )
            payload = {
                "code": code,
                "target_code": target,
                "timeout": execution_timeout,
                # Explicit budgets are one backend attempt here; the outer
                # logical-budget loop may issue a fresh attempt with the
                # remaining budget.  The legacy path keeps its configured
                # in-job retry count.
                "max_retries": backend_max_retries,
                "problem_id": task.problem_id,
                "lean_env_id": self.lean_env_id,
                "verification_profile": self.verification_profile,
                "judge_mode": self.judge_mode,
            }
            if response_profile:
                payload["response_profile"] = response_profile
            admission_deadline = time.monotonic() + self.admission_retry_seconds
            if deadline_monotonic is not None:
                admission_deadline = min(admission_deadline, deadline_monotonic)
            admission_attempt = 0
            last_admission_rejection: dict[str, Any] | None = None
            while True:
                if self.remote_unsettled_jobs > 0:
                    return self._remote_settlement_gate_verdict(
                        task,
                        started=started,
                        candidate_code=code,
                        provenance=provenance,
                    )
                if _cancel_requested(cancel_event):
                    return self._cancelled_verdict(
                        task,
                        started=started,
                        provenance=provenance,
                        job_id=None,
                        cancellation=None,
                    )
                remaining_admission = admission_deadline - time.monotonic()
                if remaining_admission <= 0:
                    horizon_elapsed = (
                        deadline_monotonic is not None
                        and time.monotonic() >= deadline_monotonic
                    )
                    rejection_response = _safe_response(
                        last_admission_rejection or {},
                        timeout_max_seconds=self.timeout_seconds,
                    )
                    rejection_response.update(
                        {
                            "reason": (
                                "run_horizon_elapsed_during_admission"
                                if horizon_elapsed
                                else "judge_admission_retry_exhausted"
                            ),
                            "retryable": True,
                            "admission_attempts": admission_attempt,
                        }
                    )
                    return Verdict(
                        task.slug,
                        "OUT_OF_HORIZON" if horizon_elapsed else "REJECTED_OVERLOADED",
                        0.0,
                        time.monotonic() - started,
                        rejection_response,
                        **provenance,
                    )
                admission_attempt += 1
                admission_retry_delay = 0.0
                submit_timeout = (
                    max(0.1, deadline_monotonic - time.monotonic())
                    if deadline_monotonic is not None
                    else max(0.1, execution_timeout + remaining_admission)
                )
                request_options: dict[str, Any] = {
                    "timeout_seconds": submit_timeout,
                }
                if cancel_event is not None:
                    request_options["cancel_event"] = cancel_event
                try:
                    submitted = self._observed_request(
                        "submit",
                        "POST",
                        "/api/lean/jobs",
                        payload,
                        task=task,
                        **request_options,
                    )
                    if not _confirmed_pre_admission_rejection(submitted):
                        break
                    last_admission_rejection = submitted
                except EvaluatorOverloadedError as exc:
                    if exc.response:
                        last_admission_rejection = exc.response
                    admission_retry_delay = float(exc.retry_after_seconds or 0.0)
                if self.remote_unsettled_jobs > 0:
                    return self._remote_settlement_gate_verdict(
                        task,
                        started=started,
                        candidate_code=code,
                        provenance=provenance,
                    )
                remaining_admission = admission_deadline - time.monotonic()
                if remaining_admission > 0:
                    cancelled = _wait_for_cancel(
                        cancel_event,
                        min(
                            remaining_admission,
                            max(
                                admission_retry_delay,
                                self.poll_interval_seconds
                                * min(4, admission_attempt),
                            ),
                        ),
                    )
                    if cancelled:
                        if self.remote_unsettled_jobs > 0:
                            return self._remote_settlement_gate_verdict(
                                task,
                                started=started,
                                candidate_code=code,
                                provenance=provenance,
                            )
                        return self._cancelled_verdict(
                            task,
                            started=started,
                            provenance=provenance,
                            job_id=None,
                            cancellation=None,
                        )
            job_id = _submission_job_identifier(submitted)
            cancel_endpoint = submitted.get("cancel_endpoint")
            response = submitted
            if not job_id:
                self._mark_remote_unsettled()
                safe_response = (
                    _safe_nonterminal_response(
                        response, timeout_max_seconds=self.timeout_seconds
                    )
                    if not _terminal(response)
                    else _safe_response(
                        response, timeout_max_seconds=self.timeout_seconds
                    )
                )
                safe_response.update(
                    {
                        "reason": "remote_settlement_unconfirmed",
                        "remote_settlement_unconfirmed": True,
                    }
                )
                return Verdict(
                    task_id=task.slug,
                    status="REMOTE_SETTLEMENT_UNCONFIRMED",
                    score=0.0,
                    elapsed_seconds=time.monotonic() - started,
                    response=safe_response,
                    error="Judge submission receipt lacked a bindable job id",
                    **provenance,
                )
            if deadline_monotonic is not None:
                settlement_grace = min(5.0, self.settlement_grace_seconds)
                settlement_deadline = deadline_monotonic + settlement_grace
            else:
                settlement_grace = self.settlement_grace_seconds
                submission_observed = time.monotonic()
                lifecycle_budget = _job_lifecycle_budget_seconds(
                    response,
                    execution_timeout=execution_timeout,
                    backend_max_retries=backend_max_retries,
                    maximum_lifecycle_seconds=self.max_lifecycle_seconds,
                )
                settlement_deadline = (
                    submission_observed + lifecycle_budget + settlement_grace
                )
            abandoned_by_client = False
            if job_id and not _terminal(response):
                while time.monotonic() < settlement_deadline:
                    if _cancel_requested(cancel_event):
                        cancellation = self._cancel_submitted_job(
                            job_id,
                            response=response,
                            cancel_endpoint=cancel_endpoint,
                            cancellation_reason=_evaluation_cancel_reason(
                                cancel_event,
                                deadline_monotonic=deadline_monotonic,
                            ),
                            on_settled=settlement_callback,
                        )
                        return self._cancelled_verdict(
                            task,
                            started=started,
                            provenance=provenance,
                            job_id=job_id,
                            cancellation=cancellation,
                        )
                    remaining = settlement_deadline - time.monotonic()
                    wait_ms = max(1, min(1_000, int(remaining * 1_000)))
                    poll_timeout = max(0.1, remaining)
                    if cancel_event is not None:
                        wait_ms = min(wait_ms, _CANCEL_AWARE_LONG_POLL_MS)
                        poll_timeout = min(
                            poll_timeout,
                            _CANCEL_AWARE_HTTP_TIMEOUT_SECONDS,
                        )
                    request_options = {"timeout_seconds": poll_timeout}
                    if cancel_event is not None:
                        request_options["cancel_event"] = cancel_event
                    try:
                        response = self._observed_request(
                            "poll",
                            "GET",
                            f"/api/lean/jobs/{quote(job_id, safe='')}?wait_ms={wait_ms}",
                            task=task,
                            **request_options,
                        )
                    except EvaluatorError as exc:
                        if (
                            exc.category == "request_cancelled"
                            or _cancel_requested(cancel_event)
                        ):
                            cancellation = self._cancel_submitted_job(
                                job_id,
                                response=response,
                                cancel_endpoint=cancel_endpoint,
                                cancellation_reason=_evaluation_cancel_reason(
                                    cancel_event,
                                    deadline_monotonic=deadline_monotonic,
                                ),
                                on_settled=settlement_callback,
                            )
                            return self._cancelled_verdict(
                                task,
                                started=started,
                                provenance=provenance,
                                job_id=job_id,
                                cancellation=cancellation,
                            )
                        last_poll_error = str(exc)
                        if time.monotonic() >= settlement_deadline:
                            break
                        _wait_for_cancel(
                            cancel_event,
                            min(
                                self.poll_interval_seconds,
                                max(0.0, settlement_deadline - time.monotonic()),
                            ),
                        )
                        continue
                    # A successful GET on the job-specific capability binds an
                    # otherwise id-less receipt to the submitted job.  An
                    # explicit contradictory id is never rewritten or scored;
                    # keep polling and, if necessary, reconcile the original
                    # job through the normal fail-closed cancellation path.
                    expected_job_id = sanitize_worker_identifier(job_id)
                    receipt_job_id = _submission_job_identifier(response)
                    if receipt_job_id is None and expected_job_id is not None:
                        response = dict(response)
                        response["job_id"] = expected_job_id
                    elif receipt_job_id != expected_job_id:
                        last_poll_error = "Judge poll receipt job id mismatch"
                        response = {
                            "job_id": expected_job_id,
                            "status": "RUNNING",
                            "reason": "poll_job_id_mismatch",
                        }
                        _wait_for_cancel(
                            cancel_event,
                            min(
                                self.poll_interval_seconds,
                                max(0.0, settlement_deadline - time.monotonic()),
                            ),
                        )
                        continue
                    if response.get("cancel_endpoint") is not None:
                        cancel_endpoint = response.get("cancel_endpoint")
                    if _terminal(response):
                        break
                    if deadline_monotonic is None:
                        # Newer Judge receipts expose the authoritative whole-
                        # job lifecycle budget.  A legacy submit response may
                        # gain it on a later poll, so only ever extend here.
                        lifecycle_budget = _job_lifecycle_budget_seconds(
                            response,
                            execution_timeout=execution_timeout,
                            backend_max_retries=backend_max_retries,
                            maximum_lifecycle_seconds=self.max_lifecycle_seconds,
                        )
                        settlement_deadline = max(
                            settlement_deadline,
                            submission_observed + lifecycle_budget + settlement_grace,
                        )
                    if _wait_for_cancel(
                        cancel_event,
                        min(
                            self.poll_interval_seconds,
                            max(0.0, settlement_deadline - time.monotonic()),
                        ),
                    ):
                        cancellation = self._cancel_submitted_job(
                            job_id,
                            response=response,
                            cancel_endpoint=cancel_endpoint,
                            cancellation_reason=_evaluation_cancel_reason(
                                cancel_event,
                                deadline_monotonic=deadline_monotonic,
                            ),
                            on_settled=settlement_callback,
                        )
                        return self._cancelled_verdict(
                            task,
                            started=started,
                            provenance=provenance,
                            job_id=job_id,
                            cancellation=cancellation,
                        )
            if job_id and not _terminal(response):
                abandoned_by_client = True
                response, cancel_error, cancel_attempted = (
                    self._cancel_and_reconcile_details(
                        job_id,
                        response,
                        cancel_endpoint=cancel_endpoint,
                        cancellation_reason=_evaluation_cancel_reason(
                            cancel_event,
                            deadline_monotonic=deadline_monotonic,
                        ),
                        on_settled=settlement_callback,
                    )
                )
                if cancel_error == "cancel_settlement_deferred":
                    cancellation = {
                        "attempted": cancel_attempted,
                        "succeeded": False,
                        "settled": False,
                        "unconfirmed": False,
                        "deferred": True,
                        "failure_category": cancel_error,
                    }
                    return self._cancelled_verdict(
                        task,
                        started=started,
                        provenance=provenance,
                        job_id=job_id,
                        cancellation=cancellation,
                    )
                if cancel_error:
                    last_poll_error = cancel_error
                    safe_response = _safe_nonterminal_response(
                        response, timeout_max_seconds=self.timeout_seconds
                    )
                    safe_response.update(
                        {
                            "reason": "remote_settlement_unconfirmed",
                            "settlement_error": cancel_error,
                            "remote_settlement_unconfirmed": True,
                        }
                    )
                    return Verdict(
                        task_id=task.slug,
                        status="REMOTE_SETTLEMENT_UNCONFIRMED",
                        score=0.0,
                        elapsed_seconds=time.monotonic() - started,
                        response=safe_response,
                        error="Judge job cancellation did not reach terminal settlement",
                        judge_job_id=sanitize_worker_identifier(job_id),
                        **provenance,
                    )
            if _retryable_admission_rejection(response):
                retried = self._retry_terminal_overload(
                    task,
                    candidate_path,
                    deadline_monotonic=deadline_monotonic,
                    started=started,
                    terminal_overload_retries=terminal_overload_retries,
                    response_profile=response_profile,
                    candidate_code=code,
                    cancel_event=cancel_event,
                    settlement_callback=settlement_callback,
                    timeout_seconds=timeout_seconds,
                )
                if retried is not None:
                    return retried
            normalized_job_id = sanitize_worker_identifier(job_id)
            if abandoned_by_client and _terminal(response):
                abandoned_status, proved, outcome_error = _settled_outcome(response)
                if outcome_error:
                    return Verdict(
                        task_id=task.slug,
                        status="EVALUATOR_ERROR",
                        score=0.0,
                        elapsed_seconds=time.monotonic() - started,
                        response=_safe_nonterminal_response(
                            response, timeout_max_seconds=self.timeout_seconds
                        ),
                        error=outcome_error,
                        judge_job_id=normalized_job_id,
                        **provenance,
                    )
                # A completion racing with DELETE remains authoritative.  A
                # cancellation caused by our own deadline is instead a client
                # lifecycle failure, never an ordinary zero-score verdict.
                if proved or abandoned_status != "CANCELLED":
                    return Verdict(
                        task_id=task.slug,
                        status="PROVED" if proved else abandoned_status,
                        score=1.0 if proved else 0.0,
                        elapsed_seconds=time.monotonic() - started,
                        response=_safe_response(
                            response, timeout_max_seconds=self.timeout_seconds
                        ),
                        judge_job_id=normalized_job_id,
                        **provenance,
                    )
                safe_response = _safe_response(
                    response, timeout_max_seconds=self.timeout_seconds
                )
                safe_response["reason"] = (
                    "run_horizon_elapsed_during_evaluation"
                    if deadline_monotonic is not None
                    else "judge_lifecycle_deadline_elapsed"
                )
                return Verdict(
                    task_id=task.slug,
                    status=(
                        "OUT_OF_HORIZON"
                        if deadline_monotonic is not None
                        else "EVALUATOR_TIMEOUT"
                    ),
                    score=0.0,
                    elapsed_seconds=time.monotonic() - started,
                    response=safe_response,
                    error=(
                        None
                        if deadline_monotonic is not None
                        else "Judge job exceeded its advertised lifecycle budget"
                    ),
                    judge_job_id=normalized_job_id,
                    **provenance,
                )
            if not _terminal(response):
                horizon_elapsed = deadline_monotonic is not None and time.monotonic() >= deadline_monotonic
                safe_response = _safe_nonterminal_response(
                    response, timeout_max_seconds=self.timeout_seconds
                )
                safe_response["reason"] = (
                    "run_horizon_elapsed_during_evaluation"
                    if horizon_elapsed
                    else "judge_settlement_timeout"
                )
                if last_poll_error:
                    safe_response["settlement_error"] = last_poll_error
                return Verdict(
                    task_id=task.slug,
                    status="OUT_OF_HORIZON" if horizon_elapsed else "EVALUATOR_TIMEOUT",
                    score=0.0,
                    elapsed_seconds=time.monotonic() - started,
                    response=safe_response,
                    error=None if horizon_elapsed else "Judge job did not reach a terminal state",
                    judge_job_id=normalized_job_id,
                    **provenance,
                )
            status, proved, outcome_error = _settled_outcome(response)
            if outcome_error:
                return Verdict(
                    task_id=task.slug,
                    status="EVALUATOR_ERROR",
                    score=0.0,
                    elapsed_seconds=time.monotonic() - started,
                    response=_safe_nonterminal_response(
                        response, timeout_max_seconds=self.timeout_seconds
                    ),
                    error=outcome_error,
                    judge_job_id=normalized_job_id,
                    **provenance,
                )
            return Verdict(
                task_id=task.slug,
                status="PROVED" if proved else status,
                score=1.0 if proved else 0.0,
                elapsed_seconds=time.monotonic() - started,
                response=_safe_response(
                    response, timeout_max_seconds=self.timeout_seconds
                ),
                judge_job_id=normalized_job_id,
                **provenance,
            )
        except (OSError, EvaluatorError, UnicodeError) as exc:
            cancellation_summary: dict[str, Any] | None = None
            cancel_error: str | None = None
            if (
                isinstance(exc, EvaluatorError)
                and not isinstance(exc, RemoteSettlementUnconfirmedError)
                and (
                    exc.category == "request_cancelled"
                    or _cancel_requested(cancel_event)
                )
            ):
                if job_id:
                    cancellation_summary = self._cancel_submitted_job(
                        job_id,
                        response=response,
                        cancel_endpoint=cancel_endpoint,
                        cancellation_reason=_evaluation_cancel_reason(
                            cancel_event,
                            deadline_monotonic=deadline_monotonic,
                        ),
                        on_settled=settlement_callback,
                    )
                return self._cancelled_verdict(
                    task,
                    started=started,
                    provenance=provenance,
                    job_id=job_id,
                    cancellation=cancellation_summary,
                )
            if job_id and not _terminal(response):
                response, cancel_error, cancel_attempted = (
                    self._cancel_and_reconcile_details(
                        str(job_id),
                        response,
                        cancel_endpoint=cancel_endpoint,
                        cancellation_reason=_evaluation_cancel_reason(
                            cancel_event,
                            deadline_monotonic=deadline_monotonic,
                        ),
                        on_settled=settlement_callback,
                    )
                )
                if cancel_error == "cancel_settlement_deferred":
                    cancellation_summary = {
                        "attempted": cancel_attempted,
                        "succeeded": False,
                        "settled": False,
                        "unconfirmed": False,
                        "deferred": True,
                        "failure_category": cancel_error,
                    }
                    return self._cancelled_verdict(
                        task,
                        started=started,
                        provenance=provenance,
                        job_id=job_id,
                        cancellation=cancellation_summary,
                    )
                if _retryable_admission_rejection(response):
                    retried = self._retry_terminal_overload(
                        task,
                        candidate_path,
                        deadline_monotonic=deadline_monotonic,
                        started=started,
                        terminal_overload_retries=terminal_overload_retries,
                        response_profile=response_profile,
                        candidate_code=(
                            code if "code" in locals() else candidate_code
                        ),
                        cancel_event=cancel_event,
                        settlement_callback=settlement_callback,
                        timeout_seconds=timeout_seconds,
                    )
                    if retried is not None:
                        return retried
                reconciled_status, proved, outcome_error = _settled_outcome(response)
                if (
                    _terminal(response)
                    and reconciled_status != "CANCELLED"
                    and outcome_error is None
                ):
                    return Verdict(
                        task_id=task.slug,
                        status="PROVED" if proved else reconciled_status,
                        score=1.0 if proved else 0.0,
                        elapsed_seconds=time.monotonic() - started,
                        response=_safe_response(
                            response, timeout_max_seconds=self.timeout_seconds
                        ),
                        judge_job_id=sanitize_worker_identifier(job_id),
                        **provenance,
                    )
                if cancel_error:
                    response = {**response, "settlement_error": cancel_error}
            if (
                isinstance(exc, RemoteSettlementUnconfirmedError)
                and exc.submission_response
            ):
                response = dict(exc.submission_response)
            safe_error_response = (
                _safe_nonterminal_response(
                    response, timeout_max_seconds=self.timeout_seconds
                )
                if _verdict_status(response) in _NONTERMINAL_STATUSES
                or not _terminal(response)
                else _safe_response(
                    response, timeout_max_seconds=self.timeout_seconds
                )
            )
            if isinstance(exc, EvaluatorError):
                safe_error_response["evaluator_failure"] = exc.public_details()
            if cancel_error:
                safe_error_response.update(
                    {
                        "reason": "remote_settlement_unconfirmed",
                        "settlement_error": cancel_error,
                        "remote_settlement_unconfirmed": True,
                    }
                )
                failure_status = "REMOTE_SETTLEMENT_UNCONFIRMED"
            elif isinstance(exc, RemoteSettlementUnconfirmedError):
                safe_error_response.update(
                    {
                        "reason": "remote_settlement_unconfirmed",
                        "remote_settlement_unconfirmed": True,
                    }
                )
                failure_status = "REMOTE_SETTLEMENT_UNCONFIRMED"
            elif (
                isinstance(exc, EvaluatorError)
                and exc.category == "request_deadline_elapsed"
                and deadline_monotonic is not None
                and time.monotonic() >= deadline_monotonic
            ):
                failure_status = "OUT_OF_HORIZON"
            elif isinstance(exc, EvaluatorError):
                failure_status = {
                    "judge_overloaded_deadline": "REJECTED_OVERLOADED",
                    "network_error": "NETWORK_ERROR",
                    "request_deadline_elapsed": "EVALUATOR_TIMEOUT",
                }.get(exc.category, "EVALUATOR_ERROR")
            else:
                failure_status = "EVALUATOR_ERROR"
            return Verdict(
                task_id=task.slug,
                status=failure_status,
                score=0.0,
                elapsed_seconds=time.monotonic() - started,
                response=safe_error_response,
                error=sanitize_worker_text(exc),
                judge_job_id=sanitize_worker_identifier(job_id),
                **provenance,
            )

    def _retry_terminal_overload(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None,
        started: float,
        terminal_overload_retries: int,
        response_profile: str | None,
        candidate_code: str | None,
        cancel_event: threading.Event | None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
    ) -> Verdict | None:
        """Resubmit once a previous job is definitively terminal and retryable."""

        if terminal_overload_retries <= 0:
            return None
        if (
            deadline_monotonic is not None
            and time.monotonic() >= deadline_monotonic
        ):
            return None
        verdict = self._evaluate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            started=started,
            terminal_overload_retries=terminal_overload_retries - 1,
            response_profile=response_profile,
            candidate_code=candidate_code,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
        )
        prior = verdict.response.get("evaluator_overload_resubmissions", 0)
        verdict.response["evaluator_overload_resubmissions"] = (
            int(prior) + 1 if isinstance(prior, int) else 1
        )
        return verdict

    def _cancel_and_reconcile(
        self,
        job_id: str,
        response: Mapping[str, Any],
        *,
        cancel_endpoint: Any = None,
        cancellation_reason: str | None = None,
        on_settled: Any | None = None,
    ) -> tuple[dict[str, Any], str | None]:
        """Boundedly cancel an abandoned job and recover its terminal receipt."""

        current, error, _attempted = self._cancel_and_reconcile_details(
            job_id,
            response,
            cancel_endpoint=cancel_endpoint,
            cancellation_reason=cancellation_reason,
            on_settled=on_settled,
        )
        return current, error

    def _cancel_and_reconcile_details(
        self,
        job_id: Any,
        response: Mapping[str, Any],
        *,
        cancel_endpoint: Any = None,
        cancellation_reason: str | None = None,
        on_settled: Any | None = None,
    ) -> tuple[dict[str, Any], str | None, bool]:
        """Cancel and confirm terminal settlement within one absolute deadline."""

        current = dict(response)
        last_nonterminal = dict(response)
        deadline = time.monotonic() + self.cancel_grace_seconds
        cancel_path, _ = self._cancel_request_path(
            job_id,
            cancel_endpoint=cancel_endpoint,
        )
        if cancel_path is None and cancel_endpoint is not None:
            cancel_path, _ = self._cancel_request_path(
                job_id,
                cancel_endpoint=None,
            )
        attempted = False
        retryable_cancel_observed = _retryable_known_cancellation(current)
        if cancel_path is not None:
            remaining = deadline - time.monotonic()
            attempted = remaining > 0
            try:
                if attempted:
                    current = self._observed_request(
                        "cancel",
                        "DELETE",
                        cancel_path,
                        timeout_seconds=min(
                            _JUDGE_CANCEL_TIMEOUT_SECONDS,
                            remaining,
                        ),
                    )
                    retryable_cancel_observed = (
                        retryable_cancel_observed
                        or _retryable_known_cancellation(current)
                    )
            except EvaluatorError:
                # A transport acknowledgement is not settlement.  Continue
                # through the same bounded reconciliation window whenever a
                # status endpoint is available.
                current = dict(response)
        if self._authoritative_terminal_receipt(current, job_id):
            return current, None, attempted
        if self._job_bound_receipt(current, job_id) and not _terminal(current):
            last_nonterminal = current

        poll_paths = self._settlement_poll_paths(
            job_id,
            current,
            response,
            cancel_endpoint=cancel_endpoint,
        )
        poll_index = 0
        while poll_paths and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            wait_ms = max(1, min(500, int(remaining * 1_000)))
            poll_path = poll_paths[poll_index % len(poll_paths)]
            separator = "&" if "?" in poll_path else "?"
            try:
                current = self._observed_request(
                    "reconcile_poll",
                    "GET",
                    f"{poll_path}{separator}wait_ms={wait_ms}",
                    timeout_seconds=remaining,
                )
            except EvaluatorError:
                poll_index += 1
            else:
                if self._authoritative_terminal_receipt(current, job_id):
                    return current, None, attempted
                if (
                    self._job_bound_receipt(current, job_id)
                    and not _terminal(current)
                ):
                    last_nonterminal = current
                    retryable_cancel_observed = (
                        retryable_cancel_observed
                        or _retryable_known_cancellation(current)
                    )
            # Every unsuccessful reconcile attempt yields; malformed terminal
            # receipts and repeated transport failures must not tight-loop.
            time.sleep(
                min(
                    self.poll_interval_seconds,
                    max(0.0, deadline - time.monotonic()),
                )
            )
        # A runner-owned cancellation is candidate-independent even when the
        # Judge does not echo the newer ``retryable``/disposition marker.  In
        # particular, the elastic runner uses ``full_score`` to stop all
        # remaining slots and may also propagate ``runner_failure`` or the
        # process settlement latch through its OR-cancel view.  Once DELETE
        # was attempted and the receipt is bound to this exact job, retaining
        # the permit behind a bounded watcher is safe: the watcher accepts a
        # terminal receipt only for this identity.  Unknown identities still
        # take the fail-closed path below.
        if attempted and (
            cancellation_reason
            in {
                "task_solved_by_peer",
                "broker_revoked",
                "horizon_elapsed",
                "full_score",
                "runner_failure",
                "remote_settlement_unconfirmed",
                "cancelled",
            }
            or retryable_cancel_observed
        ):
            # The submission identity is known and a DELETE was attempted, but
            # the terminal receipt may lag the foreground grace window.  Judge
            # routers expose this state as retryable/cancel-requested while a
            # worker is resetting.  A fixed experiment horizon is another
            # candidate-independent cancellation source: the runner issued
            # DELETE deliberately and the Judge may publish its receipt just
            # after the short foreground grace window.  Keep the job's
            # capacity permit retained by the caller and let a bounded watcher
            # release it only after a job-bound receipt.  This prevents an
            # ordinary, recoverable cancellation from latching the whole arm
            # as infrastructure failure.  Unknown identities and
            # non-retryable malformed receipts still use the fail-closed path
            # below.
            deferred = self._start_settlement_watcher(
                job_id,
                last_nonterminal,
                cancel_endpoint=cancel_endpoint,
                on_settled=on_settled,
            )
            if deferred:
                return last_nonterminal, "cancel_settlement_deferred", attempted
        self._mark_remote_unsettled()
        return last_nonterminal, "cancel_settlement_unconfirmed", attempted

    def _settlement_poll_paths(
        self,
        job_id: Any,
        *receipts: Mapping[str, Any],
        cancel_endpoint: Any = None,
    ) -> list[str]:
        """Resolve same-origin receipt capabilities in authoritative order."""

        raw_endpoints: list[Any] = []
        for receipt in receipts:
            for key in ("status_endpoint", "cancel_endpoint"):
                endpoint = _nested_value(receipt, key)
                if endpoint is not None:
                    raw_endpoints.append(endpoint)
        if cancel_endpoint is not None:
            raw_endpoints.append(cancel_endpoint)
        paths: list[str] = []
        for endpoint in raw_endpoints:
            path, _ = self._cancel_request_path(
                job_id,
                cancel_endpoint=endpoint,
            )
            if path is not None and path not in paths:
                paths.append(path)
        fallback, _ = self._cancel_request_path(job_id, cancel_endpoint=None)
        if fallback is not None and fallback not in paths:
            paths.append(fallback)
        return paths

    @staticmethod
    def _authoritative_terminal_receipt(
        response: Mapping[str, Any],
        job_id: Any,
    ) -> bool:
        """Require a terminal lifecycle receipt bound to the submitted job."""

        return _terminal(response) and LeanEvaluator._job_bound_receipt(
            response,
            job_id,
        )

    @staticmethod
    def _job_bound_receipt(
        response: Mapping[str, Any],
        job_id: Any,
    ) -> bool:
        """Reject receipts that omit or contradict the submitted job id."""

        expected_job_id = sanitize_worker_identifier(job_id)
        receipt_job_id = sanitize_worker_identifier(
            _nested_value(response, "job_id") or _nested_value(response, "id")
        )
        return (
            expected_job_id is not None
            and receipt_job_id is not None
            and receipt_job_id == expected_job_id
        )

    def _cancel_submitted_job(
        self,
        job_id: Any,
        *,
        response: Mapping[str, Any] | None = None,
        cancel_endpoint: Any = None,
        cancellation_reason: str | None = None,
        on_settled: Any | None = None,
    ) -> dict[str, Any]:
        """Cancel a job, reporting success only after terminal settlement."""

        normalized_job_id = sanitize_worker_identifier(job_id)
        if normalized_job_id is None:
            return {
                "attempted": False,
                "succeeded": False,
                "settled": False,
                "unconfirmed": True,
                "failure_category": "cancel_settlement_unconfirmed",
            }
        current, error, attempted = self._cancel_and_reconcile_details(
            normalized_job_id,
            response or {"job_id": normalized_job_id, "status": "running"},
            cancel_endpoint=cancel_endpoint,
            cancellation_reason=cancellation_reason,
            on_settled=on_settled,
        )
        settled = self._authoritative_terminal_receipt(current, normalized_job_id)
        summary = {
            "attempted": attempted,
            "succeeded": attempted and settled,
            "settled": settled,
            "unconfirmed": not settled and error != "cancel_settlement_deferred",
            "failure_category": (
                None if settled else (error or "cancel_settlement_unconfirmed")
            ),
        }
        if error == "cancel_settlement_deferred":
            summary["deferred"] = True
        return summary

    def _cancel_request_path(
        self,
        job_id: Any,
        *,
        cancel_endpoint: Any,
    ) -> tuple[str | None, str]:
        """Use a Judge-provided same-origin cancel capability when present."""

        if cancel_endpoint is not None:
            if not isinstance(cancel_endpoint, str) or not cancel_endpoint.strip():
                return None, "invalid_cancel_endpoint"
            raw_endpoint = cancel_endpoint.strip()
            try:
                endpoint_size = len(raw_endpoint.encode("utf-8"))
            except UnicodeError:
                return None, "invalid_cancel_endpoint"
            if endpoint_size > 2_048:
                return None, "invalid_cancel_endpoint"
            try:
                parsed = urlsplit(raw_endpoint)
                base = urlsplit(self.base_url)
            except ValueError:
                return None, "invalid_cancel_endpoint"
            if parsed.fragment or not parsed.path.startswith("/"):
                return None, "invalid_cancel_endpoint"
            if parsed.scheme or parsed.netloc:
                if (
                    parsed.scheme.casefold() != base.scheme.casefold()
                    or parsed.netloc.casefold() != base.netloc.casefold()
                ):
                    return None, "invalid_cancel_endpoint"
            path = parsed.path
            if parsed.query:
                path += f"?{parsed.query}"
            return path, "invalid_cancel_endpoint"

        normalized_job_id = sanitize_worker_identifier(job_id)
        if normalized_job_id is None:
            return None, "invalid_job_identifier"
        return (
            f"/api/lean/jobs/{quote(normalized_job_id, safe='')}",
            "invalid_job_identifier",
        )

    @staticmethod
    def _cancelled_verdict(
        task: Task,
        *,
        started: float,
        provenance: Mapping[str, Any],
        job_id: Any,
        cancellation: Mapping[str, Any] | None,
    ) -> Verdict:
        response: dict[str, Any] = {"reason": "cancel_event_set"}
        if cancellation is not None:
            response["judge_cancellation"] = dict(cancellation)
        return Verdict(
            task_id=task.slug,
            status="TASK_CANCELLED",
            score=0.0,
            elapsed_seconds=max(0.0, time.monotonic() - started),
            response=response,
            judge_job_id=sanitize_worker_identifier(job_id),
            candidate_sha256=provenance.get("candidate_sha256"),
            task_contract_sha256=provenance.get("task_contract_sha256"),
        )

    def _probe_cache_key(self, task: Task, code: str) -> str:
        digest = hashlib.sha256()
        digest.update(self.expected_task_contract_sha256(task).encode("ascii"))
        digest.update(b"\0")
        digest.update(candidate_sha256(code).encode("ascii"))
        return digest.hexdigest()


class CodingEvaluator(LeanEvaluator):
    """Async ContextSwarmJudge adapter for C++ contest bundles.

    The coding Judge deliberately has a separate adapter instead of trying to
    coerce an OJ receipt into the Lean schema.  It nevertheless exposes the
    same small runner protocol as :class:`LeanEvaluator`, so Mono, Parallel,
    broker checkpointing, and feedback-free closeout all remain identical.
    """

    is_mock_evaluator = False

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: int = 300,
        max_lifecycle_seconds: float = 3_600.0,
        verification_profile: str = "coding_contest",
        judge_mode: str = "coding",
        poll_interval_seconds: float = 0.25,
        cancel_grace_seconds: float = 5.0,
        backend_max_retries: int = 1,
        require_result_cache_disabled: bool = False,
        profiler: Any | None = None,
    ):
        super().__init__(
            normalize_base_url(base_url),
            lean_env_id="coding",
            timeout_seconds=timeout_seconds,
            max_lifecycle_seconds=max_lifecycle_seconds,
            verification_profile=verification_profile,
            judge_mode=judge_mode,
            poll_interval_seconds=poll_interval_seconds,
            cancel_grace_seconds=cancel_grace_seconds,
            # Coding admission has no Lean overload replay path.  The runner
            # handles retry/refill at the candidate-attempt level.
            admission_retry_seconds=min(30.0, max(0.1, float(cancel_grace_seconds))),
            backend_max_retries=backend_max_retries,
            terminal_overload_retries=0,
            profiler=profiler,
        )
        # Keep this policy on the adapter rather than putting it in candidate
        # JSON.  The Judge's dispatch contract treats cache mode as an
        # out-of-band, immutable per-job capability.
        self.require_result_cache_disabled = bool(require_result_cache_disabled)

    def expected_task_contract_sha256(self, task: Task) -> str:
        digest = hashlib.sha256()
        for value in (
            task.slug,
            task.problem_id,
            task.language,
            task.candidate_filename,
            task.problem_text,
            task.baseline_code,
            self.verification_profile,
            self.judge_mode,
        ):
            digest.update(str(value).encode("utf-8"))
            digest.update(b"\0")
        return digest.hexdigest()

    def _cancel_request_path(
        self,
        job_id: Any,
        *,
        cancel_endpoint: Any,
    ) -> tuple[str | None, str]:
        """Resolve coding Judge cancellation/status capabilities same-origin."""

        if cancel_endpoint is not None:
            if not isinstance(cancel_endpoint, str) or not cancel_endpoint.strip():
                return None, "invalid_cancel_endpoint"
            raw_endpoint = cancel_endpoint.strip()
            try:
                parsed = urlsplit(raw_endpoint)
                base = urlsplit(self.base_url)
                if parsed.fragment or not parsed.path.startswith("/"):
                    return None, "invalid_cancel_endpoint"
                if parsed.scheme or parsed.netloc:
                    if (
                        parsed.scheme.casefold() != base.scheme.casefold()
                        or parsed.netloc.casefold() != base.netloc.casefold()
                    ):
                        return None, "invalid_cancel_endpoint"
                path = parsed.path
                if parsed.query:
                    path += f"?{parsed.query}"
                return path, "ok"
            except ValueError:
                return None, "invalid_cancel_endpoint"
        normalized = sanitize_worker_identifier(job_id)
        if normalized is None:
            return None, "invalid_job_identifier"
        return f"/api/judge/jobs/{quote(normalized, safe='')}", "ok"

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/healthz", timeout_seconds=10.0)

    def evaluate(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: Any | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        started = time.monotonic()
        source = _read_candidate(candidate_path)
        if timeout_seconds is None:
            return self._evaluate_code(
                task,
                source,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                settlement_callback=settlement_callback,
                timeout_seconds=None,
            )
        return self._evaluate_with_total_budget(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            started=started,
            terminal_overload_retries=0,
            response_profile=None,
            candidate_code=source,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
            attempt_runner=lambda attempt_deadline, attempt_timeout: self._evaluate_code(
                task,
                source,
                deadline_monotonic=attempt_deadline,
                cancel_event=cancel_event,
                settlement_callback=settlement_callback,
                timeout_seconds=attempt_timeout,
            ),
        )

    def evaluate_fresh(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: Any | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        return self.evaluate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )

    def probe(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: Any | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        return self.evaluate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: Any | None = None,
        settlement_callback: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        if not isinstance(candidate_code, str):
            raise TypeError("candidate_code must be a string")
        if timeout_seconds is None:
            return self._evaluate_code(
                task,
                candidate_code,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                settlement_callback=settlement_callback,
                timeout_seconds=None,
            )
        started = time.monotonic()
        return self._evaluate_with_total_budget(
            task,
            Path("<broker-candidate-snapshot>"),
            deadline_monotonic=deadline_monotonic,
            started=started,
            terminal_overload_retries=0,
            response_profile=None,
            candidate_code=candidate_code,
            cancel_event=cancel_event,
            settlement_callback=settlement_callback,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
            attempt_runner=lambda attempt_deadline, attempt_timeout: self._evaluate_code(
                task,
                candidate_code,
                deadline_monotonic=attempt_deadline,
                cancel_event=cancel_event,
                settlement_callback=settlement_callback,
                timeout_seconds=attempt_timeout,
            ),
        )

    def _request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
        *,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = (
            json.dumps(dict(payload), ensure_ascii=False).encode("utf-8")
            if payload is not None
            else None
        )
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if (
            (
                self.require_result_cache_disabled
                or getattr(self._dispatch_context, "cache_mode", None)
                == _CACHE_MODE_DISABLED
            )
            and method.upper() == "POST"
            and path == "/api/judge/jobs"
        ):
            headers[_DISPATCH_CACHE_MODE_HEADER] = _CACHE_MODE_DISABLED
        try:
            request = Request(url, data=data, headers=headers, method=method)
            with urlopen(request, timeout=max(0.1, float(timeout_seconds))) as response:
                raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
        except HTTPError as exc:
            try:
                body = exc.read(64 * 1024).decode("utf-8", errors="replace")
            except Exception:
                body = ""
            raise EvaluatorError(
                "The coding Judge rejected the request.",
                category="http_error",
                http_status=int(exc.code),
            ) from None
        except (URLError, TimeoutError, OSError, HTTPException, UnicodeError, json.JSONDecodeError):
            raise EvaluatorError(
                "The coding Judge transport failed.",
                category="network_error",
            ) from None
        if not isinstance(parsed, dict):
            raise EvaluatorError(
                "The coding Judge returned an invalid response.",
                category="malformed_response",
            )
        return parsed

    @staticmethod
    def _terminal(payload: Mapping[str, Any]) -> bool:
        status = str(payload.get("status") or payload.get("job_status") or "").lower()
        return bool(payload.get("terminal") is True or status in {"succeeded", "failed", "cancelled"})

    @staticmethod
    def _response_status(payload: Mapping[str, Any]) -> str:
        response = payload.get("response")
        if isinstance(response, Mapping):
            submission = response.get("submission")
            if isinstance(submission, Mapping) and submission.get("status"):
                return str(submission["status"]).strip().upper()
            summary = response.get("summary")
            if isinstance(summary, Mapping) and summary.get("status"):
                return str(summary["status"]).strip().upper()
            for key in ("status", "verdict", "result"):
                if response.get(key):
                    return str(response[key]).strip().upper()
        error = payload.get("error")
        if isinstance(error, Mapping) and error.get("type"):
            return str(error["type"]).strip().upper()
        return str(payload.get("status") or "UNKNOWN").strip().upper()

    def _evaluate_code(
        self,
        task: Task,
        code: str | None,
        *,
        deadline_monotonic: float | None,
        cancel_event: Any | None,
        settlement_callback: Any | None,
        timeout_seconds: int | None,
    ) -> Verdict:
        started = time.monotonic()
        source = code or ""
        provenance = {
            "candidate_sha256": candidate_sha256(source),
            "task_contract_sha256": self.expected_task_contract_sha256(task),
        }
        if not source.strip():
            return Verdict(task.slug, "LOCAL_REJECTED", 0.0, 0.0, {"reason": "empty candidate"}, **provenance)
        if self.remote_unsettled_jobs > 0:
            return Verdict(task.slug, "REMOTE_SETTLEMENT_UNCONFIRMED", 0.0, 0.0, {"remote_settlement_unconfirmed": True}, **provenance)
        if cancel_event is not None and cancel_event.is_set():
            return Verdict(task.slug, "TASK_CANCELLED", 0.0, 0.0, {"reason": "cancel_event_set"}, **provenance)
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            return Verdict(task.slug, "OUT_OF_HORIZON", 0.0, 0.0, {"reason": "run_horizon_elapsed"}, **provenance)
        agent_timeout = self._normalize_agent_timeout(timeout_seconds)
        execution_timeout = (
            agent_timeout.effective_seconds
            if agent_timeout is not None
            else self.timeout_seconds
        )
        job_id: str | None = None
        cancel_endpoint: Any = None
        response: dict[str, Any] = {}

        def reconcile_cancel(reason: str) -> tuple[dict[str, Any], str | None, bool]:
            """Issue DELETE, then account for the job until a bound receipt."""

            current, error, attempted = self._cancel_and_reconcile_details(
                job_id,
                response,
                cancel_endpoint=cancel_endpoint,
                # The coding runner may cancel a peer or the horizon.  In
                # either case the identity is known, so retain the permit in
                # the bounded watcher path rather than latching before the
                # receipt has had a chance to settle.
                cancellation_reason=reason,
                on_settled=settlement_callback,
            )
            return current, error, attempted

        try:
            submission_payload: dict[str, Any] = {
                "problem_id": task.problem_id,
                "language": task.language,
                "code": source,
                "submission_id": f"contextswarm-{uuid.uuid4().hex}",
            }
            if agent_timeout is not None:
                remaining_budget = (
                    deadline_monotonic - time.monotonic()
                    if deadline_monotonic is not None
                    else None
                )
                if remaining_budget is not None:
                    if remaining_budget < 1.0:
                        return Verdict(
                            task.slug,
                            "OUT_OF_HORIZON",
                            0.0,
                            time.monotonic() - started,
                            {"reason": "agent_total_timeout_elapsed_before_submission"},
                            **provenance,
                        )
                    execution_timeout = min(
                        execution_timeout,
                        max(1, int(math.ceil(remaining_budget))),
                    )
                submission_payload["timeout"] = execution_timeout
            submitted = self._observed_request(
                "submit",
                "POST",
                "/api/judge/jobs",
                submission_payload,
                task=task,
                timeout_seconds=min(30.0, max(1.0, (deadline_monotonic - time.monotonic()) if deadline_monotonic else 30.0)),
            )
            raw_job_id = submitted.get("job_id") or submitted.get("id")
            job_id = sanitize_worker_identifier(raw_job_id)
            if not job_id:
                self._mark_remote_unsettled()
                return Verdict(task.slug, "REMOTE_SETTLEMENT_UNCONFIRMED", 0.0, time.monotonic() - started, {"reason": "missing_job_identifier", "remote_settlement_unconfirmed": True}, **provenance)
            response = submitted
            cancel_endpoint = submitted.get("cancel_endpoint") or submitted.get(
                "status_endpoint"
            )
            while not self._terminal(response):
                if cancel_event is not None and cancel_event.is_set():
                    response, cancel_error, attempted = reconcile_cancel(
                        _cancel_reason(cancel_event) or "cancelled"
                    )
                    if cancel_error == "cancel_settlement_deferred":
                        return Verdict(
                            task.slug,
                            "TASK_CANCELLED",
                            0.0,
                            time.monotonic() - started,
                            {
                                "reason": "cancel_settlement_deferred",
                                "judge_cancellation": {
                                    "attempted": attempted,
                                    "settled": False,
                                    "deferred": True,
                                },
                            },
                            judge_job_id=job_id,
                            **provenance,
                        )
                    if cancel_error is not None:
                        return Verdict(
                            task.slug,
                            "REMOTE_SETTLEMENT_UNCONFIRMED",
                            0.0,
                            time.monotonic() - started,
                            {
                                "reason": "cancel_settlement_unconfirmed",
                                "remote_settlement_unconfirmed": True,
                            },
                            judge_job_id=job_id,
                            **provenance,
                        )
                    break
                remaining = (deadline_monotonic - time.monotonic()) if deadline_monotonic is not None else self.max_lifecycle_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    response, cancel_error, attempted = reconcile_cancel("horizon_elapsed")
                    if cancel_error == "cancel_settlement_deferred":
                        return Verdict(
                            task.slug,
                            "OUT_OF_HORIZON",
                            0.0,
                            time.monotonic() - started,
                            {
                                "reason": "cancel_settlement_deferred",
                                "judge_cancellation": {
                                    "attempted": attempted,
                                    "settled": False,
                                    "deferred": True,
                                },
                            },
                            judge_job_id=job_id,
                            **provenance,
                        )
                    if cancel_error is not None:
                        return Verdict(
                            task.slug,
                            "REMOTE_SETTLEMENT_UNCONFIRMED",
                            0.0,
                            time.monotonic() - started,
                            {
                                "reason": "cancel_settlement_unconfirmed",
                                "remote_settlement_unconfirmed": True,
                            },
                            judge_job_id=job_id,
                            **provenance,
                        )
                    break
                wait_ms = min(1000, max(1, int(min(remaining, 1.0) * 1000)))
                response = self._observed_request(
                    "poll",
                    "GET",
                    f"/api/judge/jobs/{quote(job_id, safe='')}?wait_ms={wait_ms}",
                    task=task,
                    timeout_seconds=max(1.0, min(30.0, remaining)),
                )
                if response.get("cancel_endpoint") or response.get("status_endpoint"):
                    cancel_endpoint = response.get("cancel_endpoint") or response.get("status_endpoint")
            status = self._response_status(response)
            nested = response.get("response") if isinstance(response.get("response"), Mapping) else {}
            safe = safe_worker_response(
                nested if nested else response,
                timeout_max_seconds=self.timeout_seconds,
            )
            safe["job_status"] = str(response.get("status") or response.get("job_status") or "")[:64]
            safe["judge_job_id"] = job_id
            # Preserve Judge-side cache provenance in the worker-safe receipt.
            # This is separate from the adapter's local probe cache flag and
            # lets closeout/audit reject a supposedly independent run that was
            # actually served by a completed-result or singleflight cache.
            judge_cache_hit = bool(
                safe.get("judge_cache_hit") is True
                or safe.get("judge_cache_status") in {"hit", "singleflight_wait"}
                or safe.get("cache_reused") is True
            )
            if status == "AC":
                return Verdict(task.slug, "PROVED", 1.0, time.monotonic() - started, safe, judge_job_id=job_id, cache_reused=judge_cache_hit, **provenance)
            if status in {"WA", "PE", "CE", "MLE", "TLE", "RE"}:
                return Verdict(task.slug, status, 0.0, time.monotonic() - started, safe, judge_job_id=job_id, cache_reused=judge_cache_hit, **provenance)
            return Verdict(task.slug, "EVALUATOR_ERROR", 0.0, time.monotonic() - started, safe, error="coding Judge returned no terminal submission verdict", judge_job_id=job_id, cache_reused=judge_cache_hit, **provenance)
        except EvaluatorError as exc:
            return Verdict(task.slug, "EVALUATOR_ERROR", 0.0, time.monotonic() - started, {"evaluator_failure": exc.public_details()}, error="coding Judge transport failed", judge_job_id=job_id, **provenance)


class MockEvaluator:
    """Offline smoke evaluator; never represents a paper score."""

    is_mock_evaluator = True

    def __init__(self, *, prove_without_sorry: bool = False):
        self.prove_without_sorry = prove_without_sorry

    def health(self) -> dict[str, Any]:
        return {"ok": True, "mock": True}

    def expected_task_contract_sha256(self, task: Task) -> str:
        return task_contract_sha256(
            task,
            lean_env_id="mock",
            verification_profile="mock",
            judge_mode="mock",
        )

    def evaluate(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: Any | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        del (
            deadline_monotonic,
            timeout_seconds,
            timeout_deadline_monotonic,
            retry_admission_callback,
        )
        if _cancel_requested(cancel_event):
            return Verdict(
                task.slug,
                "TASK_CANCELLED",
                0.0,
                0.0,
                task_contract_sha256=self.expected_task_contract_sha256(task),
            )
        try:
            code = candidate_path.read_text(encoding="utf-8")
        except OSError as exc:
            return Verdict(
                task.slug,
                "MISSING_CANDIDATE",
                0.0,
                0.0,
                error=sanitize_worker_text(exc),
                task_contract_sha256=self.expected_task_contract_sha256(task),
            )
        proved = self.prove_without_sorry and "sorry" not in code and "admit" not in code
        return Verdict(
            task.slug,
            "PROVED" if proved else "MOCK_SKIPPED",
            1.0 if proved else 0.0,
            0.0,
            {"mock": True},
            candidate_sha256=candidate_sha256(code),
            task_contract_sha256=self.expected_task_contract_sha256(task),
        )

    def probe(
        self,
        task: Task,
        candidate_path: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        if _cancel_requested(cancel_event):
            return Verdict(
                task.slug,
                "TASK_CANCELLED",
                0.0,
                0.0,
                task_contract_sha256=self.expected_task_contract_sha256(task),
            )
        return self.evaluate(
            task,
            candidate_path,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            timeout_seconds=timeout_seconds,
            timeout_deadline_monotonic=timeout_deadline_monotonic,
            retry_admission_callback=retry_admission_callback,
        )

    def probe_source(
        self,
        task: Task,
        candidate_code: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
        timeout_seconds: int | None = None,
        timeout_deadline_monotonic: float | None = None,
        retry_admission_callback: Callable[[], bool] | None = None,
    ) -> Verdict:
        del (
            deadline_monotonic,
            timeout_seconds,
            timeout_deadline_monotonic,
            retry_admission_callback,
        )
        provenance = {
            "candidate_sha256": candidate_sha256(candidate_code),
            "task_contract_sha256": self.expected_task_contract_sha256(task),
        }
        if _cancel_requested(cancel_event):
            return Verdict(task.slug, "TASK_CANCELLED", 0.0, 0.0, **provenance)
        proved = (
            self.prove_without_sorry
            and "sorry" not in candidate_code
            and "admit" not in candidate_code
        )
        return Verdict(
            task.slug,
            "PROVED" if proved else "MOCK_SKIPPED",
            1.0 if proved else 0.0,
            0.0,
            {"mock": True},
            **provenance,
        )


def _status(payload: Mapping[str, Any]) -> str:
    for key in ("formal_status", "verdict", "result"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return _safe_status(value)
    canonical = payload.get("canonical_verdict")
    if isinstance(canonical, Mapping):
        value = canonical.get("status")
        if isinstance(value, str) and value.strip():
            return _safe_status(value)
    value = payload.get("status")
    if isinstance(value, str) and value.strip():
        return _safe_status(value)
    nested = payload.get("response")
    if isinstance(nested, Mapping):
        return _status(nested)
    return "UNKNOWN"


def _nested_value(payload: Mapping[str, Any], key: str) -> Any:
    value = payload.get(key)
    if value is not None:
        return value
    nested = payload.get("response")
    if isinstance(nested, Mapping):
        return _nested_value(nested, key)
    canonical = payload.get("canonical_verdict")
    if isinstance(canonical, Mapping):
        return canonical.get(key)
    return None


def _retryable_known_cancellation(payload: Mapping[str, Any]) -> bool:
    """Return whether a bound nonterminal cancellation may settle later.

    During DELETE, the ICLR router can return a job-bound ``running`` or
    ``cancel_requested`` snapshot while the worker REPL is resetting.  Newer
    receipts mark that state ``retryable``; the router also exposes a
    cancellation disposition or same-origin status capability.  Requiring an
    explicit signal (rather than merely seeing a nonterminal status) keeps
    genuinely unknown/never-settling jobs fail-closed.
    """

    if not isinstance(payload, Mapping):
        return False
    retryable = _nested_value(payload, "retryable")
    if retryable is True:
        return True
    disposition = _nested_value(payload, "router_cancel_disposition")
    if isinstance(disposition, str) and disposition.strip().lower() in {
        "cancel_requested",
        "cancel_unconfirmed",
    }:
        return True
    # ``cancel_requested`` alone is deliberately insufficient: a legacy or
    # test endpoint may expose that bit while losing the job ledger entirely.
    # The router's explicit disposition/retryable marker above is the
    # candidate-independent evidence needed to defer settlement safely.
    return False


def _raw_lifecycle_status(payload: Mapping[str, Any]) -> str:
    """Read the transport/job lifecycle status, including wrapped receipts."""

    value = payload.get("status")
    if isinstance(value, str) and value.strip():
        return _safe_status(value)
    nested = payload.get("response")
    if isinstance(nested, Mapping):
        return _raw_lifecycle_status(nested)
    return ""


def _submission_job_identifier(payload: Any) -> str | None:
    """Return the opaque identity needed to settle an admitted submission."""

    if not isinstance(payload, Mapping):
        return None
    raw = _nested_value(payload, "job_id") or _nested_value(payload, "id")
    if isinstance(raw, bool):
        return None
    return sanitize_worker_identifier(raw)


def _bindable_terminal_job_receipt(payload: Any) -> bool:
    return (
        isinstance(payload, Mapping)
        and _submission_job_identifier(payload) is not None
        and _terminal(payload)
    )


def _confirmed_pre_admission_rejection(payload: Mapping[str, Any]) -> bool:
    """Recognize only receipts which prove no remote job was admitted."""

    if _retryable_admission_rejection(payload):
        return True
    if _submission_job_identifier(payload) is not None:
        return False
    error_code = str(payload.get("error") or "").strip().lower()
    error_message = str(payload.get("message") or "").strip().lower()
    error_text = f"{error_code} {error_message}".strip()
    if error_code in {
        "admission_capacity_exceeded",
        "permit_unavailable",
    }:
        return True
    return (
        payload.get("ok") is False
        and (
            "overload" in error_text
            or (
                "queue" in error_text
                and any(word in error_text for word in ("full", "capacity"))
            )
            or ("ingress" in error_text and "capacity" in error_text)
        )
    )


def _retryable_admission_rejection(payload: Mapping[str, Any]) -> bool:
    """Return true only for a terminal overload that created no live work."""

    return (
        _raw_lifecycle_status(payload) == "REJECTED_OVERLOADED"
        and _nested_value(payload, "retryable") is True
        and _terminal(payload)
    )


def _job_lifecycle_budget_seconds(
    payload: Mapping[str, Any],
    *,
    execution_timeout: int,
    backend_max_retries: int = 1,
    maximum_lifecycle_seconds: float,
) -> float:
    """Return a conservative whole-job budget from the Judge receipt.

    ``timeout`` is per backend command, not submission-to-terminal wall time.
    A formal cache miss may legally use separate queue, header, body, signature,
    and SafeVerify stages.  New Judge versions publish their computed lifecycle
    deadline; the legacy fallback reconstructs a conservative upper bound from
    the queue deadline already present in older receipts.
    """

    def milliseconds(key: str) -> float | None:
        value = _nested_value(payload, key)
        if isinstance(value, bool):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) and parsed >= 0 else None

    timeout = max(1.0, float(execution_timeout))

    def checked(budget: float) -> float:
        result = max(timeout, budget)
        if result > maximum_lifecycle_seconds:
            raise EvaluatorError(
                "Judge lifecycle budget exceeds the client safety cap "
                f"({result:.3f}s > {maximum_lifecycle_seconds:.3f}s)"
            )
        return result

    submitted_at = milliseconds("submitted_at_ms")
    lifecycle_deadline = milliseconds("lifecycle_deadline_ms")
    if (
        submitted_at is not None
        and lifecycle_deadline is not None
        and lifecycle_deadline >= submitted_at
    ):
        return checked((lifecycle_deadline - submitted_at) / 1_000.0)

    queue_deadline = milliseconds("queue_deadline_ms")
    if (
        submitted_at is None
        or queue_deadline is None
        or queue_deadline < submitted_at
    ):
        return checked(timeout)
    queue_budget = (queue_deadline - submitted_at) / 1_000.0
    # Formal REPL cache miss: initial queue + header/body commands.  Signature
    # and SafeVerify finalization may each consume another queue/command budget.
    # This intentionally over-bounds fast/official profiles rather than
    # cancelling a valid proof before the server's own lifecycle can settle.
    # One main command plus header/signature/SafeVerify accounts for four
    # execution budgets.  Each retry configured on the *legacy single-job*
    # path adds one more.  Explicit Agent budgets use the outer logical loop,
    # where each fresh job has max_retries=0 and the absolute remaining budget
    # is enforced separately; this fallback is therefore only used for one
    # attempt at a time.
    retries = max(0, int(backend_max_retries))
    return checked((3.0 * queue_budget) + ((4 + retries) * timeout) + 20.0)


def _verdict_status(payload: Mapping[str, Any]) -> str:
    """Preserve Judge lifecycle failures instead of flattening them to network."""

    raw_status = _raw_lifecycle_status(payload)
    error_kind = str(_nested_value(payload, "error_kind") or "").strip().lower()
    terminal_reason = str(_nested_value(payload, "terminal_reason") or "").strip().lower()
    if raw_status in {"CANCELLED", "CANCELED"}:
        return "CANCELLED"
    if raw_status == "REJECTED_OVERLOADED":
        return "REJECTED_OVERLOADED"
    if raw_status == "TIMED_OUT" or error_kind == "timeout" or terminal_reason == "execution_timeout":
        return "EXECUTION_TIMEOUT"
    if error_kind in {"memory_limit_exceeded", "resource_limit", "resource_exhausted"}:
        return "RESOURCE_LIMIT"
    if error_kind == "overloaded" or terminal_reason == "queue_wait_timeout":
        return "REJECTED_OVERLOADED"
    if terminal_reason in {"cancelled", "canceled"}:
        return "CANCELLED"
    status = _status(payload)
    # Lifecycle state is authoritative over stale nested verdict fields.  A
    # failed receipt carrying an old PROVED marker must never score.
    if raw_status in _RAW_FAILURE_STATUSES and status in _PROVED_STATUSES:
        return "EVALUATOR_ERROR"
    if status == "NETWORK_ERROR":
        return "INFRASTRUCTURE_ERROR"
    return status


def _settled_outcome(
    payload: Mapping[str, Any],
) -> tuple[str, bool, str | None]:
    """Resolve a terminal receipt and fail closed on envelope contradictions."""

    status = _verdict_status(payload)
    if status in _NONTERMINAL_STATUSES:
        return (
            "EVALUATOR_ERROR",
            False,
            "Judge returned a contradictory terminal envelope",
        )
    proved = _is_proved(payload)
    if proved:
        return "PROVED", True, None
    if status in {"SUCCEEDED", "COMPLETED", "FAILED", "ERROR", "UNKNOWN"}:
        return (
            "EVALUATOR_ERROR",
            False,
            "Judge terminal receipt lacks an authoritative verdict",
        )
    if status == "EVALUATOR_ERROR":
        return (
            status,
            False,
            "Judge lifecycle failure contradicts a proof verdict",
        )
    return status, False, None


_CUSTOM_DETERMINISTIC_STATUSES = frozenset(
    {
        "PROVED",
        "AC",
        "PASS",
        "PASSED",
        "VERIFY_FAIL",
        "COMPILES_WITH_SORRY",
        "CHEATING",
        "LOCAL_REJECTED",
        "INVALID_REQUEST",
        "INVALID_TASK_SELECTION",
        "BUDGET_EXHAUSTED",
        "RESOURCE_LIMIT",
        "WA",
        "PE",
        "CE",
        "MLE",
        "TLE",
        "RE",
        "ELABORATED",
        "ELAB_FAILED",
        "MOCK_SKIPPED",
        "CANCELLED",
        "TASK_CANCELLED",
        "OUT_OF_HORIZON",
        "REMOTE_SETTLEMENT_UNCONFIRMED",
        "EXECUTION_TIMEOUT",
        "EVALUATOR_TIMEOUT",
    }
)

_CUSTOM_TRANSIENT_ERROR_KINDS = frozenset(
    {
        "workspace_not_ready",
        "runtime_exception",
        "protocol_error",
        "service_unavailable",
        "connection_reset",
        "connection_refused",
        "network_error",
    }
)


def _relabel_agent_budget_timeout(
    verdict: Verdict,
    *,
    budget_deadline: float,
    run_horizon_deadline: float | None,
) -> Verdict:
    """Enforce the absolute Agent budget at the evaluator boundary.

    ``_evaluate_once`` receives the earlier of the run horizon and the Agent
    deadline so that polling/cancellation share one absolute boundary.  Its
    historical status for that boundary is ``OUT_OF_HORIZON``.  A terminal
    receipt can nevertheless arrive during the bounded settlement grace.  It
    must not become a successful (or otherwise ordinary) result after the
    Agent's deadline, so convert late non-safety outcomes to
    ``EVALUATOR_TIMEOUT``.  A real run-horizon expiry remains
    ``OUT_OF_HORIZON``; unresolved/cancellation states remain fail-closed.
    """

    now = time.monotonic()
    if now < float(budget_deadline):
        return verdict
    if run_horizon_deadline is not None and now >= float(run_horizon_deadline):
        return verdict
    status = str(verdict.status or "").strip().upper()
    if status in {
        "REMOTE_SETTLEMENT_UNCONFIRMED",
        "TASK_CANCELLED",
        "CANCELLED",
        "EVALUATOR_TIMEOUT",
        "EXECUTION_TIMEOUT",
    }:
        response = dict(verdict.response or {})
        response["timeout_budget_exhausted"] = True
        response["timeout_budget_remaining_seconds"] = 0.0
        return Verdict(
            task_id=verdict.task_id,
            status=verdict.status,
            score=verdict.score,
            elapsed_seconds=verdict.elapsed_seconds,
            response=response,
            error=verdict.error,
            candidate_sha256=verdict.candidate_sha256,
            task_contract_sha256=verdict.task_contract_sha256,
            judge_job_id=verdict.judge_job_id,
            cache_reused=verdict.cache_reused,
        )
    response = dict(verdict.response or {})
    response.setdefault("reason", "agent_total_timeout_exhausted")
    response["timeout_budget_exhausted"] = True
    response["timeout_budget_remaining_seconds"] = 0.0
    return Verdict(
        task_id=verdict.task_id,
        status="EVALUATOR_TIMEOUT",
        score=0.0,
        elapsed_seconds=verdict.elapsed_seconds,
        response=response,
        error=verdict.error or "Agent validation budget elapsed",
        candidate_sha256=verdict.candidate_sha256,
        task_contract_sha256=verdict.task_contract_sha256,
        judge_job_id=verdict.judge_job_id,
        cache_reused=verdict.cache_reused,
    )


def _custom_retry_class(
    verdict: Verdict,
    *,
    remaining_budget_seconds: float,
    budget_seconds: int,
    attempt_elapsed_seconds: float,
) -> str | None:
    """Classify one explicit-budget result without trusting a bare flag.

    ``Judge`` marks several candidate-bound outcomes as retryable for its own
    operational purposes.  An Agent total-budget retry is narrower: it may
    repeat only a confirmed pre-admission overload or a terminal,
    candidate-independent runtime/transport failure.  Deterministic
    verification outcomes, resource limits, cancellation, unknown settlement,
    and every execution timeout stop immediately.  A timeout is therefore
    never used as a reason to spend another full-length attempt; the next
    attempt, when any, is reserved for an independently evidenced failure.
    """

    minimum_attempt_seconds = min(AGENT_TIMEOUT_MIN_SECONDS, budget_seconds)
    if (
        remaining_budget_seconds
        < minimum_attempt_seconds - _AGENT_TIMEOUT_BOUNDARY_EPSILON_SECONDS
    ):
        return None
    status = str(verdict.status or "").strip().upper()
    if status in _CUSTOM_DETERMINISTIC_STATUSES:
        return None
    response = verdict.response if isinstance(verdict.response, Mapping) else {}

    if status == "REJECTED_OVERLOADED":
        # This status is safe only when the receipt proves that no job was
        # admitted.  ``_evaluate_once`` already enforces the job-id/terminal
        # binding; keep the explicit retryable marker as a second guard.
        return "overload" if _retryable_admission_rejection(response) else None

    error_kind = str(_nested_value(response, "error_kind") or "").strip().lower()
    terminal_reason = str(
        _nested_value(response, "terminal_reason") or ""
    ).strip().lower()

    if status in {"EVALUATOR_ERROR", "INFRASTRUCTURE_ERROR", "NETWORK_ERROR"}:
        failure = response.get("evaluator_failure")
        failure_category = ""
        failure_http_status: int | None = None
        if isinstance(failure, Mapping):
            failure_category = str(failure.get("category") or "").strip().lower()
            raw_http_status = failure.get("http_status")
            if isinstance(raw_http_status, int) and not isinstance(raw_http_status, bool):
                failure_http_status = raw_http_status
        if error_kind in _CUSTOM_TRANSIENT_ERROR_KINDS:
            return "execution"
        if failure_category in _CUSTOM_TRANSIENT_ERROR_KINDS:
            return "execution"
        # ``_request`` raises this only after a confirmed pre-admission 429/
        # 503 receipt (no bindable job id).  Keep it on the overload retry
        # budget rather than flattening it into a generic execution failure.
        if failure_category == "judge_overloaded":
            return "overload"
        # A coding/HTTP adapter exposes only a bounded HTTP status in its
        # evaluator failure summary.  Retry server-side 5xx responses, but do
        # not replay a client error or an ambiguous submission.
        if failure_category == "http_error" and failure_http_status in {
            500,
            502,
            503,
            504,
        }:
            return "execution"
        # A terminal receipt may expose only ``terminal_reason``.  Accept the
        # bounded, candidate-independent labels but never a generic
        # ``retryable=true`` without one of them.
        if terminal_reason in {
            "runtime_exception",
            "workspace_not_ready",
            "protocol_error",
            "service_unavailable",
            "connection_reset",
            "connection_refused",
            "network_error",
        }:
            return "execution"
    return None


def _terminal(payload: Mapping[str, Any]) -> bool:
    normalized_raw_status = _raw_lifecycle_status(payload)
    if normalized_raw_status in _NONTERMINAL_STATUSES:
        return False
    if normalized_raw_status in {
        "SUCCEEDED",
        "COMPLETED",
        "FAILED",
        "TIMED_OUT",
        "ERROR",
        "CANCELLED",
        "CANCELED",
        "REJECTED_OVERLOADED",
    }:
        return True
    status = _status(payload)
    if status in _NONTERMINAL_STATUSES:
        return False
    return bool(
        payload.get("terminal")
        or payload.get("finished_at")
        or payload.get("finished_at_ms")
        or status in {
            "PROVED",
            "AC",
            "PASS",
            "PASSED",
            "SUCCEEDED",
            "FAILED",
            "ERROR",
            "WA",
            "VERIFY_FAIL",
            "COMPILES_WITH_SORRY",
            "REJECTED_OVERLOADED",
            "NETWORK_ERROR",
            "INFRASTRUCTURE_ERROR",
            "EXECUTION_TIMEOUT",
            "RESOURCE_LIMIT",
            "TIMED_OUT",
            "CANCELLED",
        }
    )


def _is_proved(payload: Mapping[str, Any]) -> bool:
    raw_status = _raw_lifecycle_status(payload)
    if raw_status in _NONTERMINAL_STATUSES or raw_status in _RAW_FAILURE_STATUSES:
        return False
    status = _status(payload)
    if status in _PROVED_STATUSES:
        return True
    if status in {"SUCCEEDED", "COMPLETED"}:
        for key in ("is_valid_no_sorry", "correct", "success", "accepted"):
            if payload.get(key) is True:
                return True
        nested = payload.get("response")
        if isinstance(nested, Mapping) and _is_proved(nested):
            return True
        canonical = payload.get("canonical_verdict")
        return isinstance(canonical, Mapping) and _status(canonical) in _PROVED_STATUSES
    return False


def safe_worker_response(
    payload: Mapping[str, Any] | Any,
    *,
    _depth: int = 0,
    timeout_max_seconds: int | float | None = None,
) -> dict[str, Any]:
    """Keep bounded verdict metadata while removing secrets and host details."""

    if not isinstance(payload, Mapping) or _depth > 2:
        return {}
    try:
        timeout_cap = agent_timeout_bounds(timeout_max_seconds).max_seconds
    except ValueError:
        timeout_cap = agent_timeout_bounds().max_seconds
    result: dict[str, Any] = {}
    for key in ("job_id", "id"):
        identifier = sanitize_worker_identifier(payload.get(key))
        if identifier is not None:
            result[key] = identifier
    timeout_budget_number_fields = {
        "timeout_budget_seconds",
        "timeout_budget_elapsed_seconds",
        "timeout_budget_remaining_seconds",
    }
    for key in (
        "status",
        "formal_status",
        "verdict",
        "error_code",
        "error_kind",
        "terminal_reason",
        "mathlib_revision",
        "lean_version",
        "judge_cache_backend",
        "judge_cache_status",
        "timeout_budget_mode",
        "timeout_budget_stop_reason",
        "retry_blocked_reason",
    ):
        value = payload.get(key)
        if isinstance(value, str):
            result[key] = sanitize_worker_text(value, _MAX_WORKER_STATUS_BYTES)
    for key in ("error_message", "reason", "settlement_error"):
        if key in payload:
            result[key] = sanitize_worker_text(
                payload.get(key), _MAX_WORKER_ERROR_BYTES
            )
    for key in (
        "terminal",
        "correct",
        "accepted",
        "success",
        "is_valid_no_sorry",
        "is_valid_with_sorry",
        "retryable",
        "cache_reused",
        "probe_cache_reused",
        "judge_cache_hit",
        "judge_cache_leader",
        "judge_cache_singleflight_wait",
        "cancel_requested",
        "finalization_pending",
        "remote_settlement_unconfirmed",
        "timeout_budget_exhausted",
        "formal_backend_budget_exhausted",
    ):
        if isinstance(payload.get(key), bool):
            result[key] = payload[key]
    for key in (
        "queue_wait_ms",
        "execution_ms",
        "submitted_at_ms",
        "queue_deadline_ms",
        "lifecycle_deadline_ms",
        "started_at_ms",
        "finished_at_ms",
        "queue_wait_seconds",
        "execution_seconds",
        "admission_attempts",
        "evaluator_overload_resubmissions",
        "judge_cache_wait_ms",
        "timeout_budget_seconds",
        "timeout_budget_elapsed_seconds",
        "timeout_budget_remaining_seconds",
        "judge_attempt_count",
        "judge_retry_count",
        "backend_job_count",
    ):
        number = _safe_nonnegative_number(payload.get(key))
        if number is not None:
            result[key] = (
                min(number, float(timeout_cap))
                if key in timeout_budget_number_fields
                else number
            )
    attempt_timeouts = payload.get("judge_attempt_timeouts_seconds")
    if isinstance(attempt_timeouts, (list, tuple)):
        result["judge_attempt_timeouts_seconds"] = [
            max(0, min(int(item), timeout_cap))
            for item in attempt_timeouts[:16]
            if isinstance(item, int) and not isinstance(item, bool)
        ]
    attempt_elapsed = payload.get("judge_attempt_elapsed_seconds")
    if isinstance(attempt_elapsed, (list, tuple)):
        safe_elapsed: list[float] = []
        for item in attempt_elapsed[:16]:
            if isinstance(item, bool):
                continue
            try:
                parsed = float(item)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(parsed) and parsed >= 0:
                safe_elapsed.append(round(parsed, 6))
        result["judge_attempt_elapsed_seconds"] = safe_elapsed
    retry_reasons = payload.get("judge_retry_reasons")
    if isinstance(retry_reasons, (list, tuple)):
        result["judge_retry_reasons"] = [
            sanitize_worker_text(item, 64)
            for item in retry_reasons[:16]
            if isinstance(item, str) and item.strip()
        ]
    attempt_ids = payload.get("judge_attempt_ids")
    if isinstance(attempt_ids, (list, tuple)):
        result["judge_attempt_ids"] = [
            identifier
            for item in attempt_ids[:16]
            if (identifier := sanitize_worker_identifier(item)) is not None
        ]
    backend_job_numbers = payload.get("backend_job_numbers")
    if isinstance(backend_job_numbers, (list, tuple)):
        result["backend_job_numbers"] = [
            max(0, min(int(item), 1_000_000))
            for item in backend_job_numbers[:16]
            if isinstance(item, int) and not isinstance(item, bool)
        ]
    if isinstance(payload.get("response"), Mapping):
        result["response"] = safe_worker_response(
            payload["response"],
            _depth=_depth + 1,
            timeout_max_seconds=timeout_max_seconds,
        )
    if "probe_diagnostics" in payload:
        result["probe_diagnostics"] = _safe_probe_diagnostics(
            payload.get("probe_diagnostics")
        )
    lean_environment = payload.get("lean_environment")
    if isinstance(lean_environment, Mapping):
        safe_environment: dict[str, str] = {}
        for key in ("mathlib_revision", "lean_version"):
            value = lean_environment.get(key)
            if isinstance(value, str):
                safe_environment[key] = sanitize_worker_text(
                    value,
                    _MAX_WORKER_STATUS_BYTES,
                )
        if safe_environment:
            result["lean_environment"] = safe_environment
    failure = payload.get("evaluator_failure")
    if isinstance(failure, Mapping):
        safe_failure: dict[str, Any] = {
            "category": _safe_category(failure.get("category")),
        }
        for key in ("attempts", "http_status"):
            value = failure.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                safe_failure[key] = max(0, min(value, 1_000_000))
        retry_after = _safe_nonnegative_number(failure.get("retry_after_seconds"))
        if retry_after is not None:
            safe_failure["retry_after_seconds"] = round(retry_after, 3)
        result["evaluator_failure"] = safe_failure
    cancellation = payload.get("judge_cancellation")
    if isinstance(cancellation, Mapping):
        safe_cancellation = {
            "attempted": cancellation.get("attempted") is True,
            "succeeded": cancellation.get("succeeded") is True,
            "settled": cancellation.get("settled") is True,
            "unconfirmed": cancellation.get("unconfirmed") is True,
        }
        if cancellation.get("deferred") is True:
            safe_cancellation["deferred"] = True
        failure_category = cancellation.get("failure_category")
        if isinstance(failure_category, str) and failure_category.strip():
            safe_cancellation["failure_category"] = _safe_category(
                failure_category
            )
        result["judge_cancellation"] = safe_cancellation
    canonical = payload.get("canonical_verdict")
    if isinstance(canonical, Mapping):
        safe_canonical: dict[str, Any] = {}
        for key in ("status", "source_contract_status"):
            if isinstance(canonical.get(key), str):
                safe_canonical[key] = sanitize_worker_text(
                    canonical[key], _MAX_WORKER_STATUS_BYTES
                )
        score = _safe_nonnegative_number(canonical.get("score"))
        if score is not None:
            safe_canonical["score"] = score
        for key in ("correct", "cheating"):
            if isinstance(canonical.get(key), bool):
                safe_canonical[key] = canonical[key]
        result["canonical_verdict"] = safe_canonical
    return result


def _safe_response(
    payload: Mapping[str, Any],
    *,
    timeout_max_seconds: int | float | None = None,
) -> dict[str, Any]:
    """Backward-compatible internal alias for the response sanitizer."""

    return safe_worker_response(payload, timeout_max_seconds=timeout_max_seconds)


def _safe_probe_diagnostics(value: Any) -> dict[str, Any]:
    """Re-apply the public Judge probe bounds before exposing diagnostics."""

    truncated = False
    items = value
    if isinstance(value, Mapping):
        items = value.get("items")
        truncated = value.get("truncated") is True
    if not isinstance(items, list):
        return {"items": [], "truncated": truncated}

    safe_items: list[dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, Mapping):
            continue
        if len(safe_items) >= _MAX_PROBE_DIAGNOSTICS:
            truncated = True
            break
        severity, severity_was_truncated = _bounded_utf8_text(
            sanitize_worker_text(raw.get("severity"), _MAX_PROBE_SEVERITY_BYTES),
            _MAX_PROBE_SEVERITY_BYTES,
            default="info",
        )
        data, data_was_truncated = _bounded_utf8_text(
            sanitize_worker_text(raw.get("data"), _MAX_PROBE_DATA_BYTES),
            _MAX_PROBE_DATA_BYTES,
        )
        truncated = truncated or severity_was_truncated or data_was_truncated
        safe_items.append(
            {
                "severity": severity,
                "data": data,
                "line": _bounded_position(raw.get("line")),
                "column": _bounded_position(raw.get("column")),
            }
        )
    return {"items": safe_items, "truncated": truncated}


def _bounded_utf8_text(
    value: Any,
    max_bytes: int,
    *,
    default: str = "",
) -> tuple[str, bool]:
    text = value if isinstance(value, str) else default
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, value is not None and not isinstance(value, str)
    return encoded[:max_bytes].decode("utf-8", errors="ignore"), True


def _bounded_position(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return min(max(0, value), _MAX_DIAGNOSTIC_POSITION)


def _safe_category(value: Any) -> str:
    text = str(value or "evaluator_error").strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", text):
        return "evaluator_error"
    return text


def _safe_status(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text or len(text.encode("utf-8")) > _MAX_WORKER_STATUS_BYTES:
        return "UNKNOWN"
    if not re.fullmatch(r"[A-Z][A-Z0-9_:-]*", text):
        return "UNKNOWN"
    return text


def _safe_nonnegative_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return value


def _safe_nonterminal_response(
    payload: Mapping[str, Any],
    *,
    timeout_max_seconds: int | float | None = None,
) -> dict[str, Any]:
    """Retain diagnostics without serializing a pending state as a verdict."""

    result = _safe_response(payload, timeout_max_seconds=timeout_max_seconds)
    observations: list[str] = []

    def scrub(node: dict[str, Any]) -> None:
        for key in ("status", "formal_status", "verdict"):
            value = node.get(key)
            if isinstance(value, str) and value.strip().upper() in _NONTERMINAL_STATUSES:
                observations.append(value.strip().lower())
                node.pop(key, None)
        nested = node.get("response")
        if isinstance(nested, dict):
            scrub(nested)
        canonical = node.get("canonical_verdict")
        if isinstance(canonical, dict):
            scrub(canonical)

    scrub(result)
    if observations:
        result["last_observed_lifecycle"] = observations[0]
    return result


def _local_contract_error(task: Task, code: str, target: str) -> str | None:
    if not code.strip():
        return "empty candidate"
    # Keep `sorry` candidates eligible for diagnostic Lean feedback; only the
    # judge's `is_valid_no_sorry`/canonical verdict can award a score.
    if re.search(r"\b(?:axiom|unsafe|native_decide|trustCompiler)\b", code):
        return "candidate contains a forbidden proof-bypass construct"
    if task.theorem_name and task.theorem_name not in code:
        return "target theorem name is missing"
    imports = {line.strip() for line in target.splitlines() if line.strip().startswith("import ")}
    candidate_imports = {line.strip() for line in code.splitlines() if line.strip().startswith("import ")}
    if imports != candidate_imports:
        return "imports changed"
    return None
