"""Opt-in, low-cardinality resource profiling for ContextSwarm runs.

Profiling is deliberately an observational side channel.  It is disabled by
default, writes no file when disabled, and every diagnostic operation is
fail-open.  The event stream contains bounded identifiers and scalar resource
measurements only; prompts, candidates, provider responses, credentials and
host paths are never serialized.

The implementation intentionally uses Linux ``/proc`` and cgroup files when
available, while degrading to run-level timing on other platforms.  A small
background sampler is started only after :meth:`RunProfiler.start` is called
on an enabled profiler.  Callers may also use ``sample_now`` at lifecycle
boundaries for a deterministic final sample.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Iterator, Mapping

try:  # ``resource`` is present on the supported Unix runners.
    import resource as _resource
except ImportError:  # pragma: no cover - Windows fallback
    _resource = None  # type: ignore[assignment]


PROFILE_SCHEMA_VERSION = "contextswarm_profile_event_v1"
PROFILE_FILENAME = "profiling.jsonl"
_TRUTHY = frozenset({"1", "true", "yes", "on", "enabled"})
_FALSY = frozenset({"0", "false", "no", "off", "disabled", ""})
_EVENT_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,95}$")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,255}$")
_SHA_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_PATH_RE = re.compile(r"(?:^|\s)(?:/|[A-Za-z]:[\\/])")
_URL_RE = re.compile(r"(?i)\b(?:https?|tcp|unix)://")
_SECRET_RE = re.compile(
    r"(?i)(?:bearer\s+|authorization\s*[:=]|api[_-]?key\s*[:=]|"
    r"access[_-]?token\s*[:=]|secret\s*[:=]|password\s*[:=])"
)

# A forced heartbeat is used at every Pi attempt boundary.  It must remain
# bounded even when a run has thousands of short-lived attempts/artifacts.
# The regular run sampler still observes the complete visible tree; only the
# terminal per-attempt path uses these caps.
_TERMINAL_TREE_MAX_PROCESSES = 128
_ARTIFACT_MAX_FILES = 4096
_ARTIFACT_MAX_DIRECTORIES = 1024

# Text is allow-listed.  The values are labels, not a general-purpose JSON
# escape hatch; unknown keys (including ``prompt`` and ``candidate``) vanish.
_TEXT_FIELDS = frozenset(
    {
        "phase",
        "operation",
        "stage",
        "status",
        "observed_status",
        "reason",
        "outcome",
        "source",
        "mode",
        "kind",
        "event_type",
        "error_kind",
        "selector",
        "selector_name",
        "policy",
        "purpose",
        "close_reason",
        "queue_state",
        "retry_kind",
        "settlement_state",
        "transport",
        "role",
        "process_state",
        "sample_kind",
        "scan_mode",
        "read_mode",
        "component",
        "fallback_reason",
        "disposition",
        "source_kind",
        "communication",
        "caller_phase",
        "db_operation",
        "watcher_state",
        "allocation_phase",
        "allocation_policy",
        "scheduler_outcome",
        "agent_state",
        # Low-cardinality labels used by runner/Judge recovery and trace
        # projection hooks.  These are deliberately text labels only; raw
        # error messages, SQL and endpoint/path values remain rejected.
        "recovery",
        "failure_category",
        "failure_source",
        "method",
        "query_name",
        "scan_scope",
        "cpu_denominator_source",
        "cgroup_cpu_denominator_source",
        "timeout_source",
        "timeout_budget_mode",
        "timeout_budget_stop_reason",
    }
)

# Resource, queue and lifecycle counters.  Keep this list explicit so a new
# caller cannot accidentally add a large nested payload to the JSONL stream.
_SCALAR_FIELDS = frozenset(
    {
        "accepted",
        "active",
        "active_handlers",
        "active_slots",
        "active_solver_slots",
        "active_attempts",
        "admitted",
        "agent_result_valid",
        "agent_run_horizon_reached",
        "run_horizon_reached",
        "attempt",
        "attempt_count",
        "artifact_bytes",
        # Bounded evaluator backlog capacity.  The runner emits this on the
        # queue wait/expiry notifications so high-concurrency backpressure is
        # auditable without forwarding the queue object or any payload.
        "backlog_limit",
        "bytes",
        "body_seconds",
        "candidate_count",
        "cache_reused",
        "cache_hit",
        "mocked",
        "invalid_output",
        "eligible_for_handoff",
        "scoreboard_recorded",
        "authoritative_proof_confirmed",
        "closeout_infra_incomplete",
        "prior_authoritative_proof_available",
        "fresh_closeout_confirmed",
        "authority_conflict",
        "reused_authoritative_verdict",
        "reused_authoritative_verdicts",
        "authoritative_proofs_confirmed",
        "closeout_infra_unconfirmed",
        "authority_conflicts",
        "remote_settlement_unconfirmed",
        "cache_read_tokens",
        "cache_write_tokens",
        "call_index",
        "cancelled",
        "candidates",
        "closeout_executor_limit",
        "commit_seconds",
        "connect_seconds",
        "communication_enabled",
        "complete",
        "completed",
        "context_switches",
        "cpu_system_seconds",
        "cpu_user_delta_seconds",
        "cpu_system_delta_seconds",
        "cpu_utilization",
        "cpu_utilization_cgroup",
        "cpu_quota_cores",
        "cpu_affinity_count",
        "cgroup_cpu_usage_seconds",
        "cgroup_cpu_usage_delta_seconds",
        "cgroup_cpu_utilization",
        "sample_interval_seconds",
        "cpu_throttled_seconds",
        "cpu_user_seconds",
        "cpu_thread_system_seconds",
        "cpu_thread_user_seconds",
        "db_bytes",
        "db_bytes_before",
        "db_bytes_after",
        "decision_index",
        "delivered_tokens",
        "delay_seconds",
        "disk_free_bytes",
        "drained",
        "dropped_fields",
        "elapsed_seconds",
        "horizon_seconds",
        "execution_timeout_seconds",
        "requested_timeout_seconds",
        "effective_timeout_seconds",
        "timeout_clamped",
        "timeout_budget_seconds",
        "timeout_budget_elapsed_seconds",
        "timeout_budget_remaining_seconds",
        "timeout_budget_exhausted",
        "judge_attempt_count",
        "judge_retry_count",
        "max_concurrent_evaluations",
        "evaluator_seconds",
        "audit_seconds",
        "episode",
        "episodes_per_task",
        "max_attempts_per_task",
        "initial_agents_per_task",
        "planned_agent_sessions",
        "lean_max_concurrent_evaluations",
        "aisw_max_in_flight",
        "pi_recovery_enabled",
        "pi_recovery_max_restarts",
        "error_count",
        "events",
        "fd_count",
        "gate_wait_seconds",
        "filter_seconds",
        "hash_seconds",
        "fifo_depth",
        "flush_seconds",
        "heartbeat_seq",
        "input_tokens",
        "isolated",
        "items",
        "input_bytes",
        "input_rows",
        "lock_hold_seconds",
        "lock_wait_seconds",
        "latency_seconds",
        "idle_seconds",
        "lock_queue_depth",
        "queue_residence_seconds",
        "write_active",
        "write_waiters",
        "write_sequence",
        "write_ops_total",
        "write_wall_total_seconds",
        "write_lock_wait_total_seconds",
        "write_lock_hold_total_seconds",
        "max_concurrent",
        "max_restarts",
        "max_parallel",
        "max_workers",
        "free_slots",
        "remaining_slots",
        "scheduler_reserved_slots",
        "owned_scheduler_reservation_slots",
        "capacity_seconds",
        "occupied_slot_seconds",
        "scheduler_compute_seconds",
        "scheduler_reserved_slot_seconds",
        "solver_slot_utilization",
        "compute_slot_utilization",
        "materialized_bytes",
        "materialized_rows",
        "memory_bytes",
        "memory_current_bytes",
        "memory_events_count",
        "memory_peak_bytes",
        "monotonic_elapsed_seconds",
        "oom_kill_count",
        "output_tokens",
        "pages_scanned",
        "page_count",
        "page_index",
        "peak_context_switches",
        "peak_cpu_system_seconds",
        "peak_cpu_user_seconds",
        "peak_cpu_utilization",
        "peak_cpu_utilization_cgroup",
        "peak_fd_count",
        "peak_memory_current_bytes",
        "peak_memory_peak_bytes",
        "peak_process_count",
        "peak_process_tree_count",
        "peak_pss_bytes",
        "peak_rss_bytes",
        "peak_thread_count",
        "pid",
        "pool_depth",
        "process_alive",
        "process_tree_truncated",
        "process_count",
        "process_tree_count",
        "profile_bytes",
        "proved",
        "pss_bytes",
        "probe_calls",
        "poll_count",
        "projection_call_index",
        "projection_calls",
        "reuse_count",
        "queue_depth",
        "queued_count",
        "ranked_count",
        "receipt_count",
        "records",
        "recoverable_invocation_error",
        "recoverable",
        "returncode",
        "retryable",
        "rows",
        "rows_written",
        "rows_scanned",
        "rss_bytes",
        "running_count",
        "score",
        "selected_count",
        "selection_enabled",
        "selection_candidate_count",
        "selection_ranked_count",
        "selection_persisted_count",
        "settled",
        "snapshot_count",
        "snapshot_seconds",
        "snapshot_hit",
        "snapshot_pages",
        "spawn_seconds",
        "begin_seconds",
        "query_index",
        "projection_seconds",
        "recovery_attempt",
        "recovery_count",
        "poll_count",
        "settlement_poll_seconds",
        "pending_settlement_watchers",
        "oldest_watcher_age_seconds",
        "settled_job_count",
        "cancel_job_count",
        "request_seconds",
        "poll_seconds",
        "cancel_seconds",
        "reconcile_seconds",
        "serialization_seconds",
        "serialization_inside_lock_seconds",
        "serialization_inside_lock_bytes",
        "serialization_call_count",
        "settled_job_count",
        "pending_settlement_watchers",
        "remote_unsettled_jobs",
        "oldest_watcher_age_seconds",
        "settlement_poll_seconds",
        "cancel_job_count",
        "stderr_buffer_bytes",
        "stdout_buffer_bytes",
        "system_cpu_seconds",
        "sqlite_bytes",
        "task_count",
        "thread_count",
        "total_tokens",
        "transaction_seconds",
        "timeout_seconds",
        "requested_timeout_seconds",
        "effective_timeout_seconds",
        "timeout_clamped",
        "text_chars",
        "timed_out",
        "tokenize_count",
        "token_count",
        "tokenize_seconds",
        "total_cpu_seconds",
        "total_memory_bytes",
        "wait_seconds",
        "wall_seconds",
        "worker_count",
        "wal_bytes",
        "wal_bytes_before",
        "wal_bytes_after",
        "query_count",
        "query_seconds",
        "fetch_seconds",
        "materialize_seconds",
        "read_transaction_seconds",
        "read_scope_seconds",
        "read_lock_wait_seconds",
        "busy_retry_count",
        "output_rows",
        "output_bytes",
        "checkpoint_seconds",
        "sample_seconds",
        "proc_snapshot_seconds",
        "artifact_snapshot_seconds",
        "artifact_files_scanned",
        "artifact_directories_scanned",
        "artifact_scan_truncated",
        "payload_bytes",
        "prepare_seconds",
        "prepare_rows",
        "prepare_candidate_rows",
        "prepare_ranking_rows",
        "prepare_bytes",
        "prepare_serialization_seconds",
        "prepare_hash_seconds",
        "sample_count",
        "task_set_count",
        "wal_bytes_delta",
        "db_bytes_delta",
        "workspace_bytes",
    }
)

# Opaque correlation handles are useful for joining a receipt to the runner's
# audit without admitting arbitrary text.  Values that look like paths/URLs or
# exceed the identifier grammar are replaced by a short hash in
# ``_safe_identifier``.
_IDENTIFIER_FIELDS = frozenset(
    {
        "call_id",
        "judge_job_id",
        "scheduler_call_id",
    }
)

_HASH_FIELDS = frozenset(
    {
        "candidate_sha256",
        "comparison_contract_id",
        "config_sha256",
        "request_key_sha256",
        "selector_config_id",
        "snapshot_sha256",
        "source_revision",
        "task_contract_sha256",
        "trace_set_sha256",
        "source_snapshot_sha256",
        "projection_snapshot_sha256",
        "pool_sha256",
        "eligible_pool_sha256",
        "task_set_sha256",
        "trace_watermark_sha256",
    }
)

# ``RunLogger.event`` carries the ordinary run-artifact payload, which is
# intentionally much richer than the profiling contract.  Keep the
# translation boundary explicit: lifecycle rows get only the bounded fields
# that are useful for profiling, while unknown/raw values are discarded
# before they reach ``emit`` (and therefore do not create a misleading
# ``dropped_fields`` quality failure).  Direct callers of ``emit`` still use
# the strict schema accounting below, so malformed *known* fields remain
# visible to the audit.
_LOGGER_IDENTITY_FIELDS = frozenset({"run_id", "task_id", "agent_id", "actor_id"})
_LOGGER_RESERVED_FIELDS = frozenset(
    {
        "schema_version",
        "sequence",
        "at",
        "monotonic_ns",
        "event",
        "dropped_fields",
    }
)

_LOGGER_EVENT_FIELDS: dict[str, frozenset[str]] = {
    # ``run_started`` contains nested manifest/configuration data.  Only the
    # small scalar summary is useful in a profile row; the nested allocation,
    # selection, task and model payloads stay out of the side channel.
    "run_started": frozenset(
        {
            "mode",
            "policy",
            "allocation_policy",
            "communication",
            "communication_enabled",
            "selection_enabled",
            "max_parallel",
            "worker_count",
            "task_count",
            "episodes_per_task",
            "max_attempts_per_task",
            "initial_agents_per_task",
            "planned_agent_sessions",
            "lean_max_concurrent_evaluations",
            "aisw_max_in_flight",
            "pi_recovery_enabled",
            "pi_recovery_max_restarts",
        }
    ),
    "horizon_started": frozenset({"horizon_seconds"}),
    "agent_finished": frozenset(
        {
            "episode",
            "returncode",
            "events",
            "timed_out",
            "cancelled",
            "mocked",
            "invalid_output",
            "run_horizon_reached",
            "recoverable_invocation_error",
            "decision_index",
            "scheduler_call_id",
            "scheduler_outcome",
            "agent_state",
        }
    ),
    # Solver and closeout receipts share the same bounded result vocabulary.
    # In particular, ``response`` and ``error`` are deliberately absent.
    "evaluation_finished": frozenset(
        {
            "episode",
            "status",
            "observed_status",
            "score",
            "elapsed_seconds",
            "cache_reused",
            "candidate_sha256",
            "task_contract_sha256",
            "judge_job_id",
            "source",
            "phase",
            "eligible_for_handoff",
            "scoreboard_recorded",
            "accepted",
            "proved",
            "invalid_output",
            "timeout_budget_mode",
            "timeout_budget_stop_reason",
            "timeout_budget_seconds",
            "timeout_budget_elapsed_seconds",
            "timeout_budget_remaining_seconds",
            "timeout_budget_exhausted",
            "judge_attempt_count",
            "judge_retry_count",
        }
    ),
    "closeout_started": frozenset(
        {
            "candidate_count",
            "max_concurrent_evaluations",
            "execution_timeout_seconds",
        }
    ),
    "closeout_evaluation_finished": frozenset(
        {
            "episode",
            "status",
            "observed_status",
            "score",
            "elapsed_seconds",
            "cache_reused",
            "candidate_sha256",
            "task_contract_sha256",
            "judge_job_id",
            "source",
            "phase",
            "eligible_for_handoff",
            "scoreboard_recorded",
            "authoritative_proof_confirmed",
            "closeout_infra_incomplete",
            "prior_authoritative_proof_available",
            "fresh_closeout_confirmed",
            "authority_conflict",
            "reused_authoritative_verdict",
            "accepted",
            "proved",
            "invalid_output",
            "timeout_budget_mode",
            "timeout_budget_stop_reason",
            "timeout_budget_seconds",
            "timeout_budget_elapsed_seconds",
            "timeout_budget_remaining_seconds",
            "timeout_budget_exhausted",
            "judge_attempt_count",
            "judge_retry_count",
            "error_kind",
        }
    ),
    "closeout_finished": frozenset(
        {
            "score",
            "reused_authoritative_verdicts",
            "authoritative_proofs_confirmed",
            "closeout_infra_incomplete",
            "closeout_infra_unconfirmed",
            "authority_conflicts",
            "remote_settlement_unconfirmed",
        }
    ),
    # Outer Pi/session recovery emits a small status stream from
    # ``agent_recovery.py``.  Keep each event's payload explicit so the
    # profiling side channel cannot grow into a copy of the rich run event.
    # ``exception_type`` and free-form error text are intentionally omitted.
    "agent_recovery_started": frozenset(
        {"episode", "recovery_attempt", "max_restarts"}
    ),
    "agent_recovery_scheduled": frozenset(
        {"episode", "recovery_attempt", "max_restarts", "delay_seconds"}
    ),
    "agent_recovery_succeeded": frozenset(
        {"episode", "recovery_attempt", "max_restarts", "returncode", "timed_out"}
    ),
    "agent_recovery_failure_observed": frozenset(
        {
            "episode",
            "recovery_attempt",
            "max_restarts",
            "returncode",
            "timed_out",
            "cancelled",
            "run_horizon_reached",
            "recoverable",
            "failure_category",
            "failure_source",
        }
    ),
    "agent_recovery_exhausted": frozenset(
        {
            "episode",
            "recovery_attempt",
            "max_restarts",
            "returncode",
            "recoverable",
            "reason",
        }
    ),
}

_LOGGER_GENERIC_FIELDS = frozenset(
    (_TEXT_FIELDS | _SCALAR_FIELDS | _HASH_FIELDS | _IDENTIFIER_FIELDS)
    - _LOGGER_IDENTITY_FIELDS
    - _LOGGER_RESERVED_FIELDS
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _finite_number(value: Any) -> int | float | bool | None:
    # Booleans are meaningful lifecycle counters (accepted, timed_out, etc.)
    # and must not be mistaken for integers and silently dropped.
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if abs(value) <= 10**15 else None
    if isinstance(value, float):
        return round(value, 6) if math.isfinite(value) and abs(value) <= 10**15 else None
    return None


def _safe_identifier(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or len(text) > 256:
        return None
    # ``actor_id``/``task_id`` are correlation handles, but callers may pass
    # an email or account handle as an actor identity.  Never preserve an
    # at-sign-bearing identifier verbatim; the opaque digest keeps joins
    # possible without leaking this common PII shape.
    if "@" in text:
        return "opaque:" + hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:16]
    if _ID_RE.fullmatch(text) or _SHA_RE.fullmatch(text):
        return text
    # Preserve correlation without exposing a path, URL or token-like value.
    return "opaque:" + hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:16]


def _safe_text(value: Any) -> str | None:
    text = str(value or "").replace("\x00", "").strip()
    # Labels should stay small.  A long natural-language value is much more
    # likely to be a prompt/error payload than a useful profiling dimension.
    if not text or len(text.encode("utf-8")) > 160:
        return None
    if _URL_RE.search(text) or _PATH_RE.search(text) or _SECRET_RE.search(text):
        return None
    if "\n" in text or "\r" in text:
        return None
    return text


def _safe_hash(value: Any) -> str | None:
    text = str(value or "").strip()
    return text.lower() if _SHA_RE.fullmatch(text) else None


def _safe_field(key: str, value: Any) -> Any:
    if key in _HASH_FIELDS:
        return _safe_hash(value)
    if key in _IDENTIFIER_FIELDS:
        return _safe_identifier(value)
    if key in _TEXT_FIELDS:
        return _safe_text(value)
    if key in _SCALAR_FIELDS:
        return _finite_number(value)
    return None


def _normalise_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().casefold()
    if text in _TRUTHY:
        return True
    if text in _FALSY:
        return False
    return False


def _interval_from_env(value: Any, default: float = 1.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default
    if not math.isfinite(number):
        number = default
    # Avoid profiling becoming a high-frequency workload of its own.
    return min(60.0, max(0.1, number))


def _path_inside(root: Path, candidate: Path) -> Path | None:
    try:
        resolved_root = root.expanduser().resolve()
        resolved_candidate = candidate.expanduser().resolve(strict=False)
        resolved_candidate.relative_to(resolved_root)
        return resolved_candidate
    except (OSError, RuntimeError, ValueError):
        return None


@dataclass(frozen=True)
class ProfilerSettings:
    """Resolved opt-in profiler settings."""

    enabled: bool = False
    path: Path | None = None
    heartbeat_interval_seconds: float = 1.0

    @classmethod
    def from_environment(
        cls,
        output_dir: Path,
        *,
        enabled: bool | None = None,
    ) -> "ProfilerSettings":
        if enabled is None:
            raw_enabled: Any = os.environ.get("CONTEXTSWARM_PROFILE")
            if raw_enabled is None:
                raw_enabled = os.environ.get("CONTEXTSWARM_RESOURCE_PROFILING")
            if raw_enabled is None:
                raw_enabled = os.environ.get("CONTEXTSWARM_PROFILING")
            active = _normalise_enabled(raw_enabled)
        else:
            active = bool(enabled)
        interval_value = os.environ.get(
            "CONTEXTSWARM_PROFILE_HEARTBEAT_SECONDS",
            os.environ.get("CONTEXTSWARM_PROFILE_INTERVAL_SECONDS", "1"),
        )
        interval = _interval_from_env(interval_value)
        root = Path(output_dir)
        configured = os.environ.get("CONTEXTSWARM_PROFILE_PATH", "").strip()
        path: Path | None = None
        if configured:
            candidate = Path(configured)
            path = _path_inside(root, candidate if candidate.is_absolute() else root / candidate)
        if path is None:
            path = root / PROFILE_FILENAME
        return cls(enabled=active, path=path if active else None, heartbeat_interval_seconds=interval)


class _NullSpan:
    """Allocation-free disabled span object."""

    def __enter__(self) -> "_NullSpan":
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        return None


def _cpu_times(*, thread: bool = False) -> tuple[float, float] | None:
    """Return user/system CPU seconds, or ``None`` when unsupported.

    ``resource.RUSAGE_SELF`` is a process-wide counter.  Falling back to it
    for a requested thread sample silently attributes all process work to one
    span, which is worse than omitting the optional thread metric.  Keep the
    process fallback at zero for portability, but make unsupported thread
    accounting explicit so callers can leave the fields out of the event.
    """

    if _resource is None:
        return None if thread else (0.0, 0.0)
    try:
        if thread:
            resource_kind = getattr(_resource, "RUSAGE_THREAD", None)
            if resource_kind is None:
                return None
        else:
            resource_kind = _resource.RUSAGE_SELF
        usage = _resource.getrusage(resource_kind)
        return float(usage.ru_utime), float(usage.ru_stime)
    except (AttributeError, OSError, ValueError, TypeError):
        return None if thread else (0.0, 0.0)


def _serialized_sample(method: Any) -> Any:
    """Serialize enabled ``sample_now`` calls without touching disabled ones."""

    def wrapper(self: "RunProfiler", *args: Any, **kwargs: Any) -> Any:
        # The fast path deliberately avoids even constructing/acquiring a
        # profiler lock when the feature is disabled.  ``method`` retains its
        # own fail-open guard for compatibility with unusual subclasses.
        if not getattr(self, "enabled", False):
            return method(self, *args, **kwargs)
        lock = getattr(self, "_sample_lock", None)
        if lock is None:
            return method(self, *args, **kwargs)
        with lock:
            return method(self, *args, **kwargs)

    wrapper.__name__ = getattr(method, "__name__", "sample_now")
    wrapper.__doc__ = getattr(method, "__doc__", None)
    return wrapper


def _cpu_affinity_count() -> tuple[int, str]:
    """Return the CPUs available to this process and how the value was found.

    ``os.cpu_count()`` describes the host, not necessarily the container's
    cpuset.  ``sched_getaffinity`` is the closest inexpensive denominator for
    process utilization; retain a portable fallback when it is unavailable or
    returns an unusable set.
    """

    getter = getattr(os, "sched_getaffinity", None)
    if callable(getter):
        try:
            count = len(getter(0))
            if count > 0:
                return max(1, int(count)), "sched_getaffinity"
        except (OSError, TypeError, ValueError):
            pass
    try:
        count = int(os.cpu_count() or 1)
    except (TypeError, ValueError, OverflowError):
        count = 1
    return max(1, count), "os_cpu_count"


class _Span:
    def __init__(
        self,
        profiler: "RunProfiler",
        name: str,
        *,
        task_id: Any = None,
        actor_id: Any = None,
        fields: Mapping[str, Any],
    ) -> None:
        self.profiler = profiler
        self.name = name
        self.task_id = task_id
        self.actor_id = actor_id
        self.fields = dict(fields)
        self.started = time.monotonic()
        process_cpu = _cpu_times()
        self.cpu_user_started, self.cpu_system_started = process_cpu or (0.0, 0.0)
        thread_cpu = _cpu_times(thread=True)
        self.thread_cpu_user_started, self.thread_cpu_system_started = thread_cpu or (0.0, 0.0)
        self.thread_cpu_supported = thread_cpu is not None

    def _emit(self, event: str, **fields: Any) -> None:
        # Keep correlation identities on the dedicated ``emit`` parameters so
        # they pass identifier sanitisation rather than being treated as
        # arbitrary payload fields.  This is what lets a selection/Judge span
        # be joined back to one task/agent without admitting prompt content.
        # ``span`` is instrumentation only.  Injected sinks (including test
        # adapters and future exporters) may raise arbitrary ``BaseException``
        # subclasses; never let one mask the business operation that owns the
        # span.  The surrounding context manager deliberately still returns
        # ``None`` from ``__exit__`` so exceptions raised by the business body
        # propagate normally.
        try:
            self.profiler.emit(
                event,
                task_id=self.task_id,
                actor_id=self.actor_id,
                **fields,
            )
        except BaseException:
            return

    def __enter__(self) -> "_Span":
        self._emit(self.name + ".start", **self.fields)
        return self

    def __exit__(self, exc_type: Any, exc: Any, _tb: Any) -> None:
        ended = time.monotonic()
        cpu_user, cpu_system = _cpu_times()
        thread_cpu = _cpu_times(thread=True)
        payload = dict(self.fields)
        payload.update(
            {
                "wall_seconds": ended - self.started,
                "cpu_user_seconds": max(0.0, cpu_user - self.cpu_user_started),
                "cpu_system_seconds": max(0.0, cpu_system - self.cpu_system_started),
                "status": "error" if exc_type is not None else "ok",
            }
        )
        # Never label process-wide CPU as thread CPU.  On platforms without
        # RUSAGE_THREAD the optional fields are intentionally absent.
        if self.thread_cpu_supported and thread_cpu is not None:
            payload.update(
                {
                    "cpu_thread_user_seconds": max(
                        0.0, thread_cpu[0] - self.thread_cpu_user_started
                    ),
                    "cpu_thread_system_seconds": max(
                        0.0, thread_cpu[1] - self.thread_cpu_system_started
                    ),
                }
            )
        if exc_type is not None:
            payload["error_kind"] = getattr(exc_type, "__name__", "error")
        self._emit(self.name + ".end", **payload)


class RunProfiler:
    """Thread-safe JSONL sink plus optional process/cgroup sampler."""

    def __init__(
        self,
        output_dir: Path,
        *,
        enabled: bool = False,
        path: Path | None = None,
        heartbeat_interval_seconds: float = 1.0,
        run_id: Any = None,
    ) -> None:
        self.output_dir = Path(output_dir).expanduser().resolve()
        requested_path = path if path is not None else self.output_dir / PROFILE_FILENAME
        safe_path = _path_inside(self.output_dir, Path(requested_path))
        self.path = safe_path or self.output_dir / PROFILE_FILENAME
        self.enabled = bool(enabled) and safe_path is not None
        self.heartbeat_interval_seconds = _interval_from_env(heartbeat_interval_seconds)
        # Avoid profiling-only sanitisation work when the feature is off.  A
        # disabled profiler is intentionally a no-op object for normal runs.
        self.run_id = _safe_identifier(run_id) if self.enabled else None
        self._lock = threading.RLock()
        self._sample_lock = threading.RLock()
        self._handle: Any | None = None
        self._closed = False
        self._closing = False
        self._started = False
        # Hidden/production callers may patch the monotonic clock to verify
        # that disabled runs have no profiling-only clock reads.  Do not take
        # one until an enabled profiler is actually used.
        self._started_monotonic = time.monotonic() if self.enabled else 0.0
        self._sequence = 0
        self._sample_sequence = 0
        self._last_sample = 0.0
        self._artifact_snapshot_at = 0.0
        self._artifact_metrics: dict[str, int | bool] = {}
        self._root_pid = os.getpid()
        self._processes: dict[int, dict[str, Any]] = {}
        self._aggregate_peak: dict[str, int | float] = {}
        self._last_observation_at: float | None = None
        self._last_observation_cpu: tuple[float, float] | None = None
        self._last_cgroup_cpu_usage: float | None = None
        self._last_process_cpu: dict[tuple[str, int], tuple[float, float]] = {}
        self._sampler_stop = threading.Event()
        self._sampler_wakeup = threading.Event()
        self._sampler_thread: threading.Thread | None = None

    @classmethod
    def from_environment(
        cls,
        output_dir: Path,
        *,
        enabled: bool | None = None,
        run_id: Any = None,
    ) -> "RunProfiler":
        settings = ProfilerSettings.from_environment(output_dir, enabled=enabled)
        return cls(
            output_dir,
            enabled=settings.enabled,
            path=settings.path,
            heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
            run_id=run_id,
        )

    def start(self, *, run_id: Any = None, root_pid: int | None = None) -> "RunProfiler":
        """Start low-frequency sampling; idempotent and safe to call late."""

        if not self.enabled:
            return self
        try:
            with self._lock:
                if self._closed:
                    return self
                if run_id is not None:
                    self.run_id = _safe_identifier(run_id)
                if root_pid is not None and isinstance(root_pid, int) and root_pid > 0:
                    self._root_pid = root_pid
                if not self._started:
                    self._started = True
                    self._started_monotonic = time.monotonic()
                    self._processes.setdefault(
                        self._root_pid,
                        {
                            "task_id": None,
                            "actor_id": None,
                            "role": "runner",
                            "episode": None,
                            "registered_monotonic": self._started_monotonic,
                            "sample_count": 0,
                            "peak": {},
                            "terminal_sampled": False,
                            "last_sample_monotonic": None,
                        },
                    )
                    self.emit("profile.start", phase="profiling", role="runner")
                    self._sampler_thread = threading.Thread(
                        target=self._sampler_loop,
                        name="contextswarm-profiler",
                        daemon=True,
                    )
                    self._sampler_thread.start()
        except Exception:
            return self
        return self

    def _open(self) -> Any | None:
        if not self.enabled or self._closed:
            return None
        if self._handle is not None:
            return self._handle
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                os.chmod(self.path.parent, 0o700)
            except OSError:
                pass
            if self.path.is_symlink():
                return None
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(self.path, flags, 0o600)
            try:
                os.fchmod(fd, 0o600)
            except OSError:
                pass
            self._handle = os.fdopen(fd, "a", encoding="utf-8", buffering=1)
        except OSError:
            self._handle = None
        return self._handle

    def emit(
        self,
        event: str,
        *,
        run_id: Any = None,
        task_id: Any = None,
        actor_id: Any = None,
        **fields: Any,
    ) -> None:
        """Write one sanitized profiling event, swallowing all sink errors."""

        if not self.enabled:
            return
        try:
            name = str(event or "").strip().casefold()
            if not _EVENT_RE.fullmatch(name):
                return
            row: dict[str, Any] = {
                "schema_version": PROFILE_SCHEMA_VERSION,
                "sequence": 0,
                "at": _utc_now(),
                "monotonic_ns": time.monotonic_ns(),
                "event": name,
            }
            for key, value in (
                ("run_id", self.run_id if run_id is None else run_id),
                ("task_id", task_id),
                ("actor_id", actor_id),
            ):
                safe = _safe_identifier(value)
                if safe is not None:
                    row[key] = safe
            dropped = 0
            for raw_key, value in fields.items():
                key = str(raw_key).strip()
                # Optional lifecycle fields are routinely represented as
                # ``None`` (for example an absent episode or error kind).  A
                # missing value is not an unsafe/dropped payload; omitting it
                # keeps ``dropped_fields`` focused on values that were
                # supplied but rejected by the schema.
                if value is None:
                    continue
                safe_value = _safe_field(key, value)
                if safe_value is None:
                    dropped += 1
                    continue
                row[key] = safe_value
            if dropped:
                row["dropped_fields"] = dropped
            with self._lock:
                handle = self._open()
                if handle is None:
                    return
                self._sequence += 1
                row["sequence"] = self._sequence
                handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
        # Profiling is strictly observational.  A custom sink, monkeypatch,
        # or unusual file object may raise any ``BaseException``; swallowing
        # it here keeps direct ``emit`` callers and all span boundaries
        # fail-open without changing the runner/Judge business exception.
        except BaseException:
            return

    def observe_logger_event(
        self,
        event_type: str,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        """Translate runner lifecycle events into bounded profile rows."""

        if not self.enabled:
            return
        try:
            payload = payload or {}
            event = str(event_type or "").strip().casefold()
            mapped = {
                "run_started": "run.start",
                "run_finished": "run.end",
                "run_error": "run.error",
                "dry_run_finished": "run.dry_end",
                "horizon_started": "horizon.start",
                "horizon_closed": "horizon.end",
                "agent_assigned": "allocation.assignment",
                "agent_finished": "agent.result",
                "agent_refill_scheduled": "agent.refill.scheduled",
                "agent_refill_started": "agent.refill.start",
                "agent_refill_succeeded": "agent.refill.end",
                "agent_refill_exhausted": "agent.refill.exhausted",
                # Outer Pi/session recovery lifecycle is a status stream, not
                # a synthetic span.  Keep these labels separate from the
                # ``agent.*`` process span so an exhausted retry cannot create
                # an orphan ``.end`` in profile audits.
                "agent_recovery_started": "agent.recovery.started",
                "agent_recovery_scheduled": "agent.recovery.scheduled",
                "agent_recovery_succeeded": "agent.recovery.succeeded",
                "agent_recovery_failure_observed": "agent.recovery.failure",
                "agent_recovery_exhausted": "agent.recovery.exhausted",
                "evaluation_backpressure_wait": "judge.queue.wait",
                "evaluation_backpressure_expired": "judge.queue.expired",
                "evaluation_finished": "judge.receipt",
                "allocation_decision": "allocation.decision",
                "allocation_scheduler_finished": "scheduler.invocation.end",
                "selection_runtime_initialized": "selection.runtime.start",
                "selection_runtime_closed": "selection.runtime.end",
                "closeout_started": "closeout.start",
                # The evaluator call itself owns the real
                # ``closeout.evaluation.start/end`` span in runner.py.  This
                # logger notification is only its bounded result projection;
                # mapping it to ``.end`` as well would manufacture a second
                # end and make the audit report an orphan.
                "closeout_evaluation_finished": "closeout.evaluation.receipt",
                "closeout_finished": "closeout.end",
                # JudgeBroker emits the authoritative ``drain.start/end``
                # pair.  The runner's close notification is only a summary;
                # mapping it to another ``drain.end`` would duplicate the
                # lifecycle and create an orphan end in the audit.
                "judge_broker_closed": "drain.complete",
                "broker_drain_timeout": "drain.timeout",
                "broker_close_error": "drain.error",
                "remote_settlement_unconfirmed": "judge.settlement.pending",
            }.get(event)
            if mapped is None:
                if event.startswith(("trace_", "cps_", "artifact_", "scheduler_")):
                    mapped = event.replace("_", ".")
                elif event in {"scoreboard_record", "preflight_failed"}:
                    mapped = event.replace("_", ".")
                else:
                    return
            identities = {
                key: payload.get(key)
                for key in ("run_id", "task_id", "agent_id", "actor_id")
                if key in payload
            }
            # ``agent_id`` is an input alias for ``actor_id``.  Keep the
            # explicit actor identity when both are present, and always
            # remove both source keys from the payload projection.  Testing
            # membership in the mutated ``identities`` mapping here would
            # re-admit ``agent_id`` after it was popped.
            if "agent_id" in identities:
                if "actor_id" not in identities or identities.get("actor_id") is None:
                    identities["actor_id"] = identities["agent_id"]
                identities.pop("agent_id", None)
            identity_keys = _LOGGER_IDENTITY_FIELDS
            allowed = _LOGGER_EVENT_FIELDS.get(event, _LOGGER_GENERIC_FIELDS)
            fields: dict[str, Any] = {}
            for raw_key, value in payload.items():
                key = str(raw_key).strip()
                if (
                    key in identity_keys
                    or key in _LOGGER_RESERVED_FIELDS
                    or key not in allowed
                ):
                    continue
                fields[key] = value
            # ``run_started`` carries policy/selection under nested manifest
            # objects.  Extract only their scalar labels; never forward the
            # nested objects themselves.
            if event == "run_started":
                allocation = payload.get("allocation")
                if isinstance(allocation, Mapping):
                    if "allocation_policy" not in fields and "policy" in allocation:
                        fields["allocation_policy"] = allocation.get("policy")
                selection = payload.get("selection")
                if isinstance(selection, Mapping) and "selection_enabled" not in fields:
                    fields["selection_enabled"] = selection.get("enabled")
            # Recovery events are intentionally represented as status rows,
            # rather than guessed ``.start``/``.end`` spans: one logical retry
            # can fail, exhaust, or be cancelled without a matching pair.
            # Add stable dimensions so a profile query can group all retries
            # without parsing event names or retaining rich run payloads.
            recovery_status = {
                "agent_recovery_started": "started",
                "agent_recovery_scheduled": "scheduled",
                "agent_recovery_succeeded": "succeeded",
                "agent_recovery_failure_observed": "failure",
                "agent_recovery_exhausted": "exhausted",
            }.get(event)
            if recovery_status is not None:
                fields["recovery"] = "agent_session"
                fields["status"] = recovery_status
            self.emit(mapped, **identities, **fields)
            if event in {
                "run_finished",
                "run_error",
                "dry_run_finished",
                # No later runner lifecycle event is guaranteed after a
                # preflight admission failure; close here so the sampler
                # thread and file descriptor cannot survive the failed run.
                "preflight_failed",
            }:
                self.close()
        # Keep the logger-to-profile translation boundary fail-open for
        # arbitrary injected sink/field failures.  The ordinary RunLogger
        # event has already been persisted before this hook is called, and no
        # business exception should be replaced by diagnostics.
        except BaseException:
            return

    def observe_pi_event(
        self,
        event_type: str,
        *,
        task_id: Any = None,
        actor_id: Any = None,
        episode: Any = None,
        **fields: Any,
    ) -> None:
        """Record model/tool lifecycle events without RPC content."""

        event = str(event_type or "").strip().casefold()
        mapped = {
            "message_start": "model.request.start",
            "message_end": "model.request.end",
            "tool_execution_start": "tool.start",
            "tool_execution_end": "tool.end",
            "tool_call": "tool.start",
            "tool_result": "tool.end",
            "agent_end": "agent.rpc.end",
            "agent_settled": "agent.rpc.settled",
        }.get(event)
        if mapped is None:
            return
        self.emit(mapped, task_id=task_id, actor_id=actor_id, episode=episode, **fields)

    def heartbeat(
        self,
        *,
        task_id: Any = None,
        actor_id: Any = None,
        episode: Any = None,
        force: bool = False,
        **fields: Any,
    ) -> None:
        # Keep the disabled path allocation/clock free.  In particular, a
        # Pi heartbeat is emitted for every attempt, so even a no-op call here
        # must not acquire the sampler lock when profiling is off.
        if not self.enabled:
            return
        self.emit("agent.heartbeat", task_id=task_id, actor_id=actor_id, episode=episode, **fields)

        # ``force=True`` is the Pi finally-boundary heartbeat.  Calling the
        # aggregate sampler from that boundary used to walk the complete run
        # process tree and artifact directory once per attempt.  A PID-bearing
        # forced heartbeat now takes a bounded, attributable tree sample;
        # run-wide/cgroup/artifact accounting remains owned by the periodic
        # sampler and explicit closeout.  Callers without a PID retain a
        # rate-limited aggregate observation (never a forced artifact walk).
        pid = fields.get("pid")
        if force and isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
            self.sample_process_tree(
                pid,
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                process_alive=fields.get("process_alive"),
                terminal=True,
            )
            return
        if force:
            # A failed-before-spawn attempt has no attributable tree.  Defer
            # the run-wide observation to the periodic sampler/closeout rather
            # than allowing every such attempt to trigger a global scan.
            return
        self.sample_now(force=False)

    def register_process(
        self,
        pid: int,
        *,
        task_id: Any = None,
        actor_id: Any = None,
        role: str = "solver",
        episode: Any = None,
    ) -> None:
        if not self.enabled:
            return
        try:
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                return
            with self._lock:
                self._processes[pid] = {
                    "task_id": task_id,
                    "actor_id": actor_id,
                    "role": role,
                    "episode": episode,
                    "registered_monotonic": time.monotonic(),
                    "sample_count": 0,
                    "peak": {},
                    "terminal_sampled": False,
                    "last_sample_monotonic": None,
                }
            self.emit(
                "resource.process.register",
                task_id=task_id,
                actor_id=actor_id,
                pid=pid,
                role=role,
                episode=episode,
            )
        except Exception:
            return

    def unregister_process(self, pid: int, *, status: str = "exited") -> None:
        if not self.enabled:
            return
        try:
            # ``PiAgent`` normally calls ``heartbeat(force=True)`` first.  A
            # direct unregister (or an exception before that callback) still
            # gets one best-effort terminal row, while the metadata flag keeps
            # the normal heartbeat+unregister sequence from duplicating it.
            with self._lock:
                current = self._processes.get(pid)
                terminal_sampled = bool(current and current.get("terminal_sampled"))
            if current is not None and not terminal_sampled:
                self.sample_process_tree(pid, terminal=True)
            with self._lock:
                metadata = self._processes.pop(pid, None)
                # A short-lived agent PID may be recycled.  Remove both CPU
                # baselines so the next registration starts a fresh delta and
                # stale entries do not accumulate for the run's lifetime.
                self._last_process_cpu.pop(("self", pid), None)
                self._last_process_cpu.pop(("tree", pid), None)
            if metadata is not None:
                self.emit(
                    "resource.process.unregister",
                    task_id=metadata.get("task_id"),
                    actor_id=metadata.get("actor_id"),
                    pid=pid,
                    role=metadata.get("role", "solver"),
                    episode=metadata.get("episode"),
                    status=status,
                    elapsed_seconds=max(
                        0.0,
                        time.monotonic()
                        - float(metadata.get("registered_monotonic", time.monotonic())),
                    ),
                    sample_count=int(metadata.get("sample_count", 0) or 0),
                    **dict(metadata.get("peak") or {}),
                )
        except Exception:
            return

    register = register_process
    unregister = unregister_process

    @staticmethod
    def _children(pid: int) -> tuple[int, ...]:
        try:
            raw = Path(f"/proc/{pid}/task/{pid}/children").read_text(encoding="ascii")
            return tuple(int(token) for token in raw.split() if token.isdigit() and int(token) > 0)
        except (OSError, ValueError):
            return ()

    @classmethod
    def _tree(cls, root: int) -> tuple[int, ...]:
        seen: set[int] = set()
        queue = [root]
        while queue:
            pid = queue.pop(0)
            if pid in seen or pid <= 0:
                continue
            seen.add(pid)
            queue.extend(cls._children(pid))
        return tuple(sorted(seen))

    @classmethod
    def _bounded_tree(
        cls,
        root: int,
        *,
        limit: int = _TERMINAL_TREE_MAX_PROCESSES,
    ) -> tuple[tuple[int, ...], bool]:
        """Return a bounded process tree and whether traversal was clipped.

        The regular run sampler intentionally uses :meth:`_tree` to retain a
        complete run-level view.  Attempt-final sampling is a different
        workload: it runs once for every Pi process and must not turn a large
        descendant tree into another high-churn source.  Breadth-first
        traversal stops before reading children beyond ``limit``; no command,
        path, or process metadata is retained.
        """

        try:
            maximum = int(limit)
        except (TypeError, ValueError, OverflowError):
            maximum = _TERMINAL_TREE_MAX_PROCESSES
        maximum = max(1, min(_TERMINAL_TREE_MAX_PROCESSES, maximum))
        seen: set[int] = set()
        queue = [root]
        truncated = False
        while queue:
            pid = queue.pop(0)
            if pid in seen or pid <= 0:
                continue
            if len(seen) >= maximum:
                truncated = True
                break
            seen.add(pid)
            children = cls._children(pid)
            if len(seen) + len(queue) + len(children) > maximum:
                # Keep only enough children to fill the bounded frontier, and
                # remember that at least one descendant was omitted.
                allowed = max(0, maximum - len(seen) - len(queue))
                if len(children) > allowed:
                    truncated = True
                    children = children[:allowed]
            queue.extend(children)
        if queue:
            truncated = True
        return tuple(sorted(seen)), truncated

    @staticmethod
    def _proc_snapshot(pid: int) -> dict[str, Any] | None:
        """Read bounded metrics for one Linux process."""

        if os.name != "posix":
            return None
        try:
            stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            closing = stat_line.rfind(")")
            if closing < 0:
                return None
            fields = stat_line[closing + 2 :].split()
            if len(fields) < 22:
                return None
            hz = float(os.sysconf("SC_CLK_TCK"))
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
            result: dict[str, Any] = {
                "pid": pid,
                "process_state": fields[0],
                "cpu_user_seconds": max(0.0, int(fields[11]) / hz),
                "cpu_system_seconds": max(0.0, int(fields[12]) / hz),
                "thread_count": max(0, int(fields[17])),
                "rss_bytes": max(0, int(fields[21]) * page_size),
                "pss_bytes": 0,
                "context_switches": 0,
                "fd_count": 0,
            }
            try:
                rollup = Path(f"/proc/{pid}/smaps_rollup").read_text(encoding="ascii")
                for line in rollup.splitlines():
                    if line.startswith("Pss:"):
                        result["pss_bytes"] = max(0, int(line.split()[1]) * 1024)
                        break
            except (OSError, ValueError, IndexError):
                result["pss_bytes"] = result["rss_bytes"]
            try:
                status = Path(f"/proc/{pid}/status").read_text(encoding="ascii")
                voluntary = nonvoluntary = 0
                for line in status.splitlines():
                    if line.startswith("voluntary_ctxt_switches:"):
                        voluntary = int(line.split()[-1])
                    elif line.startswith("nonvoluntary_ctxt_switches:"):
                        nonvoluntary = int(line.split()[-1])
                result["context_switches"] = max(0, voluntary + nonvoluntary)
            except (OSError, ValueError, IndexError):
                pass
            try:
                result["fd_count"] = len(os.listdir(f"/proc/{pid}/fd"))
            except OSError:
                pass
            return result
        except (OSError, ValueError, IndexError, OverflowError):
            return None

    @staticmethod
    def _cgroup_candidates(cgroup_text: str | None = None) -> tuple[Path, ...]:
        """Return the current cgroup first, with the hierarchy root as fallback.

        The cgroup path is used only for local file lookup and is never returned
        in an event.  ``0::`` is the cgroup-v2 controller line; its relative
        scope must win over the hierarchy root because the two can have very
        different memory counters in a shared host.
        """

        base = Path("/sys/fs/cgroup")
        if os.name != "posix":
            return ()
        if cgroup_text is None:
            try:
                cgroup_text = Path("/proc/self/cgroup").read_text(encoding="ascii")
            except OSError:
                cgroup_text = ""
        candidates: list[Path] = []
        for line in str(cgroup_text).splitlines():
            if not line.startswith("0::"):
                continue
            relative = line[3:].strip().lstrip("/")
            if relative and ".." not in Path(relative).parts:
                scoped = base / relative
                try:
                    scoped.relative_to(base)
                except ValueError:
                    pass
                else:
                    candidates.append(scoped)
            break
        if base not in candidates:
            candidates.append(base)
        return tuple(candidates)

    @staticmethod
    def _cgroup_snapshot() -> dict[str, Any]:
        """Read cgroup v2 counters without exposing the cgroup path."""

        if os.name != "posix":
            return {}
        try:
            candidates = RunProfiler._cgroup_candidates()
        except Exception:
            return {}
        result: dict[str, Any] = {}
        # Each cgroup file has its own first-readable-scope marker.  A valid
        # child scope value (including zero or an unlimited ``cpu.max``) must
        # never be overwritten by a hierarchy-root value.
        memory_current_read = False
        memory_peak_read = False
        memory_events_read = False
        cpu_stat_read = False
        cpu_max_read = False
        for root in candidates:
            try:
                if not root.is_dir():
                    continue
                if not memory_current_read:
                    try:
                        raw = (root / "memory.current").read_text(encoding="ascii")
                        memory_current_read = True
                        value = int(raw.strip())
                        if value >= 0:
                            result["memory_current_bytes"] = value
                    except (OSError, ValueError, TypeError, OverflowError):
                        pass
                if not memory_peak_read:
                    try:
                        raw = (root / "memory.peak").read_text(encoding="ascii")
                        memory_peak_read = True
                        value = int(raw.strip())
                        if value >= 0:
                            result["memory_peak_bytes"] = value
                    except (OSError, ValueError, TypeError, OverflowError):
                        pass
                if not memory_events_read:
                    try:
                        memory_events = (root / "memory.events").read_text(encoding="ascii")
                        memory_events_read = True
                    except (OSError, UnicodeError):
                        memory_events = None
                    if memory_events is not None:
                        total = 0
                        oom_kill: int | None = None
                        for line in memory_events.splitlines():
                            parts = line.split()
                            if len(parts) != 2:
                                continue
                            try:
                                count = int(parts[1])
                            except (TypeError, ValueError, OverflowError):
                                continue
                            if count < 0:
                                continue
                            total += count
                            if parts[0] == "oom_kill":
                                oom_kill = count
                        result["memory_events_count"] = total
                        if oom_kill is not None:
                            result["oom_kill_count"] = oom_kill
                if not cpu_stat_read:
                    try:
                        cpu_stat = (root / "cpu.stat").read_text(encoding="ascii")
                        cpu_stat_read = True
                    except (OSError, UnicodeError):
                        cpu_stat = None
                    if cpu_stat is not None:
                        usage_usec: int | None = None
                        throttled_usec: int | None = None
                        for line in cpu_stat.splitlines():
                            parts = line.split()
                            if len(parts) != 2:
                                continue
                            try:
                                value = int(parts[1])
                            except (TypeError, ValueError, OverflowError):
                                continue
                            if value < 0:
                                continue
                            if parts[0] == "usage_usec":
                                usage_usec = value
                            elif parts[0] == "throttled_usec":
                                throttled_usec = value
                        if usage_usec is not None:
                            result["cgroup_cpu_usage_seconds"] = usage_usec / 1_000_000.0
                        if throttled_usec is not None:
                            result["cpu_throttled_seconds"] = throttled_usec / 1_000_000.0
                if not cpu_max_read:
                    try:
                        cpu_max = (root / "cpu.max").read_text(encoding="ascii").split()
                        cpu_max_read = True
                    except (OSError, UnicodeError):
                        cpu_max = ()
                    if len(cpu_max) >= 2 and cpu_max[0] != "max":
                        try:
                            quota = int(cpu_max[0])
                            period = int(cpu_max[1])
                        except (TypeError, ValueError, OverflowError):
                            quota = period = 0
                        if quota > 0 and period > 0:
                            result["cpu_quota_cores"] = quota / period
            except (OSError, RuntimeError, TypeError, ValueError):
                continue
        return result

    def _artifact_snapshot(self) -> dict[str, int | bool]:
        """Collect bounded artifact totals for a run-level sample.

        Artifact accounting is intentionally lower priority than process
        liveness.  A high-concurrency run can leave a very large tree of
        session/SQLite files behind, so an unbounded ``os.walk`` would make
        profiling itself scale with the number of attempts.  The snapshot is
        rate-limited by :meth:`sample_now` and this walk has independent file
        and directory caps.  ``artifact_scan_truncated`` makes a capped total
        explicit instead of presenting it as an exact run total.
        """

        result: dict[str, int | bool] = {
            "artifact_bytes": 0,
            "sqlite_bytes": 0,
            "wal_bytes": 0,
            "profile_bytes": 0,
            "disk_free_bytes": 0,
            "artifact_files_scanned": 0,
            "artifact_directories_scanned": 0,
            "artifact_scan_truncated": False,
        }
        try:
            files_scanned = 0
            directories_scanned = 0
            truncated = False
            for root, dirs, files in os.walk(self.output_dir):
                if directories_scanned >= _ARTIFACT_MAX_DIRECTORIES:
                    truncated = True
                    break
                safe_dirs: list[str] = []
                for name in dirs:
                    if directories_scanned >= _ARTIFACT_MAX_DIRECTORIES:
                        truncated = True
                        break
                    # Count directory entries considered, not only entries
                    # that survive the symlink filter.  Otherwise a tree full
                    # of unreadable/symlinked directories could still make
                    # the supposedly bounded walk proportional to its size.
                    directories_scanned += 1
                    try:
                        if (Path(root) / name).is_symlink():
                            continue
                    except OSError:
                        continue
                    safe_dirs.append(name)
                dirs[:] = safe_dirs
                for name in files:
                    if files_scanned >= _ARTIFACT_MAX_FILES:
                        truncated = True
                        break
                    # Count attempted entries before stat(), so repeated
                    # permission/race failures cannot bypass the cap.
                    files_scanned += 1
                    path = Path(root) / name
                    try:
                        size = max(0, int(path.stat().st_size))
                    except OSError:
                        continue
                    result["artifact_bytes"] += size
                    lowered = name.casefold()
                    if lowered.endswith(".sqlite3") or lowered.endswith(".sqlite"):
                        result["sqlite_bytes"] += size
                    elif lowered.endswith("-wal") or lowered.endswith("-shm"):
                        result["wal_bytes"] += size
                    if path == self.path:
                        result["profile_bytes"] += size
                if truncated:
                    break
            statvfs = os.statvfs(self.output_dir)
            result["disk_free_bytes"] = max(0, int(statvfs.f_bavail * statvfs.f_frsize))
            result["artifact_files_scanned"] = files_scanned
            result["artifact_directories_scanned"] = directories_scanned
            result["artifact_scan_truncated"] = truncated
        except (OSError, ValueError, OverflowError):
            pass
        return result

    @staticmethod
    def _peak_update(
        peak: dict[str, int | float],
        values: Mapping[str, Any],
        *,
        prefix: str = "peak_",
    ) -> None:
        """Update a bounded max-only resource summary.

        Keeping this state in the profiler (rather than asking a report to
        reconstruct peaks from samples) is important for short-lived Pi
        processes: the final unregister row remains useful even when the
        process disappeared between two sampler ticks.  Only the explicitly
        named scalar counters are copied.
        """

        for source, target in (
            ("rss_bytes", "rss_bytes"),
            ("pss_bytes", "pss_bytes"),
            ("process_count", "process_count"),
            ("process_tree_count", "process_tree_count"),
            ("thread_count", "thread_count"),
            ("fd_count", "fd_count"),
            ("context_switches", "context_switches"),
            ("cpu_user_seconds", "cpu_user_seconds"),
            ("cpu_system_seconds", "cpu_system_seconds"),
            ("cpu_utilization", "cpu_utilization"),
            ("cpu_utilization_cgroup", "cpu_utilization_cgroup"),
            ("memory_current_bytes", "memory_current_bytes"),
            ("memory_peak_bytes", "memory_peak_bytes"),
        ):
            value = values.get(source)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            try:
                number = float(value) if isinstance(value, float) else int(value)
            except (TypeError, ValueError, OverflowError):
                continue
            if number < 0 or not math.isfinite(float(number)):
                continue
            key = prefix + target
            previous = peak.get(key, 0)
            if number > previous:
                peak[key] = number

    def sample_process_tree(
        self,
        pid: int,
        *,
        task_id: Any = None,
        actor_id: Any = None,
        episode: Any = None,
        process_alive: Any = None,
        terminal: bool = False,
    ) -> dict[str, Any]:
        """Emit a bounded sample for one registered process tree.

        This is deliberately separate from :meth:`sample_now`.  Pi invokes a
        forced heartbeat in every attempt's ``finally`` block; using the
        aggregate sampler there made each short-lived attempt repeat all root
        traversals, cgroup reads, and run-directory artifact stats.  This
        helper reads only the requested tree (capped at
        ``_TERMINAL_TREE_MAX_PROCESSES``), never reads cgroups/artifacts, and
        updates only that process's peak metadata.  ``terminal`` samples are
        de-duplicated per registration so a heartbeat followed by
        ``unregister_process`` does not do the work twice.

        A disappeared PID is still represented by a terminal row with
        ``process_count=0`` and ``sample_count`` incremented.  Consumers can
        therefore distinguish “attempt ended before a proc snapshot” from a
        missing instrumentation boundary instead of silently treating it as
        zero resource usage.
        """

        if not self.enabled:
            return {}
        try:
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                return {}
            with self._sample_lock:
                with self._lock:
                    if self._closed:
                        return {}
                    metadata = self._processes.get(pid)
                    if metadata is None:
                        return {}
                    if terminal and metadata.get("terminal_sampled"):
                        return {}
                    # Reserve terminal sampling before touching /proc.  If an
                    # emitter fails, repeating the expensive read in another
                    # racing callback would not improve the diagnostic result.
                    if terminal:
                        metadata["terminal_sampled"] = True
                    metadata_task_id = metadata.get("task_id")
                    metadata_actor_id = metadata.get("actor_id")
                    metadata_episode = metadata.get("episode")
                    metadata_role = metadata.get("role", "solver")
                    previous_at = metadata.get("last_sample_monotonic")
                    metadata["last_sample_monotonic"] = time.monotonic()

                sample_started = time.monotonic()
                tree, tree_truncated = self._bounded_tree(pid)
                snapshots = {
                    member_pid: snapshot
                    for member_pid in tree
                    if (snapshot := self._proc_snapshot(member_pid)) is not None
                }
                members = list(snapshots.values())
                now = time.monotonic()
                try:
                    sample_interval = (
                        max(0.0, now - float(previous_at))
                        if previous_at is not None
                        else 0.0
                    )
                except (TypeError, ValueError, OverflowError):
                    sample_interval = 0.0
                metrics: dict[str, Any] = {
                    "process_count": len(members),
                    "process_tree_count": len(tree),
                    "thread_count": sum(int(item.get("thread_count", 0)) for item in members),
                    "fd_count": sum(int(item.get("fd_count", 0)) for item in members),
                    "rss_bytes": sum(int(item.get("rss_bytes", 0)) for item in members),
                    "pss_bytes": sum(int(item.get("pss_bytes", 0)) for item in members),
                    "cpu_user_seconds": round(
                        sum(float(item.get("cpu_user_seconds", 0.0)) for item in members),
                        6,
                    ),
                    "cpu_system_seconds": round(
                        sum(float(item.get("cpu_system_seconds", 0.0)) for item in members),
                        6,
                    ),
                    "context_switches": sum(
                        int(item.get("context_switches", 0)) for item in members
                    ),
                    "sample_interval_seconds": sample_interval,
                    "process_tree_truncated": tree_truncated,
                }
                tree_user = float(metrics["cpu_user_seconds"])
                tree_system = float(metrics["cpu_system_seconds"])
                with self._lock:
                    previous_cpu = self._last_process_cpu.get(("tree", pid))
                    self._last_process_cpu[("tree", pid)] = (tree_user, tree_system)
                user_delta = (
                    max(0.0, tree_user - previous_cpu[0])
                    if previous_cpu is not None
                    else 0.0
                )
                system_delta = (
                    max(0.0, tree_system - previous_cpu[1])
                    if previous_cpu is not None
                    else 0.0
                )
                cpu_count, _cpu_source = _cpu_affinity_count()
                metrics.update(
                    {
                        "cpu_user_delta_seconds": user_delta,
                        "cpu_system_delta_seconds": system_delta,
                        "cpu_utilization": (
                            (user_delta + system_delta) / (sample_interval * cpu_count)
                            if sample_interval > 0
                            else 0.0
                        ),
                    }
                )
                with self._lock:
                    live_metadata = self._processes.get(pid)
                    if live_metadata is not None:
                        live_metadata["sample_count"] = int(
                            live_metadata.get("sample_count", 0) or 0
                        ) + 1
                        peak = live_metadata.setdefault("peak", {})
                        self._peak_update(peak, metrics)
                        root_peak = dict(peak)
                    else:
                        root_peak = {}
                observed_alive = (
                    process_alive
                    if isinstance(process_alive, bool)
                    else bool(members)
                )
                event_task_id = task_id if task_id is not None else metadata_task_id
                event_actor_id = actor_id if actor_id is not None else metadata_actor_id
                event_episode = episode if episode is not None else metadata_episode
                metrics["sample_seconds"] = max(
                    0.0, time.monotonic() - sample_started
                )
                self.emit(
                    "resource.process",
                    task_id=event_task_id,
                    actor_id=event_actor_id,
                    pid=pid,
                    role=metadata_role,
                    episode=event_episode,
                    sample_kind="terminal" if terminal else "attempt",
                    component="pi_process_tree",
                    **metrics,
                    **root_peak,
                    process_alive=observed_alive,
                )
                return metrics
        except BaseException:
            # Profiling is fail-open.  A /proc race, sampler monkeypatch, or
            # sink failure must never alter the agent's result path.
            return {}

    @_serialized_sample
    def sample_now(
        self,
        *,
        force: bool = False,
        _allow_closing: bool = False,
        _refresh_artifacts: bool = False,
    ) -> dict[str, Any]:
        """Take and emit one aggregate process/cgroup sample.

        ``force`` bypasses the process-sample interval, but does not by itself
        bypass the lower-rate artifact snapshot interval.  Only closeout sets
        the private ``_refresh_artifacts`` flag, keeping explicit per-attempt
        callers from turning a forced sample into an ``os.walk`` storm.
        """

        if not self.enabled:
            return {}
        try:
            sample_started = time.monotonic()
            now = time.monotonic()
            with self._lock:
                if self._closed or (self._closing and not _allow_closing):
                    return {}
                if not force and self._last_sample and now - self._last_sample < self.heartbeat_interval_seconds:
                    return {}
                self._last_sample = now
                registered = dict(self._processes)
            # The run aggregate is the total visible process tree, so it must
            # always include the profiler/runner root even when a caller has
            # not registered that PID (for example, a late-started profiler or
            # a short-lived test adapter).  Registered solver/scheduler roots
            # are added in stable order and de-duplicated; their trees may
            # overlap with the runner tree and ``all_pids`` below deliberately
            # removes that overlap before aggregation.
            roots = tuple(dict.fromkeys((self._root_pid, *registered)))
            all_pids: set[int] = set()
            root_pids: dict[int, tuple[int, ...]] = {}
            for root in roots:
                tree = self._tree(root)
                root_pids[root] = tree
                all_pids.update(tree)
            proc_snapshot_started = time.monotonic()
            snapshots = {pid: snap for pid in all_pids if (snap := self._proc_snapshot(pid)) is not None}
            proc_snapshot_seconds = max(0.0, time.monotonic() - proc_snapshot_started)
            aggregate: dict[str, Any] = {
                "process_count": len(snapshots),
                "process_tree_count": len(all_pids),
                "thread_count": sum(int(item.get("thread_count", 0)) for item in snapshots.values()),
                "fd_count": sum(int(item.get("fd_count", 0)) for item in snapshots.values()),
                "rss_bytes": sum(int(item.get("rss_bytes", 0)) for item in snapshots.values()),
                "pss_bytes": sum(int(item.get("pss_bytes", 0)) for item in snapshots.values()),
                "cpu_user_seconds": round(sum(float(item.get("cpu_user_seconds", 0.0)) for item in snapshots.values()), 6),
                "cpu_system_seconds": round(sum(float(item.get("cpu_system_seconds", 0.0)) for item in snapshots.values()), 6),
                "context_switches": sum(int(item.get("context_switches", 0)) for item in snapshots.values()),
                "monotonic_elapsed_seconds": now - self._started_monotonic,
            }
            with self._lock:
                previous_at = self._last_observation_at
                previous_cpu = self._last_observation_cpu
                self._last_observation_at = now
                self._last_observation_cpu = (
                    float(aggregate["cpu_user_seconds"]),
                    float(aggregate["cpu_system_seconds"]),
                )
            sample_interval = (
                max(0.0, now - previous_at) if previous_at is not None else 0.0
            )
            user_delta = (
                max(0.0, float(aggregate["cpu_user_seconds"]) - previous_cpu[0])
                if previous_cpu is not None
                else 0.0
            )
            system_delta = (
                max(0.0, float(aggregate["cpu_system_seconds"]) - previous_cpu[1])
                if previous_cpu is not None
                else 0.0
            )
            cpu_count, cpu_denominator_source = _cpu_affinity_count()
            aggregate.update(
                {
                    "sample_interval_seconds": sample_interval,
                    "cpu_user_delta_seconds": user_delta,
                    "cpu_system_delta_seconds": system_delta,
                    "cpu_affinity_count": cpu_count,
                    "cpu_denominator_source": cpu_denominator_source,
                    "cpu_utilization": (
                        (user_delta + system_delta) / (sample_interval * cpu_count)
                        if sample_interval > 0
                        else 0.0
                    ),
                }
            )
            cgroup_snapshot = self._cgroup_snapshot()
            aggregate.update(cgroup_snapshot)
            cgroup_usage = cgroup_snapshot.get("cgroup_cpu_usage_seconds")
            cgroup_usage_delta: float | None = None
            try:
                current_cgroup_usage = float(cgroup_usage)
                if math.isfinite(current_cgroup_usage) and current_cgroup_usage >= 0:
                    with self._lock:
                        previous_cgroup_usage = self._last_cgroup_cpu_usage
                        self._last_cgroup_cpu_usage = current_cgroup_usage
                    if previous_cgroup_usage is not None:
                        cgroup_usage_delta = max(
                            0.0, current_cgroup_usage - previous_cgroup_usage
                        )
            except (TypeError, ValueError, OverflowError):
                pass
            if cgroup_usage_delta is not None:
                aggregate["cgroup_cpu_usage_delta_seconds"] = cgroup_usage_delta
            quota_cores = aggregate.get("cpu_quota_cores")
            cgroup_denominator_source = "cpu_affinity"
            try:
                quota_cores = float(quota_cores)
            except (TypeError, ValueError, OverflowError):
                quota_cores = float(cpu_count)
            if not math.isfinite(quota_cores) or quota_cores <= 0:
                quota_cores = float(cpu_count)
            else:
                cgroup_denominator_source = "cgroup_quota"
            aggregate["cgroup_cpu_denominator_source"] = cgroup_denominator_source
            aggregate["cpu_utilization_cgroup"] = (
                (user_delta + system_delta) / (sample_interval * quota_cores)
                if sample_interval > 0
                else 0.0
            )
            if cgroup_usage_delta is not None:
                aggregate["cgroup_cpu_utilization"] = (
                    cgroup_usage_delta / (sample_interval * quota_cores)
                    if sample_interval > 0
                    else 0.0
                )
            # Directory-wide stat calls are materially more expensive than
            # the process counters (especially after a high-concurrency run
            # has produced hundreds of worker artifacts).  Keep resource
            # samples frequent, but refresh artifact/WAL totals at a bounded
            # lower rate and always force a fresh snapshot for closeout.
            artifact_interval = max(5.0, self.heartbeat_interval_seconds * 5.0)
            with self._lock:
                artifact_metrics = dict(self._artifact_metrics)
                refresh_artifacts = (
                    self._artifact_snapshot_at <= 0.0
                    or now - self._artifact_snapshot_at >= artifact_interval
                    or (_refresh_artifacts and force)
                )
            if refresh_artifacts:
                artifact_snapshot_started = time.monotonic()
                fresh_artifacts = self._artifact_snapshot()
                artifact_snapshot_seconds = max(
                    0.0, time.monotonic() - artifact_snapshot_started
                )
                with self._lock:
                    self._artifact_metrics = dict(fresh_artifacts)
                    self._artifact_snapshot_at = now
                    artifact_metrics = dict(fresh_artifacts)
            else:
                artifact_snapshot_seconds = 0.0
            aggregate.update(artifact_metrics)
            # Keep run-level maxima alongside the instantaneous sample.  These
            # fields let a single high-concurrency run answer both “what was
            # the peak?” and “what was the process-tree shape at that point?”.
            with self._lock:
                self._peak_update(self._aggregate_peak, aggregate)
            aggregate.update(self._aggregate_peak)
            aggregate["proc_snapshot_seconds"] = proc_snapshot_seconds
            aggregate["artifact_snapshot_seconds"] = artifact_snapshot_seconds
            aggregate["sample_seconds"] = max(0.0, time.monotonic() - sample_started)
            # Multiple agent threads can request a forced boundary sample at
            # nearly the same time.  Allocate the sequence number under the
            # same lock used by ``emit`` so the JSONL stream remains strictly
            # monotonic even under concurrent closeout callbacks.
            with self._lock:
                self._sample_sequence += 1
                sample_sequence = self._sample_sequence
            aggregate["snapshot_count"] = sample_sequence
            self.emit("resource.sample", role="run", sample_kind="aggregate", **aggregate)
            # The root runner process is the wrapper side of the
            # agent-vs-wrapper split.  Emit it separately from the descendant
            # tree rows: ``resource.process`` intentionally includes a
            # registered Pi tree, so that row cannot be used as pure wrapper
            # memory/CPU.  This direct-process row is still observational and
            # contains no command/path data.
            root_self = snapshots.get(self._root_pid)
            if root_self is not None:
                root_user = float(root_self.get("cpu_user_seconds", 0.0))
                root_system = float(root_self.get("cpu_system_seconds", 0.0))
                with self._lock:
                    previous_root_cpu = self._last_process_cpu.get(("self", self._root_pid))
                    self._last_process_cpu[("self", self._root_pid)] = (root_user, root_system)
                root_user_delta = (
                    max(0.0, root_user - previous_root_cpu[0])
                    if previous_root_cpu is not None
                    else 0.0
                )
                root_system_delta = (
                    max(0.0, root_system - previous_root_cpu[1])
                    if previous_root_cpu is not None
                    else 0.0
                )
                self.emit(
                    "resource.process.self",
                    pid=self._root_pid,
                    role="runner",
                    component="runner_process",
                    process_count=1,
                    process_tree_count=1,
                    thread_count=int(root_self.get("thread_count", 0)),
                    fd_count=int(root_self.get("fd_count", 0)),
                    rss_bytes=int(root_self.get("rss_bytes", 0)),
                    pss_bytes=int(root_self.get("pss_bytes", 0)),
                    cpu_user_seconds=float(root_self.get("cpu_user_seconds", 0.0)),
                    cpu_system_seconds=float(root_self.get("cpu_system_seconds", 0.0)),
                    cpu_user_delta_seconds=root_user_delta,
                    cpu_system_delta_seconds=root_system_delta,
                    sample_interval_seconds=sample_interval,
                    cpu_utilization=(
                        (root_user_delta + root_system_delta)
                        / (sample_interval * cpu_count)
                        if sample_interval > 0
                        else 0.0
                    ),
                    context_switches=int(root_self.get("context_switches", 0)),
                    process_alive=True,
                )
            for root, tree in root_pids.items():
                metadata = registered.get(root, {"task_id": None, "actor_id": None, "role": "runner"})
                members = [snapshots[pid] for pid in tree if pid in snapshots]
                if not members:
                    continue
                root_metrics = {
                    "process_count": len(members),
                    "process_tree_count": len(tree),
                    "thread_count": sum(int(item.get("thread_count", 0)) for item in members),
                    "fd_count": sum(int(item.get("fd_count", 0)) for item in members),
                    "rss_bytes": sum(int(item.get("rss_bytes", 0)) for item in members),
                    "pss_bytes": sum(int(item.get("pss_bytes", 0)) for item in members),
                    "cpu_user_seconds": round(
                        sum(float(item.get("cpu_user_seconds", 0.0)) for item in members), 6
                    ),
                    "cpu_system_seconds": round(
                        sum(float(item.get("cpu_system_seconds", 0.0)) for item in members), 6
                    ),
                    "context_switches": sum(
                        int(item.get("context_switches", 0)) for item in members
                    ),
                }
                tree_user = float(root_metrics["cpu_user_seconds"])
                tree_system = float(root_metrics["cpu_system_seconds"])
                with self._lock:
                    previous_tree_cpu = self._last_process_cpu.get(("tree", root))
                    self._last_process_cpu[("tree", root)] = (tree_user, tree_system)
                tree_user_delta = (
                    max(0.0, tree_user - previous_tree_cpu[0])
                    if previous_tree_cpu is not None
                    else 0.0
                )
                tree_system_delta = (
                    max(0.0, tree_system - previous_tree_cpu[1])
                    if previous_tree_cpu is not None
                    else 0.0
                )
                root_peak: dict[str, int | float] = {}
                # ``registered`` is a shallow copy.  Update the authoritative
                # metadata under the profiler lock so unregister sees every
                # sample even while a heartbeat races the sampler thread.
                with self._lock:
                    live_metadata = self._processes.get(root)
                    if live_metadata is not None:
                        live_metadata["sample_count"] = int(
                            live_metadata.get("sample_count", 0) or 0
                        ) + 1
                        peak = live_metadata.setdefault("peak", {})
                        self._peak_update(peak, root_metrics)
                        root_peak = dict(peak)
                self.emit(
                    "resource.process",
                    task_id=metadata.get("task_id"),
                    actor_id=metadata.get("actor_id"),
                    pid=root,
                    role=metadata.get("role", "solver"),
                    episode=metadata.get("episode"),
                    **root_metrics,
                    cpu_user_delta_seconds=tree_user_delta,
                    cpu_system_delta_seconds=tree_system_delta,
                    sample_interval_seconds=sample_interval,
                    cpu_utilization=(
                        (tree_user_delta + tree_system_delta)
                        / (sample_interval * cpu_count)
                        if sample_interval > 0
                        else 0.0
                    ),
                    **root_peak,
                    process_alive=bool(members),
                )
            return aggregate
        except Exception:
            return {}

    sample = sample_now
    snapshot = sample_now
    process_snapshot = sample_now

    def _sampler_loop(self) -> None:
        while not self._sampler_stop.wait(self.heartbeat_interval_seconds):
            self.sample_now()

    @contextmanager
    def span(
        self,
        name: str,
        *,
        task_id: Any = None,
        actor_id: Any = None,
        episode: Any = None,
        **fields: Any,
    ) -> Iterator[Any]:
        if not self.enabled:
            yield _NullSpan()
            return
        normalized = str(name or "").strip().casefold()
        if not _EVENT_RE.fullmatch(normalized):
            yield _NullSpan()
            return
        span_fields = {"episode": episode, **fields}
        with _Span(
            self,
            normalized,
            task_id=task_id,
            actor_id=actor_id,
            fields=span_fields,
        ) as span:
            yield span

    def close(self) -> None:
        """Stop sampling, flush a final sample, and close the sink."""

        if not self.enabled:
            return
        thread: threading.Thread | None = None
        try:
            with self._lock:
                # Reserve closeout exactly once.  A runner can receive both a
                # normal-finish and an error callback concurrently; only the
                # first caller may emit ``profile.end`` or close the handle.
                if self._closed or self._closing:
                    return
                self._closing = True
                self._sampler_stop.set()
                self._sampler_wakeup.set()
                thread = self._sampler_thread
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=max(1.0, self.heartbeat_interval_seconds * 2.0))
            self.sample_now(
                force=True,
                _allow_closing=True,
                _refresh_artifacts=True,
            )
            self.emit(
                "profile.end",
                phase="profiling",
                role="runner",
                elapsed_seconds=time.monotonic() - self._started_monotonic,
                snapshot_count=self._sample_sequence,
            )
        except Exception:
            pass
        finally:
            with self._lock:
                self._closed = True
                self._closing = False
                self._last_process_cpu.clear()
                self._last_cgroup_cpu_usage = None
                handle = self._handle
                self._handle = None
                if handle is not None:
                    try:
                        handle.flush()
                        handle.close()
                    except OSError:
                        pass


__all__ = ["PROFILE_FILENAME", "PROFILE_SCHEMA_VERSION", "ProfilerSettings", "RunProfiler"]
