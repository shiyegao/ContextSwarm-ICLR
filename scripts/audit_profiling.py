#!/usr/bin/env python3
"""Read-only, single-pass audit for a ContextSwarm ``profiling.jsonl``.

The profiler is intentionally a JSONL side channel.  This script consumes it
one line at a time and retains only bounded counters, a small numeric sample,
and the state needed to pair spans.  It never imports the runner, opens a
database, contacts a Judge, or prints the input path/identities/payloads.

The machine-readable report has two coverage views:

``coverage``
    Compact states used by operators: ``present``, ``partial``,
    ``conditional_missing``, ``missing``, ``not_applicable`` or ``invalid``.
``coverage_detail``
    The same result with required/observed family counters.  ``status`` also
    carries the longer historical names (``covered`` and
    ``missing_required``) for callers that used the early audit prototype.

Exit contract:

* 0 -- valid input, no quality/invariant failures, and every applicable
  target is present;
* 1 -- parseable input with a quality, terminal/span, privacy, or coverage
  problem;
* 2 -- input cannot be opened or JSONL/schema cannot be parsed.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Mapping
import datetime as _datetime
import hashlib
import json
import math
from pathlib import Path
import re
import sys
from typing import Any, Iterable, TextIO


try:
    # Keep the script usable both as ``python scripts/audit_profiling.py`` and
    # when imported by tests.  Importing this constant does not initialise the
    # runtime or perform I/O.
    from contextswarm_mini.profiling import PROFILE_FILENAME, PROFILE_SCHEMA_VERSION
except Exception:  # pragma: no cover - defensive for a copied standalone script
    PROFILE_FILENAME = "profiling.jsonl"
    PROFILE_SCHEMA_VERSION = "contextswarm_profile_event_v1"


AUDIT_SCHEMA_VERSION = "contextswarm_profile_audit_v1"
# The first six entries are the original analysis goals.  ``record_search``
# lock contention is intentionally exposed as a seventh, independent audit
# target: a selection summary or a generic SQLite event must never silently
# stand in for the write-lock transaction it is meant to explain.
PRIMARY_TARGETS = (
    "agent_wrapper",
    "selection",
    "trace_projection",
    "max_parallel",
    "cps",
    "judge",
)
TARGETS = PRIMARY_TARGETS + ("record_search_lock",)
COVERAGE_STATES = (
    "present",
    # ``partial`` means the target was applicable and at least one member of
    # its required conjunction was observed, but one or more required
    # families were absent.  Keeping this distinct from ``missing`` is useful
    # when a real run only lost one instrumentation branch (for example the
    # allocator admission marker) rather than the whole target.
    "partial",
    "conditional_missing",
    "missing",
    "not_applicable",
    "invalid",
)
LEGACY_STATUS = {
    "present": "covered",
    "partial": "missing_required",
    "conditional_missing": "conditional_missing",
    "missing": "missing_required",
    "not_applicable": "not_applicable",
    "invalid": "invalid",
}

_EVENT_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,95}$")
_SAFE_COMPONENT_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_URL_RE = re.compile(r"(?i)\b(?:https?|tcp|unix)://")
_PATH_RE = re.compile(r"(?:^|[\s=])(?:/|[A-Za-z]:[\\/])")
_AUTH_RE = re.compile(
    r"(?i)(?:bearer\s+|authorization\s*[:=]|api[_-]?key\s*[:=]|"
    r"access[_-]?token\s*[:=]|secret\s*[:=]|password\s*[:=])"
)
_EMAIL_RE = re.compile(r"(?i)\b[^\s@]+@[^\s@]+\.[^\s@]+\b")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$", re.I)

# Names in this set are safe, low-cardinality dimensions from the profiling
# contract.  Event names are checked against the fixed allowlist below before
# they are retained in the report.  In particular, a syntactically valid but
# unrecognised label such as ``selection.private_blob`` must not be allowed to
# smuggle an arbitrary component into ``event_counts``.
_EVENT_FAMILIES = {
    "profile",
    "run",
    "horizon",
    "attempt",
    "agent",
    "resource",
    "selection",
    "trace",
    "cps",
    "judge",
    "drain",
    "scheduler",
    "allocation",
    "model",
    "tool",
    "artifact",
    "closeout",
    "scoreboard",
}

_SENSITIVE_KEY_CATEGORIES = {
    "prompt": re.compile(r"(?i)(?:^|[_\.])prompt(?:$|[_\.])"),
    "candidate": re.compile(r"(?i)(?:^|[_\.])candidate(?:$|[_\.])"),
    "url": re.compile(r"(?i)(?:^|[_\.])(?:url|uri|endpoint)(?:$|[_\.])"),
    "path": re.compile(r"(?i)(?:^|[_\.])(?:path|file|directory|dir)(?:$|[_\.])"),
    # ``*_tokens`` are bounded usage counters and are explicitly allowed;
    # match credential-bearing token labels only when they are named as such.
    "credential": re.compile(
        r"(?i)(?:^|[_\.])(?:access[_-]?token|refresh[_-]?token|secret|password|"
        r"credential|authorization|api[_-]?key|node\.toml)(?:$|[_\.])"
    ),
    "identity": re.compile(r"(?i)(?:email|username|account|identity|user_id|actor_id|task_id|run_id)"),
}
# Hashes and dedicated correlation fields are permitted by the profile
# contract.  Do not classify ``candidate_sha256`` as candidate content.
_SENSITIVE_KEY_EXEMPTIONS = {
    "candidate_sha256",
    "task_contract_sha256",
    "trace_set_sha256",
    "snapshot_sha256",
    "source_snapshot_sha256",
    "projection_snapshot_sha256",
    "pool_sha256",
    "eligible_pool_sha256",
    "task_set_sha256",
    "trace_watermark_sha256",
    "request_key_sha256",
    "config_sha256",
    "comparison_contract_id",
    "selection_config_id",
    "candidate_count",
    "selection_candidate_count",
    # Bounded numeric preparation counters describe row cardinality; they do
    # not contain candidate payloads despite the field-name component.
    "prepare_candidate_rows",
    "candidates",
    "candidate_transfer",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "delivered_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    # The profiler contract permits bounded correlation identifiers.  They
    # are deliberately omitted from the audit output, but their presence is
    # not itself a sensitive-field leak.
    "run_id",
    "task_id",
    "actor_id",
    "agent_id",
    "claim",
    "judge_job_id",
}

# Bounded numeric dimensions.  The explicit set covers all metrics documented
# by profiling.py; the suffix rule catches newly-added scalar timing/resource
# fields without admitting arbitrary payloads.
_NUMERIC_NAMES = {
    "wall_seconds",
    "elapsed_seconds",
    "monotonic_elapsed_seconds",
    "cpu_user_seconds",
    "cpu_system_seconds",
    "cpu_thread_user_seconds",
    "cpu_thread_system_seconds",
    "cpu_user_delta_seconds",
    "cpu_system_delta_seconds",
    "cpu_utilization",
    "cpu_utilization_cgroup",
    "cpu_quota_cores",
    "cpu_affinity_count",
    "cgroup_cpu_usage_seconds",
    "cgroup_cpu_usage_delta_seconds",
    "sample_interval_seconds",
    "cpu_throttled_seconds",
    "rss_bytes",
    "pss_bytes",
    "memory_bytes",
    "memory_current_bytes",
    "memory_peak_bytes",
    "peak_rss_bytes",
    "peak_pss_bytes",
    "peak_memory_current_bytes",
    "peak_memory_peak_bytes",
    "process_count",
    "process_tree_count",
    "thread_count",
    "peak_process_count",
    "peak_process_tree_count",
    "peak_thread_count",
    "active_slots",
    "active_solver_slots",
    "backlog_limit",
    "remaining_slots",
    "max_parallel",
    "queue_depth",
    "lock_queue_depth",
    "write_waiters",
    "write_active",
    "lock_wait_seconds",
    "lock_hold_seconds",
    "queue_residence_seconds",
    "transaction_seconds",
    "commit_seconds",
    "connect_seconds",
    "query_seconds",
    "fetch_seconds",
    "read_transaction_seconds",
    "materialize_seconds",
    "serialization_seconds",
    "tokenize_seconds",
    "hash_seconds",
    "input_rows",
    "output_rows",
    "rows",
    "rows_scanned",
    "rows_written",
    "materialized_rows",
    "materialized_bytes",
    "input_bytes",
    "output_bytes",
    "wal_bytes",
    "wal_bytes_before",
    "wal_bytes_after",
    "db_bytes",
    "db_bytes_before",
    "db_bytes_after",
    "page_count",
    "pages_scanned",
    "poll_count",
    "settlement_poll_seconds",
    "wait_seconds",
    "latency_seconds",
    "evaluator_seconds",
    "audit_seconds",
    "request_seconds",
    "score",
    "peak_cpu_utilization",
    "peak_cpu_utilization_cgroup",
    "artifact_files_scanned",
    "artifact_directories_scanned",
}

_FIELD_GROUPS = {
    "timing": frozenset(
        {
            "wall_seconds",
            "elapsed_seconds",
            "cpu_user_seconds",
            "cpu_system_seconds",
            "cpu_thread_user_seconds",
            "cpu_thread_system_seconds",
        }
    ),
    "resource": frozenset(
        {
            "rss_bytes",
            "pss_bytes",
            "memory_current_bytes",
            "process_tree_count",
            "thread_count",
            "cpu_utilization",
            "artifact_files_scanned",
            "artifact_directories_scanned",
            "artifact_scan_truncated",
        }
    ),
    "selection": frozenset(
        {
            "input_rows",
            "output_rows",
            "materialized_bytes",
            "rows_written",
            "serialization_seconds",
            "hash_seconds",
            "lock_wait_seconds",
            "lock_hold_seconds",
            "wal_bytes_before",
            "wal_bytes_after",
        }
    ),
    "cps": frozenset(
        {
            "rows_scanned",
            "query_seconds",
            "fetch_seconds",
            "read_transaction_seconds",
            "materialize_seconds",
            "connect_seconds",
            "db_bytes",
            "wal_bytes",
        }
    ),
    "judge": frozenset(
        {
            "backlog_limit",
            "queue_depth",
            "wait_seconds",
            "evaluator_seconds",
            "settlement_poll_seconds",
            "pending_settlement_watchers",
            "remote_unsettled_jobs",
        }
    ),
}

_SPAN_BASES = {
    "profile",
    "run",
    "horizon",
    "attempt.lifecycle",
    "attempt.agent.invoke",
    "attempt.wrapper",
    "attempt.wrapper.dispatch",
    "attempt.wrapper.evaluate",
    "attempt.wrapper.prompt",
    "attempt.wrapper.settlement",
    "attempt.wrapper.workspace",
    "agent",
    "agent.rpc",
    "selection.runtime",
    "selection.search",
    "selection.read",
    "selection.eligible",
    "selection.rank",
    "selection.pack",
    "selection.persist",
    "selection.persist.call",
    "selection.payload.materialize",
    "selection.sqlite.connect",
    "trace.project",
    "trace.bridge",
    "trace.bridge.project",
    "trace.bridge.read",
    "trace.bridge.sqlite",
    "cps.progress",
    "cps.search",
    "cps.inbox",
    "cps.digest",
    "cps.write",
    "cps.sqlite.connect",
    "judge.execute",
    "judge.audit",
    "judge.broker",
    "judge.session",
    "drain",
    "scheduler.invoke",
    "scheduler.agent.invoke",
    "allocation.choose",
    "allocation.admission",
    "allocation.reservation",
    "allocation.decision.persist",
    "allocation.assignment.persist",
    "model.request",
    "tool",
    "closeout",
    "closeout.evaluation",
    "closeout.evaluation_call",
}

# Explicit non-span notifications emitted by the profiler and its lifecycle
# adapters.  Span names are added below from ``_SPAN_BASES`` so both their
# bare base (used in the bounded span summary) and their ``.start``/``.end``
# events are covered.  Keeping this list local to the audit tool means a new
# producer event has to be reviewed deliberately rather than being echoed by
# a permissive top-level-family rule.
_KNOWN_EVENT_NAMES: set[str] = {
    "agent.heartbeat",
    "agent.process_started",
    "agent.result",
    "agent.refill.scheduled",
    "agent.refill.start",
    "agent.refill.end",
    "agent.refill.exhausted",
    "agent.recovery.started",
    "agent.recovery.scheduled",
    "agent.recovery.succeeded",
    "agent.recovery.failure",
    "agent.recovery.exhausted",
    "agent.rpc.end",
    "agent.rpc.settled",
    "allocation.assignment",
    "allocation.snapshot.start",
    "allocation.snapshot.end",
    "allocation.decision",
    "allocation.snapshot.summary",
    "artifact.write",
    "attempt.admitted",
    "attempt.result",
    "attempt.solver_slot_released",
    "closeout.evaluation.receipt",
    "cps.digest.summary",
    "cps.inbox.query",
    "cps.inbox.materialize",
    "cps.progress.query",
    "cps.progress.materialize",
    "cps.progress.summary",
    "cps.search.query",
    "cps.search.materialize",
    "cps.sqlite.connect",
    "cps.sqlite.checkpoint",
    "cps.write.queue",
    "cps.write.lock",
    "cps.write.commit",
    "drain.complete",
    "drain.sample",
    "drain.timeout",
    "drain.error",
    "judge.queued",
    "judge.queue.wait",
    "judge.queue.expired",
    "judge.running",
    "judge.submitted",
    "judge.receipt",
    "judge.http.start",
    "judge.http.end",
    "judge.settlement.pending",
    "judge.settlement.watcher",
    "judge.snapshot.start",
    "judge.snapshot.end",
    "judge.audit.end",
    "resource.process",
    "resource.process.register",
    "resource.process.self",
    "resource.process.unregister",
    "resource.sample",
    "run.configuration",
    "run.dry_end",
    "run.dry_run_end",
    "run.error",
    "scheduler.invocation.end",
    "scoreboard.record",
    "selection.eligible.read",
    "selection.eligible.filter",
    "selection.eligible.query_terms",
    "selection.eligible.materialize",
    "selection.eligible.summary",
    "selection.payload.materialize",
    "selection.persist.call",
    "selection.persist.payload",
    "selection.persist.queue",
    "selection.persist.lock",
    "selection.persist.readback",
    "selection.persist.readback.query",
    "selection.read.start",
    "selection.read.end",
    "selection.sqlite.connect",
    "selection.sqlite.checkpoint",
    "selection.replay.lookup",
    "selection.replay.materialize",
    "selection.replay.read",
    "selection.replay.start",
    "selection.replay.end",
    "selection.snapshot",
    "selection.rank.summary",
    "selection.pack.summary",
    "selection.search.summary",
    "trace.project.query",
    "trace.project.read",
    "trace.project.materialize",
    "trace.project.summary",
    "trace.bridge.page",
    "trace.bridge.materialize",
    "trace.bridge.sqlite",
    "trace.bridge.sqlite.connect",
    "trace.bridge.sqlite.query",
    "trace.bridge.summary",
    "preflight.failed",
}
for _span_base in _SPAN_BASES:
    _KNOWN_EVENT_NAMES.add(_span_base)
    _KNOWN_EVENT_NAMES.add(_span_base + ".start")
    _KNOWN_EVENT_NAMES.add(_span_base + ".end")
_KNOWN_EVENT_NAMES = frozenset(_KNOWN_EVENT_NAMES)

# A few lifecycle labels are terminal notifications rather than spans in the
# shipped profiler.  Treating their ``.start``/``.end`` spelling as a pair
# would make every otherwise healthy run look truncated (for example the
# broker emits ``judge.broker.start`` once and ``agent.rpc.end`` has no start).
_UNPAIRED_START_EVENTS = {
    "judge.broker.start",
}
_UNPAIRED_END_EVENTS = {
    "agent.rpc.end",
    "agent.rpc.settled",
    "attempt.result",
    "agent.result",
    "scheduler.invocation.end",
    "judge.audit.end",
    "closeout.evaluation.end",
}

_TERMINAL_EVENTS = (
    "profile.end",
    "run.end",
    "run.error",
    "run.dry_end",
    "horizon.end",
    "agent.end",
    "judge.receipt",
    "drain.end",
    "drain.timeout",
    "drain.error",
)

_POLICIES = {
    "uniform",
    "uniform_refill",
    "task_state",
    "trace_state",
    "llm_scheduler",
    "formula",
    "agent",
}
_TRACE_POLICIES = {"trace_state", "llm_scheduler"}


class InputError(Exception):
    """An input/path/JSONL/schema failure (exit code 2)."""


def _reject_constant(value: str) -> Any:
    raise ValueError(value)


def _object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate object key")
        result[key] = value
    return result


def _parse_json_line(line: str) -> Any:
    return json.loads(
        line,
        object_pairs_hook=_object_pairs,
        parse_constant=_reject_constant,
    )


def _safe_family(event: str) -> str:
    """Return a bounded event label suitable for a report."""

    normalized = event.casefold()
    if not _EVENT_RE.fullmatch(normalized):
        return "other"
    # Syntax and a recognised top-level family are not sufficient to make an
    # event label report-safe: ``selection.private_blob`` is perfectly valid
    # according to the wire grammar but could still echo an arbitrary
    # identifier into ``event_counts``.  Keep only the reviewed, fixed
    # allowlist.  Unknown labels are counted under one bounded bucket by the
    # caller, with an ``unknown_event`` quality issue that contains no label.
    if normalized in _KNOWN_EVENT_NAMES:
        return normalized
    return "other"


def _event_has(events: Mapping[str, int], prefix: str) -> bool:
    return any(name == prefix or name.startswith(prefix + ".") for name in events)


def _event_seen(events: Mapping[str, int], name: str) -> bool:
    """Return whether one exact, reviewed event name occurred.

    Coverage requirements use this exact predicate deliberately.  The older
    prefix predicate is still useful for broad diagnostic/inference checks,
    but would let a summary (or an unrelated child event) satisfy a stage that
    needs its own query/materialization/lifecycle evidence.
    """

    return bool(events.get(name, 0))


def _span_seen(events: Mapping[str, int], base: str) -> bool:
    """Return whether both exact lifecycle endpoints of a span occurred."""

    return _event_seen(events, base + ".start") and _event_seen(events, base + ".end")


def _requirement_seen(events: Mapping[str, int], requirement: str) -> bool:
    """Evaluate a fixed coverage requirement.

    ``span:<base>`` is an internal, fixed descriptor used in the report to
    express a start/end conjunction without expanding every stage into a
    hand-written pair at call sites.  Plain names are exact event names.
    """

    if requirement.startswith("span:"):
        return _span_seen(events, requirement[5:])
    return _event_seen(events, requirement)


# Correlation is deliberately kept separate from the public event counters.
# The profiler already sanitises identifiers, but the audit hashes them again
# before using them as dictionary keys so a report can never echo an identity.
# These limits keep one malformed/high-cardinality profile from turning the
# audit itself into an unbounded memory consumer.
_CORRELATION_MAX_SCOPES = 4096
_CORRELATION_MAX_RUNS = 32
_CORRELATION_MAX_FINGERPRINTS = 16384
_CORRELATION_DIMENSIONS = ("run_id", "task_id", "actor_id", "episode")

# ``SelectionStore._write`` emits the same persistence lifecycle for several
# write operations.  The dedicated record-search lock target must not count a
# configuration/feedback write as a search transaction merely because it uses
# the shared ``selection.persist.*`` event family.  Keep this operation
# predicate deliberately exact and bounded; arbitrary operation labels are
# never copied into the report.
_RECORD_SEARCH_OPERATION = "record_search"
_RECORD_SEARCH_EVENT_NAMES = frozenset(
    {
        "selection.persist.start",
        "selection.persist.lock",
        "selection.persist.end",
        "selection.persist.queue",
        "selection.persist.payload",
        "selection.persist.readback",
        "selection.persist.readback.query",
    }
)

# Stages that must be observed under one run/task/actor/episode scope for the
# three attempt-attributed goals.  Resource aggregate samples and drain are
# intentionally not listed: those are run-level observations, while the
# process/Judge/selection rows below carry the attribution needed to prove an
# attempt-level join.
_CORRELATION_SCOPE_STAGES: dict[str, frozenset[str]] = {
    "agent_wrapper": frozenset(
        {
            "span:attempt.agent.invoke",
            "span:agent",
            "resource.process",
        }
    ),
    "selection": frozenset(
        {
            "span:selection.eligible",
            "selection.eligible.read",
            "selection.eligible.filter",
            "selection.eligible.query_terms",
            "selection.eligible.materialize",
            "selection.snapshot",
            "span:selection.rank",
            "span:selection.pack",
            "selection.payload.materialize",
            "span:selection.persist",
            "selection.persist.payload",
            "selection.persist.readback",
            "selection.persist.readback.query",
            "selection.sqlite.connect",
        }
    ),
    # ``record_search_lock`` is intentionally separate from the broader
    # selection target.  The lifecycle endpoints are represented internally
    # as the paired ``span:selection.persist`` stage, while the lock marker
    # is a plain event.  Keeping all three in this target's scope map means a
    # start/end from one writer cannot be joined with a lock row from another
    # writer (or with an unattributed row).
    "record_search_lock": frozenset(
        {
            "span:selection.persist",
            "selection.persist.lock",
        }
    ),
    "judge": frozenset({"span:judge.execute", "judge.receipt"}),
}

# Trace/CPS are intentionally run-level: they may be emitted by allocator or
# store helpers without a task/actor.  A run hash is still mandatory so a
# concatenated profile cannot join a start from one run to an end from another.
_CORRELATION_RUN_TARGETS = frozenset({"trace_projection", "cps"})
_CORRELATION_RUN_STAGES = frozenset(
    {
        "span:trace.project",
        "trace.project.query",
        "trace.project.read",
        "trace.project.materialize",
        "trace.project.summary",
        "trace.bridge.page",
        "span:trace.bridge.project",
        "trace.bridge.materialize",
        "trace.bridge.summary",
        "trace.bridge.sqlite.query",
        "span:cps.progress",
        "cps.progress.query",
        "cps.progress.materialize",
        "cps.progress.summary",
        "cps.sqlite.connect",
        "attempt.admitted",
        "resource.process",
    }
)
_CORRELATION_ATTEMPT_BASES = (
    "attempt.lifecycle",
    "attempt.wrapper",
    "attempt.wrapper.dispatch",
)


def _canonical_correlation_identity(value: Any) -> str | None:
    """Return a non-reversible, bounded key for one identity field."""

    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (str, int)):
        return None
    text = str(value).strip()
    if not text or len(text) > 256:
        return None
    return hashlib.sha256(
        ("contextswarm-audit-correlation-v1\0" + text).encode("utf-8", "replace")
    ).hexdigest()


def _canonical_episode(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 10**9:
        return None
    return f"episode:{value}"


def _correlation_stage_for_event(event: str) -> str | None:
    base = _span_base(event)
    if base is not None:
        return "span:" + base
    # Only reviewed, fixed event labels are admitted as stage keys.  Unknown
    # labels are already collapsed to ``other`` and never enter this map.
    if event in _KNOWN_EVENT_NAMES:
        return event
    return None


def _event_count(events: Mapping[str, int], prefix: str) -> int:
    return sum(count for name, count in events.items() if name == prefix or name.startswith(prefix + "."))


def _key_category(key: str) -> str | None:
    normalized = key.casefold()
    if normalized in _SENSITIVE_KEY_EXEMPTIONS or normalized.endswith("_sha256"):
        return None
    for category, pattern in _SENSITIVE_KEY_CATEGORIES.items():
        if pattern.search(normalized):
            # IDs are used only for internal correlation and are never
            # returned; classify them so a malformed input is visible.
            return category
    return None


def _scan_sensitive(value: Any, categories: Counter[str], *, key: str | None = None) -> None:
    """Count sensitive material without retaining its location or value."""

    if key is not None:
        category = _key_category(key)
        if category is not None:
            categories[category] += 1
    if isinstance(value, Mapping):
        for child_key, child in value.items():
            _scan_sensitive(child, categories, key=str(child_key))
    elif isinstance(value, list):
        for child in value:
            _scan_sensitive(child, categories)
    elif isinstance(value, str):
        if _URL_RE.search(value):
            categories["url"] += 1
        elif _PATH_RE.search(value):
            categories["path"] += 1
        elif _AUTH_RE.search(value):
            categories["credential"] += 1
        elif _EMAIL_RE.search(value):
            categories["identity"] += 1


def _is_finite_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(float(value))


def _safe_integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "1", "yes", "on", "enabled"}:
            return True
        if normalized in {"false", "0", "no", "off", "disabled"}:
            return False
    return None


def _safe_policy(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().casefold().replace("-", "_")
    return normalized if normalized in _POLICIES else None


def _safe_mode(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().casefold().replace("-", "_")
    # The runner has several modes; preserve only low-cardinality labels.
    return normalized if normalized in {"cps", "mono", "parallel", "dry_run", "dry"} else None


class _Reservoir:
    """A deterministic bounded sample for percentile estimates."""

    __slots__ = ("limit", "count", "total", "minimum", "maximum", "values")

    def __init__(self, limit: int = 512) -> None:
        self.limit = limit
        self.count = 0
        self.total = 0.0
        self.minimum = math.inf
        self.maximum = -math.inf
        self.values: list[float] = []

    def add(self, value: float) -> None:
        if not math.isfinite(value):
            return
        self.count += 1
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        # Keep a deterministic evenly-spaced sample after the cap.  This is
        # bounded and reproducible, unlike an unseeded random reservoir.
        if len(self.values) < self.limit:
            self.values.append(value)
        elif self.count % max(1, self.count // self.limit) == 0:
            index = (self.count // max(1, self.count // self.limit) - 1) % self.limit
            self.values[index] = value

    def as_dict(self) -> dict[str, Any]:
        if not self.count:
            return {"count": 0, "sum": 0.0, "min": None, "max": None, "p50": None, "p95": None, "p99": None}
        ordered = sorted(self.values)

        def percentile(q: float) -> float:
            if not ordered:
                return 0.0
            if len(ordered) == 1:
                return ordered[0]
            position = q * (len(ordered) - 1)
            lower = int(math.floor(position))
            upper = int(math.ceil(position))
            if lower == upper:
                return ordered[lower]
            weight = position - lower
            return ordered[lower] + (ordered[upper] - ordered[lower]) * weight

        return {
            "count": self.count,
            "sum": round(self.total, 6),
            "min": round(self.minimum, 6),
            "max": round(self.maximum, 6),
            "p50": round(percentile(0.50), 6),
            "p95": round(percentile(0.95), 6),
            "p99": round(percentile(0.99), 6),
            "sample_count": len(ordered),
            "sampled": len(ordered) < self.count,
        }


class _AuditState:
    """Streaming accumulator; no profile rows are retained."""

    __slots__ = (
        "rows",
        "blank_lines",
        "schema_versions",
        "events",
        "field_presence",
        "numeric",
        "dropped_rows",
        "dropped_total",
        "dropped_max",
        "sensitive",
        "issues",
        "last_sequence",
        "first_sequence",
        "last_seen_sequence",
        "sequence_gaps",
        "sequence_duplicates",
        "sequence_out_of_order",
        "span_starts",
        "span_ends",
        "span_orphans",
        "terminal_events",
        "config_rows",
        "saw_dry_run",
        # Correlation state is bounded and stores only canonical hashes,
        # reviewed stage labels and counters; raw profile rows/identities are
        # never retained.
        "correlation_runs",
        "correlation_scopes",
        "correlation_scope_events",
        "correlation_scope_span_starts",
        "correlation_scope_span_ends",
        "correlation_run_events",
        "correlation_run_span_starts",
        "correlation_run_span_ends",
        "correlation_attempt_starts",
        "correlation_attempt_ends",
        # Operation-filtered state for the dedicated record_search lock
        # contract.  These maps intentionally do not reuse the broad
        # selection maps: the latter include all selection-store writes.
        "record_search_events",
        "record_search_scope_events",
        "record_search_scope_span_starts",
        "record_search_scope_span_ends",
        "record_search_missing",
        "record_search_unattributed",
        "record_search_fingerprints",
        "record_search_duplicate_scopes",
        "record_search_duplicate_count",
        "correlation_missing",
        "correlation_unattributed",
        "correlation_fingerprints",
        "correlation_duplicate_scopes",
        "correlation_duplicate_runs",
        "correlation_duplicate_count",
        "correlation_cross_run",
        "correlation_overflow",
    )

    def __init__(self) -> None:
        self.rows = 0
        self.blank_lines = 0
        self.schema_versions: Counter[str] = Counter()
        self.events: Counter[str] = Counter()
        self.field_presence: Counter[str] = Counter()
        self.numeric: dict[str, _Reservoir] = {}
        self.dropped_rows = 0
        self.dropped_total = 0
        self.dropped_max = 0
        self.sensitive: Counter[str] = Counter()
        self.issues: list[dict[str, Any]] = []
        self.last_sequence: int | None = None
        self.first_sequence: int | None = None
        self.last_seen_sequence: int | None = None
        self.sequence_gaps = 0
        self.sequence_duplicates = 0
        self.sequence_out_of_order = 0
        self.span_starts: Counter[str] = Counter()
        self.span_ends: Counter[str] = Counter()
        self.span_orphans = 0
        self.terminal_events: Counter[str] = Counter()
        self.config_rows: list[dict[str, Any]] = []
        self.saw_dry_run = False
        self.correlation_runs: set[str] = set()
        self.correlation_scopes: set[tuple[str, str, str, str]] = set()
        self.correlation_scope_events: dict[tuple[str, str, str, str], set[str]] = {}
        self.correlation_scope_span_starts: dict[tuple[tuple[str, str, str, str], str], int] = {}
        self.correlation_scope_span_ends: dict[tuple[tuple[str, str, str, str], str], int] = {}
        self.correlation_run_events: dict[str, set[str]] = {}
        self.correlation_run_span_starts: dict[tuple[str, str], int] = {}
        self.correlation_run_span_ends: dict[tuple[str, str], int] = {}
        self.correlation_attempt_starts: dict[tuple[str, str, str, str], Counter[str]] = {}
        self.correlation_attempt_ends: dict[tuple[str, str, str, str], Counter[str]] = {}
        self.record_search_events: Counter[str] = Counter()
        self.record_search_scope_events: dict[tuple[str, str, str, str], set[str]] = {}
        self.record_search_scope_span_starts: dict[tuple[tuple[str, str, str, str], str], int] = {}
        self.record_search_scope_span_ends: dict[tuple[tuple[str, str, str, str], str], int] = {}
        self.record_search_missing: Counter[str] = Counter()
        self.record_search_unattributed: Counter[str] = Counter()
        self.record_search_fingerprints: set[str] = set()
        self.record_search_duplicate_scopes: set[tuple[str, str, str, str]] = set()
        self.record_search_duplicate_count = 0
        self.correlation_missing: Counter[str] = Counter()
        self.correlation_unattributed: Counter[str] = Counter()
        self.correlation_fingerprints: set[str] = set()
        self.correlation_duplicate_scopes: set[tuple[str, str, str, str]] = set()
        self.correlation_duplicate_runs: set[str] = set()
        self.correlation_duplicate_count = 0
        self.correlation_cross_run = False
        self.correlation_overflow = False

    def issue(self, code: str, line: int | None = None) -> None:
        # Keep issue output bounded and value-free.  Repeated rows collapse to
        # a count in the aggregate rather than growing memory with the file.
        for item in self.issues:
            if item["code"] == code:
                item["count"] = int(item.get("count", 1)) + 1
                return
        item: dict[str, Any] = {"code": code, "count": 1}
        if line is not None:
            item["first_line"] = line
        if len(self.issues) < 64:
            self.issues.append(item)

    def add_numeric(self, key: str, value: Any) -> None:
        if key not in _NUMERIC_NAMES and not (
            key.endswith(("_seconds", "_bytes", "_count", "_rows"))
            and key.startswith(("peak_", "active_", "pending_", "remote_"))
        ):
            return
        if not _is_finite_number(value):
            self.issue("non_finite_metric")
            return
        reservoir = self.numeric.get(key)
        if reservoir is None:
            if len(self.numeric) >= 128:
                return
            reservoir = self.numeric[key] = _Reservoir()
        reservoir.add(float(value))


def _span_base(event: str) -> str | None:
    if event.endswith(".start") or event.endswith(".end"):
        base = event.rsplit(".", 1)[0]
        if base in _SPAN_BASES:
            return base
        # Do not retain arbitrary future/custom span names.  They are already
        # represented as the bounded ``other`` event bucket and pairing them
        # here would create an unbounded raw-key map in the streaming state.
    return None


def _row_correlation(row: Mapping[str, Any]) -> tuple[dict[str, str | None], tuple[str, ...]]:
    """Canonicalise the four correlation dimensions without retaining input."""

    values: dict[str, str | None] = {
        "run_id": _canonical_correlation_identity(row.get("run_id")),
        "task_id": _canonical_correlation_identity(row.get("task_id")),
        "actor_id": _canonical_correlation_identity(row.get("actor_id")),
        "episode": _canonical_episode(row.get("episode")),
    }
    missing = tuple(key for key in _CORRELATION_DIMENSIONS if values[key] is None)
    return values, missing


def _correlation_scope(values: Mapping[str, str | None]) -> tuple[str, str, str, str] | None:
    if any(values.get(key) is None for key in _CORRELATION_DIMENSIONS):
        return None
    return (
        str(values["run_id"]),
        str(values["task_id"]),
        str(values["actor_id"]),
        str(values["episode"]),
    )


def _resource_process_has_concrete_pid(row: Mapping[str, Any]) -> bool:
    """Whether one process-tree row can be attributed to an attempt.

    ``resource.sample`` is the run-wide aggregate and intentionally has no
    attempt identity.  A ``resource.process`` row is different: it is only
    useful for an attempt-level claim when it carries a concrete PID.  Keep
    this check narrow and scalar so a malformed/aggregate row cannot satisfy
    the Agent-vs-wrapper conjunction merely because its event label exists.
    """

    pid = row.get("pid")
    return isinstance(pid, int) and not isinstance(pid, bool) and pid > 0


def _correlation_stage_targets(stage: str) -> tuple[str, ...]:
    return tuple(
        target
        for target, stages in _CORRELATION_SCOPE_STAGES.items()
        if stage in stages
    )


def _bounded_scope_state(
    state: _AuditState,
    scope: tuple[str, str, str, str],
) -> bool:
    if scope in state.correlation_scopes:
        return True
    if len(state.correlation_scopes) >= _CORRELATION_MAX_SCOPES:
        state.correlation_overflow = True
        return False
    state.correlation_scopes.add(scope)
    state.correlation_scope_events.setdefault(scope, set())
    return True


def _bounded_run_state(state: _AuditState, run_key: str) -> bool:
    if run_key in state.correlation_runs:
        return True
    if len(state.correlation_runs) >= _CORRELATION_MAX_RUNS:
        state.correlation_overflow = True
        return False
    state.correlation_runs.add(run_key)
    state.correlation_run_events.setdefault(run_key, set())
    return True


def _record_correlation(row: Mapping[str, Any], event: str, state: _AuditState) -> None:
    """Record bounded per-scope/per-run stage facts for later conjunction checks."""

    stage = _correlation_stage_for_event(event)
    # Only the exact operation is eligible for the independent record-search
    # lock target.  ``SelectionStore._write`` uses the same persist event
    # labels for register_selector_config/feedback and other writes; those
    # rows remain available to the broad selection target but are excluded
    # from the operation-filtered maps below.
    is_record_search = (
        row.get("operation") == _RECORD_SEARCH_OPERATION
        and event in _RECORD_SEARCH_EVENT_NAMES
    )
    if is_record_search:
        state.record_search_events[event] += 1
    # ``resource.sample`` is a run-wide aggregate and is intentionally not
    # attempt-attributed.  Conversely, an attempt-level ``resource.process``
    # row is useful only when it names a concrete process root.  Do not let a
    # malformed/aggregate process row satisfy either the run-level process
    # requirement or the same-attempt Agent-vs-wrapper conjunction merely
    # because its event label is present.
    if event == "resource.process" and not _resource_process_has_concrete_pid(row):
        state.correlation_missing["pid:agent_wrapper"] += 1
        state.correlation_unattributed["agent_wrapper:resource.process"] += 1
        state.issue("resource_process_missing_pid")
        stage = None
    values, missing = _row_correlation(row)
    run_key = values.get("run_id")
    scope = _correlation_scope(values)
    if run_key is None:
        state.correlation_missing["run_id"] += 1
        if stage in _CORRELATION_RUN_STAGES:
            state.correlation_unattributed["run:" + stage] += 1
    else:
        if run_key not in state.correlation_runs and state.correlation_runs:
            state.correlation_cross_run = True
        if _bounded_run_state(state, run_key):
            # Plain event labels are immediately usable at run scope.  Span
            # labels are paired by start/end counters below instead of being
            # marked present on one endpoint alone.
            base = _span_base(event)
            if base is None and stage is not None:
                state.correlation_run_events[run_key].add(stage)
            if base is not None:
                key = (run_key, base)
                if event.endswith(".start"):
                    state.correlation_run_span_starts[key] = state.correlation_run_span_starts.get(key, 0) + 1
                elif event.endswith(".end"):
                    state.correlation_run_span_ends[key] = state.correlation_run_span_ends.get(key, 0) + 1

    target_names = _correlation_stage_targets(stage) if stage is not None else ()
    if not is_record_search and "record_search_lock" in target_names:
        # ``selection.persist.*`` is a shared lifecycle family.  A
        # register_selector_config/feedback write may therefore have the
        # same stage label as record_search, but it must not even enter the
        # dedicated target's generic attribution counters.  The
        # operation-filtered maps below already enforce this for coverage;
        # filter the broad target list too so the top-level correlation report
        # cannot suggest that an unrelated write was observed for the lock
        # question.
        target_names = tuple(
            target for target in target_names if target != "record_search_lock"
        )
    for dimension in missing:
        if dimension != "run_id" and target_names:
            state.correlation_missing[f"{dimension}:{target_names[0]}"] += 1
    if is_record_search and stage is not None and "record_search_lock" in target_names:
        # Keep operation-filtered missing-dimension counters separate from
        # generic selection persistence rows so the report explains a real
        # record_search attribution failure rather than unrelated writes.
        for dimension in missing:
            state.record_search_missing[f"{dimension}:record_search_lock"] += 1
    if stage is None:
        return

    # Every target stage with incomplete dimensions is intentionally counted
    # as unattributed.  It may still contribute to run-level diagnostics, but
    # it can never prove an attempt-level conjunction.
    if target_names and scope is None:
        for target in target_names:
            state.correlation_unattributed[f"{target}:{stage}"] += 1
    if scope is None or not _bounded_scope_state(state, scope):
        if is_record_search and "record_search_lock" in target_names:
            state.record_search_unattributed[f"record_search_lock:{stage}"] += 1
        return

    scope_events = state.correlation_scope_events[scope]
    base = _span_base(event)
    if base is None:
        scope_events.add(stage)
    else:
        key = (scope, base)
        if event.endswith(".start"):
            state.correlation_scope_span_starts[key] = state.correlation_scope_span_starts.get(key, 0) + 1
        elif event.endswith(".end"):
            state.correlation_scope_span_ends[key] = state.correlation_scope_span_ends.get(key, 0) + 1
        if base in _CORRELATION_ATTEMPT_BASES:
            counter = state.correlation_attempt_starts if event.endswith(".start") else state.correlation_attempt_ends
            by_base = counter.setdefault(scope, Counter())
            by_base[base] += 1

    # Record the same stage in the operation-filtered scope maps only when the
    # row explicitly belongs to record_search.  This is the data source used
    # by ``_correlation_result(..., target='record_search_lock')``.
    if is_record_search and "record_search_lock" in target_names:
        record_scope_events = state.record_search_scope_events.setdefault(scope, set())
        if base is None:
            record_scope_events.add(stage)
        else:
            record_key = (scope, base)
            if event.endswith(".start"):
                state.record_search_scope_span_starts[record_key] = (
                    state.record_search_scope_span_starts.get(record_key, 0) + 1
                )
            elif event.endswith(".end"):
                state.record_search_scope_span_ends[record_key] = (
                    state.record_search_scope_span_ends.get(record_key, 0) + 1
                )

    # Fingerprints are hashes of bounded wire metadata only.  Timestamps make
    # ordinary repeated lifecycle rows distinct while an exact replay keeps a
    # stable digest even if its JSONL sequence number was renumbered.
    fingerprint_material = "|".join(
        (
            event,
            run_key or "",
            "\x1f".join(scope) if scope is not None else "",
            str(row.get("at") or ""),
            str(row.get("monotonic_ns") or ""),
        )
    )
    fingerprint = hashlib.sha256(
        ("contextswarm-audit-replay-v1\0" + fingerprint_material).encode("utf-8", "replace")
    ).hexdigest()
    if len(state.correlation_fingerprints) < _CORRELATION_MAX_FINGERPRINTS:
        if fingerprint in state.correlation_fingerprints:
            state.correlation_duplicate_count += 1
            if scope is not None:
                state.correlation_duplicate_scopes.add(scope)
            if run_key is not None:
                state.correlation_duplicate_runs.add(run_key)
        else:
            state.correlation_fingerprints.add(fingerprint)

    # Keep replay detection operation-specific for record_search as well.
    # A duplicated unrelated selection event in the same scope must not
    # poison an otherwise valid record_search target, while an exact replay
    # of a record_search marker must still prevent a false clean conjunction.
    if is_record_search and scope is not None and len(state.record_search_fingerprints) < _CORRELATION_MAX_FINGERPRINTS:
        if fingerprint in state.record_search_fingerprints:
            state.record_search_duplicate_count += 1
            state.record_search_duplicate_scopes.add(scope)
        else:
            state.record_search_fingerprints.add(fingerprint)


def _validate_row(row: Any, line: int, state: _AuditState) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise InputError("row is not an object")
    required = ("schema_version", "sequence", "at", "monotonic_ns", "event")
    for key in required:
        if key not in row:
            raise InputError("missing required schema field")
    if row.get("schema_version") != PROFILE_SCHEMA_VERSION:
        raise InputError("unsupported schema version")
    sequence = _safe_integer(row.get("sequence"))
    if sequence is None or sequence <= 0:
        raise InputError("invalid sequence type")
    if not isinstance(row.get("at"), str) or not row["at"].strip():
        raise InputError("invalid timestamp")
    monotonic_ns = _safe_integer(row.get("monotonic_ns"))
    if monotonic_ns is None or monotonic_ns < 0:
        raise InputError("invalid monotonic timestamp")
    event = row.get("event")
    if not isinstance(event, str) or not _EVENT_RE.fullmatch(event.casefold()):
        raise InputError("invalid event name")
    event = event.casefold()

    state.rows += 1
    state.schema_versions[str(row["schema_version"])] += 1
    label = _safe_family(event)
    state.events[label] += 1
    if label == "other":
        # Preserve a bounded quality signal without retaining or echoing the
        # unrecognised event name.  The fixed issue aggregator keeps memory
        # independent of the number of distinct hostile labels.
        state.issue("unknown_event", line)
    # Correlation bookkeeping is intentionally independent of the public
    # counters and runs before any field is discarded.  Only canonical hashes
    # and fixed stage labels survive in ``_AuditState``.
    _record_correlation(row, event, state)
    if state.first_sequence is None:
        state.first_sequence = sequence
    if state.last_sequence is not None:
        expected = state.last_sequence + 1
        if sequence == state.last_sequence:
            state.sequence_duplicates += 1
            state.issue("sequence_duplicate", line)
        elif sequence < expected:
            state.sequence_out_of_order += 1
            state.issue("sequence_out_of_order", line)
        elif sequence > expected:
            state.sequence_gaps += sequence - expected
            state.issue("sequence_gap", line)
    state.last_sequence = sequence
    state.last_seen_sequence = sequence

    # Sensitive inspection happens before any field is discarded.  Only fixed
    # category counts survive into the report.
    # ``state.sensitive`` is cumulative.  Comparing before/after is
    # important: checking the counter itself used to report every subsequent
    # clean row as another sensitive-field issue after the first leak.
    sensitive_before = sum(state.sensitive.values())
    _scan_sensitive(row, state.sensitive)
    if sum(state.sensitive.values()) > sensitive_before:
        state.issue("sensitive_field", line)

    dropped = row.get("dropped_fields")
    if dropped is not None:
        if isinstance(dropped, bool) or not isinstance(dropped, int) or dropped < 0:
            state.issue("invalid_dropped_fields", line)
        else:
            state.dropped_rows += 1
            state.dropped_total += dropped
            state.dropped_max = max(state.dropped_max, dropped)
            if dropped:
                state.issue("dropped_fields", line)

    for key, value in row.items():
        if key in {"schema_version", "sequence", "at", "monotonic_ns", "event"}:
            continue
        if key in {"run_id", "task_id", "actor_id", "agent_id", "claim", "judge_job_id", "pid"}:
            # Correlation fields are validated only as bounded scalar values;
            # they are intentionally never counted or returned.
            if key != "pid" and value is not None and not isinstance(value, str):
                state.issue("invalid_identifier", line)
            continue
        if key in _FIELD_GROUPS["timing"] | _FIELD_GROUPS["resource"] | _FIELD_GROUPS["selection"] | _FIELD_GROUPS["cps"] | _FIELD_GROUPS["judge"]:
            state.field_presence[key] += 1
        if isinstance(value, (dict, list, tuple)):
            # The shipped profiler emits scalar/hash/text fields only.  A
            # nested value is not fatal JSON, but it is a quality violation.
            state.issue("nested_field", line)
        state.add_numeric(key, value)

    span = _span_base(event)
    if event in _UNPAIRED_START_EVENTS or event in _UNPAIRED_END_EVENTS:
        span = None
    if span is not None:
        if event.endswith(".start"):
            state.span_starts[span] += 1
        else:
            state.span_ends[span] += 1
            if state.span_ends[span] > state.span_starts[span]:
                state.span_orphans += 1
                state.issue("span_orphan_end", line)
    if event in _TERMINAL_EVENTS:
        state.terminal_events[event] += 1
    if event in {"run.dry_end", "run.dry_run_end"}:
        state.saw_dry_run = True
    if event == "run.configuration":
        state.config_rows.append(
            {
                "mode": _safe_mode(row.get("mode")),
                "policy": _safe_policy(row.get("allocation_policy", row.get("policy"))),
                "selection_enabled": _safe_bool(row.get("selection_enabled")),
                "max_parallel": (
                    row.get("max_parallel")
                    if isinstance(row.get("max_parallel"), int)
                    and not isinstance(row.get("max_parallel"), bool)
                    and row.get("max_parallel") > 0
                    else None
                ),
            }
        )
    return row


def _read_metadata(path: Path | None) -> tuple[dict[str, Any], bool]:
    """Read safe configuration/provenance hints from run_meta.json.

    The returned mapping contains no paths or identities.  ``False`` means
    metadata was absent/unusable, not that the run was fake.
    """

    if path is None or not path.is_file():
        return {}, False
    try:
        if path.stat().st_size > 8 * 1024 * 1024:
            return {}, False
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle, object_pairs_hook=_object_pairs, parse_constant=_reject_constant)
    except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        return {}, False
    if not isinstance(value, Mapping):
        return {}, False
    allocation = value.get("allocation")
    selection = value.get("selection")
    provenance = value.get("runtime_provenance")
    result: dict[str, Any] = {
        "mode": _safe_mode(value.get("mode")),
        "policy": _safe_policy(allocation.get("policy")) if isinstance(allocation, Mapping) else None,
        "selection_enabled": _safe_bool(selection.get("enabled")) if isinstance(selection, Mapping) else None,
        "max_parallel": (
            value.get("max_parallel")
            if isinstance(value.get("max_parallel"), int)
            and not isinstance(value.get("max_parallel"), bool)
            and value.get("max_parallel") > 0
            else None
        ),
        "test_only": bool(provenance.get("test_only")) if isinstance(provenance, Mapping) else False,
        "mock": bool(value.get("mock_agent") or value.get("mock") or (provenance.get("mock_agent") if isinstance(provenance, Mapping) else False)),
        "provenance_present": isinstance(provenance, Mapping) and bool(provenance),
        "dry_run": bool(value.get("dry_run")),
    }
    return result, True


def _configuration(state: _AuditState, metadata: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    candidates = list(state.config_rows)
    if metadata:
        candidates.append(
            {
                "mode": metadata.get("mode"),
                "policy": metadata.get("policy"),
                "selection_enabled": metadata.get("selection_enabled"),
                "max_parallel": metadata.get("max_parallel"),
            }
        )
    selected: dict[str, Any] = {"mode": None, "policy": None, "selection_enabled": None, "max_parallel": None}
    conflict = False
    source = "unknown"
    for candidate in candidates:
        if not any(value is not None for value in candidate.values()):
            continue
        source = "run.configuration" if candidate in state.config_rows else "run_meta"
        for key in selected:
            value = candidate.get(key)
            if value is None:
                continue
            if selected[key] is not None and selected[key] != value:
                conflict = True
            else:
                selected[key] = value
    # Event presence is a deliberately weak fallback for old profiles that
    # predate run.configuration.  It is marked inferred so callers do not
    # mistake it for authoritative manifest data.
    if selected["selection_enabled"] is None:
        # Do not infer ``selection_enabled=false`` merely because no
        # selection event was observed.  Old profiles without configuration
        # are branch-unknown; calling them not_applicable would hide a broken
        # collector and make a missing selection path look healthy.
        if _event_has(state.events, "selection"):
            selected["selection_enabled"] = True
            source = "inferred"
    if selected["mode"] is None and _event_has(state.events, "cps"):
        selected["mode"] = "cps"
        source = "inferred"
    if conflict:
        state.issue("configuration_conflict")
    if not state.config_rows and not metadata:
        state.issue("configuration_missing")
    if selected["policy"] is None and (_event_has(state.events, "trace") or _event_has(state.events, "selection")):
        # Unknown policy makes branch-sensitive conclusions unsafe.
        state.issue("configuration_policy_unknown")
    return {
        "mode": selected["mode"] or "unknown",
        "policy": selected["policy"] or "unknown",
        "selection_enabled": selected["selection_enabled"],
        "max_parallel": selected["max_parallel"],
        "source": source,
        "trace_state_progress_exclusive": selected["policy"] in _TRACE_POLICIES,
    }, conflict


def _scope_requirement_seen(
    state: _AuditState,
    scope: tuple[str, str, str, str],
    requirement: str,
    *,
    target: str | None = None,
) -> bool:
    scope_events = state.correlation_scope_events
    scope_span_starts = state.correlation_scope_span_starts
    scope_span_ends = state.correlation_scope_span_ends
    if target == "record_search_lock":
        scope_events = state.record_search_scope_events
        scope_span_starts = state.record_search_scope_span_starts
        scope_span_ends = state.record_search_scope_span_ends
    if requirement.startswith("span:"):
        base = requirement[5:]
        return (
            scope_span_starts.get((scope, base), 0) > 0
            and scope_span_ends.get((scope, base), 0) > 0
        )
    # Some contracts intentionally expose the concrete wire endpoint names
    # (for example ``selection.persist.start``/``.end``) instead of hiding
    # them behind a span descriptor.  ``_record_correlation`` stores span
    # endpoints in paired counters, so resolve those exact names here while
    # retaining the strict start/end distinction.
    if requirement.endswith((".start", ".end")):
        base = requirement.rsplit(".", 1)[0]
        if base in _SPAN_BASES:
            counter = (
                scope_span_starts
                if requirement.endswith(".start")
                else scope_span_ends
            )
            return counter.get((scope, base), 0) > 0
    return requirement in scope_events.get(scope, set())


def _run_requirement_seen(
    state: _AuditState,
    run_key: str,
    requirement: str,
    *,
    target: str | None = None,
) -> bool:
    if requirement.startswith("span:"):
        base = requirement[5:]
        return (
            state.correlation_run_span_starts.get((run_key, base), 0) > 0
            and state.correlation_run_span_ends.get((run_key, base), 0) > 0
        )
    if requirement.endswith((".start", ".end")):
        base = requirement.rsplit(".", 1)[0]
        if base in _SPAN_BASES:
            counter = (
                state.correlation_run_span_starts
                if requirement.endswith(".start")
                else state.correlation_run_span_ends
            )
            return counter.get((run_key, base), 0) > 0
    return requirement in state.correlation_run_events.get(run_key, set())


def _scope_attempt_status(state: _AuditState, scope: tuple[str, str, str, str]) -> str:
    """Return the safest bounded attempt-boundary conclusion for one scope."""

    starts = state.correlation_attempt_starts.get(scope, Counter())
    ends = state.correlation_attempt_ends.get(scope, Counter())
    # Prefer the outer lifecycle.  If a specialized runner has no outer
    # lifecycle, use its dispatch/wrapper envelope as the boundary.  Nested
    # ``attempt.agent.invoke`` is only a final fallback.
    chosen: str | None = None
    for base in _CORRELATION_ATTEMPT_BASES:
        if starts.get(base, 0) or ends.get(base, 0):
            chosen = base
            break
    if chosen is None:
        return "unknown"
    started = starts.get(chosen, 0)
    ended = ends.get(chosen, 0)
    if started != ended:
        return "missing_terminal"
    if started > 1 or ended > 1:
        # A retry/replay using the same task/actor/episode must not let stage
        # rows from two attempts combine into one apparent complete scope.
        return "split_attempt"
    return "proven"


def _correlation_result(
    state: _AuditState,
    target: str,
    requirements: Iterable[str],
    *,
    required_any: Iterable[Iterable[str]] = (),
    run_level: bool = False,
) -> dict[str, Any]:
    """Evaluate a conjunction inside one canonical scope, never by unioning rows."""

    required_list = list(dict.fromkeys(str(item) for item in requirements))
    any_groups = [list(dict.fromkeys(str(item) for item in group)) for group in required_any]
    any_groups = [group for group in any_groups if group]
    if run_level:
        keys = tuple(state.correlation_runs)
        complete_keys = []
        observed_keys = []
        incomplete_span_keys = 0
        missing_run_stage = {
            item for item in required_list if state.correlation_unattributed.get("run:" + item, 0)
        }
        for key in keys:
            required_ok = all(_run_requirement_seen(state, key, item) for item in required_list)
            any_ok = all(any(_run_requirement_seen(state, key, item) for item in group) for group in any_groups)
            observed = any(_run_requirement_seen(state, key, item) for item in required_list) or any(
                any(_run_requirement_seen(state, key, item) for item in group) for group in any_groups
            )
            if observed:
                observed_keys.append(key)
                if any(
                    item.startswith("span:")
                    and state.correlation_run_span_starts.get((key, item[5:]), 0)
                    != state.correlation_run_span_ends.get((key, item[5:]), 0)
                    for item in required_list
                ):
                    incomplete_span_keys += 1
            if required_ok and any_ok and key not in state.correlation_duplicate_runs and not missing_run_stage:
                complete_keys.append(key)
        if state.correlation_cross_run:
            status = "cross_run"
        elif state.correlation_overflow:
            status = "overflow"
        elif complete_keys:
            status = "proven"
        elif state.correlation_duplicate_runs and observed_keys:
            status = "duplicate_replay"
        elif missing_run_stage and observed_keys:
            status = "missing_dimensions"
        elif observed_keys:
            status = (
                "split_scope"
                if len(observed_keys) > 1
                else ("missing_terminal" if incomplete_span_keys else "missing_required")
            )
        elif state.correlation_missing.get("run_id", 0):
            status = "missing_dimensions"
        else:
            status = "not_observed"
        return {
            "scope": "run",
            "state": status,
            "complete": status == "proven",
            "scope_count": len(keys),
            "observed_scope_count": len(observed_keys),
            "complete_scope_count": len(complete_keys),
            "required_families": required_list,
            "required_any_families": any_groups,
            "unattributed_stage_rows": 0,
            "missing_dimensions": {
                key: count for key, count in sorted(state.correlation_missing.items()) if key == "run_id"
            },
            "duplicate_replay_count": state.correlation_duplicate_count,
            "missing_run_stage_families": sorted(missing_run_stage),
        }

    keys = tuple(state.correlation_scopes)
    # Operation-filtered targets maintain their own scope key set.  Using the
    # broad correlation scope set here would let a non-record_search write
    # make a record_search lock target appear observed (or join its stages).
    if target == "record_search_lock":
        keys = tuple(state.record_search_scope_events)
    complete_keys: list[tuple[str, str, str, str]] = []
    observed_keys: list[tuple[str, str, str, str]] = []
    boundary_states: Counter[str] = Counter()
    target_duplicate_scopes = (
        state.record_search_duplicate_scopes
        if target == "record_search_lock"
        else state.correlation_duplicate_scopes
    )
    target_duplicate_count = (
        state.record_search_duplicate_count
        if target == "record_search_lock"
        else state.correlation_duplicate_count
    )
    target_unattributed = (
        state.record_search_unattributed
        if target == "record_search_lock"
        else state.correlation_unattributed
    )
    for key in keys:
        required_ok = all(_scope_requirement_seen(state, key, item, target=target) for item in required_list)
        any_ok = all(any(_scope_requirement_seen(state, key, item, target=target) for item in group) for group in any_groups)
        observed = any(_scope_requirement_seen(state, key, item, target=target) for item in required_list) or any(
            any(_scope_requirement_seen(state, key, item, target=target) for item in group) for group in any_groups
        )
        if observed:
            observed_keys.append(key)
        boundary = _scope_attempt_status(state, key)
        boundary_states[boundary] += 1
        boundary_ok = boundary == "proven" or target == "record_search_lock"
        if required_ok and any_ok and boundary_ok and key not in target_duplicate_scopes:
            complete_keys.append(key)

    # ``complete_keys`` is the only path to ``proven``.  In particular, the
    # union of complementary stage rows from two scopes is never accepted.
    if complete_keys:
        status = "proven"
    elif state.correlation_overflow:
        status = "overflow"
    elif target_duplicate_count and observed_keys:
        status = "duplicate_replay"
    elif boundary_states.get("split_attempt") and observed_keys:
        status = "split_attempt"
    elif boundary_states.get("missing_terminal") and observed_keys:
        status = "missing_terminal"
    elif len(observed_keys) > 1:
        status = "split_scope"
    elif observed_keys:
        status = "missing_required"
    elif any(key.startswith(target + ":") for key in target_unattributed):
        status = "missing_dimensions"
    else:
        status = "not_observed"
    target_missing = (
        state.record_search_missing
        if target == "record_search_lock"
        else state.correlation_missing
    )
    missing_dimensions = {
        key: count
        for key, count in sorted(target_missing.items())
        if key.endswith(":" + target) or key in _CORRELATION_DIMENSIONS
    }
    return {
        "scope": "run_task_actor_episode",
        "state": status,
        "complete": status == "proven",
        "scope_count": len(keys),
        "observed_scope_count": len(observed_keys),
        "complete_scope_count": len(complete_keys),
        "required_families": required_list,
        "required_any_families": any_groups,
        "unattributed_stage_rows": sum(
            count for key, count in target_unattributed.items() if key.startswith(target + ":")
        ),
        "missing_dimensions": missing_dimensions,
        "duplicate_replay_count": state.correlation_duplicate_count,
        "boundary_states": dict(sorted(boundary_states.items())),
    }


def _correlation_report(state: _AuditState) -> dict[str, Any]:
    """Return only bounded counts/statuses; no canonical key is serialized."""

    return {
        "canonicalization": "sha256_internal_only",
        "run_count": len(state.correlation_runs),
        "scope_count": len(state.correlation_scopes),
        "missing_dimensions": dict(sorted(state.correlation_missing.items())),
        "unattributed_stage_rows": dict(sorted(state.correlation_unattributed.items())),
        "duplicate_replay_count": state.correlation_duplicate_count,
        "cross_run": state.correlation_cross_run,
        "overflow": state.correlation_overflow,
    }


def _coverage(state: _AuditState, config: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, dict[str, Any]], dict[str, str]]:
    """Evaluate the reviewed target contracts from exact stage events.

    The audit intentionally has two views for every target.  ``plumbing``
    records which individual hooks fired, while ``goal_complete`` is true
    only when the target's full conjunction is present.  This prevents a
    summary event (for example ``selection.rank.summary``) from masquerading
    as a complete rank stage.  A metadata-marked test/mock run is reported as
    ``not_applicable`` at the goal level but retains the plumbing details for
    collector smoke diagnostics.
    """

    events = state.events
    policy = config.get("policy")
    mode = config.get("mode")
    selection_enabled = config.get("selection_enabled")
    max_parallel = config.get("max_parallel")
    trace_policy = policy in _TRACE_POLICIES
    dry = state.saw_dry_run
    plumbing_only = bool(config.get("plumbing_only"))

    simple: dict[str, str] = {}
    details: dict[str, dict[str, Any]] = {}
    legacy: dict[str, str] = {}

    def put(
        target: str,
        state_name: str,
        *,
        required: Iterable[str] = (),
        required_any: Iterable[Iterable[str]] = (),
        observed: Iterable[str] = (),
        auxiliary: Iterable[str] = (),
        event_counts: Mapping[str, int] | None = None,
        note: str = "",
        correlation: Mapping[str, Any] | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        """Store one target result, using exact event/span conjunctions."""

        if state_name not in COVERAGE_STATES:
            state_name = "invalid"
        required_list = list(dict.fromkeys(str(item) for item in required))
        required_any_list: list[list[str]] = []
        for group in required_any:
            normalized_group = list(dict.fromkeys(str(option) for option in group))
            if normalized_group:
                required_any_list.append(normalized_group)
        observed_list = list(dict.fromkeys(str(item) for item in observed))
        auxiliary_list = list(dict.fromkeys(str(item) for item in auxiliary))

        def seen(requirement: str) -> bool:
            # Most targets use the global reviewed event counter.  The
            # record_search target supplies an operation-filtered counter so
            # a shared selection.persist.* label from another write cannot
            # satisfy its required conjunction.
            return _requirement_seen(events if event_counts is None else event_counts, requirement)

        missing_required = [item for item in required_list if not seen(item)]
        missing_required_any = [
            group for group in required_any_list if not any(seen(option) for option in group)
        ]
        required_complete = not missing_required and not missing_required_any
        correlation_complete = True if correlation is None else bool(correlation.get("complete"))
        correlation_state = "not_required" if correlation is None else str(correlation.get("state") or "unknown")
        presence: dict[str, Any] = {
            "families": {item: seen(item) for item in required_list},
            "required_any": [
                {"families": {option: seen(option) for option in group}, "present": any(seen(option) for option in group)}
                for group in required_any_list
            ],
            "any": bool(observed_list)
            or any(any(seen(option) for option in group) for group in required_any_list),
        }
        # Auxiliary observations are deliberately kept outside the required
        # conjunction.  In particular, ``resource.sample`` is a run-level
        # aggregate and normally has no task/actor/episode attribution; it
        # must never make (or break) an attempt-level Agent-vs-wrapper claim.
        auxiliary_presence = {item: seen(item) for item in auxiliary_list}
        observed_any = bool(presence["any"])
        if state_name in {"present", "partial", "missing"} and not required_complete:
            state_name = "partial" if observed_any else "missing"
        if state_name == "present" and not correlation_complete:
            state_name = "partial" if observed_any else "missing"

        # Correlation failures are fixed, bounded issue labels.  Never derive
        # an issue code from a caller/event identity or arbitrary text.
        if (
            correlation is not None
            and not correlation_complete
            and not plumbing_only
            and state_name in {"present", "partial", "missing"}
        ):
            correlation_issue = {
                "missing_dimensions": "correlation_missing",
                "split_scope": "correlation_split_scope",
                "split_attempt": "correlation_split_attempt",
                "missing_terminal": "correlation_missing_terminal",
                "duplicate_replay": "correlation_duplicate_replay",
                "cross_run": "correlation_cross_run",
                "overflow": "correlation_overflow",
                "missing_required": "correlation_missing_required",
                "not_observed": "correlation_not_observed",
            }.get(correlation_state, "correlation_unknown")
            state.issue(correlation_issue)

        # A test/mock profile is useful for checking the event plumbing but
        # must not be accepted as a real goal baseline.  Preserve its detailed
        # per-stage presence and expose the distinction explicitly.
        raw_state = state_name
        goal_complete = bool(required_complete and correlation_complete and state_name == "present" and not plumbing_only)
        if plumbing_only and state_name not in {"invalid"}:
            state_name = "not_applicable"
        simple[target] = state_name
        legacy[target] = LEGACY_STATUS[state_name]
        detail: dict[str, Any] = {
            "state": state_name,
            "status": LEGACY_STATUS[state_name],
            "required_families": required_list,
            "required_any_families": required_any_list,
            "observed_families": observed_list,
            "auxiliary_families": auxiliary_list,
            "auxiliary_presence": auxiliary_presence,
            "missing_required_families": missing_required if state_name != "not_applicable" else [],
            "missing_required_any_families": missing_required_any if state_name != "not_applicable" else [],
            "present": bool(required_list or required_any_list) and required_complete and correlation_complete,
            "plumbing_presence": presence,
            "goal_complete": goal_complete,
            "evaluation": "plumbing_only" if plumbing_only else ("not_applicable" if raw_state == "not_applicable" else "goal"),
            "correlation": dict(correlation) if correlation is not None else {"state": "not_required", "complete": True},
        }
        if raw_state != state_name and plumbing_only:
            detail["plumbing_state"] = raw_state
            detail["plumbing_missing_required_families"] = missing_required
            detail["plumbing_missing_required_any_families"] = missing_required_any
            if correlation is not None:
                detail["plumbing_correlation_state"] = correlation_state
        if note:
            detail["condition"] = note
        if extra:
            detail.update(extra)
        details[target] = detail

    # Agent/wrapper: require the supervision span, logical Pi lifecycle, and
    # an attempt-attributed process-tree observation.  The run-wide
    # ``resource.sample`` aggregate is deliberately auxiliary: it has no
    # task/actor/episode identity in real profiles and therefore cannot be
    # part of this same-attempt conjunction.
    agent_required = (
        "span:attempt.agent.invoke",
        "span:agent",
        "resource.process",
    )
    # ``attempt.wrapper.dispatch`` is the concrete envelope emitted by the
    # specialized/mock runner; older runners use the broader wrapper span or
    # the recovery-oriented lifecycle span.  Any one complete envelope is
    # sufficient, but no summary-only event can satisfy this variant.
    agent_required_any = (("span:attempt.lifecycle", "span:attempt.wrapper", "span:attempt.wrapper.dispatch"),)
    agent_observed = [
        item
        for item in (
            *agent_required,
            "span:attempt.lifecycle",
            "span:attempt.wrapper",
            "span:attempt.wrapper.dispatch",
            "resource.process.self",
            "resource.process.register",
            "resource.process.unregister",
            "agent.heartbeat",
            "resource.sample",
        )
        if _requirement_seen(events, item)
    ]
    agent_resource_unknown = (
        _requirement_seen(events, "resource.process.register")
        and _requirement_seen(events, "resource.process.unregister")
        and not _requirement_seen(events, "resource.process")
    )
    agent_correlation = _correlation_result(
        state,
        "agent_wrapper",
        agent_required,
        required_any=agent_required_any,
    )
    put(
        "agent_wrapper",
        "present" if agent_observed else "missing",
        required=agent_required,
        required_any=agent_required_any,
        observed=agent_observed,
        auxiliary=("resource.sample", "resource.process.self"),
        correlation=agent_correlation,
        extra={
            "resource_observation": "unknown" if agent_resource_unknown else (
                "present" if _requirement_seen(events, "resource.process") else "missing"
            ),
            "aggregate_resource_observation": (
                "present" if _requirement_seen(events, "resource.sample") else "unknown"
            ),
        },
    )

    # Selection requires every material stage, not merely the high-level
    # summary spans.  Each ``span:`` item means exact start+end endpoints.
    selection_required = (
        "span:selection.eligible",
        "selection.eligible.read",
        "selection.eligible.filter",
        "selection.eligible.query_terms",
        "selection.eligible.materialize",
        "selection.snapshot",
        "span:selection.rank",
        "span:selection.pack",
        "selection.payload.materialize",
        "span:selection.persist",
        "selection.persist.payload",
        "selection.persist.readback",
        "selection.persist.readback.query",
        "selection.sqlite.connect",
    )
    selection_observed = [item for item in selection_required if _requirement_seen(events, item)] + [
        item
        for item in ("selection.rank.summary", "selection.pack.summary", "selection.eligible.summary", "selection.search.summary")
        if _event_seen(events, item)
    ]
    selection_correlation = _correlation_result(
        state,
        "selection",
        selection_required,
    )
    if selection_enabled is False:
        put("selection", "not_applicable", required=selection_required, observed=selection_observed, note="selection disabled")
    else:
        put(
            "selection",
            "present" if selection_observed else ("invalid" if selection_enabled is None else "missing"),
            required=selection_required,
            observed=selection_observed,
            correlation=selection_correlation,
        )

    # Trace projection has two concrete branches.  Trace-aware allocation
    # uses the bridge chain; legacy policies may use SelectionRuntime's
    # ``trace.project`` chain.  A bridge page/summary without a DB query is a
    # valid zero/fallback branch, but is explicitly conditional rather than a
    # complete query measurement.
    trace_project_required = (
        "span:trace.project",
        "trace.project.query",
        "trace.project.read",
        "trace.project.materialize",
        "trace.project.summary",
    )
    trace_bridge_required = (
        "trace.bridge.page",
        "span:trace.bridge.project",
        "trace.bridge.materialize",
        "trace.bridge.summary",
        "trace.bridge.sqlite.query",
    )
    if selection_enabled is False:
        put(
            "trace_projection",
            "not_applicable",
            required=trace_project_required + trace_bridge_required,
            observed=(),
            note="selection disabled",
        )
    elif trace_policy:
        bridge_observed = [item for item in trace_bridge_required if _requirement_seen(events, item)]
        bridge_any = bool(bridge_observed)
        bridge_query_missing = not _requirement_seen(events, "trace.bridge.sqlite.query")
        trace_state = "present" if len(bridge_observed) == len(trace_bridge_required) else (
            "conditional_missing" if bridge_any and bridge_query_missing else ("partial" if bridge_any else "missing")
        )
        put(
            "trace_projection",
            trace_state,
            required=trace_bridge_required,
            observed=bridge_observed,
            note="trace policy bridge branch; zero/fallback source may omit sqlite query" if bridge_query_missing else "trace policy bridge branch",
            correlation=_correlation_result(state, "trace_projection", trace_bridge_required, run_level=True),
        )
    elif policy in _POLICIES:
        project_observed = [item for item in trace_project_required if _requirement_seen(events, item)]
        project_any = bool(project_observed)
        put(
            "trace_projection",
            "present" if len(project_observed) == len(trace_project_required) else ("partial" if project_any else "conditional_missing"),
            required=trace_project_required,
            observed=project_observed,
            note="legacy allocation/selection trace.project branch",
            correlation=_correlation_result(state, "trace_projection", trace_project_required, run_level=True),
        )
    else:
        put("trace_projection", "invalid", required=trace_bridge_required, note="unknown policy")

    if max_parallel is not None and isinstance(max_parallel, int) and max_parallel <= 1:
        put("max_parallel", "not_applicable", required=("attempt.admitted", "resource.sample", "resource.process"), note="max_parallel <= 1")
    else:
        max_required = ("attempt.admitted", "resource.sample", "resource.process")
        max_observed = [item for item in max_required + ("attempt.solver_slot_released",) if _requirement_seen(events, item)]
        put("max_parallel", "present" if max_observed else "missing", required=max_required, observed=max_observed, correlation=_correlation_result(state, "max_parallel", ("attempt.admitted", "resource.process"), run_level=True), extra={"resource_observation": "present" if _event_seen(events, "resource.sample") else "unknown"})

    cps_required = (
        "span:cps.progress",
        "cps.progress.query",
        "cps.progress.materialize",
        "cps.progress.summary",
        "cps.sqlite.connect",
    )
    cps_observed = [item for item in cps_required if _requirement_seen(events, item)] + [
        item for item in ("cps.search.query", "cps.search.materialize", "cps.inbox.query", "cps.inbox.materialize", "cps.digest.summary", "cps.write.queue", "cps.write.lock", "cps.write.commit") if _event_seen(events, item)
    ]
    if mode not in {"cps"}:
        put("cps", "not_applicable", required=cps_required, observed=cps_observed, note="mode is not cps")
    elif trace_policy:
        progress_complete = all(_requirement_seen(events, item) for item in cps_required)
        cps_state = "present" if progress_complete else "conditional_missing"
        put("cps", cps_state, required=cps_required, observed=cps_observed, note="trace_state/llm_scheduler skips ordinary progress_snapshot", correlation=_correlation_result(state, "cps", cps_required, run_level=True))
    else:
        put("cps", "present" if all(_requirement_seen(events, item) for item in cps_required) else ("partial" if cps_observed else "missing"), required=cps_required, observed=cps_observed, correlation=_correlation_result(state, "cps", cps_required, run_level=True))

    judge_required = ("span:judge.execute", "judge.receipt", "span:drain")
    judge_observed = [item for item in judge_required if _requirement_seen(events, item)] + [item for item in ("judge.http.start", "judge.http.end", "judge.audit.end", "drain.sample") if _event_seen(events, item)]
    if dry:
        put("judge", "not_applicable", required=judge_required, observed=judge_observed, note="dry run")
    else:
        judge_correlation = _correlation_result(state, "judge", ("span:judge.execute", "judge.receipt"))
        put("judge", "present" if judge_observed else "missing", required=judge_required, observed=judge_observed, correlation=judge_correlation)

    # ``record_search_lock`` is deliberately audited independently from the
    # broader selection chain.  A selection summary or a generic SQLite
    # connect row cannot answer the lock-contention question.  Require the
    # exact writer lifecycle markers and prove that all three belong to one
    # run/task/actor/episode scope.  ``selection.persist.start`` and
    # ``selection.persist.end`` are span endpoints in the correlation state;
    # the concrete names remain in the public contract and are resolved by
    # ``_scope_requirement_seen`` above.
    record_search_required = (
        "selection.persist.start",
        "selection.persist.lock",
        "selection.persist.end",
    )
    record_search_events = state.record_search_events
    record_search_observed = [
        item for item in record_search_required if _event_seen(record_search_events, item)
    ] + [
        item
        for item in (
            "selection.persist.queue",
            "selection.persist.payload",
            "selection.persist.readback",
            "selection.persist.readback.query",
        )
        if _event_seen(record_search_events, item)
    ]
    record_search_correlation = _correlation_result(
        state,
        "record_search_lock",
        record_search_required,
    )
    if selection_enabled is False:
        put(
            "record_search_lock",
            "not_applicable",
            required=record_search_required,
            observed=record_search_observed,
            note="selection disabled",
            extra={"operation_filter": "operation == record_search"},
        )
    elif selection_enabled is True:
        # No writer invocation is a conditional branch rather than evidence
        # that the lock path is healthy.  Keep it visible and non-zero so a
        # real profiling run cannot silently claim lock coverage when no
        # record_search operation happened.
        if not any(_event_seen(record_search_events, item) for item in record_search_required):
            record_search_state = "conditional_missing"
        else:
            record_search_state = (
                "present"
                if all(_event_seen(record_search_events, item) for item in record_search_required)
                else "partial"
            )
        put(
            "record_search_lock",
            record_search_state,
            required=record_search_required,
            observed=record_search_observed,
            event_counts=record_search_events,
            correlation=record_search_correlation,
            note="record_search writer transaction; exact start/lock/end markers",
            extra={"operation_filter": "operation == record_search"},
        )
    else:
        # Unknown selection configuration must not be interpreted as a
        # disabled path.  If any marker exists, report the partial evidence;
        # otherwise retain an explicit missing state for the unknown branch.
        put(
            "record_search_lock",
            "partial" if record_search_observed else "missing",
            required=record_search_required,
            observed=record_search_observed,
            event_counts=record_search_events,
            correlation=record_search_correlation,
            note="selection applicability unknown",
            extra={"operation_filter": "operation == record_search"},
        )

    # The mutually-exclusive branch is an invariant, not merely a coverage
    # hint.  A trace policy that emits the ordinary progress chain is
    # contradictory unless a future explicit observational-probe flag is
    # added to the run configuration.
    if trace_policy and any(_requirement_seen(events, item) for item in cps_required):
        state.issue("trace_progress_exclusive_violation")
        details["cps"]["state"] = "invalid"
        details["cps"]["status"] = "invalid"
        details["cps"]["goal_complete"] = False
        simple["cps"] = "invalid"
        legacy["cps"] = "invalid"
    return simple, details, legacy


def _empty_report(*, exit_code: int, issue: str) -> dict[str, Any]:
    simple = {target: "invalid" for target in TARGETS}
    detail = {
        target: {
            "state": "invalid",
            "status": "invalid",
            "required_families": [],
            "required_any_families": [],
            "observed_families": [],
            "present": False,
            "plumbing_presence": {"families": {}, "required_any": [], "any": False},
            "goal_complete": False,
            "evaluation": "invalid",
            "correlation": {"state": "invalid", "complete": False},
        }
        for target in TARGETS
    }
    return {
        "audit_schema_version": AUDIT_SCHEMA_VERSION,
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "ok": False,
        "exit_code": exit_code,
        "coverage": simple,
        "coverage_status": {target: "invalid" for target in TARGETS},
        "coverage_detail": detail,
        "correlation": {"state": "invalid", "complete": False},
        "issues": [{"code": issue, "count": 1}],
        "errors": [{"code": issue, "count": 1}],
    }


def _resolve_input(input_path: str | Path) -> tuple[Path, Path | None]:
    try:
        candidate = Path(input_path)
    except (TypeError, ValueError, OSError) as exc:
        raise InputError("invalid input") from exc
    if candidate.is_dir():
        profile = candidate / PROFILE_FILENAME
        metadata = candidate / "run_meta.json"
    else:
        profile = candidate
        metadata = candidate.parent / "run_meta.json"
    if not profile.is_file():
        raise InputError("profile file not found")
    return profile, metadata if metadata.is_file() else None


def audit_profiling(input_path: str | Path, *, run_meta: str | Path | Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Audit one profile file or run directory with a single streaming pass."""

    try:
        profile_path, default_meta = _resolve_input(input_path)
    except InputError as exc:
        return _empty_report(exit_code=2, issue="input_unreadable")

    metadata: dict[str, Any] = {}
    metadata_available = False
    if isinstance(run_meta, Mapping):
        # Only consume the same safe fields as _read_metadata; never retain the
        # caller's mapping or expose arbitrary keys.
        raw = run_meta
        allocation = raw.get("allocation")
        selection = raw.get("selection")
        provenance = raw.get("runtime_provenance")
        metadata = {
            "mode": _safe_mode(raw.get("mode")),
            "policy": _safe_policy(allocation.get("policy")) if isinstance(allocation, Mapping) else None,
            "selection_enabled": _safe_bool(selection.get("enabled")) if isinstance(selection, Mapping) else None,
            "max_parallel": raw.get("max_parallel") if isinstance(raw.get("max_parallel"), int) and not isinstance(raw.get("max_parallel"), bool) else None,
            "test_only": bool(provenance.get("test_only")) if isinstance(provenance, Mapping) else False,
            "mock": bool(raw.get("mock_agent") or raw.get("mock") or (provenance.get("mock_agent") if isinstance(provenance, Mapping) else False)),
            "provenance_present": isinstance(provenance, Mapping) and bool(provenance),
            "dry_run": bool(raw.get("dry_run")),
        }
        metadata_available = True
    else:
        metadata_path = Path(run_meta) if run_meta is not None else default_meta
        metadata, metadata_available = _read_metadata(metadata_path)

    state = _AuditState()
    try:
        with profile_path.open("r", encoding="utf-8", errors="strict") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    state.blank_lines += 1
                    continue
                # Refuse pathological records before json.loads allocates an
                # unbounded object.  The profiler emits much smaller rows.
                if len(line.encode("utf-8")) > 4 * 1024 * 1024:
                    raise InputError("profile row too large")
                try:
                    row = _parse_json_line(line)
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    raise InputError("invalid JSONL row") from exc
                _validate_row(row, line_number, state)
    except InputError as exc:
        return _empty_report(exit_code=2, issue="profile_schema_invalid")
    except (OSError, UnicodeError):
        return _empty_report(exit_code=2, issue="input_unreadable")

    if state.rows == 0:
        state.issue("empty_profile")
    if state.first_sequence != 1:
        state.issue("sequence_does_not_start_at_one")
    for base, starts in state.span_starts.items():
        ends = state.span_ends.get(base, 0)
        if starts > ends:
            state.issue("span_missing_end")
    if state.span_ends.get("profile", 0) == 0:
        state.issue("profile_terminal_missing")
    if state.span_starts.get("profile", 0) == 0:
        state.issue("profile_start_missing")
    if state.terminal_events.get("profile.end", 0) > 1:
        state.issue("profile_terminal_duplicate")
    if state.terminal_events.get("run.end", 0) + state.terminal_events.get("run.error", 0) + state.terminal_events.get("run.dry_end", 0) > 1:
        state.issue("run_terminal_duplicate")

    config, _ = _configuration(state, metadata)
    # A plumbing smoke (the local mock/test-only runner) exercises collector
    # wiring but is not evidence for a real agent/selection baseline.  Carry
    # only this bounded boolean into coverage evaluation; never copy the
    # provenance object or any caller-provided metadata into the report.
    config["plumbing_only"] = bool(metadata.get("test_only") or metadata.get("mock"))
    if metadata.get("dry_run"):
        state.saw_dry_run = True
    simple, detail, legacy = _coverage(state, config)

    # Any issue means the profile is not a clean pass.  Conditional/missing
    # applicable coverage is intentionally non-zero under the exit contract.
    quality_failure = bool(state.issues)
    # ``partial`` is an applicable target with an incomplete conjunction.  It
    # must fail the clean-profile gate just like ``missing``: otherwise a run
    # that omitted (for example) the resource or admission half of a target
    # would be reported as a successful baseline while the detail object says
    # ``missing_required``.
    coverage_failure = any(
        value in {"partial", "missing", "conditional_missing", "invalid"}
        for value in simple.values()
    )
    ok = not quality_failure and not coverage_failure
    exit_code = 0 if ok else 1

    numeric = {key: value.as_dict() for key, value in sorted(state.numeric.items())}
    peaks = {key: data["max"] for key, data in numeric.items() if data.get("max") is not None}
    percentiles = {
        key: {name: data[name] for name in ("p50", "p95", "p99")}
        for key, data in numeric.items()
    }
    dropped = {
        "rows": state.dropped_rows,
        "total": state.dropped_total,
        "max_per_row": state.dropped_max,
    }
    sensitive_total = sum(state.sensitive.values())
    sensitive = {
        "total": sensitive_total,
        "categories": {key: state.sensitive[key] for key in sorted(state.sensitive)},
    }
    sequence = {
        "valid": not any(
            (state.sequence_gaps, state.sequence_duplicates, state.sequence_out_of_order)
        ) and state.first_sequence == 1,
        "rows": state.rows,
        "first": state.first_sequence,
        "last": state.last_seen_sequence,
        "gaps": state.sequence_gaps,
        "duplicates": state.sequence_duplicates,
        "out_of_order": state.sequence_out_of_order,
    }
    span_bases = sorted(set(state.span_starts) | set(state.span_ends))
    spans = {
        "valid": not any(item["code"].startswith("span_") for item in state.issues) and state.span_orphans == 0,
        "starts": sum(state.span_starts.values()),
        "ends": sum(state.span_ends.values()),
        "open": sum(max(0, state.span_starts.get(base, 0) - state.span_ends.get(base, 0)) for base in span_bases),
        "orphan_ends": state.span_orphans,
        "families": {base: {"starts": state.span_starts.get(base, 0), "ends": state.span_ends.get(base, 0)} for base in span_bases if _safe_family(base) != "other"},
    }
    terminal = {
        "valid": (
            state.events.get("profile.start", 0) == 1
            and state.terminal_events.get("profile.end", 0) == 1
        ),
        "events": {key: state.terminal_events[key] for key in _TERMINAL_EVENTS if state.terminal_events.get(key)},
        "profile_started": bool(state.events.get("profile.start")),
        "profile_ended": bool(state.terminal_events.get("profile.end")),
    }
    issues = list(state.issues)
    report = {
        "audit_schema_version": AUDIT_SCHEMA_VERSION,
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "ok": ok,
        "exit_code": exit_code,
        "realness": (
            "non_real"
            if metadata.get("test_only") or metadata.get("mock")
            else ("real" if metadata_available and metadata.get("provenance_present") else "realness_unknown")
        ),
        "configuration": config,
        "coverage": simple,
        "coverage_status": legacy,
        "coverage_detail": detail,
        "correlation": _correlation_report(state),
        "profile": {
            "rows": state.rows,
            "blank_lines": state.blank_lines,
            "schema": {
                "valid": True,
                "versions": dict(state.schema_versions),
            },
            "sequence": sequence,
            "spans": spans,
            "termination": terminal,
            "dropped_fields": dropped,
            "sensitive_fields": sensitive,
            "field_presence": {key: state.field_presence[key] for key in sorted(state.field_presence)},
        },
        "termination": terminal,
        "spans": spans,
        "dropped_fields": dropped,
        "sensitive_fields": sensitive,
        "counts": dict(sorted(state.events.items())),
        "event_counts": dict(sorted(state.events.items())),
        "aggregates": {
            "numeric": numeric,
            "peaks": peaks,
            "percentiles": percentiles,
        },
        "peaks": peaks,
        "percentiles": percentiles,
        "issues": issues,
        "errors": issues,
    }
    return report


# Compatibility aliases used by small callers and earlier audit drafts.
audit_profile = audit_profiling
audit_run = audit_profiling


def _text_report(report: Mapping[str, Any]) -> str:
    lines = [
        f"profile audit: {'PASS' if report.get('ok') else 'FAIL'}",
        f"exit_code: {report.get('exit_code', 2)}",
        f"rows: {report.get('profile', {}).get('rows', 0) if isinstance(report.get('profile'), Mapping) else 0}",
        "coverage:",
    ]
    coverage = report.get("coverage")
    if isinstance(coverage, Mapping):
        for target in TARGETS:
            lines.append(f"  {target}: {coverage.get(target, 'invalid')}")
    profile = report.get("profile")
    if isinstance(profile, Mapping):
        dropped = profile.get("dropped_fields", {})
        sensitive = profile.get("sensitive_fields", {})
        lines.append(f"dropped_fields_total: {dropped.get('total', 0) if isinstance(dropped, Mapping) else 0}")
        lines.append(f"sensitive_fields_total: {sensitive.get('total', 0) if isinstance(sensitive, Mapping) else 0}")
        termination = profile.get("termination", {})
        lines.append(f"profile_terminal: {bool(termination.get('profile_ended')) if isinstance(termination, Mapping) else False}")
    return "\n".join(lines) + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit a ContextSwarm profiling JSONL stream (read-only).")
    parser.add_argument("input", nargs="?", help="profiling.jsonl or its run directory")
    parser.add_argument("--input", "--profile", dest="input_option", help=argparse.SUPPRESS)
    parser.add_argument("--run-meta", dest="run_meta", help="optional metadata file (read-only)")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--text", action="store_true", help="shorthand for --format text")
    parser.add_argument("--output", help="write the sanitized report to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    input_value = args.input_option or args.input
    if not input_value:
        parser.print_usage(sys.stderr)
        return 2
    report = audit_profiling(input_value, run_meta=args.run_meta)
    rendered = _text_report(report) if args.text or args.format == "text" else json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if args.output:
        try:
            Path(args.output).write_text(rendered, encoding="utf-8")
        except (OSError, UnicodeError):
            # Do not include the requested path in diagnostics.
            print("profile audit: unable to write report", file=sys.stderr)
            return 2
    else:
        sys.stdout.write(rendered)
    return int(report.get("exit_code", 2))


if __name__ == "__main__":  # pragma: no cover - exercised by CLI smoke tests
    raise SystemExit(main())


__all__ = [
    "AUDIT_SCHEMA_VERSION",
    "COVERAGE_STATES",
    "PROFILE_FILENAME",
    "PROFILE_SCHEMA_VERSION",
    "TARGETS",
    "audit_profile",
    "audit_profiling",
    "audit_run",
    "main",
]
