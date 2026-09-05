"""Runner-owned bridge between CPS pieces and the pure Issue #38 selectors.

The legacy CPS API deliberately remains unchanged.  When a Figure 3 selection
manifest is enabled, :class:`SelectionRuntime` builds one project-wide,
read-only snapshot, invokes the selected pure policy, applies the common packer,
and persists the complete search/exposure chain in :class:`SelectionStore`.
Both prompt digest construction and interactive ``cps_search`` call this same
``search`` method; no direct-message or candidate-file state is included in the
snapshot.
"""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import threading
import time
from typing import Any, Iterable, Mapping

from .cps import CPSStore
from .selection import (
    FeedbackStats,
    SelectionRequest,
    TraceCandidate,
    make_snapshot,
    pack_ranked_by_token_budget,
    build_selector,
    token_count,
)
from .selection_store import CANONICAL_FEEDBACK_KINDS, SelectionStore


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _manifest_sha(value: Any) -> str:
    """Match config.SelectionConfig.selection_config_id serialization."""

    raw = json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _store_identity_sha(value: Any) -> str:
    """Mirror SelectionStore's string-hash identity rule for retry checks."""

    text = str(value or "").strip()
    if re.fullmatch(r"[0-9a-fA-F]{64}", text):
        return text.lower()
    return _sha(value)


# CPS communication/evaluator records are not reusable task knowledge.  Keep
# this explicit denylist so messages, control rows, and runner-owned verdicts
# cannot enter the selector's eligible pool.
_INELIGIBLE_KINDS = frozenset(
    {
        "actor_profile", "candidate", "candidate_file", "candidate_payload",
        "candidate_solution", "control", "control_event", "delivery_receipt",
        "exposure", "feedback", "feedback_event", "lifecycle", "maintenance",
        "message", "runner_control", "search_event", "solution_candidate",
        "validation_result", "verifier_evidence",
    }
)


def _value(value: Any, key: str, default: Any = None) -> Any:
    return value.get(key, default) if isinstance(value, Mapping) else getattr(value, key, default)


def _nonnegative_int(value: Any, name: str, *, default: int | None = None) -> int:
    """Validate request counters before any coercion can alter the contract."""

    if value is None and default is not None:
        value = default
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"selection {name} must be a non-negative integer")
    return value


def _topology_tag(tags: Iterable[str], prefixes: tuple[str, ...]) -> str:
    """Read explicit topology metadata from bounded CPS tags.

    Tags are the only topology carrier available in the mini CPS schema.  A
    prefixed tag is structured metadata; absent metadata remains a singleton
    in the selector rather than being inferred from free text.
    """

    for raw in tags:
        value = str(raw).strip()
        lowered = value.casefold()
        for prefix in prefixes:
            if lowered.startswith(prefix):
                candidate = value[len(prefix):].strip()
                if candidate:
                    return candidate
    return ""


@dataclass(frozen=True)
class _RuntimeTraceCandidate(TraceCandidate):
    """Snapshot candidate exposing aggregate feedback fields to pure policies."""

    relevance: float = 0.0
    evidence_score: float = 0.0
    structure_score: float = 0.0
    state_score: float = 0.0
    lineage_id: str = ""

    @property
    def worker_exposure(self) -> float:
        return float(max(0, int(self.feedback.exposure_count)))

    @property
    def effective_exposures(self) -> float:
        return self.worker_exposure

    @property
    def worker_feedback(self) -> tuple[Mapping[str, Any], ...]:
        if self.feedback.effective_terminal_count <= 0:
            return ()
        return ({"actor": "worker", "value": float(self.feedback.signed_weight_sum),
                 "exposure": self.worker_exposure},)


class SelectionRuntime:
    """Pure-policy coordinator backed by CPS and the attribution store."""

    _MAX_PROJECTION_IDENTITIES = 4096

    def __init__(
        self,
        cps_store: CPSStore,
        selection_store: SelectionStore,
        selection_config: Any,
        *,
        run_id: str = "",
        paired_seed: int = 0,
        comparison_contract_id: str = "",
        profiler: Any | None = None,
    ) -> None:
        if cps_store is None or selection_store is None:
            raise ValueError("selection runtime requires both CPS and selection stores")
        self.cps_store = cps_store
        self.selection_store = selection_store
        self.config = selection_config
        self.run_id = str(run_id)
        self.paired_seed = int(paired_seed)
        self.comparison_contract_id = str(comparison_contract_id)
        self.profiler = profiler if profiler is not None else getattr(selection_store, "profiler", None)
        try:
            self._profiling_enabled = bool(
                self.profiler is not None
                and getattr(self.profiler, "enabled", False)
            )
        except BaseException:
            self._profiling_enabled = False
        if self._profiling_enabled:
            self._profile_context_local = threading.local()
        self.selector_name = str(getattr(selection_config, "selector_name", "") or "")
        if not self.selector_name:
            raise ValueError("selection_config.selector_name is required")
        self.shared_trace_visibility = str(
            getattr(selection_config, "visibility", "project_shared") or ""
        )
        if self.shared_trace_visibility != "project_shared":
            raise ValueError("selection runtime requires shared_trace_visibility=project_shared")
        self._ordinal_lock = threading.Lock()
        self._ordinals: dict[tuple[str, str], int] = {}
        self._projection_lock = threading.Lock()
        self._projection_calls = 0
        # Projection reads are intentionally not cached today.  Keep a
        # run-local, bounded identity counter so a profile can tell the
        # difference between a genuinely new trace set and repeatedly
        # materializing the same snapshot.  The counter is diagnostic only;
        # it must never become selector state or alter the returned values.
        # Keep the diagnostic identity map disabled when profiling is off.  A
        # selection run must not pay for an otherwise-unused cache-like data
        # structure on the baseline path.  The map is bounded below so a long
        # run with many unique candidate sets cannot turn profiling itself
        # into an unbounded memory consumer.
        self._projection_seen: dict[str, int] | None = (
            {} if self._profiling_enabled else None
        )
        self.config_identity = self._config_identity()
        declared_config_id = str(
            getattr(selection_config, "selection_config_id", "") or ""
        )
        computed_config_id = _manifest_sha(self.config_identity)
        if declared_config_id and declared_config_id != computed_config_id:
            raise ValueError("selection_config_id does not match the canonical configuration")
        self.selection_config_id = declared_config_id or computed_config_id
        self.feedback_values = self._feedback_values()
        self.policy_config = self._policy_config()
        # Validate the selected policy during run initialization, before any
        # solver admission or horizon work.  The pure selector is immutable and
        # reused for all requests.
        self.selector = build_selector(self.selector_name, self.policy_config)
        self.selector_config_record = self.selection_store.register_selector_config(
            selector_name=self.selector_name,
            config=self.config_identity,
        )
        self.config_sha256 = str(self.selector_config_record.get("config_sha256") or "")
        self.selector_config_id = str(
            self.selector_config_record.get("selector_config_id") or ""
        )

    def _profile_event(self, event: str, **fields: Any) -> None:
        if not self._profiling_enabled:
            return
        profiler = self.profiler
        try:
            context = getattr(self, "_profile_context_local", None)
            values = getattr(context, "value", None) if context is not None else None
            if isinstance(values, tuple) and len(values) == 3:
                context_task_id, context_actor_id, context_episode = values
                fields.setdefault("task_id", context_task_id)
                fields.setdefault("actor_id", context_actor_id)
                fields.setdefault("episode", context_episode)
            profiler.emit(event, **fields)
        except BaseException:
            return

    @contextmanager
    def _profile_request_context(
        self,
        *,
        task_id: Any,
        actor_id: Any,
        episode: Any,
    ):
        """Propagate one search's identities into low-level store events.

        SelectionStore intentionally keeps its historical method signatures;
        this narrow context is the diagnostic bridge for connection/query/
        persistence/readback helpers that otherwise cannot see the request
        tuple.  It is a no-op when profiling is disabled.
        """

        context_factory = getattr(self.selection_store, "profile_context", None)
        if not self._profiling_enabled:
            yield
            return
        runtime_local = self._profile_context_local
        marker = object()
        previous_runtime = getattr(runtime_local, "value", marker)
        runtime_local.value = (task_id, actor_id, episode)
        store_context = None
        if callable(context_factory):
            try:
                store_context = context_factory(
                    task_id=task_id,
                    actor_id=actor_id,
                    episode=episode,
                )
                store_context.__enter__()
            except BaseException:
                # A custom store adapter's diagnostic context is optional.  A
                # failed enter must not mask the real selection operation.
                store_context = None
        try:
            yield
        except BaseException as body_error:
            try:
                if store_context is not None:
                    store_context.__exit__(
                        type(body_error), body_error, body_error.__traceback__
                    )
            except BaseException:
                pass
            raise
        else:
            try:
                if store_context is not None:
                    store_context.__exit__(None, None, None)
            except BaseException:
                pass
        finally:
            if previous_runtime is marker:
                try:
                    del runtime_local.value
                except AttributeError:
                    pass
            else:
                runtime_local.value = previous_runtime

    @staticmethod
    def _db_context(store: Any, operation: str):
        """Open an operation-labelled DB with pre-profiling adapter support."""

        try:
            return store._db(operation=operation)
        except TypeError:
            return store._db()

    @contextmanager
    def _profile_span(self, name: str, **fields: Any):
        """Yield an observational span without letting sink failures escape.

        Profiling is deliberately fail-open.  A custom sink/context manager
        may fail during ``__enter__`` or ``__exit__`` (for example when a
        profile file is full); those failures must never change selector or
        replay semantics.  Body exceptions are always re-raised unchanged.
        """

        if not self._profiling_enabled:
            yield
            return
        profiler = self.profiler
        span = getattr(profiler, "span", None) if profiler is not None else None
        if not callable(span):
            yield
            return
        context = getattr(self, "_profile_context_local", None)
        values = getattr(context, "value", None) if context is not None else None
        if isinstance(values, tuple) and len(values) == 3:
            context_task_id, context_actor_id, context_episode = values
            fields.setdefault("task_id", context_task_id)
            fields.setdefault("actor_id", context_actor_id)
            fields.setdefault("episode", context_episode)
        try:
            context = span(name, **fields)
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
        except BaseException as body_error:
            try:
                context.__exit__(
                    type(body_error), body_error, body_error.__traceback__
                )
            except BaseException:
                pass
            raise
        else:
            try:
                context.__exit__(None, None, None)
            except BaseException:
                pass

    def _config_identity(self) -> dict[str, Any]:
        if hasattr(self.config, "public_dict") and callable(self.config.public_dict):
            value = self.config.public_dict()
            if isinstance(value, Mapping):
                result = dict(value)
                result.pop("selection_config_id", None)
                return result
        params = getattr(self.config, "policy_params", None)
        if params is None:
            params = getattr(self.config, "parameters", {})
        return {
            "selector_name": self.selector_name,
            "selector_version": str(getattr(self.config, "selector_version", "")),
            "parameters": dict(params or {}),
            "visibility": str(getattr(self.config, "visibility", "project_shared")),
            "trace_slot_limit": int(getattr(self.config, "trace_slot_limit", 0) or 0),
            "context_token_budget": int(getattr(self.config, "context_token_budget", 0) or 0),
            "tokenizer": str(getattr(self.config, "tokenizer", "")),
            "seed": int(getattr(self.config, "seed", 0) or 0),
            "tie_break": str(getattr(self.config, "tie_break", "trace_id_asc")),
        }

    def _feedback_values(self) -> Mapping[str, float]:
        params = getattr(self.config, "policy_params", None)
        if params is None:
            params = getattr(self.config, "parameters", {})
        raw = (params or {}).get("feedback_values") if isinstance(params, Mapping) else None
        feedback_selectors = {
            "feedback_diversity", "no_interaction_feedback",
            "unnormalized_feedback", "nustigmergy",
        }
        if raw is None:
            if self.selector_name in feedback_selectors:
                raise ValueError("feedback-aware selectors require explicit feedback_values")
            return {}
        if not isinstance(raw, Mapping):
            raise ValueError("policy_params.feedback_values must be a mapping")
        keys = {str(key) for key in raw}
        missing = CANONICAL_FEEDBACK_KINDS - keys
        unknown = keys - CANONICAL_FEEDBACK_KINDS
        if missing or unknown:
            detail = []
            if missing:
                detail.append("missing " + ",".join(sorted(missing)))
            if unknown:
                detail.append("unknown " + ",".join(sorted(unknown)))
            raise ValueError("feedback_values must cover exactly the canonical kinds: " + "; ".join(detail))
        result: dict[str, float] = {}
        for kind in sorted(CANONICAL_FEEDBACK_KINDS):
            value = raw[kind]
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"feedback_values.{kind} must be a finite number")
            number = float(value)
            if not (-float("inf") < number < float("inf")):
                raise ValueError(f"feedback_values.{kind} must be a finite number")
            result[kind] = number
        return result

    def _policy_config(self) -> dict[str, Any]:
        params = getattr(self.config, "policy_params", None)
        if params is None:
            params = getattr(self.config, "parameters", {})
        # Selector implementations consume the explicit policy mapping.
        return {"parameters": dict(params or {})}

    def _next_ordinal(self, task_id: str, actor_id: str) -> int:
        key = (str(task_id), str(actor_id))
        with self._ordinal_lock:
            value = self._ordinals.get(key, 0)
            self._ordinals[key] = value + 1
            return value

    def _lookup_prior_search(self, request_key: str) -> Mapping[str, Any] | None:
        """Look up a request-key chain and expose replay-read cost.

        ``SelectionStore.get_search_by_request_key`` predates the profiler and
        is intentionally kept API-compatible.  The runtime boundary is the
        one place that knows this lookup is part of an idempotent replay, so a
        small envelope here makes that otherwise invisible DB/readback work
        joinable with the replay materialization events below.  The disabled
        path calls the store directly and performs no timing or hashing.
        """

        if not self._profiling_enabled:
            return self.selection_store.get_search_by_request_key(request_key)
        started = time.monotonic()
        result: Mapping[str, Any] | None = None
        status = "error"
        error_kind: str | None = None
        try:
            result = self.selection_store.get_search_by_request_key(request_key)
            status = "hit" if result is not None else "miss"
            return result
        except Exception as exc:
            error_kind = type(exc).__name__
            raise
        finally:
            output_rows = 0
            if isinstance(result, Mapping):
                output_rows = (
                    int(result.get("search_event") is not None)
                    + int(result.get("exposure") is not None)
                    + len(result.get("rankings", ()) or ())
                    + len(result.get("candidates", ()) or ())
                    + len(result.get("items", ()) or ())
                )
            self._profile_event(
                "selection.replay.lookup",
                operation="request_key_lookup",
                read_mode="selection_store_chain",
                request_key_sha256=hashlib.sha256(
                    str(request_key).encode("utf-8", "replace")
                ).hexdigest(),
                query_count=1,
                output_rows=output_rows,
                rows_scanned=output_rows,
                read_scope_seconds=max(0.0, time.monotonic() - started),
                read_transaction_seconds=max(0.0, time.monotonic() - started),
                status=status,
                error_kind=error_kind,
            )

    def _trace_stats_impl(
        self,
        trace_ids: Iterable[str],
        *,
        trace_set_sha256: str | None = None,
        projection_call_index: int | None = None,
        reuse_count: int = 0,
    ) -> tuple[Mapping[str, FeedbackStats], Mapping[str, Mapping[str, Any]]]:
        """Aggregate feedback, evidence, maintenance, and relations in one read txn."""

        ordered = tuple(dict.fromkeys(str(trace_id) for trace_id in trace_ids if trace_id))
        if not ordered:
            return {}, {}
        placeholders = ",".join("?" for _ in ordered)
        exposures: Counter[str] = Counter()
        effective_rows: dict[str, list[Mapping[str, Any]]] = {trace_id: [] for trace_id in ordered}
        projections: dict[str, dict[str, Any]] = {
            trace_id: {"evidence": 0.0, "relations": Counter(), "lifecycle": "active"}
            for trace_id in ordered
        }
        if not self._profiling_enabled:
            # Preserve the original read path when profiling is disabled.
            # In particular, do not create an observer closure, call clocks,
            # materialize query rows for counters, or hash the trace set.
            with self.selection_store._db() as db:
                db.execute("BEGIN")
                for row in db.execute(
                    f"""SELECT trace_id, COUNT(*) AS n FROM exposure_items
                          WHERE trace_id IN ({placeholders}) GROUP BY trace_id""",
                    ordered,
                ):
                    exposures[str(row["trace_id"])] = int(row["n"])
                for row in db.execute(
                    f"""SELECT trace_id, feedback_kind FROM feedback_events
                          WHERE event_class = 'worker_interaction'
                            AND terminal = 1 AND effective = 1
                            AND trace_id IN ({placeholders})
                          ORDER BY feedback_event_id""",
                    ordered,
                ):
                    effective_rows[str(row["trace_id"])].append(dict(row))
                for row in db.execute(
                    f"""SELECT trace_id, status FROM verifier_evidence
                          WHERE trace_id IN ({placeholders})
                          ORDER BY created_at,evidence_event_id""",
                    ordered,
                ):
                    projections[str(row["trace_id"])] ["evidence"] += 1.0
                relation_params = tuple(ordered) + tuple(ordered)
                for row in db.execute(
                    f"""SELECT source_trace_id,target_trace_id,relation_kind
                          FROM trace_relations
                          WHERE source_trace_id IN ({placeholders})
                             OR target_trace_id IN ({placeholders})
                          ORDER BY created_at,relation_id""",
                    relation_params,
                ):
                    kind = str(row["relation_kind"] or "")
                    for trace_id in (
                        str(row["source_trace_id"]),
                        str(row["target_trace_id"]),
                    ):
                        if trace_id in projections:
                            projections[trace_id]["relations"][kind] += 1
                for row in db.execute(
                    f"""SELECT trace_id,maintenance_kind FROM maintenance_events
                          WHERE trace_id IN ({placeholders})
                          ORDER BY created_at,maintenance_event_id""",
                    ordered,
                ):
                    projections[str(row["trace_id"])] ["lifecycle"] = str(
                        row["maintenance_kind"] or "active"
                    )
                db.execute("COMMIT")
            result: dict[str, FeedbackStats] = {}
            for trace_id in ordered:
                kinds: Counter[str] = Counter()
                signed = 0.0
                positive = negative = 0
                for row in effective_rows[trace_id]:
                    kind = str(row.get("feedback_kind") or "")
                    if kind not in CANONICAL_FEEDBACK_KINDS:
                        raise ValueError(f"noncanonical effective feedback kind: {kind}")
                    kinds[kind] += 1
                    if self.feedback_values:
                        if kind not in self.feedback_values:
                            raise ValueError(f"registered feedback_values lacks kind: {kind}")
                        weight = float(self.feedback_values[kind])
                        signed += weight
                        if weight > 0:
                            positive += 1
                        elif weight < 0:
                            negative += 1
                result[trace_id] = FeedbackStats(
                    exposure_count=int(exposures[trace_id]),
                    effective_terminal_count=len(effective_rows[trace_id]),
                    kind_counts=dict(kinds),
                    signed_weight_sum=signed,
                    positive_count=positive,
                    negative_count=negative,
                )
            return result, projections
        # These identities are supplied by ``_trace_stats`` on the profiled
        # path.  Direct callers of this private helper may still request a
        # profiled read, so derive the identity only in that case; the normal
        # disabled path never hashes a trace set for diagnostics.
        if self._profiling_enabled and trace_set_sha256 is None:
            trace_set_sha256 = _sha(sorted(ordered))
        query_count = 0
        query_rows = 0
        query_seconds_total = 0.0
        fetch_seconds_total = 0.0
        read_started = time.monotonic() if self._profiling_enabled else 0.0
        read_scope_started = 0.0

        def _query(operation: str, statement: str, params: Any) -> list[Any]:
            nonlocal query_count, query_rows, query_seconds_total, fetch_seconds_total
            if not self._profiling_enabled:
                # Preserve the original cursor/streaming behavior on the
                # disabled path.  Materializing every auxiliary query just to
                # count rows would change the baseline's memory profile.
                return db.execute(statement, params)
            started = time.monotonic()
            cursor = db.execute(statement, params)
            query_seconds = max(0.0, time.monotonic() - started)
            fetch_started = time.monotonic()
            rows = list(cursor)
            fetch_seconds = max(0.0, time.monotonic() - fetch_started)
            query_count += 1
            query_rows += len(rows)
            query_seconds_total += query_seconds
            fetch_seconds_total += fetch_seconds
            self._profile_event(
                "trace.project.query",
                db_operation=operation,
                rows=len(rows),
                query_seconds=query_seconds,
                fetch_seconds=fetch_seconds,
                input_rows=len(ordered),
                trace_set_sha256=trace_set_sha256,
                projection_call_index=projection_call_index,
                reuse_count=reuse_count,
            )
            return rows

        # BEGIN is deferred here; it does not acquire the SQLite writer lock.
        # Do not mislabel BEGIN elapsed time as a read-lock wait.  Any actual
        # contention appears in the per-query execute timings below.
        read_lock_wait_seconds = 0.0
        begin_seconds = 0.0
        db_context = (
            self._db_context(self.selection_store, "trace_project")
            if self._profiling_enabled
            else self.selection_store._db()
        )
        projection_status = "error"
        projection_error_kind: str | None = None

        @contextmanager
        def _observed_projection_db():
            nonlocal projection_status, projection_error_kind
            try:
                with db_context as observed_db:
                    yield observed_db
            except Exception as exc:
                projection_error_kind = type(exc).__name__
                raise
            else:
                projection_status = "ok"
            finally:
                if self._profiling_enabled:
                    elapsed = max(0.0, time.monotonic() - read_started)
                    self._profile_event(
                        "trace.project.read",
                        db_operation="selection_store",
                        query_count=query_count,
                        input_rows=len(ordered),
                        output_rows=query_rows,
                        query_seconds=query_seconds_total,
                        fetch_seconds=fetch_seconds_total,
                        read_lock_wait_seconds=read_lock_wait_seconds,
                        lock_wait_seconds=0.0,
                        begin_seconds=begin_seconds,
                        read_mode="deferred_snapshot",
                        trace_set_sha256=trace_set_sha256,
                        projection_call_index=projection_call_index,
                        reuse_count=reuse_count,
                        read_transaction_seconds=elapsed,
                        read_scope_seconds=(
                            max(0.0, time.monotonic() - read_scope_started)
                            if read_scope_started
                            else 0.0
                        ),
                        status=projection_status,
                        error_kind=projection_error_kind,
                    )

        with _observed_projection_db() as db:  # package-private read-only projection
            if self._profiling_enabled:
                read_scope_started = time.monotonic()
                begin_started = read_scope_started
            db.execute("BEGIN")
            if self._profiling_enabled:
                begin_seconds = max(0.0, time.monotonic() - begin_started)
            for row in _query(
                "exposure_count",
                f"""SELECT trace_id, COUNT(*) AS n FROM exposure_items
                      WHERE trace_id IN ({placeholders}) GROUP BY trace_id""",
                ordered,
            ):
                exposures[str(row["trace_id"])] = int(row["n"])
            for row in _query(
                "effective_feedback",
                f"""SELECT trace_id, feedback_kind FROM feedback_events
                      WHERE event_class = 'worker_interaction'
                        AND terminal = 1 AND effective = 1
                        AND trace_id IN ({placeholders})
                      ORDER BY feedback_event_id""",
                ordered,
            ):
                effective_rows[str(row["trace_id"])].append(dict(row))
            for row in _query(
                "verifier_evidence",
                f"""SELECT trace_id, status FROM verifier_evidence
                      WHERE trace_id IN ({placeholders})
                      ORDER BY created_at,evidence_event_id""",
                ordered,
            ):
                projections[str(row["trace_id"])]["evidence"] += 1.0
            relation_params = tuple(ordered) + tuple(ordered)
            for row in _query(
                "trace_relations",
                f"""SELECT source_trace_id,target_trace_id,relation_kind
                      FROM trace_relations
                      WHERE source_trace_id IN ({placeholders})
                         OR target_trace_id IN ({placeholders})
                      ORDER BY created_at,relation_id""",
                relation_params,
            ):
                kind = str(row["relation_kind"] or "")
                for trace_id in (str(row["source_trace_id"]), str(row["target_trace_id"])):
                    if trace_id in projections:
                        projections[trace_id]["relations"][kind] += 1
            for row in _query(
                "maintenance_events",
                f"""SELECT trace_id,maintenance_kind FROM maintenance_events
                      WHERE trace_id IN ({placeholders})
                      ORDER BY created_at,maintenance_event_id""",
                ordered,
            ):
                projections[str(row["trace_id"])]["lifecycle"] = str(row["maintenance_kind"] or "active")
            db.execute("COMMIT")
        materialize_started = time.monotonic() if self._profiling_enabled else 0.0
        result: dict[str, FeedbackStats] = {}
        for trace_id in ordered:
            kinds: Counter[str] = Counter()
            signed = 0.0
            positive = negative = 0
            for row in effective_rows[trace_id]:
                kind = str(row.get("feedback_kind") or "")
                if kind not in CANONICAL_FEEDBACK_KINDS:
                    raise ValueError(f"noncanonical effective feedback kind: {kind}")
                kinds[kind] += 1
                if self.feedback_values:
                    if kind not in self.feedback_values:
                        raise ValueError(f"registered feedback_values lacks kind: {kind}")
                    weight = float(self.feedback_values[kind])
                    signed += weight
                    if weight > 0:
                        positive += 1
                    elif weight < 0:
                        negative += 1
            result[trace_id] = FeedbackStats(
                exposure_count=int(exposures[trace_id]),
                effective_terminal_count=len(effective_rows[trace_id]),
                kind_counts=dict(kinds),
                signed_weight_sum=signed,
                positive_count=positive,
                negative_count=negative,
            )
        if self._profiling_enabled:
            self._profile_event(
                "trace.project.materialize",
                input_rows=query_rows,
                output_rows=len(result),
                materialized_rows=len(result),
                materialized_bytes=sum(
                    len(str(item).encode("utf-8")) for item in projections.values()
                ),
                trace_set_sha256=trace_set_sha256,
                projection_call_index=projection_call_index,
                reuse_count=reuse_count,
                materialize_seconds=max(0.0, time.monotonic() - materialize_started),
            )
        return result, projections

    def _trace_stats(
        self, trace_ids: Iterable[str]
    ) -> tuple[Mapping[str, FeedbackStats], Mapping[str, Mapping[str, Any]]]:
        if not self._profiling_enabled:
            return self._trace_stats_impl(trace_ids)
        # A projection's identity is the normalized trace *set*, not the
        # caller's incidental iterator order or duplicate entries.  This is
        # the unit we want to compare when looking for repeated reads.
        ordered = tuple(dict.fromkeys(str(item) for item in trace_ids if item))
        trace_set_sha256 = _sha(sorted(ordered))
        with self._projection_lock:
            self._projection_calls += 1
            projection_call_index = self._projection_calls
            seen = self._projection_seen
            prior_reads = seen.get(trace_set_sha256, 0) if seen is not None else 0
            if seen is not None:
                if (
                    trace_set_sha256 not in seen
                    and len(seen) >= self._MAX_PROJECTION_IDENTITIES
                ):
                    # Diagnostic-only reset.  It bounds memory without
                    # changing selector state or the returned projection.
                    seen.clear()
                    prior_reads = 0
                seen[trace_set_sha256] = prior_reads + 1
            projection_calls = self._projection_calls
        with self._profile_span(
            "trace.project",
            records=len(ordered),
            projection_call_index=projection_call_index,
            trace_set_sha256=trace_set_sha256,
            snapshot_hit=False,
            reuse_count=prior_reads,
        ):
            result = self._trace_stats_impl(
                ordered,
                trace_set_sha256=trace_set_sha256,
                projection_call_index=projection_call_index,
                reuse_count=prior_reads,
            )
        self._profile_event(
            "trace.project.summary",
            records=len(ordered),
            task_count=len({str(item) for item in ordered}),
            projection_call_index=projection_call_index,
            projection_calls=projection_calls,
            trace_set_sha256=trace_set_sha256,
            snapshot_hit=False,
            reuse_count=prior_reads,
        )
        return result

    def _eligible_impl(self, *, query: str = "", task_family: str = "") -> tuple[TraceCandidate, ...]:
        """Read a project-wide committed-piece snapshot without control rows."""

        # Keep all timing/byte instrumentation behind this branch.  In
        # particular, do not invoke monotonic clocks or construct diagnostic
        # byte summaries for the normal (profiling-off) execution.
        read_started = time.monotonic() if self._profiling_enabled else 0.0
        query_seconds = 0.0
        fetch_seconds = 0.0
        db_context = (
            self._db_context(self.cps_store, "selection_eligible")
            if self._profiling_enabled
            else self.cps_store._db()
        )
        with db_context as db:  # short, read-only CPS snapshot
            query_started = time.monotonic() if self._profiling_enabled else 0.0
            cursor = db.execute(
                """SELECT rowid, id, task_id, author, kind, title, body, tags, created_at
                     FROM pieces WHERE active = 1
                     ORDER BY rowid ASC, id ASC"""
            )
            if self._profiling_enabled:
                query_seconds = max(0.0, time.monotonic() - query_started)
                fetch_started = time.monotonic()
                rows = cursor.fetchall()
                fetch_seconds = max(0.0, time.monotonic() - fetch_started)
            else:
                rows = cursor.fetchall()
        if self._profiling_enabled:
            self._profile_event(
                "selection.eligible.read",
                db_operation="cps_pieces",
                operation="eligible_candidate_scan",
                scan_mode="full_active_piece_scan",
                read_mode="autocommit_select",
                input_rows=len(rows),
                output_rows=len(rows),
                rows_scanned=len(rows),
                query_seconds=query_seconds,
                fetch_seconds=fetch_seconds,
                read_scope_seconds=max(0.0, time.monotonic() - query_started),
                read_transaction_seconds=max(0.0, time.monotonic() - read_started),
                input_bytes=sum(
                    len(str(row["title"] or "").encode("utf-8"))
                    + len(str(row["body"] or "").encode("utf-8"))
                    for row in rows
                ),
            )
        filter_started = time.monotonic() if self._profiling_enabled else 0.0
        raw_row_count = len(rows)
        rows = [row for row in rows if str(row["kind"] or "").casefold() not in _INELIGIBLE_KINDS]
        if self._profiling_enabled:
            self._profile_event(
                "selection.eligible.filter",
                input_rows=raw_row_count,
                output_rows=len(rows),
                materialize_seconds=max(0.0, time.monotonic() - filter_started),
                filter_seconds=max(0.0, time.monotonic() - filter_started),
                scan_mode="in_memory_kind_filter",
            )
        feedback_by_trace, projection_by_trace = self._trace_stats(str(row["id"] or "") for row in rows)
        query_token_started = time.monotonic() if self._profiling_enabled else 0.0
        query_terms = {term.casefold() for term in re.findall(r"[A-Za-z0-9_\u0080-\uffff]+", str(query or ""))}
        if self._profiling_enabled:
            self._profile_event(
                "selection.eligible.query_terms",
                input_rows=1,
                output_rows=len(query_terms),
                tokenize_count=len(query_terms),
                tokenize_seconds=max(0.0, time.monotonic() - query_token_started),
            )
        materialize_started = time.monotonic() if self._profiling_enabled else 0.0
        tokenize_seconds = 0.0
        tokenized = 0
        candidates: list[TraceCandidate] = []
        for row in rows:
            try:
                decoded_tags = json.loads(row["tags"] or "[]")
                tags = (
                    tuple(str(item) for item in decoded_tags)
                    if isinstance(decoded_tags, (list, tuple))
                    else ()
                )
            except (TypeError, json.JSONDecodeError):
                tags = ()
            body = str(row["body"] or "")
            title = str(row["title"] or "")
            trace_id = str(row["id"] or "")
            if not trace_id:
                continue
            candidate_task = str(row["task_id"] or "")
            haystack_terms = {
                term.casefold()
                for term in re.findall(
                    r"[A-Za-z0-9_\u0080-\uffff]+",
                    " ".join((title, body, *[str(item) for item in tags])),
                )
            }
            relevance = (
                float(len(query_terms & haystack_terms)) / float(len(query_terms))
                if query_terms else 0.0
            )
            projection = projection_by_trace.get(trace_id, {})
            relation_counts = dict(projection.get("relations", {}))
            state = str(projection.get("lifecycle") or "active")
            cluster_id = _topology_tag(
                tags, ("cluster:", "cluster=", "cluster_id:", "cluster_id=")
            ) or trace_id
            lineage_id = _topology_tag(
                tags, ("lineage:", "lineage=", "lineage_id:", "lineage_id=")
            ) or trace_id
            token_started = time.monotonic() if self._profiling_enabled else 0.0
            token_value = token_count({"title": title, "body": body, "tags": tags})
            if self._profiling_enabled:
                tokenize_seconds += max(0.0, time.monotonic() - token_started)
            tokenized += 1
            candidates.append(
                _RuntimeTraceCandidate(
                    trace_id=trace_id,
                    source_task_id=candidate_task,
                    # Legacy CPS pieces carry a source task but no task-family
                    # column.  Do not relabel every shared candidate with the
                    # requesting task's family.
                    task_family="",
                    author_id=str(row["author"] or ""),
                    scope_key="project_shared",
                    visibility=self.shared_trace_visibility,
                    kind=str(row["kind"] or "note"),
                    title=title,
                    body=body,
                    tags=tags,
                    created_at=str(row["created_at"] or ""),
                    commit_seq=int(row["rowid"]),
                    # Preserve the maintenance projection in the frozen
                    # candidate snapshot.  The previous hard-coded
                    # ``active`` label made stale/refuted traces look active
                    # in artifacts even though ``state_score`` had already
                    # reflected their lifecycle event.
                    lifecycle=state,
                    cluster_id=cluster_id,
                    content_sha256=_sha({"title": title, "body": body, "tags": tags}),
                    token_count=token_value,
                    feedback=feedback_by_trace.get(trace_id, FeedbackStats()),
                    relevance=relevance,
                    evidence_score=float(projection.get("evidence", 0.0)),
                    structure_score=float(sum(relation_counts.values())),
                    state_score=1.0 if state.casefold() in {"active", "current", "published"} else 0.0,
                    lineage_id=lineage_id,
                    # feedback.extract_features treats a numeric ``evidence``
                    # scalar as the common feature; retain the richer count in
                    # the audit mapping only when needed by callers.
                    evidence=float(projection.get("evidence", 0.0)),
                    relations=relation_counts,
                )
            )
        if self._profiling_enabled:
            materialize_seconds = max(0.0, time.monotonic() - materialize_started)
            self._profile_event(
                "selection.eligible.materialize",
                input_rows=len(rows),
                output_rows=len(candidates),
                materialized_rows=len(candidates),
                materialized_bytes=sum(
                    len(candidate.body.encode("utf-8"))
                    + len(candidate.title.encode("utf-8"))
                    for candidate in candidates
                ),
                materialize_seconds=materialize_seconds,
                tokenize_count=tokenized,
                tokenize_seconds=tokenize_seconds,
                operation="candidate_materialize_and_tokenize",
            )
        return tuple(candidates)

    def _eligible(self, *, query: str = "", task_family: str = "") -> tuple[TraceCandidate, ...]:
        if not self._profiling_enabled:
            return self._eligible_impl(query=query, task_family=task_family)
        with self._profile_span(
            "selection.eligible",
            operation="read_candidates",
            task_count=1,
        ):
            candidates = self._eligible_impl(query=query, task_family=task_family)
        self._profile_event(
            "selection.eligible.summary",
            operation="read_candidates",
            candidate_count=len(candidates),
            selection_candidate_count=len(candidates),
        )
        return candidates

    def _piece_metadata(self, trace_ids: Iterable[str]) -> Mapping[str, Mapping[str, Any]]:
        """Read bounded rendered fields for a prior committed search chain."""

        ordered = tuple(dict.fromkeys(str(item) for item in trace_ids if item))
        if not ordered:
            return {}
        placeholders = ",".join("?" for _ in ordered)
        if not self._profiling_enabled:
            # Keep replay compatibility reads byte-for-byte on the ordinary
            # path: no diagnostic clock, file-size probe, or row accounting.
            with self.cps_store._db() as db:
                rows = db.execute(
                    f"SELECT id,kind,title,body,tags FROM pieces WHERE id IN ({placeholders})",
                    ordered,
                ).fetchall()
        else:
            started = time.monotonic()
            query_started = time.monotonic()
            status = "error"
            error_kind: str | None = None
            rows: list[Any] = []
            try:
                with self._db_context(self.cps_store, "selection_replay") as db:
                    cursor = db.execute(
                        f"SELECT id,kind,title,body,tags FROM pieces WHERE id IN ({placeholders})",
                        ordered,
                    )
                    query_seconds = max(0.0, time.monotonic() - query_started)
                    fetch_started = time.monotonic()
                    rows = cursor.fetchall()
                    fetch_seconds = max(0.0, time.monotonic() - fetch_started)
                status = "ok"
            except Exception as exc:
                error_kind = type(exc).__name__
                query_seconds = max(0.0, time.monotonic() - query_started)
                fetch_seconds = 0.0
                raise
            finally:
                self._profile_event(
                    "selection.replay.read",
                    db_operation="cps_pieces",
                    read_mode="autocommit_select",
                    input_rows=len(ordered),
                    output_rows=len(rows),
                    rows_scanned=len(rows),
                    query_seconds=query_seconds,
                    fetch_seconds=fetch_seconds,
                    read_scope_seconds=max(0.0, time.monotonic() - started),
                    read_transaction_seconds=max(0.0, time.monotonic() - started),
                    status=status,
                    error_kind=error_kind,
                )
        result: dict[str, Mapping[str, Any]] = {}
        materialize_started = time.monotonic() if self._profiling_enabled else 0.0
        for row in rows:
            try:
                decoded_tags = json.loads(row["tags"] or "[]")
                tags = decoded_tags if isinstance(decoded_tags, (list, tuple)) else []
            except (TypeError, json.JSONDecodeError):
                tags = []
            result[str(row["id"])] = {
                "title": str(row["title"] or ""),
                "body": str(row["body"] or ""),
                "kind": str(row["kind"] or "note"),
                "tags": [str(item) for item in tags],
            }
        if self._profiling_enabled:
            self._profile_event(
                "selection.replay.materialize",
                input_rows=len(rows),
                output_rows=len(result),
                materialized_rows=len(result),
                materialized_bytes=sum(
                    len(str(item.get("title", "")).encode("utf-8"))
                    + len(str(item.get("body", "")).encode("utf-8"))
                    for item in result.values()
                ),
                materialize_seconds=max(0.0, time.monotonic() - materialize_started),
            )
        return result

    def _replay_search(
        self,
        persisted: Mapping[str, Any],
        *,
        task_id: str,
        actor_id: str,
        request_key: str,
        query_identity: Mapping[str, Any],
        started: float,
    ) -> dict[str, Any]:
        """Return a committed chain without reranking feedback-mutated state."""

        replay_started = time.monotonic() if self._profiling_enabled else 0.0
        event = dict(persisted.get("search_event") or {})
        stored_query = event.get("query")
        if (
            event.get("task_id") != task_id
            or event.get("actor_id") != actor_id
            or event.get("selector_config_id") != self.selector_config_id
            or event.get("comparison_sha256") != _store_identity_sha(self.comparison_contract_id or "")
            or not isinstance(stored_query, Mapping)
            or any(stored_query.get(key) != query_identity.get(key) for key in ("text", "episode", "search_ordinal", "task_family", "max_items", "paired_seed"))
        ):
            raise ValueError("request_key is already bound to a different selection search")
        # A replay must describe the committed snapshot, never the current
        # live CPS state.  The store persists the exact watermark alongside
        # the candidate payloads; restore it here so restart/retry responses
        # are semantically identical to the first response.
        snapshot_watermarks = event.get("snapshot_watermarks")
        if snapshot_watermarks is None:
            # Older rows predate eligible-pool persistence.  Retain a safe,
            # explicit empty watermark instead of inventing a live one.
            snapshot_watermarks = {}
        if not isinstance(snapshot_watermarks, Mapping):
            raise ValueError("persisted selection search has invalid snapshot watermarks")
        snapshot_watermarks = dict(snapshot_watermarks)
        snapshot_event_seq = snapshot_watermarks.get("cps_pieces_rowid", 0)
        if isinstance(snapshot_event_seq, bool) or not isinstance(snapshot_event_seq, int):
            raise ValueError("persisted selection search has invalid CPS watermark")
        if snapshot_event_seq < 0:
            raise ValueError("persisted selection search has negative CPS watermark")
        persisted_candidates = list(persisted.get("candidates", []))
        if self._profiling_enabled:
            self._profile_event(
                "selection.replay.start",
                task_id=task_id,
                actor_id=actor_id,
                operation="request_key_replay",
                input_rows=len(persisted_candidates),
                candidate_count=len(persisted_candidates),
            )
        eligible_trace_ids: list[str] = []
        piece_by_trace: dict[str, Mapping[str, Any]] = {}
        for candidate in persisted_candidates:
            payload = candidate.get("candidate_payload")
            if not isinstance(payload, Mapping):
                payload = {}
            trace_id = str(candidate.get("trace_id") or payload.get("trace_id") or "")
            if not trace_id:
                continue
            eligible_trace_ids.append(trace_id)
            piece_by_trace[trace_id] = {
                "title": str(payload.get("title") or ""),
                "body": str(payload.get("body") or ""),
                "kind": str(payload.get("kind") or "note"),
                "tags": [str(item) for item in (payload.get("tags") or [])],
            }
        eligible_trace_ids = [trace_id for trace_id in eligible_trace_ids if trace_id]
        # Legacy search rows may have no candidate payload.  Only those traces
        # need a bounded CPS fallback; modern rows remain independent of any
        # later CPS mutation/deletion.
        missing_piece_traces = [
            str(row.get("trace_id") or "")
            for row in persisted.get("rankings", [])
            if str(row.get("trace_id") or "") not in piece_by_trace
        ]
        if missing_piece_traces:
            piece_by_trace.update(self._piece_metadata(missing_piece_traces))
        items_by_trace = {
            str(item.get("trace_id") or ""): item for item in persisted.get("items", [])
        }
        replay_materialize_started = (
            time.monotonic() if self._profiling_enabled else 0.0
        )
        rankings: list[dict[str, Any]] = []
        selected_items: list[dict[str, Any]] = []
        for raw in persisted.get("rankings", []):
            trace_id = str(raw.get("trace_id") or "")
            payload = dict(raw.get("ranking_payload") or {})
            row = {
                "trace_id": trace_id,
                "rank": int(raw.get("rank") or 0),
                "selected": raw.get("selected") is True,
                "component_scores": dict(raw.get("component_scores") or {}),
                "payload": payload,
            }
            rankings.append(row)
            if row["selected"]:
                item = dict(row)
                item.update(piece_by_trace.get(trace_id, {}))
                prior_item = items_by_trace.get(trace_id, {})
                item.update({
                    "exposure_item_id": prior_item.get("exposure_item_id"),
                    "exposure_id": (persisted.get("exposure") or {}).get("exposure_id"),
                    "search_event_id": event.get("search_event_id"),
                })
                selected_items.append(item)
        result = {
            "ok": True,
            "items": selected_items,
            "ranked": rankings,
            "request_key": request_key,
            "search_event_id": event.get("search_event_id"),
            "exposure_id": (persisted.get("exposure") or {}).get("exposure_id"),
            "exposure_item_ids": [item.get("exposure_item_id") for item in selected_items],
            "eligible_pool_sha256": event.get("pool_sha256"),
            "snapshot_sha256": event.get("snapshot_sha256"),
            "snapshot_event_seq": snapshot_event_seq,
            "snapshot_watermarks": snapshot_watermarks,
            "eligible_candidate_count": len(eligible_trace_ids),
            "eligible_trace_ids": eligible_trace_ids,
            "shared_trace_visibility": self.shared_trace_visibility,
            "selector_config_id": self.selector_config_id,
            "comparison_contract_id": self.comparison_contract_id,
            "delivered_tokens": sum(int(row["payload"].get("token_count") or 0) for row in rankings if row["selected"]),
            "latency_seconds": round(time.monotonic() - started, 6),
            "idempotent": True,
        }
        if self._profiling_enabled:
            self._profile_event(
                "selection.replay.materialize",
                task_id=task_id,
                actor_id=actor_id,
                operation="request_key_replay",
                input_rows=len(persisted.get("rankings", [])),
                output_rows=len(selected_items),
                materialized_rows=len(selected_items),
                materialized_bytes=len(_json(result).encode("utf-8")),
                materialize_seconds=max(
                    0.0, time.monotonic() - replay_materialize_started
                ),
                status="ok",
            )
            self._profile_event(
                "selection.replay.end",
                task_id=task_id,
                actor_id=actor_id,
                operation="request_key_replay",
                candidate_count=len(eligible_trace_ids),
                selected_count=len(selected_items),
                ranked_count=len(rankings),
                wall_seconds=max(0.0, time.monotonic() - replay_started),
                status="ok",
            )
        return result

    def _search_impl(
        self,
        task_id: str,
        actor_id: str,
        query: str = "",
        *,
        limit: int | None = None,
        request_key: str | None = None,
        episode: int = 0,
        search_ordinal: int | None = None,
        paired_seed: int | None = None,
        task_family: str = "",
    ) -> dict[str, Any]:
        started = time.monotonic()
        task_id = str(task_id).strip()
        actor_id = str(actor_id).strip()
        if not task_id or not actor_id:
            raise ValueError("selection search requires task_id and actor_id")
        prior = (
            self._lookup_prior_search(str(request_key))
            if request_key is not None
            else None
        )
        episode = _nonnegative_int(episode, "episode")
        if search_ordinal is not None:
            search_ordinal = _nonnegative_int(search_ordinal, "search_ordinal")
        if paired_seed is not None:
            paired_seed = _nonnegative_int(paired_seed, "paired_seed")
        if request_key is not None:
            if not isinstance(request_key, str) or not request_key.strip():
                raise ValueError("selection request_key must be a non-empty string")
            request_key = request_key.strip()
        if search_ordinal is None:
            prior_query = (prior or {}).get("search_event", {}).get("query", {})
            if isinstance(prior_query, Mapping) and "search_ordinal" in prior_query:
                search_ordinal = _nonnegative_int(
                    prior_query["search_ordinal"], "search_ordinal"
                )
            else:
                search_ordinal = self._next_ordinal(task_id, actor_id)
        if request_key is None:
            request_key = f"{self.run_id}:{task_id}:{actor_id}:{episode}:{search_ordinal}"
        configured_max = int(getattr(self.config, "trace_slot_limit", 0) or 0)
        if configured_max <= 0:
            raise ValueError("selection trace_slot_limit must be positive")
        requested_max = (
            configured_max
            if limit is None
            else _nonnegative_int(limit, "limit")
        )
        if requested_max <= 0:
            raise ValueError("selection search limit must be positive")
        max_items = min(requested_max, configured_max, 50)
        token_budget = int(getattr(self.config, "context_token_budget", 0) or 0)
        if token_budget <= 0:
            raise ValueError("selection context_token_budget must be positive")
        query_identity = {
            "text": str(query or ""),
            "episode": episode,
            "search_ordinal": _nonnegative_int(search_ordinal, "search_ordinal"),
            "task_family": str(task_family or ""),
            "max_items": int(max_items),
            "paired_seed": self.paired_seed if paired_seed is None else paired_seed,
        }
        if prior is None:
            prior = self._lookup_prior_search(str(request_key))
        if prior is not None:
            return self._replay_search(
                prior, task_id=task_id, actor_id=actor_id, request_key=str(request_key),
                query_identity=query_identity, started=started,
            )
        request = SelectionRequest(
            run_id=self.run_id,
            request_key=str(request_key),
            actor_id=actor_id,
            task_id=task_id,
            task_family=str(task_family or ""),
            query=str(query or ""),
            episode=episode,
            search_ordinal=_nonnegative_int(search_ordinal, "search_ordinal"),
            paired_seed=self.paired_seed if paired_seed is None else paired_seed,
            max_items=max_items,
            context_token_budget=token_budget,
            selector_config_id=self.selector_config_id,
            comparison_contract_id=self.comparison_contract_id,
        )
        candidates = self._eligible(query=str(query or ""), task_family=str(task_family or ""))
        snapshot_started = time.monotonic() if self._profiling_enabled else 0.0
        snapshot = make_snapshot(
            request,
            candidates,
            snapshot_event_seq=max((c.commit_seq for c in candidates), default=0),
        )
        snapshot_seconds = (
            max(0.0, time.monotonic() - snapshot_started)
            if self._profiling_enabled
            else 0.0
        )
        if self._profiling_enabled:
            self._profile_event(
                "selection.snapshot",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                candidate_count=len(candidates),
                snapshot_seconds=snapshot_seconds,
                hash_seconds=snapshot_seconds,
                snapshot_sha256=snapshot.snapshot_sha256,
                eligible_pool_sha256=snapshot.eligible_pool_sha256,
                trace_set_sha256=_sha([candidate.trace_id for candidate in candidates]),
                snapshot_hit=False,
                reuse_count=0,
            )
        if self._profiling_enabled:
            with self._profile_span(
                "selection.rank",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                selector=self.selector_name,
                operation="rank_sort",
                candidate_count=len(candidates),
            ):
                ranked = self.selector.rank(snapshot)
            self._profile_event(
                "selection.rank.summary",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                input_rows=len(candidates),
                output_rows=len(ranked),
                ranked_count=len(ranked),
            )
            with self._profile_span(
                "selection.pack",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                operation="token_budget_pack",
                candidate_count=len(ranked),
            ):
                packed = pack_ranked_by_token_budget(
                    ranked,
                    max_items=max_items,
                    context_token_budget=token_budget,
                )
            self._profile_event(
                "selection.pack.summary",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                input_rows=len(ranked),
                output_rows=len(packed),
                selected_count=sum(1 for row in packed if row.selected),
                delivered_tokens=sum(
                    int(row.token_count) for row in packed if row.selected
                ),
            )
        else:
            ranked = self.selector.rank(snapshot)
            packed = pack_ranked_by_token_budget(
                ranked,
                max_items=max_items,
                context_token_budget=token_budget,
            )
        ranking_rows = []
        for row in packed:
            ranking_rows.append({
                "trace_id": row.trace_id,
                "rank": row.rank,
                "selected": row.selected,
                "component_scores": dict(row.component_scores),
                "payload": {
                    "total_score": row.total_score,
                    "tie_key": row.tie_key,
                    "token_count": row.token_count,
                    "drop_reason": row.drop_reason,
                },
            })
        eligible_payload = []
        payload_started = time.monotonic() if self._profiling_enabled else 0.0
        for candidate in candidates:
            eligible_payload.append({
                "trace_id": candidate.trace_id,
                "source_task_id": candidate.source_task_id,
                "task_family": candidate.task_family,
                "author_id": candidate.author_id,
                "scope_key": candidate.scope_key,
                "visibility": candidate.visibility,
                "kind": candidate.kind,
                "title": candidate.title,
                "body": candidate.body,
                "tags": list(candidate.tags),
                "created_at": candidate.created_at,
                "commit_seq": candidate.commit_seq,
                "lifecycle": candidate.lifecycle,
                "cluster_id": candidate.cluster_id,
                "content_sha256": candidate.content_sha256,
                "token_count": candidate.token_count,
                "evidence": candidate.evidence,
                "relations": candidate.relations,
                "feedback": {
                    "exposure_count": candidate.feedback.exposure_count,
                    "effective_terminal_count": candidate.feedback.effective_terminal_count,
                    "kind_counts": dict(candidate.feedback.kind_counts),
                    "signed_weight_sum": candidate.feedback.signed_weight_sum,
                    "positive_count": candidate.feedback.positive_count,
                    "negative_count": candidate.feedback.negative_count,
                },
                "relevance": float(getattr(candidate, "relevance", 0.0)),
                "evidence_score": float(getattr(candidate, "evidence_score", 0.0)),
                "structure_score": float(getattr(candidate, "structure_score", 0.0)),
                "state_score": float(getattr(candidate, "state_score", 0.0)),
                "lineage_id": str(getattr(candidate, "lineage_id", "")),
            })
        if self._profiling_enabled:
            payload_bytes = sum(
                len(_json(item).encode("utf-8")) for item in eligible_payload
            )
            self._profile_event(
                "selection.payload.materialize",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                input_rows=len(candidates),
                output_rows=len(eligible_payload),
                materialized_rows=len(eligible_payload),
                materialized_bytes=payload_bytes,
                serialization_seconds=max(0.0, time.monotonic() - payload_started),
            )
        snapshot_watermarks = {
            "cps_pieces_rowid": snapshot.snapshot_event_seq,
            "selection_feedback_trace_ids": [candidate.trace_id for candidate in candidates],
        }
        selected_rows = [row for row in ranking_rows if row["selected"]]
        if not selected_rows:
            return {
                "ok": True,
                "items": [],
                "search_event_id": None,
                "exposure_id": None,
                "exposure_item_ids": [],
                "request_key": str(request_key),
                "eligible_pool_sha256": snapshot.eligible_pool_sha256,
                "snapshot_sha256": snapshot.snapshot_sha256,
                "snapshot_event_seq": snapshot.snapshot_event_seq,
                "snapshot_watermarks": snapshot_watermarks,
                "eligible_candidate_count": len(candidates),
                "eligible_trace_ids": [candidate.trace_id for candidate in candidates],
                "shared_trace_visibility": self.shared_trace_visibility,
                "selector_config_id": self.selector_config_id,
                "comparison_contract_id": self.comparison_contract_id,
                "delivered_tokens": 0,
                "persistence_status": "empty_selection_no_exposure",
                "ranked": ranking_rows,
            }
        persisted = self.selection_store.record_search(
            request_key=str(request_key),
            task_id=task_id,
            actor_id=actor_id,
            selector_config_id=self.selector_config_id,
            query=query_identity,
            comparison_identity=self.comparison_contract_id or "",
            snapshot_identity=snapshot.snapshot_sha256,
            pool_identity=snapshot.eligible_pool_sha256,
            rankings=ranking_rows,
            search_identity={"run_id": self.run_id, "request_key": str(request_key)},
            eligible_candidates=eligible_payload,
            snapshot_watermarks=snapshot_watermarks,
        )
        persisted_search = persisted.get("search_event", {})
        if (
            persisted_search.get("task_id") != task_id
            or persisted_search.get("actor_id") != actor_id
            or persisted_search.get("selector_config_id") != self.selector_config_id
            or persisted_search.get("snapshot_sha256") != snapshot.snapshot_sha256
            or persisted_search.get("pool_sha256") != snapshot.eligible_pool_sha256
            or persisted_search.get("query") != query_identity
        ):
            raise ValueError("request_key is already bound to a different selection search")
        persisted_rankings = [
            {
                "trace_id": str(row.get("trace_id") or ""),
                "rank": int(row.get("rank") or 0),
                "selected": row.get("selected") is True,
                "component_scores": dict(row.get("component_scores") or {}),
                "payload": dict(row.get("ranking_payload") or {}),
            }
            for row in persisted.get("rankings", [])
        ]
        if persisted_rankings != ranking_rows:
            raise ValueError("request_key is already bound to a different selector ranking")
        item_by_trace = {str(item["trace_id"]): item for item in persisted.get("items", [])}
        candidate_by_trace = {candidate.trace_id: candidate for candidate in candidates}
        items = []
        for row in selected_rows:
            item = dict(row)
            persisted_item = item_by_trace.get(row["trace_id"], {})
            item.update({
                "exposure_item_id": persisted_item.get("exposure_item_id"),
                "exposure_id": persisted.get("exposure", {}).get("exposure_id"),
                "search_event_id": persisted.get("search_event", {}).get("search_event_id"),
            })
            candidate = candidate_by_trace.get(row["trace_id"])
            if candidate is not None:
                item.update({"title": candidate.title, "body": candidate.body, "kind": candidate.kind, "tags": list(candidate.tags)})
            items.append(item)
        return {
            "ok": True,
            "items": items,
            "ranked": ranking_rows,
            "request_key": str(request_key),
            "search_event_id": persisted.get("search_event", {}).get("search_event_id"),
            "exposure_id": persisted.get("exposure", {}).get("exposure_id"),
            "eligible_pool_sha256": snapshot.eligible_pool_sha256,
            "snapshot_sha256": snapshot.snapshot_sha256,
            "snapshot_event_seq": snapshot.snapshot_event_seq,
            "snapshot_watermarks": snapshot_watermarks,
            "eligible_candidate_count": len(candidates),
            "eligible_trace_ids": [candidate.trace_id for candidate in candidates],
            "shared_trace_visibility": self.shared_trace_visibility,
            "selector_config_id": self.selector_config_id,
            "comparison_contract_id": self.comparison_contract_id,
            "exposure_item_ids": [item.get("exposure_item_id") for item in items],
            "delivered_tokens": sum(int(row["payload"]["token_count"]) for row in selected_rows),
            "latency_seconds": round(time.monotonic() - started, 6),
        }

    def search(
        self,
        task_id: str,
        actor_id: str,
        query: str = "",
        *,
        limit: int | None = None,
        request_key: str | None = None,
        episode: int = 0,
        search_ordinal: int | None = None,
        paired_seed: int | None = None,
        task_family: str = "",
    ) -> dict[str, Any]:
        """Run one selection search with an observational lifecycle span."""

        if not self._profiling_enabled:
            return self._search_impl(
                task_id=task_id,
                actor_id=actor_id,
                query=query,
                limit=limit,
                request_key=request_key,
                episode=episode,
                search_ordinal=search_ordinal,
                paired_seed=paired_seed,
                task_family=task_family,
            )
        with self._profile_request_context(
            task_id=task_id,
            actor_id=actor_id,
            episode=episode,
        ):
            with self._profile_span(
                "selection.search",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                operation="search",
            ):
                result = self._search_impl(
                    task_id=task_id,
                    actor_id=actor_id,
                    query=query,
                    limit=limit,
                    request_key=request_key,
                    episode=episode,
                    search_ordinal=search_ordinal,
                    paired_seed=paired_seed,
                    task_family=task_family,
                )
            self._profile_event(
                "selection.search.summary",
                task_id=task_id,
                actor_id=actor_id,
                episode=episode,
                candidate_count=int(result.get("eligible_candidate_count", 0) or 0),
                selected_count=len(result.get("items", [])),
                ranked_count=len(result.get("ranked", [])),
                delivered_tokens=int(result.get("delivered_tokens", 0) or 0),
                snapshot_sha256=result.get("snapshot_sha256"),
                eligible_pool_sha256=result.get("eligible_pool_sha256"),
                snapshot_hit=bool(result.get("idempotent", False)),
                reuse_count=1 if result.get("idempotent", False) else 0,
            )
            return result

    def digest(
        self,
        task_id: str,
        actor_id: str,
        query: str = "",
        *,
        limit: int | None = None,
        request_key: str | None = None,
        episode: int = 0,
        search_ordinal: int | None = None,
        paired_seed: int | None = None,
        task_family: str = "",
    ) -> str:
        digest_request_key = request_key or f"{self.run_id}:digest:{task_id}:{actor_id}:{episode}"
        result = self.search(
            task_id=task_id,
            actor_id=actor_id,
            query=query,
            limit=limit,
            request_key=digest_request_key,
            episode=episode,
            # A digest request key is deterministic.  Use a stable ordinal for
            # retries/re-rendering rather than consuming the process-local
            # auto-increment sequence.
            search_ordinal=(
                _nonnegative_int(search_ordinal, "search_ordinal")
                if search_ordinal is not None
                else _nonnegative_int(episode, "episode")
            ),
            paired_seed=paired_seed,
            task_family=task_family,
        )
        lines = []
        for item in result.get("items", []):
            lines.append(
                f"[piece:{item.get('kind', 'note')} feedback_ref: "
                f"trace_id={item.get('trace_id', '')} "
                f"exposure_item_id={item.get('exposure_item_id', '')}] "
                f"{item.get('title', '')}\n{item.get('body', '')}"
            )
        text = "\n\n".join(lines).strip()
        # The manifest token budget already bounds complete selected items.
        # Do not truncate after exposure persistence: every delivered exposure
        # must correspond to a fully rendered, feedback-addressable item.
        return text

    def broker_search(self, claim: Any, query: str, limit: int) -> Mapping[str, Any]:
        task_id = next(iter(getattr(claim, "candidates", {}) or {}), "")
        actor_id = str(getattr(claim, "actor_id", ""))
        episode = getattr(claim, "episode", 0)
        return self.search(
            task_id=task_id,
            actor_id=actor_id,
            query=query,
            limit=limit,
            episode=episode,
        )

    def summary(self) -> dict[str, Any]:
        """Return bounded run-local selector/exposure counters for closeout."""

        with self.selection_store._db() as db:
            counts = {
                table: int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("search_events", "search_rankings", "exposures", "exposure_items", "feedback_events")
            }
        return {
            "selector_name": self.selector_name,
            "selector_config_id": self.selector_config_id,
            "config_sha256": self.config_sha256,
            "comparison_contract_id": self.comparison_contract_id,
            "shared_trace_visibility": self.shared_trace_visibility,
            **counts,
        }

    def export_events(self, destination: Path | str) -> None:
        """Export deterministic, payload-bounded search/exposure audit rows."""

        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.selection_store._db() as db:
            searches = db.execute(
                """SELECT search_event_id,request_key,task_id,actor_id,selector_config_id,
                          search_sha256,config_sha256,comparison_sha256,snapshot_sha256,
                          pool_sha256,query_json,created_at
                     FROM search_events ORDER BY created_at,search_event_id"""
            ).fetchall()
            rankings = db.execute(
                """SELECT search_event_id,search_ranking_id,trace_id,rank,selected,
                          component_scores_json,ranking_payload_json
                     FROM search_rankings ORDER BY search_event_id,rank,trace_id"""
            ).fetchall()
            exposures = db.execute(
                """SELECT e.search_event_id,e.exposure_id,i.exposure_item_id,i.trace_id,i.rank
                     FROM exposures e JOIN exposure_items i ON i.exposure_id=e.exposure_id
                     ORDER BY e.search_event_id,i.rank,i.trace_id"""
            ).fetchall()
            feedback = db.execute(
                """SELECT feedback_event_id,request_key,exposure_item_id,trace_id,actor_id,
                          feedback_kind,origin,terminal,effective,conflicts_with_feedback_event_id,
                          created_at
                     FROM feedback_events ORDER BY created_at,feedback_event_id"""
            ).fetchall()
        rows: list[dict[str, Any]] = []
        for row in searches:
            item = dict(row)
            item["event_type"] = "search"
            item["query"] = json.loads(item.pop("query_json"))
            rows.append(item)
        for row in rankings:
            item = dict(row)
            item["event_type"] = "ranking"
            item["selected"] = bool(item["selected"])
            item["component_scores"] = json.loads(item.pop("component_scores_json"))
            item["ranking_payload"] = json.loads(item.pop("ranking_payload_json"))
            rows.append(item)
        for row in exposures:
            item = dict(row)
            item["event_type"] = "exposure_item"
            rows.append(item)
        for row in feedback:
            item = dict(row)
            item["event_type"] = "feedback"
            item["terminal"] = bool(item["terminal"])
            item["effective"] = bool(item["effective"])
            rows.append(item)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(_json(row) + "\n")


__all__ = ["SelectionRuntime"]
