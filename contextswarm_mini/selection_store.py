"""Durable selection exposure and feedback attribution.

This store is deliberately independent from the legacy CPS database.  It owns
the auditable chain used by feedback-aware selectors::

    search_event -> exposure -> exposure_item -> worker feedback

Verifier evidence, trace maintenance, and trace relations have their own event
tables.  In particular, none of those event classes can accidentally occupy a
worker interaction's single effective terminal-feedback slot.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Sequence


SCHEMA_VERSION = "contextswarm_selection_store_v1"
EXPORT_SCHEMA_VERSION = "contextswarm_selection_store_export_v1"
REQUEST_KEY_CONFLICT = "REQUEST_KEY_CONFLICT"

CANONICAL_FEEDBACK_KINDS = frozenset(
    {
        "useful",
        "not_useful",
        "misleading",
        "stale",
        "unsafe",
        "duplicate",
        "diagnostic_useful",
        "needs_refinement",
        "not_used",
        "route_attempted",
        "route_improving",
    }
)

CANONICAL_RELATIONS = frozenset(
    {
        "supports",
        "refutes",
        "duplicates",
        "supersedes",
        "depends_on",
        "generalizes",
        "specializes",
    }
)

_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")
_JSON_COLUMNS = frozenset(
    {
        "config_json",
        "query_json",
        "component_scores_json",
        "ranking_payload_json",
        "payload_json",
        "evidence_json",
        "snapshot_watermarks_json",
        "candidate_payload_json",
        "feedback_snapshot_json",
    }
)

# Public JSONL record types deliberately follow the semantic singular of each
# table.  Keeping this mapping centralized makes both the export order and its
# count reconciliation deterministic without exposing arbitrary SQL names to
# callers.
_EXPORT_TABLES = (
    ("selector_config", "selector_configs", "selector_config_id"),
    ("search_event", "search_events", "search_event_id"),
    ("search_ranking", "search_rankings", "search_ranking_id"),
    ("exposure", "exposures", "exposure_id"),
    ("exposure_item", "exposure_items", "exposure_item_id"),
    ("feedback_event", "feedback_events", "feedback_event_id"),
    ("verifier_evidence", "verifier_evidence", "evidence_event_id"),
    ("maintenance_event", "maintenance_events", "maintenance_event_id"),
    ("trace_relation", "trace_relations", "relation_id"),
)

# Eligible-pool rows were added after the original attribution schema.  Keep
# this table optional in summaries/exports so old callers and old databases
# retain their exact record-type/count contract when no pool snapshot was
# supplied.  Once a search carries candidates, the rows are emitted directly
# after its search_event and before rankings.
_OPTIONAL_EXPORT_TABLES = (
    ("search_candidate", "search_candidates", "search_candidate_id"),
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _required(value: Any, name: str, *, limit: int = 512) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} must be non-empty")
    if len(text) > limit:
        raise ValueError(f"{name} exceeds {limit} characters")
    return text


def _hash(kind: str, *parts: Any) -> str:
    """Return a domain-separated deterministic identifier.

    Length-prefixed canonical JSON avoids ambiguous concatenation.  The kind
    prefix keeps exported records readable while the full digest makes IDs
    stable across processes and retries.
    """

    digest = hashlib.sha256()
    for part in (kind, *parts):
        encoded = _json(part).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return f"{kind}_{digest.hexdigest()}"


def _identity_sha256(value: Any) -> str:
    if isinstance(value, str) and _SHA256_RE.fullmatch(value.strip()):
        return value.strip().lower()
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _metric_int(metrics: Mapping[str, Any], key: str, default: int = 0) -> int:
    """Read a bounded integer diagnostic without ever affecting store semantics."""

    value = metrics.get(key, default)
    if isinstance(value, bool):
        return int(value)
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return int(default)
    return max(0, number)


def _metric_float(metrics: Mapping[str, Any], key: str) -> float | None:
    """Read a finite optional diagnostic value from an instrumentation map."""

    value = metrics.get(key)
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _decode_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    for column in _JSON_COLUMNS:
        if column in result:
            result[column.removesuffix("_json")] = json.loads(result.pop(column))
    for column in ("selected", "terminal", "effective"):
        if column in result:
            result[column] = bool(result[column])
    return result


class RequestKeyConflictError(ValueError):
    """Raised when a request key is retried with different search inputs."""

    code = REQUEST_KEY_CONFLICT

    def __init__(self, mismatched_fields: Sequence[str]):
        self.mismatched_fields = tuple(mismatched_fields)
        fields = ", ".join(self.mismatched_fields)
        super().__init__(
            f"{REQUEST_KEY_CONFLICT}: request_key is already bound to different {fields}"
        )


class SelectionStore:
    """SQLite persistence for selector inputs, deliveries, and attribution.

    Connections are operation-scoped and writes use ``BEGIN IMMEDIATE``.  This
    makes the "first terminal interaction wins" rule atomic across threads and
    processes while keeping lock-holding transactions short.
    """

    def __init__(self, path: Path | str, profiler: Any | None = None):
        self.path = Path(path)
        self.profiler = profiler
        try:
            self._profiling_enabled = bool(
                profiler is not None and getattr(profiler, "enabled", False)
            )
        except Exception:
            self._profiling_enabled = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # These are descriptive counters for contenders/in-flight writers in
        # this SelectionStore instance; they are not an application queue.
        # SQLite's measured lock_wait_seconds is the authoritative
        # cross-thread/process wait.  queue_residence is measured from local
        # contender registration until BEGIN IMMEDIATE acquires the lock.
        self._write_state_lock = threading.Lock()
        self._write_waiters = 0
        self._write_active = 0
        self._write_sequence = 0
        self._write_wall_total_seconds = 0.0
        self._write_lock_wait_total_seconds = 0.0
        self._write_lock_hold_total_seconds = 0.0
        if self._profiling_enabled:
            self._profile_deferred_local = threading.local()
            # SelectionRuntime enters this context around one logical search.
            # Store helpers are intentionally API-compatible and do not carry
            # attribution arguments through every SQL method; a thread-local
            # envelope lets their diagnostic rows inherit the request scope
            # without changing any persisted data or business return values.
            self._profile_context_local = threading.local()
        self._init_schema()

    @contextmanager
    def profile_context(
        self,
        *,
        task_id: Any = None,
        actor_id: Any = None,
        episode: Any = None,
    ):
        """Attach logical-search attribution to nested profiling events.

        This is an opt-in diagnostic context only.  The disabled path remains
        a no-op and therefore does not allocate a context record, take a
        clock, or alter the ordinary SQLite call sequence.  Nested contexts
        restore the previous value so concurrent worker threads cannot leak
        one request's identifiers into another request's rows.
        """

        if not self._profiling_enabled:
            yield
            return
        local = self._profile_context_local
        marker = object()
        previous = getattr(local, "value", marker)
        local.value = (task_id, actor_id, episode)
        try:
            yield
        finally:
            if previous is marker:
                try:
                    del local.value
                except AttributeError:
                    pass
            else:
                local.value = previous

    def _profile_context_values(self) -> tuple[Any, Any, Any] | None:
        local = getattr(self, "_profile_context_local", None)
        value = getattr(local, "value", None) if local is not None else None
        if not isinstance(value, tuple) or len(value) != 3:
            return None
        return value

    def _profile_event(self, event: str, **fields: Any) -> None:
        if not self._profiling_enabled:
            return
        profiler = self.profiler
        try:
            context = self._profile_context_values()
            if context is not None:
                context_task_id, context_actor_id, context_episode = context
                fields.setdefault("task_id", context_task_id)
                fields.setdefault("actor_id", context_actor_id)
                fields.setdefault("episode", context_episode)
            # Keep identity fields on the dedicated profiler parameters.  This
            # makes them pass the same sanitisation path as runner/Judge rows,
            # while preserving explicit event fields when a low-level helper
            # supplied one itself.
            identity: dict[str, Any] = {}
            for key in ("task_id", "actor_id", "episode"):
                value = fields.pop(key, None)
                if value is not None:
                    identity[key] = value
            profiler.emit(event, **identity, **fields)
        except BaseException:
            # Profiling must never turn a SQLite diagnostic into a runtime
            # failure or alter attribution semantics.
            return

    def _defer_profile_event(self, event: str, **fields: Any) -> None:
        """Queue an event for the current write, or emit it immediately.

        ``record_search`` performs its readback from inside ``_write``.  A
        thread-local deferred-event buffer lets that caller defer sink I/O
        without exposing an application-queue argument through the public
        store API; direct helper callers outside a write retain the immediate
        profiling behavior.
        """

        if not self._profiling_enabled:
            return
        local = getattr(self, "_profile_deferred_local", None)
        queue = getattr(local, "queue", None) if local is not None else None
        if queue is not None:
            queue.append((event, dict(fields)))
            return
        self._profile_event(event, **fields)

    @contextmanager
    def _profile_span(self, name: str, **fields: Any):
        if not self._profiling_enabled:
            yield
            return
        profiler = self.profiler
        span = getattr(profiler, "span", None) if profiler is not None else None
        if callable(span):
            context = self._profile_context_values()
            if context is not None:
                context_task_id, context_actor_id, context_episode = context
                fields.setdefault("task_id", context_task_id)
                fields.setdefault("actor_id", context_actor_id)
                fields.setdefault("episode", context_episode)
            try:
                context = span(name, **fields)
            except BaseException:
                context = None
            if context is not None:
                # A diagnostic sink is optional and must remain fail-open.
                # Enter/exit errors from a custom sink are swallowed while an
                # exception raised by the wrapped store operation is re-raised
                # unchanged (and never suppressed by ``__exit__``).
                try:
                    context.__enter__()
                except BaseException:
                    yield
                    return
                try:
                    yield
                except BaseException as operation_error:
                    try:
                        context.__exit__(
                            type(operation_error),
                            operation_error,
                            operation_error.__traceback__,
                        )
                    except BaseException:
                        pass
                    raise
                else:
                    try:
                        context.__exit__(None, None, None)
                    except BaseException:
                        pass
                return
        yield

    def _connect(self, *, operation: str = "generic") -> sqlite3.Connection:
        # Connection setup is intentionally measured separately from query and
        # transaction spans.  The selection path opens short-lived SQLite
        # connections, so repeated setup can dominate at high concurrency.
        started = time.monotonic() if self._profiling_enabled else 0.0
        db: sqlite3.Connection | None = None
        error_kind: str | None = None
        try:
            db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            return db
        except Exception as exc:
            error_kind = type(exc).__name__
            raise
        finally:
            if self._profiling_enabled:
                self._profile_event(
                    "selection.sqlite.connect",
                    db_operation=operation,
                    connect_seconds=max(0.0, time.monotonic() - started),
                    status="ok" if db is not None else "error",
                    error_kind=error_kind,
                    db_bytes=self._file_size(self.path),
                    wal_bytes=self._file_size(Path(str(self.path) + "-wal")),
                )

    @contextmanager
    def _db(self, *, operation: str = "generic"):
        db = self._connect(operation=operation)
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _write(
        self,
        operation: str = "write",
        *,
        metrics: Mapping[str, Any] | None = None,
    ):
        if not self._profiling_enabled:
            # Preserve the original transaction path exactly when profiling is
            # disabled: no profiling clocks, file stats, or event dictionaries.
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                try:
                    yield db
                    db.execute("COMMIT")
                except Exception:
                    if db.in_transaction:
                        db.execute("ROLLBACK")
                    raise
            return
        started = time.monotonic()
        lock_started = started
        lock_acquired_at: float | None = None
        lock_wait = 0.0
        lock_hold = 0.0
        lock_released_at: float | None = None
        queue_residence = 0.0
        committed = False
        error_kind: str | None = None
        commit_seconds = 0.0
        body_seconds = 0.0
        changes_before = 0
        changes_after = 0
        metrics = metrics if isinstance(metrics, dict) else dict(metrics or {})
        queued_at = time.monotonic()
        wal_before = self._file_size(Path(str(self.path) + "-wal"))
        db_before = self._file_size(self.path)
        with self._write_state_lock:
            self._write_waiters += 1
            queue_waiters = self._write_waiters
            queue_active = self._write_active
            queue_depth = queue_waiters + queue_active
        waiter_registered = True
        writer_active = False
        end_emitted = False
        lock_event_emitted = False
        db: sqlite3.Connection | None = None
        # Profiling sinks write a separate JSONL file.  Never invoke them
        # while BEGIN IMMEDIATE owns SQLite's writer lock: at high concurrency
        # that I/O would inflate lock_hold_seconds and make the measurement
        # perturb the contention being measured.  Keep event payloads bounded
        # and flush them from _emit_end, which runs after _db closes.
        deferred_profile_events: list[tuple[str, dict[str, Any]]] = []

        # ``record_search`` is implemented above this context manager and
        # cannot see its deferred-event buffer directly.  Expose only the
        # current thread's buffer for the duration of this write; nested writes
        # restore the previous value on exit.
        profile_local = getattr(self, "_profile_deferred_local", None)
        previous_profile_queue = (
            getattr(profile_local, "queue", None) if profile_local is not None else None
        )
        if profile_local is not None:
            profile_local.queue = deferred_profile_events

        def _defer_profile_event(event: str, **fields: Any) -> None:
            if self._profiling_enabled:
                deferred_profile_events.append((event, dict(fields)))

        def _flush_deferred_profile_events() -> None:
            pending = tuple(deferred_profile_events)
            deferred_profile_events.clear()
            for event, fields in pending:
                try:
                    self._profile_event(event, **fields)
                except BaseException:
                    # ``_profile_event`` is fail-open itself; retain the
                    # guard here as well so a test/custom override cannot
                    # suppress the terminal persistence event.
                    pass

        self._profile_event(
            "selection.persist.queue",
            operation=operation,
            queue_state="waiting",
            lock_queue_depth=queue_depth,
            write_waiters=queue_waiters,
            write_active=queue_active,
        )
        self._profile_event("selection.persist.start", operation=operation)

        def _dequeue_waiter() -> None:
            """Release this attempt's local contender reservation exactly once."""

            nonlocal waiter_registered, queue_waiters, queue_active, queue_depth
            if not waiter_registered:
                return
            with self._write_state_lock:
                self._write_waiters = max(0, self._write_waiters - 1)
                queue_waiters = self._write_waiters
                queue_active = self._write_active
                queue_depth = queue_waiters + queue_active
            waiter_registered = False

        def _activate_writer() -> None:
            """Atomically move this attempt from waiting to active."""

            nonlocal waiter_registered, writer_active
            nonlocal queue_waiters, queue_active, queue_depth
            with self._write_state_lock:
                if waiter_registered:
                    self._write_waiters = max(0, self._write_waiters - 1)
                    waiter_registered = False
                self._write_active += 1
                queue_waiters = self._write_waiters
                queue_active = self._write_active
                queue_depth = queue_waiters + queue_active
            writer_active = True

        def _deactivate_writer() -> None:
            """Drop the local active-writer count exactly once."""

            nonlocal writer_active, queue_waiters, queue_active, queue_depth
            if not writer_active:
                return
            with self._write_state_lock:
                self._write_active = max(0, self._write_active - 1)
                queue_waiters = self._write_waiters
                queue_active = self._write_active
                queue_depth = queue_waiters + queue_active
            writer_active = False

        def _emit_end() -> None:
            """Emit one terminal row, including setup/BEGIN failure paths."""

            nonlocal end_emitted, lock_hold, queue_waiters, queue_active, queue_depth
            if end_emitted:
                return
            # A failed BEGIN never transitions to active; a failed connection
            # still owns a waiter reservation.  Release either state before
            # taking the final queue snapshot so the row cannot leak depth.
            _deactivate_writer()
            _dequeue_waiter()
            if lock_acquired_at is not None:
                lock_hold = max(
                    0.0,
                    (lock_released_at or time.monotonic()) - lock_acquired_at,
                )
            wall_seconds = max(0.0, time.monotonic() - started)
            with self._write_state_lock:
                self._write_sequence += 1
                write_sequence = self._write_sequence
                self._write_wall_total_seconds += wall_seconds
                self._write_lock_wait_total_seconds += lock_wait
                self._write_lock_hold_total_seconds += lock_hold
                write_wall_total_seconds = self._write_wall_total_seconds
                write_lock_wait_total_seconds = self._write_lock_wait_total_seconds
                write_lock_hold_total_seconds = self._write_lock_hold_total_seconds
                queue_waiters = self._write_waiters
                queue_active = self._write_active
                queue_depth = queue_waiters + queue_active
            wal_after = self._file_size(Path(str(self.path) + "-wal"))
            db_after = self._file_size(self.path)
            # ``total_changes`` counts statements even when the transaction
            # is rolled back.  Report committed rows here; failed attempts
            # remain visible through status/error_kind and must not inflate a
            # persistence-throughput denominator.
            rows_written = (
                max(0, changes_after - changes_before) if committed else 0
            )
            # Mark the terminal path before handing control to a custom sink;
            # a re-entrant/failing sink cannot cause duplicate flushes.
            end_emitted = True
            _flush_deferred_profile_events()
            self._profile_event(
                "selection.persist.end",
                operation=operation,
                lock_wait_seconds=lock_wait,
                lock_hold_seconds=lock_hold,
                queue_residence_seconds=queue_residence,
                lock_queue_depth=queue_depth,
                write_waiters=queue_waiters,
                write_active=queue_active,
                # ``transaction_seconds`` is deliberately the time spent
                # after BEGIN IMMEDIATE acquired the SQLite writer lock.
                # Admission delay is reported separately by lock_wait and
                # queue_residence, avoiding double-counting in reports.
                transaction_seconds=lock_hold,
                body_seconds=body_seconds,
                commit_seconds=commit_seconds,
                wall_seconds=wall_seconds,
                write_sequence=write_sequence,
                write_ops_total=write_sequence,
                write_wall_total_seconds=write_wall_total_seconds,
                write_lock_wait_total_seconds=write_lock_wait_total_seconds,
                write_lock_hold_total_seconds=write_lock_hold_total_seconds,
                status="ok" if committed else "error",
                error_kind=error_kind,
                db_bytes=db_after,
                wal_bytes=wal_after,
                wal_bytes_before=wal_before,
                wal_bytes_after=wal_after,
                # Keep an unavailable baseline as absent instead of treating
                # it as zero (which would look like a full-file allocation).
                wal_bytes_delta=(
                    wal_after - wal_before
                    if wal_before is not None and wal_after is not None
                    else None
                ),
                input_bytes=_metric_int(metrics, "input_bytes"),
                rows_written=rows_written,
                payload_bytes=_metric_int(metrics, "payload_bytes"),
                request_key_sha256=metrics.get("request_key_sha256"),
                snapshot_sha256=metrics.get("snapshot_sha256"),
                pool_sha256=metrics.get("pool_sha256"),
                serialization_seconds=_metric_float(metrics, "serialization_seconds"),
                serialization_inside_lock_seconds=_metric_float(
                    metrics, "serialization_inside_lock_seconds"
                ),
                serialization_inside_lock_bytes=_metric_int(
                    metrics, "serialization_inside_lock_bytes"
                ),
                serialization_call_count=_metric_int(metrics, "serialization_call_count"),
                hash_seconds=_metric_float(metrics, "hash_seconds"),
                prepare_seconds=_metric_float(metrics, "prepare_seconds"),
                prepare_rows=_metric_int(metrics, "prepare_rows"),
                prepare_candidate_rows=_metric_int(metrics, "prepare_candidate_rows"),
                prepare_ranking_rows=_metric_int(metrics, "prepare_ranking_rows"),
                prepare_bytes=_metric_int(metrics, "prepare_bytes"),
                prepare_serialization_seconds=_metric_float(
                    metrics, "prepare_serialization_seconds"
                ),
                prepare_hash_seconds=_metric_float(metrics, "prepare_hash_seconds"),
                db_bytes_before=db_before,
                db_bytes_delta=(
                    db_after - db_before
                    if db_before is not None and db_after is not None
                    else None
                ),
            )

        try:
            try:
                with self._db(operation=f"write:{operation}") as db_connection:
                    db = db_connection
                    lock_started = time.monotonic()
                    changes_before = int(getattr(db, "total_changes", 0) or 0)
                    try:
                        try:
                            db.execute("BEGIN IMMEDIATE")
                            lock_acquired_at = time.monotonic()
                            lock_wait = max(0.0, lock_acquired_at - lock_started)
                            queue_residence = max(0.0, lock_acquired_at - queued_at)
                            # Admission is one state transition: remove the
                            # waiter and increment active atomically.
                            _activate_writer()
                            _defer_profile_event(
                                "selection.persist.lock",
                                operation=operation,
                                lock_wait_seconds=lock_wait,
                                queue_residence_seconds=queue_residence,
                                lock_queue_depth=queue_depth,
                                write_waiters=queue_waiters,
                                write_active=queue_active,
                                status="acquired",
                            )
                        except Exception as exc:
                            error_kind = type(exc).__name__
                            lock_wait = max(0.0, time.monotonic() - lock_started)
                            queue_residence = max(0.0, time.monotonic() - queued_at)
                            lock_event_emitted = True
                            _defer_profile_event(
                                "selection.persist.lock",
                                operation=operation,
                                lock_wait_seconds=lock_wait,
                                queue_residence_seconds=queue_residence,
                                lock_queue_depth=queue_depth,
                                write_waiters=queue_waiters,
                                write_active=queue_active,
                                status="error",
                                error_kind=error_kind,
                            )
                            raise
                        body_started = time.monotonic()
                        try:
                            yield db
                        finally:
                            body_seconds = max(0.0, time.monotonic() - body_started)
                            # Capture this while the connection is alive;
                            # ``_emit_end`` runs after ``_db`` closes it.
                            changes_after = int(getattr(db, "total_changes", 0) or 0)
                        commit_started = time.monotonic()
                        db.execute("COMMIT")
                        committed = True
                        commit_seconds = max(0.0, time.monotonic() - commit_started)
                    except Exception as exc:
                        error_kind = error_kind or type(exc).__name__ or "transaction_error"
                        if db.in_transaction:
                            db.execute("ROLLBACK")
                        raise
                    finally:
                        # Capture lock release before the connection context
                        # closes.  The terminal event is emitted outside that
                        # context and should not charge close/profile work to
                        # SQLite lock hold.
                        if lock_acquired_at is not None:
                            lock_released_at = time.monotonic()
            except Exception as exc:
                # _db() can fail before a Connection object exists (for
                # example, a read-only directory).  Preserve the original
                # exception while still closing the profiling episode.
                error_kind = error_kind or type(exc).__name__ or "write_error"
                if db is None and not lock_event_emitted:
                    lock_event_emitted = True
                    _defer_profile_event(
                        "selection.persist.lock",
                        operation=operation,
                        lock_wait_seconds=0.0,
                        queue_residence_seconds=max(0.0, time.monotonic() - queued_at),
                        lock_queue_depth=queue_depth,
                        write_waiters=queue_waiters,
                        write_active=queue_active,
                        status="error",
                        error_kind=error_kind,
                    )
                raise
        finally:
            try:
                _emit_end()
            finally:
                if profile_local is not None:
                    if previous_profile_queue is None:
                        try:
                            del profile_local.queue
                        except AttributeError:
                            pass
                    else:
                        profile_local.queue = previous_profile_queue

    @contextmanager
    def _read_snapshot(self, operation: str = "read"):
        """Yield one consistent read snapshot without blocking WAL writers."""

        if not self._profiling_enabled:
            with self._db() as db:
                db.execute("BEGIN")
                try:
                    yield db
                    db.execute("COMMIT")
                except Exception:
                    if db.in_transaction:
                        db.execute("ROLLBACK")
                    raise
            return
        started = time.monotonic()
        committed = False
        error_kind: str | None = None
        transaction_started: float | None = None
        transaction_finished: float | None = None
        scope_started: float | None = None
        begin_seconds = 0.0
        read_scope_seconds = 0.0
        commit_seconds = 0.0
        # ``BEGIN`` is intentionally deferred in SQLite WAL mode.  It does
        # not acquire a shared/read lock, so its elapsed time is setup
        # overhead, not lock contention.  Keep the explicit zero-valued
        # metric so reports do not mistake BEGIN latency for lock wait.
        read_lock_wait_seconds = 0.0
        db: sqlite3.Connection | None = None
        self._profile_event("selection.read.start", operation=operation)
        try:
            try:
                with self._db(operation=f"read:{operation}") as db_connection:
                    db = db_connection
                    transaction_started = time.monotonic()
                    begin_started = transaction_started
                    try:
                        db.execute("BEGIN")
                        begin_seconds = max(0.0, time.monotonic() - begin_started)
                        scope_started = time.monotonic()
                        try:
                            yield db
                        finally:
                            if scope_started is not None:
                                read_scope_seconds = max(
                                    0.0, time.monotonic() - scope_started
                                )
                        commit_started = time.monotonic()
                        db.execute("COMMIT")
                        committed = True
                        commit_seconds = max(0.0, time.monotonic() - commit_started)
                        transaction_finished = time.monotonic()
                    except Exception as exc:
                        error_kind = type(exc).__name__ or "transaction_error"
                        if db.in_transaction:
                            db.execute("ROLLBACK")
                        transaction_finished = time.monotonic()
                        raise
            except Exception as exc:
                # Connection setup can fail before a transaction exists.  The
                # terminal event below still closes the profiling episode and
                # retains the original exception for the caller.
                error_kind = error_kind or type(exc).__name__ or "read_error"
                raise
        finally:
            read_transaction_seconds = (
                max(
                    0.0,
                    (transaction_finished or time.monotonic()) - transaction_started,
                )
                if transaction_started is not None
                else 0.0
            )
            self._profile_event(
                "selection.read.end",
                operation=operation,
                wall_seconds=max(0.0, time.monotonic() - started),
                # Transaction includes BEGIN, the yielded read scope, and
                # COMMIT.  Scope excludes BEGIN/COMMIT so query work can be
                # compared directly with readback timings.
                read_transaction_seconds=read_transaction_seconds,
                read_scope_seconds=read_scope_seconds,
                begin_seconds=begin_seconds,
                commit_seconds=commit_seconds,
                read_lock_wait_seconds=read_lock_wait_seconds,
                # Kept as a compatibility alias; it is always zero for this
                # deferred WAL read path and must not be interpreted as the
                # BEGIN duration.
                lock_wait_seconds=read_lock_wait_seconds,
                read_mode="deferred_wal",
                status="ok" if committed else "error",
                error_kind=error_kind,
                db_bytes=self._file_size(self.path),
                wal_bytes=self._file_size(Path(str(self.path) + "-wal")),
            )

    @staticmethod
    def _file_size(path: Path) -> int:
        try:
            return max(0, int(path.stat().st_size))
        except OSError:
            return 0

    def _profile_checkpoint(self, operation: str) -> None:
        """Measure an explicit WAL checkpoint only when a caller requests one."""

        if not self._profiling_enabled:
            return
        started = time.monotonic()
        try:
            with self._db(operation=f"checkpoint:{operation}") as db:
                result = db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            busy = int(result[0]) if result is not None else 0
            frames = int(result[1]) if result is not None else 0
            checkpointed = int(result[2]) if result is not None else 0
            self._profile_event(
                "selection.sqlite.checkpoint",
                db_operation=operation,
                checkpoint_seconds=max(0.0, time.monotonic() - started),
                busy_retry_count=max(0, busy),
                rows=frames,
                output_rows=checkpointed,
                wal_bytes=self._file_size(Path(str(self.path) + "-wal")),
            )
        except Exception as exc:
            self._profile_event(
                "selection.sqlite.checkpoint",
                db_operation=operation,
                checkpoint_seconds=max(0.0, time.monotonic() - started),
                status="error",
                error_kind=type(exc).__name__,
            )

    def _init_schema(self) -> None:
        with self._db() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS selection_store_metadata (
                    schema_version TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS selector_configs (
                    selector_config_id TEXT PRIMARY KEY,
                    selector_name TEXT NOT NULL,
                    config_sha256 TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(selector_name, config_sha256)
                );

                CREATE TABLE IF NOT EXISTS search_events (
                    search_event_id TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    task_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    selector_config_id TEXT NOT NULL
                        REFERENCES selector_configs(selector_config_id),
                    search_sha256 TEXT NOT NULL,
                    config_sha256 TEXT NOT NULL,
                    comparison_sha256 TEXT NOT NULL,
                    snapshot_sha256 TEXT NOT NULL,
                    pool_sha256 TEXT NOT NULL,
                    query_json TEXT NOT NULL,
                    -- These fields bind the complete eligible-pool snapshot
                    -- to the request key.  Empty values retain compatibility
                    -- with searches recorded before pool artifacts existed.
                    eligible_candidates_sha256 TEXT NOT NULL DEFAULT '',
                    snapshot_watermarks_sha256 TEXT NOT NULL DEFAULT '',
                    snapshot_watermarks_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS search_events_actor_task
                    ON search_events(actor_id, task_id, created_at);

                CREATE TABLE IF NOT EXISTS search_candidates (
                    search_candidate_id TEXT PRIMARY KEY,
                    search_event_id TEXT NOT NULL
                        REFERENCES search_events(search_event_id) ON DELETE CASCADE,
                    trace_id TEXT NOT NULL,
                    pool_order INTEGER NOT NULL CHECK(pool_order >= 1),
                    candidate_sha256 TEXT NOT NULL,
                    candidate_payload_json TEXT NOT NULL,
                    feedback_snapshot_json TEXT NOT NULL,
                    snapshot_watermarks_json TEXT NOT NULL,
                    UNIQUE(search_event_id, trace_id),
                    UNIQUE(search_event_id, pool_order)
                );
                CREATE INDEX IF NOT EXISTS search_candidates_trace
                    ON search_candidates(trace_id, search_event_id);

                CREATE TABLE IF NOT EXISTS search_rankings (
                    search_ranking_id TEXT PRIMARY KEY,
                    search_event_id TEXT NOT NULL
                        REFERENCES search_events(search_event_id) ON DELETE CASCADE,
                    trace_id TEXT NOT NULL,
                    rank INTEGER NOT NULL CHECK(rank >= 1),
                    selected INTEGER NOT NULL CHECK(selected IN (0, 1)),
                    component_scores_json TEXT NOT NULL,
                    ranking_payload_json TEXT NOT NULL,
                    UNIQUE(search_event_id, trace_id),
                    UNIQUE(search_event_id, rank)
                );
                CREATE INDEX IF NOT EXISTS search_rankings_trace
                    ON search_rankings(trace_id, search_event_id);

                CREATE TABLE IF NOT EXISTS exposures (
                    exposure_id TEXT PRIMARY KEY,
                    search_event_id TEXT NOT NULL UNIQUE
                        REFERENCES search_events(search_event_id) ON DELETE CASCADE,
                    actor_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS exposure_items (
                    exposure_item_id TEXT PRIMARY KEY,
                    exposure_id TEXT NOT NULL
                        REFERENCES exposures(exposure_id) ON DELETE CASCADE,
                    search_ranking_id TEXT NOT NULL UNIQUE
                        REFERENCES search_rankings(search_ranking_id) ON DELETE CASCADE,
                    trace_id TEXT NOT NULL,
                    rank INTEGER NOT NULL CHECK(rank >= 1),
                    created_at TEXT NOT NULL,
                    UNIQUE(exposure_id, trace_id),
                    UNIQUE(exposure_id, rank),
                    UNIQUE(exposure_id, trace_id, rank)
                );
                CREATE INDEX IF NOT EXISTS exposure_items_trace
                    ON exposure_items(trace_id, exposure_id);

                CREATE TABLE IF NOT EXISTS feedback_events (
                    feedback_event_id TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    exposure_item_id TEXT NOT NULL
                        REFERENCES exposure_items(exposure_item_id),
                    trace_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    event_class TEXT NOT NULL DEFAULT 'worker_interaction'
                        CHECK(event_class = 'worker_interaction'),
                    feedback_kind TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    terminal INTEGER NOT NULL CHECK(terminal IN (0, 1)),
                    effective INTEGER NOT NULL CHECK(effective IN (0, 1)),
                    conflicts_with_feedback_event_id TEXT
                        REFERENCES feedback_events(feedback_event_id),
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    CHECK(effective = 0 OR terminal = 1)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_effective_terminal_worker_interaction
                    ON feedback_events(exposure_item_id)
                    WHERE terminal = 1 AND effective = 1;
                CREATE INDEX IF NOT EXISTS feedback_events_trace
                    ON feedback_events(trace_id, created_at);

                CREATE TABLE IF NOT EXISTS verifier_evidence (
                    evidence_event_id TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    trace_id TEXT NOT NULL,
                    task_id TEXT,
                    verifier_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS verifier_evidence_trace
                    ON verifier_evidence(trace_id, created_at);

                CREATE TABLE IF NOT EXISTS maintenance_events (
                    maintenance_event_id TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    trace_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    maintenance_kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS maintenance_events_trace
                    ON maintenance_events(trace_id, created_at);

                CREATE TABLE IF NOT EXISTS trace_relations (
                    relation_id TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    source_trace_id TEXT NOT NULL,
                    target_trace_id TEXT NOT NULL,
                    relation_kind TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    CHECK(source_trace_id <> target_trace_id)
                );
                CREATE INDEX IF NOT EXISTS trace_relations_source
                    ON trace_relations(source_trace_id, relation_kind);
                CREATE INDEX IF NOT EXISTS trace_relations_target
                    ON trace_relations(target_trace_id, relation_kind);

                CREATE TRIGGER IF NOT EXISTS exposure_actor_matches_search
                BEFORE INSERT ON exposures
                WHEN NOT EXISTS (
                    SELECT 1 FROM search_events AS search
                    WHERE search.search_event_id = NEW.search_event_id
                      AND search.actor_id = NEW.actor_id
                )
                BEGIN
                    SELECT RAISE(ABORT, 'exposure actor does not match search actor');
                END;

                CREATE TRIGGER IF NOT EXISTS exposure_item_matches_selected_ranking
                BEFORE INSERT ON exposure_items
                WHEN NOT EXISTS (
                    SELECT 1
                    FROM exposures AS exposure
                    JOIN search_rankings AS ranking
                      ON ranking.search_event_id = exposure.search_event_id
                    WHERE exposure.exposure_id = NEW.exposure_id
                      AND ranking.search_ranking_id = NEW.search_ranking_id
                      AND ranking.trace_id = NEW.trace_id
                      AND ranking.rank = NEW.rank
                      AND ranking.selected = 1
                )
                BEGIN
                    SELECT RAISE(ABORT, 'exposure item does not match selected ranking');
                END;

                CREATE TRIGGER IF NOT EXISTS feedback_actor_and_trace_binding
                BEFORE INSERT ON feedback_events
                WHEN NOT EXISTS (
                    SELECT 1
                    FROM exposure_items AS item
                    JOIN exposures AS exposure
                      ON exposure.exposure_id = item.exposure_id
                    WHERE item.exposure_item_id = NEW.exposure_item_id
                      AND item.trace_id = NEW.trace_id
                      AND exposure.actor_id = NEW.actor_id
                )
                BEGIN
                    SELECT RAISE(ABORT, 'feedback actor or trace does not match exposure item');
                END;
                """
            )
            # The store is intentionally restartable.  A run may reopen a DB
            # created by the pre-pool-artifact implementation, so add the new
            # request-identity columns lazily instead of requiring a destructive
            # migration.  SQLite's ALTER TABLE is atomic under the connection
            # and the defaults make old rows semantically "no pool artifact".
            existing_columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(search_events)")
            }
            for column, definition in (
                ("eligible_candidates_sha256", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_watermarks_sha256", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_watermarks_json", "TEXT NOT NULL DEFAULT '{}'"),
            ):
                if column not in existing_columns:
                    db.execute(f"ALTER TABLE search_events ADD COLUMN {column} {definition}")
            db.execute(
                "INSERT OR IGNORE INTO selection_store_metadata(schema_version, created_at) VALUES(?, ?)",
                (SCHEMA_VERSION, _now()),
            )

    def register_selector_config(
        self, *, selector_name: str, config: Mapping[str, Any]
    ) -> dict[str, Any]:
        selector_name = _required(selector_name, "selector_name", limit=128)
        config_json = _json(dict(config))
        config_sha256 = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
        selector_config_id = _hash("selector_config", selector_name, config_sha256)
        created_at = _now()
        with self._write(operation="register_selector_config") as db:
            db.execute(
                """INSERT OR IGNORE INTO selector_configs(
                       selector_config_id, selector_name, config_sha256, config_json, created_at
                   ) VALUES(?, ?, ?, ?, ?)""",
                (selector_config_id, selector_name, config_sha256, config_json, created_at),
            )
            row = db.execute(
                "SELECT * FROM selector_configs WHERE selector_config_id = ?",
                (selector_config_id,),
            ).fetchone()
        assert row is not None
        return _decode_row(row) or {}

    def _record_search_impl(
        self,
        *,
        request_key: str,
        task_id: str,
        actor_id: str,
        selector_config_id: str,
        query: Any,
        comparison_identity: Any,
        snapshot_identity: Any,
        pool_identity: Any | None,
        rankings: Sequence[Mapping[str, Any]],
        search_identity: Any | None = None,
        eligible_candidates: Sequence[Mapping[str, Any]] | None = None,
        snapshot_watermarks: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically persist a ranking and the selected items it delivered.

        Calling this method again with the same request key and the same
        canonical inputs returns the already committed chain.  Reusing a key
        for different inputs raises :class:`RequestKeyConflictError`; it never
        silently returns an unrelated exposure chain.  At least one ranking
        must be selected; its exposure and stable item IDs are created in the
        same transaction.
        """

        request_key = _required(request_key, "request_key")
        task_id = _required(task_id, "task_id")
        actor_id = _required(actor_id, "actor_id")
        selector_config_id = _required(selector_config_id, "selector_config_id")
        # Keep serialization and hashing visible as separate phases.  These
        # operations are intentionally done before BEGIN IMMEDIATE so their
        # cost cannot be confused with time spent holding the SQLite write
        # lock.  The disabled path does not take any profiling-only clocks.
        prepare_started = time.monotonic() if self._profiling_enabled else 0.0
        prepare_metrics: dict[str, Any] = {}
        prepared = self._prepare_rankings(rankings)
        prepared_candidates = self._prepare_candidates(
            eligible_candidates,
            metrics=prepare_metrics if self._profiling_enabled else None,
        )
        prepared_watermarks = self._prepare_snapshot_watermarks(snapshot_watermarks)
        prepare_seconds = (
            max(0.0, time.monotonic() - prepare_started)
            if self._profiling_enabled
            else 0.0
        )
        # JSON validation and candidate content hashing happen while the
        # bounded input is normalized.  Keep that work separate from the
        # later persistence serializer so reports can identify expensive
        # candidate preparation without charging it to SQLite lock hold.
        prepare_metrics["prepare_candidate_rows"] = len(prepared_candidates)
        prepare_metrics["prepare_ranking_rows"] = len(prepared)
        prepare_metrics["prepare_rows"] = len(prepared_candidates) + len(prepared)
        prepare_metrics["prepare_seconds"] = prepare_seconds
        prepare_metrics.setdefault("prepare_bytes", 0)
        serialize_started = time.monotonic() if self._profiling_enabled else 0.0
        serialization_seconds = (
            max(0.0, time.monotonic() - serialize_started)
            if self._profiling_enabled
            else 0.0
        )
        measured_serialization_seconds = serialization_seconds
        measured_serialization_bytes = 0
        serialization_inside_lock_seconds = 0.0
        serialization_inside_lock_bytes = 0
        serialization_call_count = 0
        serialization_inside_lock = False

        def _serialize(value: Any) -> str:
            nonlocal measured_serialization_seconds, measured_serialization_bytes
            nonlocal serialization_inside_lock_seconds, serialization_inside_lock_bytes
            nonlocal serialization_call_count
            if not self._profiling_enabled:
                return _json(value)
            started = time.monotonic()
            encoded = _json(value)
            elapsed = max(0.0, time.monotonic() - started)
            encoded_bytes = len(encoded.encode("utf-8"))
            measured_serialization_seconds += elapsed
            measured_serialization_bytes += encoded_bytes
            serialization_call_count += 1
            if serialization_inside_lock:
                serialization_inside_lock_seconds += elapsed
                serialization_inside_lock_bytes += encoded_bytes
            return encoded

        if not any(item["selected"] for item in prepared):
            raise ValueError("rankings must contain at least one selected item")
        if (eligible_candidates is None) != (snapshot_watermarks is None):
            raise ValueError(
                "eligible_candidates and snapshot_watermarks must be supplied together"
            )
        if eligible_candidates is not None:
            candidate_trace_ids = {
                item["trace_id"] for item in prepared_candidates
            }
            ranking_trace_ids = {item["trace_id"] for item in prepared}
            missing = sorted(ranking_trace_ids - candidate_trace_ids)
            if missing:
                raise ValueError(
                    "rankings contain traces absent from eligible_candidates: "
                    + ", ".join(missing)
                )

        hash_started = time.monotonic() if self._profiling_enabled else 0.0
        search_event_id = _hash("search_event", request_key)
        exposure_id = _hash("exposure", search_event_id)
        query_json = _serialize(query)
        search_sha256 = _identity_sha256(
            search_identity
            if search_identity is not None
            else {"task_id": task_id, "actor_id": actor_id, "query": query}
        )
        comparison_sha256 = _identity_sha256(comparison_identity)
        snapshot_sha256 = _identity_sha256(snapshot_identity)
        pool_sha256 = _identity_sha256(
            pool_identity
            if pool_identity is not None
            else [{"trace_id": item["trace_id"], "rank": item["rank"]} for item in prepared]
        )
        candidates_sha256 = (
            _identity_sha256([item["payload"] for item in prepared_candidates])
            if eligible_candidates is not None
            else ""
        )
        watermarks_sha256 = (
            _identity_sha256(prepared_watermarks)
            if snapshot_watermarks is not None
            else ""
        )
        watermarks_json = _serialize(prepared_watermarks)
        hash_seconds = (
            max(0.0, time.monotonic() - hash_started)
            if self._profiling_enabled
            else 0.0
        )
        request_key_sha256 = (
            hashlib.sha256(request_key.encode("utf-8", "replace")).hexdigest()
            if self._profiling_enabled
            else None
        )
        if self._profiling_enabled:
            self._profile_event(
                "selection.persist.payload",
                operation="record_search",
                input_rows=len(prepared_candidates) + len(prepared),
                input_bytes=measured_serialization_bytes,
                payload_bytes=measured_serialization_bytes,
                serialization_seconds=measured_serialization_seconds,
                hash_seconds=hash_seconds,
                request_key_sha256=request_key_sha256,
                snapshot_sha256=snapshot_sha256,
                pool_sha256=pool_sha256,
                serialization_inside_lock_seconds=serialization_inside_lock_seconds,
                serialization_inside_lock_bytes=serialization_inside_lock_bytes,
                serialization_call_count=serialization_call_count,
                prepare_seconds=prepare_metrics.get("prepare_seconds"),
                prepare_rows=prepare_metrics.get("prepare_rows"),
                prepare_candidate_rows=prepare_metrics.get("prepare_candidate_rows"),
                prepare_ranking_rows=prepare_metrics.get("prepare_ranking_rows"),
                prepare_bytes=prepare_metrics.get("prepare_bytes"),
                prepare_serialization_seconds=prepare_metrics.get(
                    "prepare_serialization_seconds"
                ),
                prepare_hash_seconds=prepare_metrics.get("prepare_hash_seconds"),
                phase="pre_lock",
            )
        created_at = _now()

        write_metrics = {
            "input_bytes": measured_serialization_bytes,
            "payload_bytes": measured_serialization_bytes,
            "request_key_sha256": request_key_sha256,
            "snapshot_sha256": snapshot_sha256,
            "pool_sha256": pool_sha256,
            "serialization_seconds": measured_serialization_seconds,
            "serialization_inside_lock_seconds": serialization_inside_lock_seconds,
            "serialization_inside_lock_bytes": serialization_inside_lock_bytes,
            "serialization_call_count": serialization_call_count,
            "hash_seconds": hash_seconds,
            **prepare_metrics,
        }
        try:
            with self._write(
                operation="record_search",
                metrics=write_metrics,
            ) as db:
                # Candidate/ranking JSON is part of the historical transaction
                # body.  Mark this interval so the final payload event and the
                # persist.end row can show how much lock hold is attributable
                # to serialization rather than silently moving work outside
                # BEGIN IMMEDIATE.
                serialization_inside_lock = self._profiling_enabled
                prior = db.execute(
                    "SELECT * FROM search_events WHERE request_key = ?", (request_key,)
                ).fetchone()
                if prior is not None:
                    self._assert_search_retry_matches(
                        db,
                        prior=prior,
                        task_id=task_id,
                        actor_id=actor_id,
                        selector_config_id=selector_config_id,
                        search_sha256=search_sha256,
                        comparison_sha256=comparison_sha256,
                        snapshot_sha256=snapshot_sha256,
                        pool_sha256=pool_sha256,
                        query_json=query_json,
                        rankings=prepared,
                        candidates_sha256=candidates_sha256,
                        watermarks_sha256=watermarks_sha256,
                        watermarks_json=watermarks_json,
                        candidates=prepared_candidates,
                    )
                    result = self._profiled_search_chain(
                        db,
                        str(prior["search_event_id"]),
                        operation="idempotent_retry",
                    )
                    result["idempotent"] = True
                    write_metrics["payload_bytes"] = measured_serialization_bytes
                    write_metrics["input_bytes"] = measured_serialization_bytes
                    write_metrics["serialization_seconds"] = measured_serialization_seconds
                    write_metrics["serialization_inside_lock_seconds"] = serialization_inside_lock_seconds
                    write_metrics["serialization_inside_lock_bytes"] = serialization_inside_lock_bytes
                    write_metrics["serialization_call_count"] = serialization_call_count
                    write_metrics["prepare_seconds"] = prepare_metrics.get("prepare_seconds")
                    write_metrics["prepare_rows"] = prepare_metrics.get("prepare_rows")
                    write_metrics["prepare_candidate_rows"] = prepare_metrics.get("prepare_candidate_rows")
                    write_metrics["prepare_ranking_rows"] = prepare_metrics.get("prepare_ranking_rows")
                    write_metrics["prepare_bytes"] = prepare_metrics.get("prepare_bytes")
                    return result

                config = db.execute(
                    "SELECT config_sha256 FROM selector_configs WHERE selector_config_id = ?",
                    (selector_config_id,),
                ).fetchone()
                if config is None:
                    raise ValueError(f"unknown selector_config_id: {selector_config_id}")

                db.execute(
                    """INSERT INTO search_events(
                           search_event_id, request_key, task_id, actor_id, selector_config_id,
                           search_sha256, config_sha256, comparison_sha256, snapshot_sha256,
                           pool_sha256, query_json, eligible_candidates_sha256,
                           snapshot_watermarks_sha256, snapshot_watermarks_json, created_at
                       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        search_event_id,
                        request_key,
                        task_id,
                        actor_id,
                        selector_config_id,
                        search_sha256,
                        config["config_sha256"],
                        comparison_sha256,
                        snapshot_sha256,
                        pool_sha256,
                        query_json,
                        candidates_sha256,
                        watermarks_sha256,
                        watermarks_json,
                        created_at,
                    ),
                )
                for candidate in prepared_candidates:
                    candidate_id = _hash(
                        "search_candidate", search_event_id, candidate["trace_id"]
                    )
                    db.execute(
                        """INSERT INTO search_candidates(
                               search_candidate_id, search_event_id, trace_id, pool_order,
                               candidate_sha256, candidate_payload_json,
                               feedback_snapshot_json, snapshot_watermarks_json
                           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            candidate_id,
                            search_event_id,
                            candidate["trace_id"],
                            candidate["pool_order"],
                            candidate["candidate_sha256"],
                            _serialize(candidate["payload"]),
                            _serialize(candidate["feedback_snapshot"]),
                            watermarks_json,
                        ),
                    )
                db.execute(
                    "INSERT INTO exposures(exposure_id, search_event_id, actor_id, created_at) VALUES(?, ?, ?, ?)",
                    (exposure_id, search_event_id, actor_id, created_at),
                )
                for ranking in prepared:
                    ranking_id = _hash(
                        "search_ranking", search_event_id, ranking["trace_id"], ranking["rank"]
                    )
                    db.execute(
                        """INSERT INTO search_rankings(
                               search_ranking_id, search_event_id, trace_id, rank, selected,
                               component_scores_json, ranking_payload_json
                           ) VALUES(?, ?, ?, ?, ?, ?, ?)""",
                        (
                            ranking_id,
                            search_event_id,
                            ranking["trace_id"],
                            ranking["rank"],
                            int(ranking["selected"]),
                            _serialize(ranking["component_scores"]),
                            _serialize(ranking["payload"]),
                        ),
                    )
                    if ranking["selected"]:
                        exposure_item_id = _hash(
                            "exposure_item", exposure_id, ranking["trace_id"], ranking["rank"]
                        )
                        db.execute(
                            """INSERT INTO exposure_items(
                                   exposure_item_id, exposure_id, search_ranking_id,
                                   trace_id, rank, created_at
                               ) VALUES(?, ?, ?, ?, ?, ?)""",
                            (
                                exposure_item_id,
                                exposure_id,
                                ranking_id,
                                ranking["trace_id"],
                                ranking["rank"],
                                created_at,
                            ),
                        )
                result = self._profiled_search_chain(
                    db,
                    search_event_id,
                    operation="new_search",
                )
                result["idempotent"] = False
                write_metrics["payload_bytes"] = measured_serialization_bytes
                write_metrics["input_bytes"] = measured_serialization_bytes
                write_metrics["serialization_seconds"] = measured_serialization_seconds
                write_metrics["serialization_inside_lock_seconds"] = serialization_inside_lock_seconds
                write_metrics["serialization_inside_lock_bytes"] = serialization_inside_lock_bytes
                write_metrics["serialization_call_count"] = serialization_call_count
                write_metrics["prepare_seconds"] = prepare_metrics.get("prepare_seconds")
                write_metrics["prepare_rows"] = prepare_metrics.get("prepare_rows")
                write_metrics["prepare_candidate_rows"] = prepare_metrics.get("prepare_candidate_rows")
                write_metrics["prepare_ranking_rows"] = prepare_metrics.get("prepare_ranking_rows")
                write_metrics["prepare_bytes"] = prepare_metrics.get("prepare_bytes")
                return result
        finally:
            serialization_inside_lock = False
            if self._profiling_enabled:
                self._profile_event(
                    "selection.persist.payload",
                    operation="record_search",
                    phase="total",
                    input_rows=len(prepared_candidates) + len(prepared),
                    input_bytes=measured_serialization_bytes,
                    payload_bytes=measured_serialization_bytes,
                    serialization_seconds=measured_serialization_seconds,
                    serialization_inside_lock_seconds=serialization_inside_lock_seconds,
                    serialization_inside_lock_bytes=serialization_inside_lock_bytes,
                    serialization_call_count=serialization_call_count,
                    hash_seconds=hash_seconds,
                    prepare_seconds=prepare_metrics.get("prepare_seconds"),
                    prepare_rows=prepare_metrics.get("prepare_rows"),
                    prepare_candidate_rows=prepare_metrics.get("prepare_candidate_rows"),
                    prepare_ranking_rows=prepare_metrics.get("prepare_ranking_rows"),
                    prepare_bytes=prepare_metrics.get("prepare_bytes"),
                    prepare_serialization_seconds=prepare_metrics.get(
                        "prepare_serialization_seconds"
                    ),
                    prepare_hash_seconds=prepare_metrics.get("prepare_hash_seconds"),
                    request_key_sha256=request_key_sha256,
                    snapshot_sha256=snapshot_sha256,
                    pool_sha256=pool_sha256,
                )

    def record_search(
        self,
        *,
        request_key: str,
        task_id: str,
        actor_id: str,
        selector_config_id: str,
        query: Any,
        comparison_identity: Any,
        snapshot_identity: Any,
        pool_identity: Any | None,
        rankings: Sequence[Mapping[str, Any]],
        search_identity: Any | None = None,
        eligible_candidates: Sequence[Mapping[str, Any]] | None = None,
        snapshot_watermarks: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Profile a search persistence call without changing its contract."""

        if not self._profiling_enabled:
            return self._record_search_impl(
                request_key=request_key,
                task_id=task_id,
                actor_id=actor_id,
                selector_config_id=selector_config_id,
                query=query,
                comparison_identity=comparison_identity,
                snapshot_identity=snapshot_identity,
                pool_identity=pool_identity,
                rankings=rankings,
                search_identity=search_identity,
                eligible_candidates=eligible_candidates,
                snapshot_watermarks=snapshot_watermarks,
            )
        candidate_count = len(eligible_candidates) if eligible_candidates is not None else 0
        ranked_count = len(rankings)
        with self._profile_span(
            # Keep the high-level Python call distinct from the transactional
            # ``selection.persist.start/end`` pair emitted by ``_write``.
            # Using the same event name at both layers made one search look
            # like two SQLite writes in aggregate reports.
            "selection.persist.call",
            task_id=task_id,
            actor_id=actor_id,
            operation="record_search",
            candidate_count=candidate_count,
            ranked_count=ranked_count,
        ):
            return self._record_search_impl(
                request_key=request_key,
                task_id=task_id,
                actor_id=actor_id,
                selector_config_id=selector_config_id,
                query=query,
                comparison_identity=comparison_identity,
                snapshot_identity=snapshot_identity,
                pool_identity=pool_identity,
                rankings=rankings,
                search_identity=search_identity,
                eligible_candidates=eligible_candidates,
                snapshot_watermarks=snapshot_watermarks,
            )

    @staticmethod
    def _assert_search_retry_matches(
        db: sqlite3.Connection,
        *,
        prior: sqlite3.Row,
        task_id: str,
        actor_id: str,
        selector_config_id: str,
        search_sha256: str,
        comparison_sha256: str,
        snapshot_sha256: str,
        pool_sha256: str,
        query_json: str,
        rankings: Sequence[Mapping[str, Any]],
        candidates_sha256: str,
        watermarks_sha256: str,
        watermarks_json: str,
        candidates: Sequence[Mapping[str, Any]],
    ) -> None:
        """Fail closed unless a request-key retry is canonically identical."""

        expected_fields = {
            "task_id": task_id,
            "actor_id": actor_id,
            "selector_config_id": selector_config_id,
            "search_sha256": search_sha256,
            "comparison_sha256": comparison_sha256,
            "snapshot_sha256": snapshot_sha256,
            "pool_sha256": pool_sha256,
            "query_json": query_json,
            "eligible_candidates_sha256": candidates_sha256,
            "snapshot_watermarks_sha256": watermarks_sha256,
            "snapshot_watermarks_json": watermarks_json,
        }
        public_names = {
            "selector_config_id": "config_identity",
            "search_sha256": "search_identity",
            "comparison_sha256": "comparison_identity",
            "snapshot_sha256": "snapshot_identity",
            "pool_sha256": "pool_identity",
            "query_json": "query",
            "eligible_candidates_sha256": "eligible_candidates",
            "snapshot_watermarks_sha256": "snapshot_watermarks",
            "snapshot_watermarks_json": "snapshot_watermarks",
        }
        mismatches = [
            public_names.get(column, column)
            for column, expected in expected_fields.items()
            if str(prior[column]) != expected
        ]

        stored_rows = db.execute(
            """SELECT trace_id, rank, selected, component_scores_json,
                      ranking_payload_json
               FROM search_rankings
               WHERE search_event_id = ?
               ORDER BY rank, trace_id""",
            (prior["search_event_id"],),
        ).fetchall()
        try:
            stored_rankings = [
                {
                    "trace_id": str(row["trace_id"]),
                    "rank": int(row["rank"]),
                    "selected": bool(row["selected"]),
                    "component_scores": json.loads(row["component_scores_json"]),
                    "payload": json.loads(row["ranking_payload_json"]),
                }
                for row in stored_rows
            ]
            rankings_match = _json(stored_rankings) == _json(list(rankings))
        except (TypeError, ValueError, json.JSONDecodeError):
            rankings_match = False
        if not rankings_match:
            mismatches.append("rankings_identity")

        stored_candidates = db.execute(
            """SELECT trace_id, pool_order, candidate_sha256,
                      candidate_payload_json, feedback_snapshot_json
                 FROM search_candidates
                WHERE search_event_id = ?
                ORDER BY pool_order, trace_id""",
            (prior["search_event_id"],),
        ).fetchall()
        try:
            candidate_rows_match = _json(
                [
                    {
                        "trace_id": str(row["trace_id"]),
                        "pool_order": int(row["pool_order"]),
                        "candidate_sha256": str(row["candidate_sha256"]),
                        "payload": json.loads(row["candidate_payload_json"]),
                        "feedback_snapshot": json.loads(row["feedback_snapshot_json"]),
                    }
                    for row in stored_candidates
                ]
            ) == _json(list(candidates))
        except (TypeError, ValueError, json.JSONDecodeError):
            candidate_rows_match = False
        if not candidate_rows_match:
            mismatches.append("eligible_candidates")

        if mismatches:
            raise RequestKeyConflictError(mismatches)

    @staticmethod
    def _prepare_rankings(rankings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        traces: set[str] = set()
        ranks: set[int] = set()
        for raw in rankings:
            trace_id = _required(raw.get("trace_id"), "ranking.trace_id")
            try:
                rank = int(raw.get("rank"))
            except (TypeError, ValueError) as exc:
                raise ValueError("ranking.rank must be a positive integer") from exc
            if rank < 1 or isinstance(raw.get("rank"), bool):
                raise ValueError("ranking.rank must be a positive integer")
            if trace_id in traces or rank in ranks:
                raise ValueError("ranking trace_id and rank must each be unique")
            traces.add(trace_id)
            ranks.add(rank)
            component_scores = raw.get("component_scores", {})
            payload = raw.get("payload", {})
            if not isinstance(component_scores, Mapping) or not isinstance(payload, Mapping):
                raise ValueError("ranking component_scores and payload must be mappings")
            prepared.append(
                {
                    "trace_id": trace_id,
                    "rank": rank,
                    "selected": raw.get("selected") is True,
                    "component_scores": dict(component_scores),
                    "payload": dict(payload),
                }
            )
        if not prepared:
            raise ValueError("rankings must be non-empty")
        return sorted(prepared, key=lambda item: (item["rank"], item["trace_id"]))

    @staticmethod
    def _prepare_snapshot_watermarks(
        snapshot_watermarks: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if snapshot_watermarks is None:
            return {}
        if not isinstance(snapshot_watermarks, Mapping):
            raise ValueError("snapshot_watermarks must be a mapping")
        prepared = dict(snapshot_watermarks)
        # Validate canonical JSON now, before opening a write transaction.
        _json(prepared)
        return prepared

    @staticmethod
    def _prepare_candidates(
        eligible_candidates: Sequence[Mapping[str, Any]] | None,
        *,
        metrics: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if eligible_candidates is None:
            return []
        if isinstance(eligible_candidates, (str, bytes)) or not isinstance(
            eligible_candidates, Sequence
        ):
            raise ValueError("eligible_candidates must be a sequence of mappings")
        payloads: list[dict[str, Any]] = []
        traces: set[str] = set()
        prepare_serialization_seconds = 0.0
        prepare_hash_seconds = 0.0
        prepare_bytes = 0
        for raw in eligible_candidates:
            if is_dataclass(raw):
                raw = asdict(raw)
            if not isinstance(raw, Mapping):
                raise ValueError("eligible candidate must be a mapping")
            payload = dict(raw)
            trace_id = _required(
                payload.get("trace_id", payload.get("id")),
                "eligible_candidate.trace_id",
            )
            if trace_id in traces:
                raise ValueError("eligible candidate trace_id values must be unique")
            traces.add(trace_id)
            payload["trace_id"] = trace_id
            payload.pop("id", None)
            feedback = payload.get("feedback", {})
            if is_dataclass(feedback):
                feedback = asdict(feedback)
            if feedback is None:
                feedback = {}
            if not isinstance(feedback, Mapping):
                raise ValueError("eligible candidate feedback must be a mapping")
            payload["feedback"] = dict(feedback)
            validation_started = time.monotonic() if metrics is not None else 0.0
            encoded_payload = _json(payload)
            if metrics is not None:
                prepare_serialization_seconds += max(
                    0.0, time.monotonic() - validation_started
                )
                prepare_bytes += len(encoded_payload.encode("utf-8"))
            payloads.append(payload)
        payloads.sort(key=lambda item: item["trace_id"])
        prepared: list[dict[str, Any]] = []
        for index, payload in enumerate(payloads, 1):
            hash_started = time.monotonic() if metrics is not None else 0.0
            candidate_sha = _identity_sha256(payload)
            if metrics is not None:
                prepare_hash_seconds += max(0.0, time.monotonic() - hash_started)
            prepared.append(
                {
                    "trace_id": payload["trace_id"],
                    "pool_order": index,
                    "candidate_sha256": candidate_sha,
                    "payload": payload,
                    "feedback_snapshot": dict(payload.get("feedback", {})),
                }
            )
        if metrics is not None:
            metrics["prepare_serialization_seconds"] = prepare_serialization_seconds
            metrics["prepare_hash_seconds"] = prepare_hash_seconds
            metrics["prepare_bytes"] = prepare_bytes
            metrics["prepare_rows"] = len(prepared)
        return prepared

    def record_feedback(
        self,
        *,
        request_key: str,
        exposure_item_id: str,
        actor_id: str,
        trace_id: str,
        feedback_kind: str,
        origin: str,
        terminal: bool = True,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Record worker interaction feedback with atomic terminal arbitration.

        The first committed terminal event for an exposure item is effective.
        Later terminal events are durably recorded as ineffective and return
        ``ALREADY_FINAL`` with the winning event ID.  A retry with the same
        canonical request fields returns its original row; reusing a key for a
        different interaction raises :class:`RequestKeyConflictError`.
        """

        request_key = _required(request_key, "request_key")
        exposure_item_id = _required(exposure_item_id, "exposure_item_id")
        actor_id = _required(actor_id, "actor_id")
        trace_id = _required(trace_id, "trace_id")
        feedback_kind = _required(feedback_kind, "feedback_kind", limit=64)
        origin = _required(origin, "origin", limit=128)
        if feedback_kind not in CANONICAL_FEEDBACK_KINDS:
            raise ValueError(f"unsupported feedback_kind: {feedback_kind}")
        if not isinstance(terminal, bool):
            raise ValueError("terminal must be a bool")
        payload_json = _json(dict(payload or {}))
        feedback_event_id = _hash("feedback_event", request_key)

        with self._write(operation="record_feedback") as db:
            prior = db.execute(
                "SELECT * FROM feedback_events WHERE request_key = ?", (request_key,)
            ).fetchone()
            if prior is not None:
                self._assert_feedback_retry_matches(
                    prior,
                    exposure_item_id=exposure_item_id,
                    actor_id=actor_id,
                    trace_id=trace_id,
                    feedback_kind=feedback_kind,
                    origin=origin,
                    terminal=terminal,
                    payload_json=payload_json,
                )
                return self._feedback_result(prior, idempotent=True)

            item = db.execute(
                """SELECT item.trace_id, exposure.actor_id
                   FROM exposure_items AS item
                   JOIN exposures AS exposure ON exposure.exposure_id = item.exposure_id
                   WHERE item.exposure_item_id = ?""",
                (exposure_item_id,),
            ).fetchone()
            if item is None:
                raise ValueError(f"unknown exposure_item_id: {exposure_item_id}")
            if str(item["actor_id"]) != actor_id:
                raise ValueError("feedback actor_id does not match the exposure/search actor")
            if str(item["trace_id"]) != trace_id:
                raise ValueError("feedback trace_id does not match the exposure item")

            winner = None
            if terminal:
                winner = db.execute(
                    """SELECT feedback_event_id FROM feedback_events
                       WHERE exposure_item_id = ? AND terminal = 1 AND effective = 1""",
                    (exposure_item_id,),
                ).fetchone()
            effective = bool(terminal and winner is None)
            conflicts_with = None if winner is None else str(winner["feedback_event_id"])
            db.execute(
                """INSERT INTO feedback_events(
                       feedback_event_id, request_key, exposure_item_id, trace_id, actor_id,
                       event_class, feedback_kind, origin, terminal, effective,
                       conflicts_with_feedback_event_id, payload_json, created_at
                   ) VALUES(?, ?, ?, ?, ?, 'worker_interaction', ?, ?, ?, ?, ?, ?, ?)""",
                (
                    feedback_event_id,
                    request_key,
                    exposure_item_id,
                    trace_id,
                    actor_id,
                    feedback_kind,
                    origin,
                    int(terminal),
                    int(effective),
                    conflicts_with,
                    payload_json,
                    _now(),
                ),
            )
            row = db.execute(
                "SELECT * FROM feedback_events WHERE feedback_event_id = ?", (feedback_event_id,)
            ).fetchone()
            assert row is not None
            return self._feedback_result(row, idempotent=False)

    @staticmethod
    def _assert_feedback_retry_matches(
        prior: sqlite3.Row,
        *,
        exposure_item_id: str,
        actor_id: str,
        trace_id: str,
        feedback_kind: str,
        origin: str,
        terminal: bool,
        payload_json: str,
    ) -> None:
        """Reject request-key reuse for a different worker interaction.

        ``request_key`` is the caller's idempotency token, not an event lookup
        token that can be reused for another semantic event.  Compare all
        fields that affect attribution or selector feedback while still
        treating mapping key order and ``None``/empty payload normalization as
        canonicalized by ``_json(dict(payload or {}))``.
        """

        expected = {
            "exposure_item_id": exposure_item_id,
            "actor_id": actor_id,
            "trace_id": trace_id,
            "feedback_kind": feedback_kind,
            "origin": origin,
            "terminal": int(terminal),
            "payload_json": payload_json,
        }
        public_names = {"payload_json": "payload"}
        mismatches = [
            public_names.get(column, column)
            for column, value in expected.items()
            if prior[column] != value
        ]
        if mismatches:
            raise RequestKeyConflictError(mismatches)

    @staticmethod
    def _feedback_result(row: sqlite3.Row, *, idempotent: bool) -> dict[str, Any]:
        result = _decode_row(row) or {}
        result["status"] = "ALREADY_FINAL" if result["conflicts_with_feedback_event_id"] else "RECORDED"
        result["idempotent"] = idempotent
        return result

    def record_verifier_evidence(
        self,
        *,
        request_key: str,
        trace_id: str,
        verifier_id: str,
        status: str,
        evidence: Mapping[str, Any] | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        values = {
            "request_key": _required(request_key, "request_key"),
            "trace_id": _required(trace_id, "trace_id"),
            "verifier_id": _required(verifier_id, "verifier_id"),
            "status": _required(status, "status", limit=128),
            "task_id": str(task_id).strip() if task_id is not None else None,
            "evidence_json": _json(dict(evidence or {})),
        }
        event_id = _hash("evidence_event", values["request_key"])
        return self._insert_idempotent_event(
            table="verifier_evidence",
            id_column="evidence_event_id",
            event_id=event_id,
            values=values,
        )

    def record_maintenance_event(
        self,
        *,
        request_key: str,
        trace_id: str,
        actor_id: str,
        maintenance_kind: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        values = {
            "request_key": _required(request_key, "request_key"),
            "trace_id": _required(trace_id, "trace_id"),
            "actor_id": _required(actor_id, "actor_id"),
            "maintenance_kind": _required(maintenance_kind, "maintenance_kind", limit=128),
            "payload_json": _json(dict(payload or {})),
        }
        event_id = _hash("maintenance_event", values["request_key"])
        return self._insert_idempotent_event(
            table="maintenance_events",
            id_column="maintenance_event_id",
            event_id=event_id,
            values=values,
        )

    def record_relation(
        self,
        *,
        request_key: str,
        source_trace_id: str,
        target_trace_id: str,
        relation_kind: str,
        actor_id: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        values = {
            "request_key": _required(request_key, "request_key"),
            "source_trace_id": _required(source_trace_id, "source_trace_id"),
            "target_trace_id": _required(target_trace_id, "target_trace_id"),
            "relation_kind": _required(relation_kind, "relation_kind", limit=64),
            "actor_id": _required(actor_id, "actor_id"),
            "payload_json": _json(dict(payload or {})),
        }
        if values["source_trace_id"] == values["target_trace_id"]:
            raise ValueError("a trace relation cannot target itself")
        if values["relation_kind"] not in CANONICAL_RELATIONS:
            raise ValueError(f"unsupported relation_kind: {values['relation_kind']}")
        event_id = _hash("relation", values["request_key"])
        return self._insert_idempotent_event(
            table="trace_relations", id_column="relation_id", event_id=event_id, values=values
        )

    def _insert_idempotent_event(
        self,
        *,
        table: str,
        id_column: str,
        event_id: str,
        values: Mapping[str, Any],
    ) -> dict[str, Any]:
        # Table and column are private constants supplied only by methods above.
        with self._write(operation=f"insert_{table}") as db:
            prior = db.execute(
                f"SELECT * FROM {table} WHERE request_key = ?", (values["request_key"],)
            ).fetchone()
            if prior is not None:
                self._assert_event_retry_matches(prior, values)
                result = _decode_row(prior) or {}
                result["idempotent"] = True
                return result
            columns = [id_column, *values.keys(), "created_at"]
            parameters = [event_id, *values.values(), _now()]
            placeholders = ", ".join("?" for _ in columns)
            db.execute(
                f"INSERT INTO {table}({', '.join(columns)}) VALUES({placeholders})", parameters
            )
            row = db.execute(f"SELECT * FROM {table} WHERE {id_column} = ?", (event_id,)).fetchone()
            assert row is not None
            result = _decode_row(row) or {}
            result["idempotent"] = False
            return result

    @staticmethod
    def _assert_event_retry_matches(
        prior: sqlite3.Row, values: Mapping[str, Any]
    ) -> None:
        """Fail closed for verifier/maintenance/relation key reuse too."""

        public_names = {
            "evidence_json": "evidence",
            "payload_json": "payload",
        }
        mismatches = [
            public_names.get(column, column)
            for column, expected in values.items()
            if column != "request_key" and prior[column] != expected
        ]
        if mismatches:
            raise RequestKeyConflictError(mismatches)

    def get_search(self, search_event_id: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute(
                "SELECT 1 FROM search_events WHERE search_event_id = ?", (search_event_id,)
            ).fetchone()
            return self._search_chain(db, search_event_id) if row is not None else None

    def get_search_by_request_key(self, request_key: str) -> dict[str, Any] | None:
        with self._db() as db:
            row = db.execute(
                "SELECT search_event_id FROM search_events WHERE request_key = ?", (request_key,)
            ).fetchone()
            return self._search_chain(db, str(row["search_event_id"])) if row is not None else None

    @staticmethod
    def _search_chain(
        db: sqlite3.Connection,
        search_event_id: str,
        *,
        observer: Callable[[str, int, float, float], None] | None = None,
    ) -> dict[str, Any]:
        """Read one persisted chain, optionally reporting query/fetch phases."""

        def _query(
            operation: str,
            statement: str,
            parameters: tuple[Any, ...],
            *,
            many: bool = False,
        ) -> Any:
            query_started = time.monotonic() if observer is not None else 0.0
            cursor = db.execute(statement, parameters)
            query_seconds = (
                max(0.0, time.monotonic() - query_started)
                if observer is not None
                else 0.0
            )
            fetch_started = time.monotonic() if observer is not None else 0.0
            rows = cursor.fetchall() if many else cursor.fetchone()
            fetch_seconds = (
                max(0.0, time.monotonic() - fetch_started)
                if observer is not None
                else 0.0
            )
            if observer is not None:
                observer(
                    operation,
                    len(rows) if many else int(rows is not None),
                    query_seconds,
                    fetch_seconds,
                )
            return rows

        search = _decode_row(
            _query(
                "search_event",
                "SELECT * FROM search_events WHERE search_event_id = ?",
                (search_event_id,),
            )
        )
        if search is None:
            raise ValueError(f"unknown search_event_id: {search_event_id}")
        exposure = _decode_row(
            _query(
                "exposure",
                "SELECT * FROM exposures WHERE search_event_id = ?",
                (search_event_id,),
            )
        )
        rankings = [
            _decode_row(row) or {}
            for row in _query(
                "rankings",
                "SELECT * FROM search_rankings WHERE search_event_id = ? ORDER BY rank",
                (search_event_id,),
                many=True,
            )
        ]
        candidates = [
            _decode_row(row) or {}
            for row in _query(
                "candidates",
                """SELECT * FROM search_candidates
                   WHERE search_event_id = ? ORDER BY pool_order, trace_id""",
                (search_event_id,),
                many=True,
            )
        ]
        items: list[dict[str, Any]] = []
        if exposure is not None:
            items = [
                _decode_row(row) or {}
                for row in _query(
                    "exposure_items",
                    "SELECT * FROM exposure_items WHERE exposure_id = ? ORDER BY rank",
                    (exposure["exposure_id"],),
                    many=True,
                )
            ]
        return {
            "search_event": search,
            "candidates": candidates,
            "rankings": rankings,
            "exposure": exposure,
            "items": items,
        }

    def _profiled_search_chain(
        self,
        db: sqlite3.Connection,
        search_event_id: str,
        *,
        operation: str,
        event_sink: Callable[..., None] | None = None,
    ) -> dict[str, Any]:
        """Read the persisted chain and expose bounded readback cost.

        ``record_search`` performs this readback while its write transaction is
        still open.  Without a separate row, its SELECT/JSON materialization
        time is indistinguishable from the actual INSERT/commit lock hold.
        Keep the canonical static reader unchanged for compatibility and add a
        profiling-only envelope around it.
        """

        if not self._profiling_enabled:
            return self._search_chain(db, search_event_id)
        started = time.monotonic()
        # ``record_search`` invokes this helper before its BEGIN IMMEDIATE
        # transaction is released.  Keep the query/aggregate observations in
        # the caller-provided bounded queue so the actual profiling sink is
        # never doing JSONL I/O while SQLite owns the writer lock.  Direct
        # callers retain the historical immediate sink behavior.
        emit = self._defer_profile_event if event_sink is None else event_sink
        query_seconds = 0.0
        fetch_seconds = 0.0
        rows_scanned = 0
        query_count = 0

        def observe(
            db_operation: str,
            rows: int,
            query_elapsed: float,
            fetch_elapsed: float,
        ) -> None:
            nonlocal query_seconds, fetch_seconds, rows_scanned, query_count
            query_count += 1
            rows_scanned += max(0, int(rows))
            query_seconds += max(0.0, float(query_elapsed))
            fetch_seconds += max(0.0, float(fetch_elapsed))
            emit(
                "selection.persist.readback.query",
                operation=operation,
                db_operation=db_operation,
                rows=rows,
                query_seconds=query_elapsed,
                fetch_seconds=fetch_elapsed,
            )

        result = self._search_chain(db, search_event_id, observer=observe)
        # ``_search_chain`` issues one query for the parent, exposure,
        # rankings and candidates, plus a conditional item query only when an
        # exposure row exists.  Report the actual query cardinality instead of
        # assuming every historical row is perfectly formed.
        rows = (
            int(result.get("search_event") is not None)
            + int(result.get("exposure") is not None)
            + len(result.get("rankings", ()))
            + len(result.get("candidates", ()))
            + len(result.get("items", ()))
        )
        materialized_bytes = len(_json(result).encode("utf-8"))
        emit(
            "selection.persist.readback",
            operation=operation,
            db_operation="search_chain",
            query_count=query_count,
            rows_scanned=rows_scanned,
            query_seconds=query_seconds,
            fetch_seconds=fetch_seconds,
            output_rows=rows,
            materialized_rows=rows,
            materialized_bytes=materialized_bytes,
            materialize_seconds=max(
                0.0,
                time.monotonic() - started - query_seconds - fetch_seconds,
            ),
        )
        return result

    def attribution_chain(self, exposure_item_id: str) -> dict[str, Any] | None:
        """Return one item with its search, ranking, and separately typed events."""

        with self._db() as db:
            item = _decode_row(
                db.execute(
                    "SELECT * FROM exposure_items WHERE exposure_item_id = ?", (exposure_item_id,)
                ).fetchone()
            )
            if item is None:
                return None
            exposure = _decode_row(
                db.execute("SELECT * FROM exposures WHERE exposure_id = ?", (item["exposure_id"],)).fetchone()
            )
            assert exposure is not None
            search = _decode_row(
                db.execute(
                    "SELECT * FROM search_events WHERE search_event_id = ?",
                    (exposure["search_event_id"],),
                ).fetchone()
            )
            ranking = _decode_row(
                db.execute(
                    "SELECT * FROM search_rankings WHERE search_ranking_id = ?",
                    (item["search_ranking_id"],),
                ).fetchone()
            )
            feedback = [
                _decode_row(row) or {}
                for row in db.execute(
                    "SELECT * FROM feedback_events WHERE exposure_item_id = ? ORDER BY created_at, feedback_event_id",
                    (exposure_item_id,),
                )
            ]
            trace_id = item["trace_id"]
            evidence = [
                _decode_row(row) or {}
                for row in db.execute(
                    "SELECT * FROM verifier_evidence WHERE trace_id = ? ORDER BY created_at, evidence_event_id",
                    (trace_id,),
                )
            ]
            maintenance = [
                _decode_row(row) or {}
                for row in db.execute(
                    "SELECT * FROM maintenance_events WHERE trace_id = ? ORDER BY created_at, maintenance_event_id",
                    (trace_id,),
                )
            ]
            relations = [
                _decode_row(row) or {}
                for row in db.execute(
                    """SELECT * FROM trace_relations
                       WHERE source_trace_id = ? OR target_trace_id = ?
                       ORDER BY created_at, relation_id""",
                    (trace_id, trace_id),
                )
            ]
        return {
            "search_event": search,
            "exposure": exposure,
            "exposure_item": item,
            "ranking": ranking,
            "feedback_events": feedback,
            "verifier_evidence": evidence,
            "maintenance_events": maintenance,
            "relations": relations,
        }

    def effective_feedback(self, *, trace_id: str | None = None) -> list[dict[str, Any]]:
        where = " AND trace_id = ?" if trace_id is not None else ""
        parameters: Iterable[Any] = (trace_id,) if trace_id is not None else ()
        with self._db() as db:
            return [
                _decode_row(row) or {}
                for row in db.execute(
                    """SELECT * FROM feedback_events
                       WHERE event_class = 'worker_interaction'
                         AND terminal = 1 AND effective = 1"""
                    + where
                    + " ORDER BY created_at, feedback_event_id",
                    parameters,
                )
            ]

    @staticmethod
    def _summary_from_db(db: sqlite3.Connection, *, db_name: str) -> dict[str, Any]:
        """Build a bounded audit summary from the caller's SQLite snapshot."""

        counts = {
            table: int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for _record_type, table, _id_column in _EXPORT_TABLES
        }
        for _record_type, table, _id_column in _OPTIONAL_EXPORT_TABLES:
            count = int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            if count:
                counts[table] = count
        feedback_counts = db.execute(
            """SELECT
                   COALESCE(SUM(CASE WHEN terminal = 1 THEN 1 ELSE 0 END), 0)
                       AS terminal_count,
                   COALESCE(SUM(CASE WHEN terminal = 0 THEN 1 ELSE 0 END), 0)
                       AS nonterminal_count,
                   COALESCE(SUM(CASE WHEN effective = 1 THEN 1 ELSE 0 END), 0)
                       AS effective_count,
                   COALESCE(SUM(CASE
                       WHEN terminal = 1 AND effective = 0 THEN 1 ELSE 0 END), 0)
                       AS conflicting_terminal_count
               FROM feedback_events"""
        ).fetchone()
        assert feedback_counts is not None
        counts.update(
            {
                "terminal_feedback_events": int(feedback_counts["terminal_count"]),
                "nonterminal_feedback_events": int(feedback_counts["nonterminal_count"]),
                "effective_feedback_events": int(feedback_counts["effective_count"]),
                "conflicting_terminal_feedback_events": int(
                    feedback_counts["conflicting_terminal_count"]
                ),
            }
        )
        selector_configs = [
            {
                "selector_config_id": str(row["selector_config_id"]),
                "selector_name": str(row["selector_name"]),
                "config_sha256": str(row["config_sha256"]),
            }
            for row in db.execute(
                """SELECT selector_config_id, selector_name, config_sha256
                   FROM selector_configs
                   ORDER BY selector_config_id"""
            )
        ]
        comparison_sha256s = [
            str(row["comparison_sha256"])
            for row in db.execute(
                """SELECT DISTINCT comparison_sha256
                   FROM search_events
                   ORDER BY comparison_sha256"""
            )
        ]
        selector_config_ids = [
            item["selector_config_id"] for item in selector_configs
        ]
        return {
            "schema_version": SCHEMA_VERSION,
            "db": db_name,
            "counts": counts,
            "selector_config_ids": selector_config_ids,
            "selector_configs": selector_configs,
            # Search persistence canonicalizes the comparison identity to a
            # SHA-256 value.  Expose both its storage name and the public
            # contract terminology so closeout code need not reinterpret it.
            "comparison_sha256s": comparison_sha256s,
            "comparison_contract_ids": list(comparison_sha256s),
        }

    @staticmethod
    def _export_tables_for_db(
        db: sqlite3.Connection,
    ) -> tuple[tuple[str, str, str], ...]:
        """Return referential export order, omitting unused optional tables."""

        optional = {
            table: int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for _record_type, table, _id_column in _OPTIONAL_EXPORT_TABLES
        }
        result: list[tuple[str, str, str]] = []
        for entry in _EXPORT_TABLES:
            result.append(entry)
            if entry[0] == "search_event":
                result.extend(
                    candidate_entry
                    for candidate_entry in _OPTIONAL_EXPORT_TABLES
                    if optional[candidate_entry[1]]
                )
        return tuple(result)

    def summary(self) -> dict[str, Any]:
        """Return counts and selector/comparison identities from one snapshot."""

        with self._read_snapshot(operation="summary") as db:
            return self._summary_from_db(db, db_name=self.path.name)

    def export_jsonl(self, destination: Path | str) -> dict[str, Any]:
        """Atomically export the complete attribution store as typed JSONL.

        Records are grouped in referential order and sorted by stable primary
        key.  The returned summary and the file are produced from the same
        SQLite read transaction, so concurrent writers cannot tear counts away
        from the exported records.  The destination is replaced only after a
        complete file has been flushed and synced.
        """

        destination = Path(destination)
        if destination.resolve() == self.path.resolve():
            raise ValueError("selection JSONL destination cannot replace the SQLite store")
        destination.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        )
        temporary = Path(handle.name)
        digest = hashlib.sha256()
        record_count = 0
        record_type_counts: dict[str, int] = {}
        try:
            with handle:
                with self._read_snapshot(operation="export_jsonl") as db:
                    summary = self._summary_from_db(db, db_name=self.path.name)
                    for record_type, table, id_column in self._export_tables_for_db(db):
                        type_count = 0
                        rows = db.execute(
                            f"SELECT * FROM {table} ORDER BY {id_column}"
                        )
                        for row in rows:
                            envelope = {
                                "schema": EXPORT_SCHEMA_VERSION,
                                "record_type": record_type,
                                "record": _decode_row(row) or {},
                            }
                            encoded = (_json(envelope) + "\n").encode("utf-8")
                            handle.write(encoded)
                            digest.update(encoded)
                            record_count += 1
                            type_count += 1
                        record_type_counts[record_type] = type_count
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

        return {
            "schema": EXPORT_SCHEMA_VERSION,
            "path": str(destination),
            "sha256": digest.hexdigest(),
            "record_count": record_count,
            "record_type_counts": record_type_counts,
            "summary": summary,
        }


# A semantic alias for callers that use the issue's exposure terminology.
ExposureStore = SelectionStore


__all__ = [
    "CANONICAL_FEEDBACK_KINDS",
    "CANONICAL_RELATIONS",
    "EXPORT_SCHEMA_VERSION",
    "ExposureStore",
    "REQUEST_KEY_CONFLICT",
    "RequestKeyConflictError",
    "SCHEMA_VERSION",
    "SelectionStore",
]
