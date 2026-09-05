"""Minimal event-backed communication and context-piece store.

The store is intentionally boring: SQLite WAL plus JSON payloads.  This keeps
the experiment surface inspectable while allowing the communication policy to
be replaced without changing the agent runner.
"""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
import json
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Iterable, Mapping


_WORD_RE = re.compile(r"[A-Za-z0-9_\u4e00-\u9fff]+")
_MAX_TEXT = 8_000


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _clip(value: Any, limit: int = _MAX_TEXT) -> str:
    text = str(value or "").strip()
    return text if len(text) <= limit else text[:limit] + "…"


def _tokens(text: str) -> set[str]:
    return {item.lower() for item in _WORD_RE.findall(text)}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _metric_int(metrics: Mapping[str, Any], key: str) -> int | None:
    value = metrics.get(key)
    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _metric_float(metrics: Mapping[str, Any], key: str) -> float | None:
    value = metrics.get(key)
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else 0.0


@dataclass
class _WriteObservation:
    """Timing and queue state for one profiled CPS write transaction.

    This object is created only on the opt-in profiling path.  ``transaction``
    starts immediately before ``BEGIN IMMEDIATE`` so it includes SQLite lock
    admission; ``lock_acquired_at`` lets the final event report the distinct
    lock-hold interval (the mutation body and COMMIT, but not admission wait).
    """

    operation: str
    operation_started: float
    transaction_started: float
    queued_at: float
    wal_bytes_before: int
    db_bytes_before: int
    lock_acquired_at: float | None = None
    lock_wait_seconds: float = 0.0
    queue_residence_seconds: float = 0.0
    active: bool = False
    finalized: bool = False


class CPSStore:
    """Thread/process-safe store; each operation uses a short SQLite txn."""

    def __init__(self, path: Path, profiler: Any | None = None):
        self.path = Path(path)
        self.profiler = profiler
        try:
            self._profiling_enabled = bool(
                profiler is not None and getattr(profiler, "enabled", False)
            )
        except Exception:
            self._profiling_enabled = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        # These are descriptive counters for local in-flight/contending writers
        # in this CPSStore instance, not an application queue.  SQLite's
        # ``lock_wait_seconds`` remains the cross-thread/process measurement;
        # queue_residence spans local contender registration through
        # BEGIN IMMEDIATE lock acquisition, without changing the disabled path.
        self._write_state_lock = threading.Lock()
        self._write_waiters = 0
        self._write_active = 0
        self._write_sequence = 0
        self._write_wall_total_seconds = 0.0
        self._write_lock_wait_total_seconds = 0.0
        self._write_lock_hold_total_seconds = 0.0
        self._init_schema()

    def _profile_event(self, event: str, **fields: Any) -> None:
        if not self._profiling_enabled:
            return
        profiler = self.profiler
        try:
            profiler.emit(event, **fields)
        except BaseException:
            return

    @contextmanager
    def _profile_span(self, name: str, **fields: Any):
        """Run a best-effort profiling span without changing CPS semantics.

        The profiler is an observational side channel.  A custom sink can
        fail while creating a context manager, entering it, or leaving it;
        none of those failures may turn a successful CPS operation into an
        error.  Conversely, an exception raised by the wrapped business code
        must always be re-raised, even when a sink's ``__exit__`` returns a
        truthy value (the normal context-manager suppression convention).
        """

        if not self._profiling_enabled:
            yield
            return
        profiler = self.profiler
        span = getattr(profiler, "span", None) if profiler is not None else None
        if not callable(span):
            yield
            return
        try:
            context = span(name, **fields)
        except BaseException:
            # A custom diagnostic sink is outside CPS's failure domain.
            # Continue the business operation even if context construction
            # itself raises (including a non-Exception BaseException).
            context = None
        if context is None:
            yield
            return

        try:
            context.__enter__()
        except BaseException:
            # A diagnostic sink must be fail-open.  Do not call __exit__ after
            # a failed __enter__, matching Python's with semantics; continue
            # with the business operation uninstrumented.
            yield
            return

        business_error: BaseException | None = None
        try:
            yield
        except BaseException as exc:
            business_error = exc
            raise
        finally:
            try:
                # Ignore both sink failures and the suppression return value.
                # The latter is important: a profiler must never swallow an
                # exception from CPS business logic.
                context.__exit__(
                    type(business_error) if business_error is not None else None,
                    business_error,
                    business_error.__traceback__ if business_error is not None else None,
                )
            except BaseException:
                pass

    @staticmethod
    def _file_size(path: Path) -> int:
        try:
            return max(0, int(path.stat().st_size))
        except OSError:
            return 0

    def _profile_checkpoint(self, operation: str) -> None:
        """Record a passive WAL checkpoint when explicitly invoked by a run."""

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
                "cps.sqlite.checkpoint",
                db_operation=operation,
                checkpoint_seconds=max(0.0, time.monotonic() - started),
                busy_retry_count=max(0, busy),
                rows=frames,
                output_rows=checkpointed,
                wal_bytes=self._file_size(Path(str(self.path) + "-wal")),
            )
        except Exception as exc:
            self._profile_event(
                "cps.sqlite.checkpoint",
                db_operation=operation,
                checkpoint_seconds=max(0.0, time.monotonic() - started),
                status="error",
                error_kind=type(exc).__name__,
            )

    def _connect(self, *, operation: str = "generic") -> sqlite3.Connection:
        # CPS deliberately opens operation-scoped connections.  At scale the
        # PRAGMA/connection setup can become a measurable fraction of the
        # allocator budget, so keep it distinct from SQL execution time.
        started = time.monotonic() if self._profiling_enabled else 0.0
        connection: sqlite3.Connection | None = None
        error_kind: str | None = None
        connected_ok = False
        try:
            connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA busy_timeout=30000")
            connected_ok = True
            return connection
        except Exception as exc:
            error_kind = type(exc).__name__
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            raise
        finally:
            if self._profiling_enabled:
                self._profile_event(
                    "cps.sqlite.connect",
                    db_operation=operation,
                    connect_seconds=max(0.0, time.monotonic() - started),
                    status="ok" if connected_ok else "error",
                    error_kind=error_kind,
                    db_bytes=self._file_size(self.path),
                    wal_bytes=self._file_size(Path(str(self.path) + "-wal")),
                )

    @contextmanager
    def _db(self, *, operation: str = "generic"):
        connection = self._connect(operation=operation)
        try:
            yield connection
        finally:
            connection.close()

    def _finalize_profiled_write(
        self,
        observation: _WriteObservation,
        *,
        status: str,
        reason: str | None = None,
        error_kind: str | None = None,
        body_seconds: float = 0.0,
        commit_seconds: float = 0.0,
        rows_written: int = 0,
        metrics: Mapping[str, Any] | None = None,
        finished_at: float | None = None,
        deferred_events: list[tuple[str, dict[str, Any]]] | None = None,
    ) -> None:
        """Emit exactly one terminal event and release local writer state.

        This method is profiling-only.  It is intentionally fail-open through
        :meth:`_profile_event`, and it tolerates a connection/BEGIN failure
        where no SQLite transaction was ever acquired.  In that case the
        terminal event is ``status=skipped`` with a bounded ``reason``.
        """

        if observation.finalized:
            return
        # ``finished_at`` is captured immediately after COMMIT/ROLLBACK (or
        # the failed BEGIN), before file-size probes and sink bookkeeping.  A
        # missing value is only possible for an unexpected outer failure.
        now = finished_at if finished_at is not None else time.monotonic()
        lock_hold = (
            max(0.0, now - observation.lock_acquired_at)
            if observation.lock_acquired_at is not None
            else 0.0
        )
        transaction_seconds = max(0.0, now - observation.transaction_started)
        with self._write_state_lock:
            if observation.active:
                self._write_active = max(0, self._write_active - 1)
            else:
                self._write_waiters = max(0, self._write_waiters - 1)
            queue_waiters = self._write_waiters
            queue_active = self._write_active
            queue_depth = queue_waiters + queue_active
            self._write_sequence += 1
            write_sequence = self._write_sequence
            wall_seconds = max(0.0, now - observation.operation_started)
            self._write_wall_total_seconds += wall_seconds
            self._write_lock_wait_total_seconds += observation.lock_wait_seconds
            self._write_lock_hold_total_seconds += lock_hold
            write_wall_total_seconds = self._write_wall_total_seconds
            write_lock_wait_total_seconds = self._write_lock_wait_total_seconds
            write_lock_hold_total_seconds = self._write_lock_hold_total_seconds
        wal_after = self._file_size(Path(str(self.path) + "-wal"))
        db_after = self._file_size(self.path)
        values = metrics if isinstance(metrics, Mapping) else {}
        # Mark before handing control to the sink: a pathological sink that
        # re-enters the store must not produce a duplicate terminal event.
        observation.finalized = True
        # ``total_changes`` includes statements executed before a rollback.
        # Only a durable COMMIT may contribute to the persisted-row
        # denominator; failed/skipped transactions must report zero even when
        # their mutation body reached SQLite before the error.
        reported_rows_written = (
            max(0, int(rows_written or 0)) if status == "ok" else 0
        )
        # COMMIT/ROLLBACK (or a failed BEGIN) has already completed before
        # this finalizer is called, so SQLite no longer owns the writer lock.
        # Flush lock/transaction-local observations only now.  Keeping the
        # sink I/O out of the BEGIN..COMMIT interval prevents profiling from
        # inflating the lock-hold measurement and avoids adding contention at
        # high concurrency.  Marking ``observation.finalized`` above makes the
        # flush idempotent even if a sink re-enters the store.
        if deferred_events:
            pending = tuple(deferred_events)
            deferred_events.clear()
            for event, fields in pending:
                try:
                    self._profile_event(event, **fields)
                except BaseException:
                    # ``_profile_event`` is fail-open itself; keep this guard
                    # so a test/custom override cannot turn a durable write
                    # into a profiling exception or skip its terminal row.
                    pass
        self._profile_event(
            "cps.write.commit",
            db_operation=observation.operation,
            queue_state="finished",
            status=status,
            reason=reason,
            error_kind=error_kind,
            lock_wait_seconds=observation.lock_wait_seconds,
            lock_hold_seconds=lock_hold,
            transaction_seconds=transaction_seconds,
            queue_residence_seconds=observation.queue_residence_seconds,
            lock_queue_depth=queue_depth,
            write_waiters=queue_waiters,
            write_active=queue_active,
            body_seconds=max(0.0, body_seconds),
            commit_seconds=max(0.0, commit_seconds),
            wall_seconds=wall_seconds,
            write_sequence=write_sequence,
            write_ops_total=write_sequence,
            write_wall_total_seconds=write_wall_total_seconds,
            write_lock_wait_total_seconds=write_lock_wait_total_seconds,
            write_lock_hold_total_seconds=write_lock_hold_total_seconds,
            rows_written=reported_rows_written,
            input_bytes=_metric_int(values, "input_bytes"),
            payload_bytes=_metric_int(values, "payload_bytes"),
            request_key_sha256=values.get("request_key_sha256"),
            snapshot_sha256=values.get("snapshot_sha256"),
            pool_sha256=values.get("pool_sha256"),
            serialization_seconds=_metric_float(values, "serialization_seconds"),
            serialization_inside_lock_seconds=_metric_float(
                values, "serialization_inside_lock_seconds"
            ),
            serialization_inside_lock_bytes=_metric_int(
                values, "serialization_inside_lock_bytes"
            ),
            serialization_call_count=_metric_int(values, "serialization_call_count"),
            hash_seconds=_metric_float(values, "hash_seconds"),
            prepare_seconds=_metric_float(values, "prepare_seconds"),
            prepare_rows=_metric_int(values, "prepare_rows"),
            prepare_candidate_rows=_metric_int(values, "prepare_candidate_rows"),
            prepare_ranking_rows=_metric_int(values, "prepare_ranking_rows"),
            prepare_bytes=_metric_int(values, "prepare_bytes"),
            prepare_serialization_seconds=_metric_float(
                values, "prepare_serialization_seconds"
            ),
            prepare_hash_seconds=_metric_float(values, "prepare_hash_seconds"),
            db_bytes=db_after,
            wal_bytes=wal_after,
            db_bytes_before=observation.db_bytes_before,
            db_bytes_delta=db_after - observation.db_bytes_before,
            wal_bytes_before=observation.wal_bytes_before,
            wal_bytes_after=wal_after,
            wal_bytes_delta=wal_after - observation.wal_bytes_before,
        )

    @contextmanager
    def _write_transaction(
        self,
        operation: str,
        *,
        metrics: Mapping[str, Any] | None = None,
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ):
        """Run one CPS write with an opt-in, complete timing envelope.

        The non-profiled branch intentionally retains the original sequence of
        ``_db`` → ``_begin_write`` → body → ``_commit_write`` and does not call
        ``time.monotonic`` or inspect file sizes.  The profiled branch adds
        only side-channel bookkeeping around that same SQL transaction.
        """

        if not self._profiling_enabled:
            with self._db() as db:
                self._begin_write(
                    db,
                    deadline_epoch_ms=deadline_epoch_ms,
                    cancel_guard=cancel_guard,
                )
                try:
                    yield db
                    self._commit_write(
                        db,
                        deadline_epoch_ms=deadline_epoch_ms,
                        cancel_guard=cancel_guard,
                    )
                except Exception:
                    if db.in_transaction:
                        db.execute("ROLLBACK")
                    raise
            return

        operation_started = time.monotonic()
        observation = _WriteObservation(
            operation=operation,
            operation_started=operation_started,
            # Filled immediately before BEGIN IMMEDIATE, after connection
            # setup and local contender registration.  This makes
            # transaction_seconds include SQLite lock wait but keeps
            # connection/queue residence separately attributable.
            transaction_started=operation_started,
            queued_at=operation_started,
            wal_bytes_before=self._file_size(Path(str(self.path) + "-wal")),
            db_bytes_before=self._file_size(self.path),
        )
        with self._write_state_lock:
            self._write_waiters += 1
            queue_waiters = self._write_waiters
            queue_active = self._write_active
            queue_depth = queue_waiters + queue_active
        self._profile_event(
            "cps.write.queue",
            db_operation=operation,
            queue_state="waiting",
            lock_queue_depth=queue_depth,
            write_waiters=queue_waiters,
            write_active=queue_active,
        )
        db: sqlite3.Connection | None = None
        changes_before = 0
        body_seconds = 0.0
        # Keep lock-scoped events in memory until COMMIT/ROLLBACK has released
        # SQLite's writer lock.  The queue is per-attempt and bounded by the
        # handful of lifecycle events emitted by this transaction.
        deferred_profile_events: list[tuple[str, dict[str, Any]]] = []

        def _defer_profile_event(event: str, **fields: Any) -> None:
            if self._profiling_enabled:
                deferred_profile_events.append((event, dict(fields)))

        try:
            try:
                with self._db(operation=f"write:{operation}") as db:
                    changes_before = int(getattr(db, "total_changes", 0) or 0)
                    lock_started = time.monotonic()
                    observation.transaction_started = lock_started
                    try:
                        self._begin_write(
                            db,
                            deadline_epoch_ms=deadline_epoch_ms,
                            cancel_guard=cancel_guard,
                        )
                    except BaseException as exc:
                        # BEGIN may fail before acquiring a transaction (for
                        # example SQLite busy timeout or a revoked capability).
                        # Emit both the lock error and an explicit terminal
                        # commit-skipped record so every write attempt closes.
                        _defer_profile_event(
                            "cps.write.lock",
                            db_operation=operation,
                            lock_wait_seconds=max(0.0, time.monotonic() - lock_started),
                            status="error",
                            error_kind=type(exc).__name__,
                        )
                        observation.lock_wait_seconds = max(
                            0.0, time.monotonic() - lock_started
                        )
                        begin_finished_at = time.monotonic()
                        self._finalize_profiled_write(
                            observation,
                            status="skipped",
                            reason="begin_failed",
                            error_kind=type(exc).__name__,
                            metrics=metrics,
                            finished_at=begin_finished_at,
                            deferred_events=deferred_profile_events,
                        )
                        raise
                    observation.lock_acquired_at = time.monotonic()
                    observation.lock_wait_seconds = max(
                        0.0, observation.lock_acquired_at - lock_started
                    )
                    observation.queue_residence_seconds = max(
                        0.0, observation.lock_acquired_at - observation.queued_at
                    )
                    with self._write_state_lock:
                        self._write_waiters = max(0, self._write_waiters - 1)
                        self._write_active += 1
                        queue_waiters = self._write_waiters
                        queue_active = self._write_active
                        queue_depth = queue_waiters + queue_active
                    observation.active = True
                    _defer_profile_event(
                        "cps.write.lock",
                        db_operation=operation,
                        lock_wait_seconds=observation.lock_wait_seconds,
                        queue_residence_seconds=observation.queue_residence_seconds,
                        lock_queue_depth=queue_depth,
                        write_waiters=queue_waiters,
                        write_active=queue_active,
                        status="acquired",
                    )
                    body_started = time.monotonic()
                    try:
                        yield db
                    except BaseException as exc:
                        body_seconds = max(0.0, time.monotonic() - body_started)
                        if db.in_transaction:
                            try:
                                db.execute("ROLLBACK")
                            except Exception:
                                # Preserve the original mutation exception;
                                # the terminal profile still records failure.
                                pass
                        body_finished_at = time.monotonic()
                        self._finalize_profiled_write(
                            observation,
                            status="error",
                            reason="body_failed",
                            error_kind=type(exc).__name__,
                            body_seconds=body_seconds,
                            rows_written=max(
                                0,
                                int(getattr(db, "total_changes", 0) or 0)
                                - changes_before,
                            ),
                            metrics=metrics,
                            finished_at=body_finished_at,
                            deferred_events=deferred_profile_events,
                        )
                        raise
                    body_seconds = max(0.0, time.monotonic() - body_started)
                    commit_started = time.monotonic()
                    try:
                        self._commit_write(
                            db,
                            deadline_epoch_ms=deadline_epoch_ms,
                            cancel_guard=cancel_guard,
                        )
                    except BaseException as exc:
                        commit_seconds = max(0.0, time.monotonic() - commit_started)
                        if db.in_transaction:
                            try:
                                db.execute("ROLLBACK")
                            except Exception:
                                pass
                        commit_finished_at = time.monotonic()
                        self._finalize_profiled_write(
                            observation,
                            status="error",
                            reason="commit_failed",
                            error_kind=type(exc).__name__,
                            body_seconds=body_seconds,
                            commit_seconds=commit_seconds,
                            rows_written=max(
                                0,
                                int(getattr(db, "total_changes", 0) or 0)
                                - changes_before,
                            ),
                            metrics=metrics,
                            finished_at=commit_finished_at,
                            deferred_events=deferred_profile_events,
                        )
                        raise
                    commit_seconds = max(0.0, time.monotonic() - commit_started)
                    committed_finished_at = time.monotonic()
                    self._finalize_profiled_write(
                        observation,
                        status="ok",
                        body_seconds=body_seconds,
                        commit_seconds=commit_seconds,
                        rows_written=max(
                            0,
                            int(getattr(db, "total_changes", 0) or 0) - changes_before,
                        ),
                        metrics=metrics,
                        finished_at=committed_finished_at,
                        deferred_events=deferred_profile_events,
                    )
            except BaseException as exc:
                if not observation.finalized:
                    # Failure while opening the connection (or an unexpected
                    # adapter failure before BEGIN) has no lock/commit event;
                    # close the attempt explicitly as skipped/error.
                    self._finalize_profiled_write(
                        observation,
                        status="skipped" if db is None else "error",
                        reason="connection_failed" if db is None else "transaction_failed",
                        error_kind=type(exc).__name__,
                        metrics=metrics,
                        deferred_events=deferred_profile_events,
                    )
                raise
        finally:
            if not observation.finalized:
                self._finalize_profiled_write(
                    observation,
                    status="error",
                    reason="transaction_abandoned",
                    metrics=metrics,
                    deferred_events=deferred_profile_events,
                )

    def _init_schema(self) -> None:
        with self._db(operation="init_schema" if self._profiling_enabled else "generic") as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS pieces (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    author TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    tags TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS pieces_task_created
                    ON pieces(task_id, created_at DESC);
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    recipient TEXT,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    acked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS messages_inbox
                    ON messages(task_id, recipient, created_at DESC);
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    task_id TEXT,
                    actor_id TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    @classmethod
    def _begin_write(
        cls,
        db: sqlite3.Connection,
        *,
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ) -> None:
        """Acquire the write lock, then revalidate the write capability."""

        db.execute("BEGIN IMMEDIATE")
        try:
            cls._validate_write_capability(
                db,
                deadline_epoch_ms=deadline_epoch_ms,
                cancel_guard=cancel_guard,
            )
        except Exception:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise

    @staticmethod
    def _validate_write_capability(
        db: sqlite3.Connection,
        *,
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ) -> None:
        """Validate cancellation and horizon inside the active write txn."""

        if cancel_guard is not None and cancel_guard():
            raise RuntimeError("CPS communication capability has been revoked")
        if deadline_epoch_ms is None:
            return
        now_epoch_ms = int(
            db.execute(
                "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
            ).fetchone()[0]
        )
        if now_epoch_ms >= int(deadline_epoch_ms):
            raise RuntimeError("CPS communication horizon has elapsed")

    @classmethod
    def _commit_write(
        cls,
        db: sqlite3.Connection,
        *,
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ) -> None:
        """Revalidate immediately before making a write transaction durable."""

        cls._validate_write_capability(
            db,
            deadline_epoch_ms=deadline_epoch_ms,
            cancel_guard=cancel_guard,
        )
        db.execute("COMMIT")

    @staticmethod
    def _insert_event(
        db: sqlite3.Connection,
        event_type: str,
        *,
        task_id: str | None,
        actor_id: str | None,
        payload: Mapping[str, Any] | None,
    ) -> str:
        event_id = uuid.uuid4().hex
        db.execute(
            "INSERT INTO events(event_id,event_type,task_id,actor_id,payload,created_at) VALUES(?,?,?,?,?,?)",
            (event_id, event_type, task_id, actor_id, _json(dict(payload or {})), utc_now()),
        )
        return event_id

    def record_event(
        self,
        event_type: str,
        *,
        task_id: str | None = None,
        actor_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ) -> str:
        with self._write_transaction(
            "record_event",
            deadline_epoch_ms=deadline_epoch_ms,
            cancel_guard=cancel_guard,
            metrics=(
                {"input_bytes": len(_json(dict(payload or {})).encode("utf-8"))}
                if self._profiling_enabled
                else None
            ),
        ) as db:
            event_id = self._insert_event(
                db,
                event_type,
                task_id=task_id,
                actor_id=actor_id,
                payload=payload,
            )
        return event_id

    def create_piece(
        self,
        *,
        task_id: str,
        author: str,
        kind: str,
        title: str,
        body: str,
        tags: Iterable[str] = (),
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        piece_id = uuid.uuid4().hex
        row = {
            "id": piece_id,
            "task_id": _clip(task_id, 256),
            "author": _clip(author, 256),
            "kind": _clip(kind, 64) or "note",
            "title": _clip(title, 300) or "untitled",
            "body": _clip(body),
            "tags": sorted({_clip(tag, 64) for tag in tags if _clip(tag, 64)}),
            "created_at": utc_now(),
        }
        row_payload = _json(row) if self._profiling_enabled else ""
        with self._write_transaction(
            "create_piece",
            deadline_epoch_ms=deadline_epoch_ms,
            cancel_guard=cancel_guard,
            metrics=(
                {"input_bytes": len(row_payload.encode("utf-8"))}
                if self._profiling_enabled
                else None
            ),
        ) as db:
            db.execute(
                "INSERT INTO pieces(id,task_id,author,kind,title,body,tags,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    row["id"],
                    row["task_id"],
                    row["author"],
                    row["kind"],
                    row["title"],
                    row["body"],
                    _json(row["tags"]),
                    row["created_at"],
                ),
            )
            self._insert_event(
                db,
                "piece_created",
                task_id=task_id,
                actor_id=author,
                payload=row,
            )
        return row

    def search(
        self,
        *,
        task_id: str,
        query: str = "",
        limit: int = 8,
        include_global: bool = False,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 50))
        profiling = self._profiling_enabled
        started = time.monotonic() if profiling else 0.0
        query_seconds = 0.0
        fetch_seconds = 0.0
        read_scope_seconds = 0.0
        with self._db(operation="search" if profiling else "generic") as db:
            query_started = time.monotonic() if profiling else 0.0
            if include_global:
                cursor = db.execute(
                    """SELECT * FROM pieces
                       WHERE active=1 AND (task_id=? OR task_id='__global__')
                       ORDER BY created_at DESC LIMIT ?""",
                    (task_id, max(limit * 8, 32)),
                )
            else:
                cursor = db.execute(
                    "SELECT * FROM pieces WHERE active=1 AND task_id=? ORDER BY created_at DESC LIMIT ?",
                    (task_id, max(limit * 8, 32)),
                )
            if profiling:
                query_seconds = max(0.0, time.monotonic() - query_started)
                fetch_started = time.monotonic()
                rows = cursor.fetchall()
                fetch_seconds = max(0.0, time.monotonic() - fetch_started)
                read_scope_seconds = max(0.0, time.monotonic() - query_started)
            else:
                rows = cursor.fetchall()
        if profiling:
            self._profile_event(
                "cps.search.query",
                db_operation="pieces_search",
                task_count=1,
                rows_scanned=len(rows),
                input_rows=len(rows),
                input_bytes=sum(
                    len(str(row["title"] or "").encode("utf-8"))
                    + len(str(row["body"] or "").encode("utf-8"))
                    for row in rows
                ),
                query_seconds=query_seconds,
                fetch_seconds=fetch_seconds,
                read_scope_seconds=read_scope_seconds,
                read_transaction_seconds=read_scope_seconds,
                read_mode="autocommit_select",
            )
        materialize_started = time.monotonic() if profiling else 0.0
        wanted = _tokens(query)
        scored: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            item = dict(row)
            try:
                item["tags"] = json.loads(item.get("tags") or "[]")
            except json.JSONDecodeError:
                item["tags"] = []
            haystack = " ".join(
                [str(item.get("title", "")), str(item.get("body", "")), " ".join(item["tags"])]
            )
            overlap = len(wanted & _tokens(haystack)) if wanted else 0
            # Newer pieces win ties; an explicit query match dominates recency.
            try:
                recency = int(str(item.get("id", ""))[-6:] or "0", 16) / 16_777_215
            except ValueError:
                recency = 0.0
            score = overlap * 10.0 + recency
            scored.append((score, item))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        result = [item for _, item in scored[:limit]]
        if profiling:
            self._profile_event(
                "cps.search.materialize",
                operation="pieces_search",
                input_rows=len(rows),
                output_rows=len(result),
                materialized_rows=len(result),
                materialized_bytes=sum(
                    len(str(item.get("title", "")).encode("utf-8"))
                    + len(str(item.get("body", "")).encode("utf-8"))
                    for item in result
                ),
                tokenize_count=len(rows) + 1,
                materialize_seconds=max(0.0, time.monotonic() - materialize_started),
                read_scope_seconds=read_scope_seconds,
                wall_seconds=max(0.0, time.monotonic() - started),
            )
        return result

    def send_message(
        self,
        *,
        task_id: str,
        sender: str,
        recipient: str | None,
        body: str,
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        message = {
            "id": uuid.uuid4().hex,
            "task_id": _clip(task_id, 256),
            "sender": _clip(sender, 256),
            "recipient": _clip(recipient, 256) if recipient else None,
            "body": _clip(body),
            "created_at": utc_now(),
            "acked_at": None,
        }
        message_payload = _json(message) if self._profiling_enabled else ""
        with self._write_transaction(
            "send_message",
            deadline_epoch_ms=deadline_epoch_ms,
            cancel_guard=cancel_guard,
            metrics=(
                {"input_bytes": len(message_payload.encode("utf-8"))}
                if self._profiling_enabled
                else None
            ),
        ) as db:
            db.execute(
                "INSERT INTO messages(id,task_id,sender,recipient,body,created_at) VALUES(?,?,?,?,?,?)",
                (
                    message["id"],
                    message["task_id"],
                    message["sender"],
                    message["recipient"],
                    message["body"],
                    message["created_at"],
                ),
            )
            self._insert_event(
                db,
                "message_sent",
                task_id=task_id,
                actor_id=sender,
                payload=message,
            )
        return message

    def inbox(
        self,
        *,
        task_id: str,
        recipient: str,
        limit: int = 8,
        include_global: bool = False,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 50))
        profiling = self._profiling_enabled
        started = time.monotonic() if profiling else 0.0
        query_seconds = 0.0
        fetch_seconds = 0.0
        read_scope_seconds = 0.0
        with self._db(operation="inbox" if profiling else "generic") as db:
            query_started = time.monotonic() if profiling else 0.0
            if include_global:
                cursor = db.execute(
                    """SELECT * FROM messages
                       WHERE task_id IN (?, '__global__') AND acked_at IS NULL
                         AND (recipient IS NULL OR recipient=? OR recipient='*')
                       ORDER BY created_at DESC LIMIT ?""",
                    (task_id, recipient, limit),
                )
            else:
                cursor = db.execute(
                    """SELECT * FROM messages
                       WHERE task_id=? AND acked_at IS NULL
                         AND (recipient IS NULL OR recipient=? OR recipient='*')
                       ORDER BY created_at DESC LIMIT ?""",
                    (task_id, recipient, limit),
                )
            if profiling:
                query_seconds = max(0.0, time.monotonic() - query_started)
                fetch_started = time.monotonic()
                rows = cursor.fetchall()
                fetch_seconds = max(0.0, time.monotonic() - fetch_started)
                read_scope_seconds = max(0.0, time.monotonic() - query_started)
            else:
                rows = cursor.fetchall()
        if profiling:
            self._profile_event(
                "cps.inbox.query",
                db_operation="messages_inbox",
                task_count=1,
                rows_scanned=len(rows),
                input_rows=len(rows),
                query_seconds=query_seconds,
                fetch_seconds=fetch_seconds,
                read_scope_seconds=read_scope_seconds,
                read_transaction_seconds=read_scope_seconds,
                read_mode="autocommit_select",
            )
        materialize_started = time.monotonic() if profiling else 0.0
        result = [dict(row) for row in rows]
        if profiling:
            self._profile_event(
                "cps.inbox.materialize",
                operation="messages_inbox",
                input_rows=len(rows),
                output_rows=len(result),
                materialized_rows=len(result),
                materialized_bytes=sum(
                    len(str(item.get("body", "")).encode("utf-8")) for item in result
                ),
                materialize_seconds=max(0.0, time.monotonic() - materialize_started),
                read_scope_seconds=read_scope_seconds,
                wall_seconds=max(0.0, time.monotonic() - started),
            )
        return result

    def ack_message(
        self,
        message_id: str,
        actor_id: str,
        *,
        deadline_epoch_ms: int | None = None,
        cancel_guard: Callable[[], bool] | None = None,
    ) -> bool:
        now = utc_now()
        with self._write_transaction(
            "ack_message",
            deadline_epoch_ms=deadline_epoch_ms,
            cancel_guard=cancel_guard,
            metrics=(
                {"input_bytes": len(str(message_id).encode("utf-8"))}
                if self._profiling_enabled
                else None
            ),
        ) as db:
            cursor = db.execute(
                "UPDATE messages SET acked_at=? WHERE id=? AND acked_at IS NULL",
                (now, message_id),
            )
            if cursor.rowcount:
                self._insert_event(
                    db,
                    "message_acked",
                    task_id=None,
                    actor_id=actor_id,
                    payload={"id": message_id},
                )
            rowcount = int(cursor.rowcount or 0)
        return bool(rowcount)

    def digest(
        self,
        *,
        task_id: str,
        actor_id: str,
        query: str = "",
        limit: int = 8,
        include_global: bool = False,
    ) -> dict[str, Any]:
        profiling = self._profiling_enabled
        if not profiling:
            pieces = self.search(task_id=task_id, query=query, limit=limit, include_global=include_global)
            messages = self.inbox(
                task_id=task_id,
                recipient=actor_id,
                limit=limit,
                include_global=include_global,
            )
            return {"pieces": pieces, "messages": messages}
        started = time.monotonic()
        with self._profile_span(
            "cps.digest",
            operation="worker_context_digest",
            task_id=task_id,
            actor_id=actor_id,
        ):
            pieces = self.search(
                task_id=task_id,
                query=query,
                limit=limit,
                include_global=include_global,
            )
            messages = self.inbox(
                task_id=task_id,
                recipient=actor_id,
                limit=limit,
                include_global=include_global,
            )
        self._profile_event(
            "cps.digest.summary",
            operation="worker_context_digest",
            task_id=task_id,
            actor_id=actor_id,
            output_rows=len(pieces) + len(messages),
            materialized_rows=len(pieces) + len(messages),
            wall_seconds=max(0.0, time.monotonic() - started),
        )
        return {"pieces": pieces, "messages": messages}

    def _progress_snapshot_impl(
        self,
        task_ids: Iterable[str],
        *,
        recent_limit: int = 3,
        body_chars: int = 1_200,
    ) -> dict[str, dict[str, Any]]:
        """Return bounded per-task CPS statistics in one read transaction.

        Allocation policies use this projection instead of receiving a database
        handle.  The scheduler therefore cannot publish pieces or accidentally
        feed its own decisions back into the communication substrate.
        """
        ordered_ids = tuple(dict.fromkeys(str(task_id) for task_id in task_ids))
        recent_limit = max(1, min(int(recent_limit), 20))
        body_chars = max(1, min(int(body_chars), _MAX_TEXT))
        result: dict[str, dict[str, Any]] = {
            task_id: {
                "piece_count": 0,
                "validation_piece_count": 0,
                "strategy_piece_count": 0,
                "duplicate_piece_count": 0,
                "latest_created_at": "",
                "recent_pieces": [],
            }
            for task_id in ordered_ids
        }
        if not ordered_ids:
            return result
        placeholders = ",".join("?" for _ in ordered_ids)
        if not self._profiling_enabled:
            # Preserve the baseline exactly: no explicit transaction, timing,
            # row/byte accounting, or extra materialization is introduced
            # when profiling is disabled.
            with self._db() as db:
                rows = db.execute(
                    f"""SELECT rowid,id,task_id,author,kind,title,body,created_at
                        FROM pieces
                        WHERE active=1 AND task_id IN ({placeholders})
                        ORDER BY rowid DESC""",
                    ordered_ids,
                ).fetchall()
            query_seconds = fetch_seconds = 0.0
            read_transaction_seconds = 0.0
            read_lock_wait_seconds = 0.0
        else:
            # The production progress path is an autocommit SELECT.  Keep the
            # profiled path on that same code path: introducing an explicit
            # BEGIN only for profiling would hold a snapshot longer and make
            # the lock/WAL comparison self-referential.  SQLite's implicit
            # read scope is measured from execute through fetchall instead.
            query_seconds = fetch_seconds = 0.0
            read_transaction_seconds = 0.0
            read_lock_wait_seconds = 0.0
            with self._db(operation="progress_snapshot") as db:
                query_started = time.monotonic()
                cursor = db.execute(
                    f"""SELECT rowid,id,task_id,author,kind,title,body,created_at
                        FROM pieces
                        WHERE active=1 AND task_id IN ({placeholders})
                        ORDER BY rowid DESC""",
                    ordered_ids,
                )
                query_seconds = max(0.0, time.monotonic() - query_started)
                fetch_started = time.monotonic()
                rows = cursor.fetchall()
                fetch_seconds = max(0.0, time.monotonic() - fetch_started)
                # With isolation_level=None SQLite releases the implicit read
                # transaction when the cursor is exhausted/closed.  This is
                # therefore the closest observable read-scope duration; any
                # lock acquisition is included in query_seconds rather than
                # fabricated as a separately measurable wait.
                read_transaction_seconds = max(
                    0.0, time.monotonic() - query_started
                )
        if self._profiling_enabled:
            input_bytes = sum(
                len(str(row["title"] or "").encode("utf-8"))
                + len(str(row["body"] or "").encode("utf-8"))
                for row in rows
            )
            self._profile_event(
                "cps.progress.query",
                operation="progress_snapshot",
                scan_mode="full_active_piece_scan",
                read_mode="autocommit_select",
                task_count=len(ordered_ids),
                rows_scanned=len(rows),
                input_rows=len(rows),
                input_bytes=input_bytes,
                query_seconds=query_seconds,
                fetch_seconds=fetch_seconds,
                read_transaction_seconds=read_transaction_seconds,
                read_scope_seconds=max(0.0, query_seconds + fetch_seconds),
                read_lock_wait_seconds=read_lock_wait_seconds,
                db_bytes=self._file_size(self.path),
                wal_bytes=self._file_size(Path(str(self.path) + "-wal")),
            )
        titles: dict[str, dict[str, int]] = {task_id: {} for task_id in ordered_ids}
        materialize_started = time.monotonic() if self._profiling_enabled else 0.0
        for raw in rows:
            item = dict(raw)
            task_id = str(item["task_id"])
            stats = result[task_id]
            stats["piece_count"] += 1
            kind = str(item.get("kind") or "")
            if kind == "validation_result" and _is_authoritative_validation_piece(item):
                stats["validation_piece_count"] += 1
            elif kind in {"proof_strategy", "strategy", "handoff", "lemma", "blocker"}:
                stats["strategy_piece_count"] += 1
            normalized_title = " ".join(str(item.get("title") or "").lower().split())
            if normalized_title and kind != "validation_result":
                titles[task_id][normalized_title] = titles[task_id].get(normalized_title, 0) + 1
            if not stats["latest_created_at"]:
                stats["latest_created_at"] = str(item.get("created_at") or "")
            if len(stats["recent_pieces"]) < recent_limit:
                body = str(item.get("body") or "")
                stats["recent_pieces"].append(
                    {
                        "piece_id": str(item.get("id") or ""),
                        "kind": kind,
                        "title": str(item.get("title") or "")[:300],
                        "body": body if len(body) <= body_chars else body[:body_chars] + "…",
                        "author": str(item.get("author") or "")[:256],
                        "created_at": str(item.get("created_at") or ""),
                    }
                )
        for task_id, counts in titles.items():
            result[task_id]["duplicate_piece_count"] = sum(
                max(0, count - 1) for count in counts.values()
            )
        if self._profiling_enabled:
            materialize_seconds = max(0.0, time.monotonic() - materialize_started)
            recent_rows = sum(
                len(item.get("recent_pieces", [])) for item in result.values()
            )
            materialized_bytes = sum(
                len(str(piece.get("title", "")).encode("utf-8"))
                + len(str(piece.get("body", "")).encode("utf-8"))
                for stats in result.values()
                for piece in stats.get("recent_pieces", [])
            )
            self._profile_event(
                "cps.progress.materialize",
                operation="progress_snapshot",
                scan_mode="full_active_piece_scan",
                input_rows=len(rows),
                output_rows=recent_rows,
                materialized_rows=recent_rows,
                materialized_bytes=materialized_bytes,
                materialize_seconds=materialize_seconds,
            )
        return result

    def progress_snapshot(
        self,
        task_ids: Iterable[str],
        *,
        recent_limit: int = 3,
        body_chars: int = 1_200,
    ) -> dict[str, dict[str, Any]]:
        """Profile the bounded CPS progress projection."""

        if not self._profiling_enabled:
            return self._progress_snapshot_impl(
                task_ids,
                recent_limit=recent_limit,
                body_chars=body_chars,
            )
        with self._profile_span("cps.progress", operation="progress_snapshot"):
            result = self._progress_snapshot_impl(
                task_ids,
                recent_limit=recent_limit,
                body_chars=body_chars,
            )
        self._profile_event(
            "cps.progress.summary",
            operation="progress_snapshot",
            task_count=len(result),
            rows_scanned=sum(int(item.get("piece_count", 0) or 0) for item in result.values()),
            output_rows=sum(
                len(item.get("recent_pieces", [])) for item in result.values()
            ),
        )
        return result

    def summary(self) -> dict[str, Any]:
        with self._db(operation="summary" if self._profiling_enabled else "generic") as db:
            pieces = int(db.execute("SELECT COUNT(*) FROM pieces").fetchone()[0])
            messages = int(db.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
            events = int(db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        return {"pieces": pieces, "messages": messages, "events": events, "db": self.path.name}

    def export_events(self, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with self._db(operation="export_events" if self._profiling_enabled else "generic") as db:
            rows = db.execute(
                "SELECT seq,event_id,event_type,task_id,actor_id,payload,created_at FROM events ORDER BY seq"
            ).fetchall()
        with destination.open("w", encoding="utf-8") as handle:
            for row in rows:
                item = dict(row)
                try:
                    item["payload"] = json.loads(item.get("payload") or "{}")
                except json.JSONDecodeError:
                    item["payload"] = {}
                handle.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")


def render_digest(digest: Mapping[str, Any], *, max_chars: int = 6_000) -> str:
    """Render only task-relevant content into a worker prompt."""
    lines: list[str] = []
    for item in digest.get("pieces", []):
        lines.append(
            f"[piece:{item.get('kind','note')}] {item.get('title','')}\n{item.get('body','')}"
        )
    for item in digest.get("messages", []):
        lines.append(f"[message from {item.get('sender','?')}] {item.get('body','')}")
    text = "\n\n".join(lines).strip()
    return text if len(text) <= max_chars else text[:max_chars] + "\n[context truncated]"


def _is_authoritative_validation_piece(item: Mapping[str, Any]) -> bool:
    if str(item.get("author") or "") != "runner":
        return False
    try:
        payload = json.loads(str(item.get("body") or ""))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, Mapping):
        return False
    return all(
        isinstance(payload.get(key), str)
        and re.fullmatch(r"[0-9a-f]{64}", str(payload[key]).lower()) is not None
        for key in ("candidate_sha256", "task_contract_sha256")
    )


@dataclass
class CommunicationPolicy:
    """Policy facade used by the runner; methods are no-ops for baseline mode."""

    name: str
    store: CPSStore | None

    @property
    def enabled(self) -> bool:
        return self.store is not None and self.name != "none"

    def digest(self, task_id: str, actor_id: str, query: str = "") -> str:
        if not self.enabled:
            return ""
        assert self.store is not None
        return render_digest(
            self.store.digest(
                task_id=task_id,
                actor_id=actor_id,
                query=query,
                include_global=self.name == "hybrid",
            )
        )

    def publish(
        self,
        task_id: str,
        actor_id: str,
        *,
        title: str,
        body: str,
        kind: str = "handoff",
        tags: Iterable[str] = (),
        deadline_epoch_ms: int | None = None,
    ) -> None:
        if not self.enabled:
            return
        assert self.store is not None
        self.store.create_piece(
            task_id=task_id,
            author=actor_id,
            kind=kind,
            title=title,
            body=body,
            tags=tags,
            deadline_epoch_ms=deadline_epoch_ms,
        )

    def send(
        self,
        task_id: str,
        actor_id: str,
        body: str,
        recipient: str | None = None,
        *,
        deadline_epoch_ms: int | None = None,
    ) -> None:
        if not self.enabled or self.name == "blackboard":
            return
        assert self.store is not None
        self.store.send_message(
            task_id=task_id,
            sender=actor_id,
            recipient=recipient,
            body=body,
            deadline_epoch_ms=deadline_epoch_ms,
        )


def make_policy(name: str, store: CPSStore | None) -> CommunicationPolicy:
    normalized = str(name or "none").strip().lower()
    if normalized == "simple":
        normalized = "blackboard"
    if normalized not in {"none", "blackboard", "direct", "hybrid"}:
        raise ValueError(f"unknown communication policy: {name}")
    return CommunicationPolicy(normalized, store if normalized != "none" else None)
