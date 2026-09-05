"""Durable selection exposure and feedback attribution.

This store is deliberately independent from the legacy CPS database.  It owns
the auditable chain used by feedback-aware selectors::

    search_event -> exposure -> exposure_item -> worker feedback

Verifier evidence, trace maintenance, and trace relations have their own event
tables.  In particular, none of those event classes can accidentally occupy a
worker interaction's single effective terminal-feedback slot.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "contextswarm_selection_store_v1"
EXPORT_SCHEMA_VERSION = "contextswarm_selection_store_export_v1"
REQUEST_KEY_CONFLICT = "REQUEST_KEY_CONFLICT"

# ``SCHEMA_VERSION`` is intentionally kept at v1: the public JSONL contract
# and the logical selection-store schema did not change.  The candidate pool
# table did, however, gain a storage-only migration which moves the immutable
# watermark to its parent ``search_events`` row.  The physical column itself
# is the restart marker, so no new public schema/version field is needed.

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

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    @contextmanager
    def _db(self):
        db = self._connect()
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _write(self):
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

    @contextmanager
    def _read_snapshot(self):
        """Yield one consistent read snapshot without blocking WAL writers."""

        with self._db() as db:
            db.execute("BEGIN")
            try:
                yield db
                db.execute("COMMIT")
            except Exception:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

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
            parent_definitions = (
                ("eligible_candidates_sha256", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_watermarks_sha256", "TEXT NOT NULL DEFAULT ''"),
                ("snapshot_watermarks_json", "TEXT NOT NULL DEFAULT '{}'"),
            )
            existing_columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(search_events)")
            }
            candidate_columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            needs_schema_lock = (
                any(column not in existing_columns for column, _definition in parent_definitions)
                or "snapshot_watermarks_json" in candidate_columns
            )
            if needs_schema_lock:
                # Parent-column additions and candidate-column removal share
                # one transaction.  This prevents concurrent openers from
                # racing into duplicate-column errors and rolls back all
                # schema changes if watermark validation fails.
                db.execute("BEGIN IMMEDIATE")
                try:
                    existing_columns = {
                        str(row[1]) for row in db.execute("PRAGMA table_info(search_events)")
                    }
                    for column, definition in parent_definitions:
                        if column not in existing_columns:
                            db.execute(
                                f"ALTER TABLE search_events ADD COLUMN {column} {definition}"
                            )
                            existing_columns.add(column)
                    self._migrate_candidate_watermark_column(
                        db, manage_transaction=False
                    )
                    db.execute("COMMIT")
                except BaseException:
                    if db.in_transaction:
                        db.execute("ROLLBACK")
                    raise
            db.execute(
                "INSERT OR IGNORE INTO selection_store_metadata(schema_version, created_at) VALUES(?, ?)",
                (SCHEMA_VERSION, _now()),
            )

    @staticmethod
    def _watermark_from_json(raw: Any, *, context: str) -> tuple[dict[str, Any], str]:
        """Decode and canonicalize one persisted watermark object."""

        try:
            value = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"{context} contains invalid snapshot watermarks JSON") from exc
        if not isinstance(value, Mapping):
            raise ValueError(f"{context} snapshot watermarks must be an object")
        prepared = dict(value)
        try:
            canonical = _json(prepared)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{context} snapshot watermarks are not canonical JSON") from exc
        return prepared, canonical

    def _migrate_candidate_watermark_column(
        self, db: sqlite3.Connection, *, manage_transaction: bool = True
    ) -> None:
        """Drop the legacy per-candidate watermark column without data loss.

        The migration is deliberately data-aware.  A candidate watermark is
        immutable selection-level evidence, so every non-empty legacy value
        for one ``search_event`` must agree with the parent.  If an old parent
        was created with the migration defaults (``{}``/empty digest), the
        common candidate value is promoted first.  Contradictory or malformed
        rows fail closed and leave the old table untouched for operator
        inspection/retry.
        """

        owns_transaction = manage_transaction
        if owns_transaction:
            # Avoid taking a writer lock for the overwhelmingly common
            # fresh/newly migrated path.  This is only a fast-path hint: it
            # must be rechecked after locking below because another opener may
            # drop the column between this read and BEGIN IMMEDIATE.
            columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            if "snapshot_watermarks_json" not in columns:
                return
            if sqlite3.sqlite_version_info < (3, 35, 0):
                raise RuntimeError(
                    "selection store snapshot deduplication requires SQLite >= 3.35"
                )
            # Holding the schema rewrite and parent normalization under
            # BEGIN IMMEDIATE keeps concurrent store openers from observing a
            # half-migrated table.
            db.execute("BEGIN IMMEDIATE")
        try:
            # Re-check after acquiring the writer lock.  Another opener may
            # have completed the DROP COLUMN while this connection waited;
            # querying the legacy column before this point would race with
            # that schema rewrite.
            columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(search_candidates)")
            }
            if "snapshot_watermarks_json" not in columns:
                if owns_transaction:
                    db.execute("COMMIT")
                return
            if sqlite3.sqlite_version_info < (3, 35, 0):
                raise RuntimeError(
                    "selection store snapshot deduplication requires SQLite >= 3.35"
                )
            # A non-empty parent pool digest with no child rows cannot be
            # replayed after the legacy column is removed.  Treat that state
            # as corruption and fail closed instead of preserving an
            # apparently valid-but-unjoinable parent record.
            dangling_pool = db.execute(
                """SELECT search_event_id
                     FROM search_events AS e
                    WHERE COALESCE(e.eligible_candidates_sha256, '') <> ''
                      AND NOT EXISTS (
                          SELECT 1
                            FROM search_candidates AS c
                           WHERE c.search_event_id = e.search_event_id
                      )
                    ORDER BY search_event_id
                    LIMIT 1"""
            ).fetchone()
            if dangling_pool is not None:
                raise ValueError(
                    "cannot deduplicate search event with pool digest but no "
                    f"candidate rows {dangling_pool['search_event_id']!r}"
                )
            # Stream candidates in selection order.  A historical run can
            # contain millions of rows and hundreds of megabytes of repeated
            # JSON; retaining a list/dict for the whole table would recreate
            # the very memory pressure this migration is meant to remove.
            candidate_rows = db.execute(
                """SELECT c.search_candidate_id, c.search_event_id,
                          c.snapshot_watermarks_json,
                          c.candidate_payload_json,
                          c.trace_id,
                          c.pool_order,
                          c.candidate_sha256,
                          c.feedback_snapshot_json,
                          e.snapshot_watermarks_json AS parent_watermarks_json,
                          e.snapshot_watermarks_sha256 AS parent_watermarks_sha256,
                          e.eligible_candidates_sha256 AS parent_candidates_sha256
                    FROM search_candidates AS c
                LEFT JOIN search_events AS e
                       ON e.search_event_id = c.search_event_id
                    ORDER BY c.search_event_id, c.pool_order"""
            )
            current_search_id: str | None = None
            current_candidate_value: dict[str, Any] | None = None
            current_candidate_canonical: str | None = None
            current_parent_value: dict[str, Any] | None = None
            current_parent_canonical: str | None = None
            current_parent_raw: str | None = None
            current_parent_hash = ""
            current_parent_candidates_hash = ""
            current_candidates_hash = hashlib.sha256()
            current_candidate_count = 0

            def flush_current() -> None:
                nonlocal current_search_id, current_candidate_value
                nonlocal current_candidate_canonical, current_parent_value
                nonlocal current_parent_canonical, current_parent_raw, current_parent_hash
                nonlocal current_parent_candidates_hash, current_candidates_hash
                nonlocal current_candidate_count
                if current_search_id is None:
                    return
                assert current_candidate_canonical is not None
                assert current_parent_canonical is not None
                assert current_parent_value is not None
                # ``_identity_sha256(list)`` is the SHA of a canonical JSON
                # array.  Stream its delimiters/elements so reconstruction of
                # a missing parent pool hash remains bounded by one payload.
                current_candidates_hash.update(b"]")
                computed_candidates_hash = current_candidates_hash.hexdigest()
                if current_parent_candidates_hash:
                    if current_parent_candidates_hash != computed_candidates_hash:
                        raise ValueError(
                            "cannot deduplicate candidate pool: parent digest "
                            f"is invalid for search event {current_search_id!r}"
                        )
                else:
                    db.execute(
                        """UPDATE search_events
                              SET eligible_candidates_sha256 = ?
                            WHERE search_event_id = ?""",
                        (computed_candidates_hash, current_search_id),
                    )
                parent_hash = _identity_sha256(current_parent_value)
                if current_parent_hash and current_parent_hash != parent_hash:
                    raise ValueError(
                        "cannot deduplicate snapshot watermarks: parent digest "
                        f"is invalid for search event {current_search_id!r}"
                    )
                if current_parent_canonical != current_candidate_canonical:
                    # A default/empty parent is the only legacy state that can
                    # be repaired without choosing between two immutable
                    # records.  Otherwise fail closed rather than discarding a
                    # candidate-specific value during DROP COLUMN.  An
                    # explicitly hashed empty object is immutable too, so only
                    # an empty digest permits promotion.
                    if current_parent_canonical != "{}" or current_parent_hash:
                        raise ValueError(
                            "cannot deduplicate snapshot watermarks: parent and "
                            f"candidate values differ for search event {current_search_id!r}"
                        )
                    assert current_candidate_value is not None
                    db.execute(
                        """UPDATE search_events
                              SET snapshot_watermarks_json = ?,
                                  snapshot_watermarks_sha256 = ?
                            WHERE search_event_id = ?""",
                        (
                            current_candidate_canonical,
                            _identity_sha256(current_candidate_value),
                            current_search_id,
                        ),
                    )
                elif (
                    not current_parent_hash
                    or current_parent_hash != parent_hash
                    or current_parent_raw != current_parent_canonical
                ):
                    # Normalize legacy whitespace/key ordering and repair a
                    # missing digest while we still hold the migration lock.
                    # Even an empty/default watermark gets a digest when a
                    # candidate pool proves that the parent participates in
                    # the pool identity contract.
                    db.execute(
                        """UPDATE search_events
                              SET snapshot_watermarks_json = ?,
                                  snapshot_watermarks_sha256 = ?
                            WHERE search_event_id = ?""",
                        (current_parent_canonical, parent_hash, current_search_id),
                    )

            for row in candidate_rows:
                search_event_id = str(row["search_event_id"])
                if search_event_id != current_search_id:
                    flush_current()
                    current_search_id = search_event_id
                    if row["parent_watermarks_json"] is None:
                        raise ValueError(
                            "cannot migrate search candidate with unknown search event "
                            f"{search_event_id!r}"
                        )
                    current_parent_value, current_parent_canonical = self._watermark_from_json(
                        row["parent_watermarks_json"],
                        context=f"search event {search_event_id}",
                    )
                    current_parent_raw = str(row["parent_watermarks_json"])
                    current_parent_hash = str(row["parent_watermarks_sha256"] or "")
                    current_parent_candidates_hash = str(
                        row["parent_candidates_sha256"] or ""
                    )
                    current_candidate_value = None
                    current_candidate_canonical = None
                    current_candidates_hash = hashlib.sha256(b"[")
                    current_candidate_count = 0
                expected_pool_order = current_candidate_count + 1
                try:
                    actual_pool_order = int(row["pool_order"])
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "cannot migrate candidate with invalid pool order "
                        f"{row['search_candidate_id']!r}"
                    ) from exc
                if actual_pool_order != expected_pool_order:
                    raise ValueError(
                        "cannot deduplicate candidate pool with non-contiguous "
                        f"pool order for search event {search_event_id!r}"
                    )
                candidate_value, candidate_canonical = self._watermark_from_json(
                    row["snapshot_watermarks_json"],
                    context=f"search candidate {row['search_candidate_id']}",
                )
                if current_candidate_canonical is None:
                    current_candidate_value = candidate_value
                    current_candidate_canonical = candidate_canonical
                elif current_candidate_canonical != candidate_canonical:
                    raise ValueError(
                        "cannot deduplicate conflicting snapshot watermarks for "
                        f"search event {search_event_id!r}"
                    )
                try:
                    payload = json.loads(str(row["candidate_payload_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        "cannot migrate candidate with invalid payload JSON "
                        f"{row['search_candidate_id']!r}"
                    ) from exc
                if not isinstance(payload, Mapping):
                    raise ValueError(
                        "cannot migrate candidate payload that is not an object "
                        f"{row['search_candidate_id']!r}"
                    )
                if payload.get("trace_id") != str(row["trace_id"]):
                    raise ValueError(
                        "cannot migrate candidate payload trace_id mismatch "
                        f"{row['search_candidate_id']!r}"
                    )
                candidate_hash = _identity_sha256(dict(payload))
                if str(row["candidate_sha256"]) != candidate_hash:
                    raise ValueError(
                        "cannot migrate candidate payload hash mismatch "
                        f"{row['search_candidate_id']!r}"
                    )
                try:
                    feedback_snapshot = json.loads(str(row["feedback_snapshot_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        "cannot migrate candidate with invalid feedback snapshot JSON "
                        f"{row['search_candidate_id']!r}"
                    ) from exc
                if not isinstance(feedback_snapshot, Mapping):
                    raise ValueError(
                        "cannot migrate candidate feedback snapshot that is not an object "
                        f"{row['search_candidate_id']!r}"
                    )
                payload_feedback = payload.get("feedback", {})
                if not isinstance(payload_feedback, Mapping) or dict(payload_feedback) != dict(
                    feedback_snapshot
                ):
                    raise ValueError(
                        "cannot migrate candidate feedback snapshot mismatch "
                        f"{row['search_candidate_id']!r}"
                    )
                if current_candidate_count:
                    current_candidates_hash.update(b",")
                current_candidates_hash.update(_json(dict(payload)).encode("utf-8"))
                current_candidate_count += 1
            flush_current()

            # SQLite 3.35+ supports an atomic DROP COLUMN and is part of the
            # project runtime baseline.  Keep a clear error for an older
            # embedded SQLite instead of silently retaining the duplication.
            try:
                db.execute(
                    "ALTER TABLE search_candidates DROP COLUMN snapshot_watermarks_json"
                )
            except sqlite3.OperationalError as exc:
                raise RuntimeError(
                    "selection store requires SQLite DROP COLUMN support to "
                    "deduplicate candidate snapshot watermarks"
                ) from exc
            if owns_transaction:
                db.execute("COMMIT")
        except BaseException:
            if owns_transaction and db.in_transaction:
                db.execute("ROLLBACK")
            raise

    def register_selector_config(
        self, *, selector_name: str, config: Mapping[str, Any]
    ) -> dict[str, Any]:
        selector_name = _required(selector_name, "selector_name", limit=128)
        config_json = _json(dict(config))
        config_sha256 = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
        selector_config_id = _hash("selector_config", selector_name, config_sha256)
        created_at = _now()
        with self._write() as db:
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
        prepared = self._prepare_rankings(rankings)
        prepared_candidates = self._prepare_candidates(eligible_candidates)
        prepared_watermarks = self._prepare_snapshot_watermarks(snapshot_watermarks)
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

        search_event_id = _hash("search_event", request_key)
        exposure_id = _hash("exposure", search_event_id)
        query_json = _json(query)
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
        watermarks_json = _json(prepared_watermarks)
        created_at = _now()

        with self._write() as db:
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
                result = self._search_chain(db, str(prior["search_event_id"]))
                result["idempotent"] = True
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
                           feedback_snapshot_json
                       ) VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    (
                        candidate_id,
                        search_event_id,
                        candidate["trace_id"],
                        candidate["pool_order"],
                        candidate["candidate_sha256"],
                        _json(candidate["payload"]),
                        _json(candidate["feedback_snapshot"]),
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
                        _json(ranking["component_scores"]),
                        _json(ranking["payload"]),
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
            result = self._search_chain(db, search_event_id)
            result["idempotent"] = False
            return result

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
    ) -> list[dict[str, Any]]:
        if eligible_candidates is None:
            return []
        if isinstance(eligible_candidates, (str, bytes)) or not isinstance(
            eligible_candidates, Sequence
        ):
            raise ValueError("eligible_candidates must be a sequence of mappings")
        payloads: list[dict[str, Any]] = []
        traces: set[str] = set()
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
            _json(payload)
            payloads.append(payload)
        payloads.sort(key=lambda item: item["trace_id"])
        return [
            {
                "trace_id": payload["trace_id"],
                "pool_order": index,
                "candidate_sha256": _identity_sha256(payload),
                "payload": payload,
                "feedback_snapshot": dict(payload.get("feedback", {})),
            }
            for index, payload in enumerate(payloads, 1)
        ]

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

        with self._write() as db:
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
        with self._write() as db:
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
    def _search_chain(db: sqlite3.Connection, search_event_id: str) -> dict[str, Any]:
        search = _decode_row(
            db.execute("SELECT * FROM search_events WHERE search_event_id = ?", (search_event_id,)).fetchone()
        )
        if search is None:
            raise ValueError(f"unknown search_event_id: {search_event_id}")
        exposure = _decode_row(
            db.execute("SELECT * FROM exposures WHERE search_event_id = ?", (search_event_id,)).fetchone()
        )
        rankings = [
            _decode_row(row) or {}
            for row in db.execute(
                "SELECT * FROM search_rankings WHERE search_event_id = ? ORDER BY rank",
                (search_event_id,),
            )
        ]
        candidates = []
        for row in db.execute(
            """SELECT * FROM search_candidates
               WHERE search_event_id = ? ORDER BY pool_order, trace_id""",
            (search_event_id,),
        ):
            candidate = _decode_row(row) or {}
            # Keep the historical in-memory/API shape for callers that use a
            # candidate as a self-contained replay object.  This is a derived
            # projection; the immutable value is physically stored once on
            # ``search_events``.
            candidate.setdefault(
                "snapshot_watermarks",
                copy.deepcopy(search.get("snapshot_watermarks", {})),
            )
            candidates.append(candidate)
        items: list[dict[str, Any]] = []
        if exposure is not None:
            items = [
                _decode_row(row) or {}
                for row in db.execute(
                    "SELECT * FROM exposure_items WHERE exposure_id = ? ORDER BY rank",
                    (exposure["exposure_id"],),
                )
            ]
        return {
            "search_event": search,
            "candidates": candidates,
            "rankings": rankings,
            "exposure": exposure,
            "items": items,
        }

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

        with self._read_snapshot() as db:
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
                with self._read_snapshot() as db:
                    summary = self._summary_from_db(db, db_name=self.path.name)
                    for record_type, table, id_column in self._export_tables_for_db(db):
                        type_count = 0
                        rows = db.execute(
                            f"SELECT * FROM {table} ORDER BY {id_column}"
                        )
                        for row in rows:
                            decoded = _decode_row(row) or {}
                            # The immutable snapshot watermark is exported
                            # once on the parent search_event.  Legacy exports
                            # may still contain a candidate-level copy and are
                            # accepted by the artifact validator, but new
                            # exports deliberately omit that redundant field.
                            envelope = {
                                "schema": EXPORT_SCHEMA_VERSION,
                                "record_type": record_type,
                                "record": decoded,
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
