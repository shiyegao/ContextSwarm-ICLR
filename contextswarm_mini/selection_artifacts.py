"""Portable, fail-closed artifacts for selector and Figure 3 analyses.

This module deliberately has no runner dependency.  It can therefore be used
to audit a completed run without changing the experiment implementation.
"""

from __future__ import annotations

import json
import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "contextswarm_selection_artifacts_v1"
STORE_SCHEMA_VERSION = "contextswarm_selection_store_v1"
STORE_EXPORT_SCHEMA_VERSION = "contextswarm_selection_store_export_v1"

_STORE_RECORD_TYPES = (
    "selector_config",
    "search_event",
    "search_ranking",
    "exposure",
    "exposure_item",
    "feedback_event",
    "verifier_evidence",
    "maintenance_event",
    "trace_relation",
)
_OPTIONAL_STORE_RECORD_TYPES = ("search_candidate",)
_STORE_EXPORT_RECORD_TYPES = ("selector_config", "search_event", "search_candidate", "search_ranking", "exposure", "exposure_item", "feedback_event", "verifier_evidence", "maintenance_event", "trace_relation")
_STORE_PRIMARY_KEYS = {
    "selector_config": "selector_config_id",
    "search_event": "search_event_id",
    "search_ranking": "search_ranking_id",
    "exposure": "exposure_id",
    "exposure_item": "exposure_item_id",
    "feedback_event": "feedback_event_id",
    "verifier_evidence": "evidence_event_id",
    "maintenance_event": "maintenance_event_id",
    "trace_relation": "relation_id",
    "search_candidate": "search_candidate_id",
}
_STORE_TABLE_NAMES = {
    "selector_config": "selector_configs",
    "search_event": "search_events",
    "search_ranking": "search_rankings",
    "exposure": "exposures",
    "exposure_item": "exposure_items",
    "feedback_event": "feedback_events",
    "verifier_evidence": "verifier_evidence",
    "maintenance_event": "maintenance_events",
    "trace_relation": "trace_relations",
    "search_candidate": "search_candidates",
}
_CANONICAL_FEEDBACK_KINDS = frozenset(
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
_CANONICAL_RELATIONS = frozenset(
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


class ArtifactValidationError(ValueError):
    """An artifact is malformed, ambiguous, or cannot be fairly compared."""


def _number(value: Any, name: str, *, nonnegative: bool = True) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ArtifactValidationError(f"{name} must be a finite number")
    result = float(value)
    if not math.isfinite(result) or (nonnegative and result < 0):
        raise ArtifactValidationError(f"{name} must be a finite non-negative number")
    return result


def _text(row: Mapping[str, Any], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value:
        raise ArtifactValidationError(f"{name} must be a non-empty string")
    return value


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ArtifactValidationError(f"{name} must be an object")
    return value


def _validate(row: Mapping[str, Any], kind: str) -> dict[str, Any]:
    result = dict(_mapping(row, kind))
    schema = result.get("schema", SCHEMA_VERSION)
    if schema != SCHEMA_VERSION:
        raise ArtifactValidationError(f"{kind} has unsupported schema {schema!r}")
    result["schema"] = SCHEMA_VERSION
    for name in {"decision": ("decision_id", "selected_task_id", "policy"),
                 "exposure": ("exposure_id", "decision_id", "task_id"),
                 "feedback": ("feedback_id", "exposure_id", "task_id"),
                 "relation": ("relation_id", "decision_id", "exposure_id", "feedback_id", "task_id")}[kind]:
        _text(result, name)
    if kind == "decision":
        _number(result.get("elapsed_seconds", 0), "elapsed_seconds")
    elif kind == "exposure":
        _number(result.get("started_elapsed_seconds", 0), "started_elapsed_seconds")
    elif kind == "feedback":
        _number(result.get("elapsed_seconds"), "elapsed_seconds")
        _number(result.get("score"), "score")
    return result


def write_jsonl(path: str | Path, rows: Iterable[Mapping[str, Any]], kind: str) -> None:
    """Write a typed JSONL artifact after validating every row.

    The function refuses partial output: all rows are validated before the
    destination is replaced.
    """
    rendered = [_validate(row, kind) for row in rows]
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rendered), encoding="utf-8")
    temporary.replace(destination)


def read_jsonl(path: str | Path, kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ArtifactValidationError(f"cannot read {path}: {exc}") from exc
    for line_number, line in enumerate(lines, 1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ArtifactValidationError(f"invalid JSON at {path}:{line_number}") from exc
        rows.append(_validate(row, kind))
    return rows


def write_selector_decisions(path: str | Path, rows: Iterable[Mapping[str, Any]]) -> None:
    write_jsonl(path, rows, "decision")


def read_selector_decisions(path: str | Path) -> list[dict[str, Any]]:
    return read_jsonl(path, "decision")


def write_exposures(path: str | Path, rows: Iterable[Mapping[str, Any]]) -> None:
    write_jsonl(path, rows, "exposure")


def read_exposures(path: str | Path) -> list[dict[str, Any]]:
    return read_jsonl(path, "exposure")


def write_feedback(path: str | Path, rows: Iterable[Mapping[str, Any]]) -> None:
    write_jsonl(path, rows, "feedback")


def read_feedback(path: str | Path) -> list[dict[str, Any]]:
    return read_jsonl(path, "feedback")


def write_relations(path: str | Path, rows: Iterable[Mapping[str, Any]]) -> None:
    write_jsonl(path, rows, "relation")


def read_relations(path: str | Path) -> list[dict[str, Any]]:
    return read_jsonl(path, "relation")


def _index(rows: Sequence[Mapping[str, Any]], key: str, name: str) -> dict[str, Mapping[str, Any]]:
    indexed: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        identifier = _text(row, key)
        if identifier in indexed:
            raise ArtifactValidationError(f"duplicate {name} {identifier!r}")
        indexed[identifier] = row
    return indexed


def validate_attribution_joins(
    decisions: Sequence[Mapping[str, Any]], exposures: Sequence[Mapping[str, Any]],
    feedback: Sequence[Mapping[str, Any]], relations: Sequence[Mapping[str, Any]],
) -> None:
    """Prove every result has a consistent decision -> exposure -> feedback path."""
    decisions = [_validate(row, "decision") for row in decisions]
    exposures = [_validate(row, "exposure") for row in exposures]
    feedback = [_validate(row, "feedback") for row in feedback]
    relations = [_validate(row, "relation") for row in relations]
    decision_ids = _index(decisions, "decision_id", "decision")
    exposure_ids = _index(exposures, "exposure_id", "exposure")
    feedback_ids = _index(feedback, "feedback_id", "feedback")
    for exposure in exposures:
        decision = decision_ids.get(exposure["decision_id"])
        if decision is None:
            raise ArtifactValidationError(f"orphan exposure {exposure['exposure_id']!r}")
        if decision["selected_task_id"] != exposure["task_id"]:
            raise ArtifactValidationError("exposure task does not match its selector decision")
    for item in feedback:
        exposure = exposure_ids.get(item["exposure_id"])
        if exposure is None:
            raise ArtifactValidationError(f"orphan feedback {item['feedback_id']!r}")
        if exposure["task_id"] != item["task_id"]:
            raise ArtifactValidationError("feedback task does not match its exposure")
    for relation in relations:
        decision = decision_ids.get(relation["decision_id"])
        exposure = exposure_ids.get(relation["exposure_id"])
        item = feedback_ids.get(relation["feedback_id"])
        if decision is None or exposure is None or item is None:
            raise ArtifactValidationError(f"relation {relation['relation_id']!r} has an orphan reference")
        if not (decision["selected_task_id"] == exposure["task_id"] == item["task_id"] == relation["task_id"]):
            raise ArtifactValidationError("relation attribution task mismatch")


def reconstruct_metrics(
    feedback: Sequence[Mapping[str, Any]], *, task_order: Sequence[str], horizon_seconds: float,
    exposures: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Reconstruct score-time metrics solely from attributed terminal feedback."""
    horizon = _number(horizon_seconds, "horizon_seconds")
    tasks = list(task_order)
    if not tasks or not all(isinstance(item, str) and item for item in tasks):
        raise ArtifactValidationError("task_order must contain unique non-empty task ids")
    if len(set(tasks)) != len(tasks):
        raise ArtifactValidationError("task_order must contain unique non-empty task ids")
    known = set(tasks)
    events: list[tuple[float, str, float]] = []
    for raw in feedback:
        item = _validate(raw, "feedback")
        if item["task_id"] not in known:
            raise ArtifactValidationError(f"feedback task {item['task_id']!r} is outside task_order")
        events.append((min(item["elapsed_seconds"], horizon), item["task_id"], item["score"]))
    events.sort()
    score_by_task = {task: 0.0 for task in tasks}
    reached: dict[int, float] = {}
    area = previous_time = total = 0.0
    for elapsed, task, score in events:
        area += total * (elapsed - previous_time)
        previous_time = elapsed
        old = score_by_task[task]
        new = max(old, min(1.0, score))
        score_by_task[task] = new
        total += new - old
        for k in range(1, len(tasks) + 1):
            if k not in reached and total >= k:
                reached[k] = elapsed
    area += total * (horizon - previous_time)
    exposure_rows = [_validate(row, "exposure") for row in exposures]
    return {
        "final_score": total,
        "time_to_k_proofs_seconds": {str(k): reached.get(k) for k in range(1, len(tasks) + 1)},
        "normalized_score_time_auc": area / (horizon * len(tasks)) if horizon and tasks else 0.0,
        "usage": {"exposure_count": len(exposure_rows), "tasks_exposed": len({row["task_id"] for row in exposure_rows})},
    }


_PAIR_FIELDS = (
    "comparison_contract",
    "task_order",
    "paired_seed",
    "model",
    "horizon_seconds",
    "cps_capacity",
    "evaluator",
    "runtime",
)
_FIGURE3_CONTRACT_SCHEMA = "contextswarm_figure3_contract_v1"
_SELECTION_EXPORT_FILENAMES = (
    # ``selection_events.jsonl`` is the runner closeout spelling.  The other
    # names are retained for exports produced by the standalone auditor and
    # early development runs.
    "selection_events.jsonl",
    "selection_export.jsonl",
    "selection_attribution.probe.jsonl",
)


def _canonical_json(value: Any) -> str:
    """Render an input for an identity comparison without accepting NaN."""

    try:
        return json.dumps(
            value,
            # SelectionStore's canonical identity helper serializes Unicode
            # directly (rather than escaping it).  Keep this byte-for-byte
            # compatible so non-ASCII trace titles/bodies and comparison
            # contracts validate against the exported hashes.
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ArtifactValidationError("comparison_contract is not JSON-compatible") from exc


def _identity_sha256(value: Any) -> str:
    """Match SelectionStore's canonical identity hashing rule."""

    if isinstance(value, str):
        candidate = value.strip().lower()
        if len(candidate) == 64 and all(character in "0123456789abcdef" for character in candidate):
            return candidate
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _contract_id(value: Any) -> str | None:
    """Extract an operator-supplied contract identity.

    The runner records the opaque ``comparison_contract_id``.  Callers may
    pass that ID directly, a mapping containing the ID, or the full canonical
    contract object (whose identity is its SHA-256).  We never compare a
    pretty-printed mapping or silently accept a different contract.
    """

    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    if isinstance(value, Mapping):
        for key in ("comparison_contract_id", "comparison_contract", "contract_id", "id"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
    raise ArtifactValidationError("comparison_contract must be a string or object")


def _validated_task_order(value: Any, name: str = "task_order") -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ArtifactValidationError(f"{name} must contain unique non-empty task ids")
    result = list(value)
    if not result or not all(isinstance(task, str) and task.strip() for task in result):
        raise ArtifactValidationError(f"{name} must contain unique non-empty task ids")
    if len(set(result)) != len(result):
        raise ArtifactValidationError(f"{name} must contain unique non-empty task ids")
    return [task.strip() for task in result]


def _validated_figure3_contract(meta: Mapping[str, Any]) -> dict[str, Any] | None:
    """Read the runner-owned Figure 3 contract from ``run_meta.json``.

    A missing block is tolerated only for legacy, selection-less fixtures
    created before Issue #38.  Once a run declares selection metadata, the
    contract is mandatory; guessing it from a caller argument would make a
    paired comparison non-reproducible.
    """

    raw = meta.get("figure3")
    if raw is None:
        selection = meta.get("selection")
        if isinstance(selection, Mapping) and selection.get("enabled") is True:
            raise ArtifactValidationError(
                "selection-enabled run_meta is missing runner-owned figure3 contract"
            )
        return None
    block = dict(_mapping(raw, "run_meta.figure3"))
    schema = block.get("schema_version", _FIGURE3_CONTRACT_SCHEMA)
    if schema != _FIGURE3_CONTRACT_SCHEMA:
        raise ArtifactValidationError(
            f"run_meta.figure3 has unsupported schema {schema!r}"
        )
    contract_id = block.get("comparison_contract_id")
    if not isinstance(contract_id, str) or not contract_id.strip():
        raise ArtifactValidationError("run_meta.figure3.comparison_contract_id is required")
    tasks = _validated_task_order(block.get("task_order"), "run_meta.figure3.task_order")
    seed = block.get("paired_seed")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ArtifactValidationError("run_meta.figure3.paired_seed must be non-negative")
    result = {
        "schema_version": _FIGURE3_CONTRACT_SCHEMA,
        "comparison_contract_id": contract_id.strip(),
        "task_order": tasks,
        "paired_seed": seed,
    }
    for field in ("selector_name", "selector_version", "selection_config_id"):
        value = block.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ArtifactValidationError(f"run_meta.figure3.{field} is required")
        result[field] = value.strip()
    declared_seed = meta.get("seed")
    if declared_seed is not None and declared_seed != result["paired_seed"]:
        raise ArtifactValidationError("run_meta.seed disagrees with run_meta.figure3.paired_seed")
    selection = meta.get("selection")
    if isinstance(selection, Mapping):
        if selection.get("enabled") is False:
            raise ArtifactValidationError("run_meta.figure3 requires enabled selection")
        for field in ("selector_name", "selector_version", "selection_config_id"):
            actual = selection.get(field)
            if actual is not None and actual != result[field]:
                raise ArtifactValidationError(
                    f"run_meta.selection.{field} disagrees with run_meta.figure3"
                )
        for field in ("direct_messages", "candidate_transfer"):
            if selection.get(field) is True:
                raise ArtifactValidationError(
                    f"run_meta.selection.{field} violates Figure 3 isolation"
                )
    return result


def _check_identity_file(
    root: Path,
    filename: str,
    contract: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    """Cross-check optional runner selection artifacts against Figure 3."""

    path = root / filename
    if not path.exists():
        return None
    value = _read_object(path, filename)
    for field in ("comparison_contract_id", "selector_name", "selector_version", "selection_config_id"):
        expected = contract[field]
        actual = value.get(field)
        if actual is not None and actual != expected:
            raise ArtifactValidationError(f"{filename}.{field} disagrees with run_meta.figure3")
    status = value.get("status")
    if status is not None and status not in {"closed", "broker_not_drained", "dry_run"}:
        raise ArtifactValidationError(f"{filename}.status is not a closed selection status")
    return value


def _safe_artifact_path(root: Path, raw: Any, name: str) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise ArtifactValidationError(f"{name}.path must be a non-empty relative path")
    candidate = Path(raw)
    if candidate.is_absolute():
        raise ArtifactValidationError(f"{name}.path must be relative to the run directory")
    resolved_root = root.resolve()
    resolved = (root / candidate).resolve()
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise ArtifactValidationError(f"{name}.path escapes the run directory")
    return resolved


def _read_accepted_score_history(
    root: Path,
    task_order: Sequence[str],
    horizon: float,
) -> list[dict[str, Any]]:
    """Return the complete bounded monotonic accepted-score event history."""

    path = root / "scoreboard_history.jsonl"
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ArtifactValidationError(f"cannot read scoreboard_history.jsonl: {exc}") from exc
    known = set(task_order)
    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ArtifactValidationError(
                f"invalid scoreboard history JSON at line {line_number}"
            ) from exc
        if not isinstance(raw, Mapping):
            raise ArtifactValidationError("scoreboard history rows must be objects")
        task = raw.get("task_id")
        if not isinstance(task, str) or task not in known:
            raise ArtifactValidationError("scoreboard history contains an unknown task")
        score = raw.get("score")
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            raise ArtifactValidationError("scoreboard history has an invalid score")
        score_float = float(score)
        if not math.isfinite(score_float) or score_float < 0.0 or score_float > 1.0:
            raise ArtifactValidationError("scoreboard history has an invalid score")
        if str(raw.get("source") or "") == "closeout":
            # Closeout verification is not an in-horizon accepted-score event.
            continue
        elapsed = raw.get("horizon_elapsed_seconds", raw.get("elapsed_seconds"))
        if elapsed is not None:
            if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)):
                raise ArtifactValidationError("scoreboard history has invalid elapsed time")
            elapsed = float(elapsed)
            if not math.isfinite(elapsed) or elapsed < 0.0:
                raise ArtifactValidationError("scoreboard history elapsed time is outside horizon")
            # A solver attempt may finish just after the fixed horizon while
            # the runner is settling its terminal receipt.  Such a row is
            # still an ordinary candidate-attempt outcome, not an invalid
            # artifact.  The score-time contract treats progress observed at
            # or after the deadline as occurring at the deadline, so clamp it
            # here to keep export validation consistent with runner metrics.
            elapsed = min(elapsed, max(0.0, horizon))
        episode = raw.get("episode")
        if episode is not None and (
            isinstance(episode, bool) or not isinstance(episode, int) or episode < 0
        ):
            raise ArtifactValidationError("scoreboard history has an invalid episode")
        events.append(
            {
                "task_id": task,
                "episode": episode,
                "score": score_float,
                "elapsed_seconds": elapsed,
            }
        )
    events.sort(
        key=lambda row: (
            float("inf") if row["elapsed_seconds"] is None else row["elapsed_seconds"],
            row["task_id"],
            row.get("episode") if row.get("episode") is not None else -1,
        )
    )
    best = {task: 0.0 for task in task_order}
    accepted: list[dict[str, Any]] = []
    for event in events:
        task = event["task_id"]
        if event["score"] <= best[task]:
            continue
        best[task] = event["score"]
        accepted.append(event)
    return accepted


def _selection_artifact_evidence(
    root: Path,
    selection_summary: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Locate, validate, and summarize a typed selection JSONL export."""

    declared: Mapping[str, Any] | None = None
    if selection_summary is not None:
        for key in ("artifact", "export", "selection_artifact"):
            candidate = selection_summary.get(key)
            if isinstance(candidate, Mapping):
                declared = candidate
                break
    candidates: list[Path] = []
    if declared is not None:
        if declared.get("path") is None:
            raise ArtifactValidationError("selection artifact declaration is missing path")
        candidates.append(_safe_artifact_path(root, declared.get("path"), "selection artifact"))
    for filename in _SELECTION_EXPORT_FILENAMES:
        path = root / filename
        if path.exists() and path not in candidates:
            candidates.append(path)
    if not candidates:
        return None, None
    path = candidates[0]
    if not path.exists():
        raise ArtifactValidationError(f"declared selection artifact is missing: {path.name}")
    artifact_summary = validate_selection_store_export(path)
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    info: dict[str, Any] = dict(declared or {})
    try:
        relative = path.resolve().relative_to(root.resolve())
        info["path"] = str(relative)
    except ValueError as exc:
        raise ArtifactValidationError("selection artifact path escapes run directory") from exc
    info.update(
        {
            "schema": STORE_EXPORT_SCHEMA_VERSION,
            "sha256": digest,
            "record_count": len(raw.splitlines()),
            "record_type_counts": artifact_summary.get("record_type_counts", {}),
            "summary": artifact_summary,
        }
    )
    if artifact_summary.get("record_count") != info["record_count"]:
        raise ArtifactValidationError("selection artifact row count does not reconcile")
    if declared is not None:
        expected_digest = declared.get("sha256")
        if expected_digest is not None and expected_digest != digest:
            raise ArtifactValidationError("selection artifact sha256 does not match declaration")
        expected_count = declared.get("record_count")
        if expected_count is not None and expected_count != info["record_count"]:
            raise ArtifactValidationError("selection artifact record_count does not match declaration")
        expected_counts = declared.get("record_type_counts")
        if expected_counts is not None:
            if not isinstance(expected_counts, Mapping):
                raise ArtifactValidationError("selection artifact record_type_counts must be an object")
            if dict(expected_counts) != info["record_type_counts"]:
                raise ArtifactValidationError("selection artifact record_type_counts do not match declaration")
        expected_schema = declared.get("schema")
        if expected_schema is not None and expected_schema != STORE_EXPORT_SCHEMA_VERSION:
            raise ArtifactValidationError("selection artifact schema does not match declaration")
    if selection_summary is not None:
        store_summary = selection_summary.get("store_summary")
        if store_summary is not None:
            store_summary = _mapping(store_summary, "selection_summary.store_summary")
            for field in (
                "counts",
                "selector_config_ids",
                "selector_configs",
                "comparison_contract_ids",
            ):
                if store_summary.get(field) != artifact_summary.get(field):
                    raise ArtifactValidationError(
                        f"selection_summary.store_summary.{field} does not match selection artifact"
                    )
        top_counts = selection_summary.get("counts")
        if top_counts is not None and top_counts != artifact_summary.get("counts"):
            raise ArtifactValidationError(
                "selection_summary.counts do not match selection artifact"
            )
    return info, artifact_summary


def write_figure3_run_summary(path: str | Path, summary: Mapping[str, Any]) -> None:
    row = dict(_mapping(summary, "figure3 summary"))
    row["schema"] = SCHEMA_VERSION
    _text(row, "run_id")
    metadata = _mapping(row.get("metadata"), "metadata")
    for field in _PAIR_FIELDS:
        if field not in metadata:
            raise ArtifactValidationError(f"metadata missing {field}")
    metrics = _mapping(row.get("metrics"), "metrics")
    for field in (
        "accepted_score_history",
        "final_score",
        "time_to_k_proofs_seconds",
        "normalized_score_time_auc",
        "usage",
    ):
        if field not in metrics:
            raise ArtifactValidationError(f"metrics missing {field}")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(row, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def read_figure3_run_summary(path: str | Path) -> dict[str, Any]:
    try:
        row = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError(f"cannot read Figure 3 summary {path}") from exc
    # Reuse writer validation without mutating the caller's path.
    if row.get("schema", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise ArtifactValidationError("figure3 summary has unsupported schema")
    _text(row, "run_id")
    metadata = _mapping(row.get("metadata"), "metadata")
    for field in _PAIR_FIELDS:
        if field not in metadata:
            raise ArtifactValidationError(f"metadata missing {field}")
    metrics = _mapping(row.get("metrics"), "metrics")
    for field in (
        "accepted_score_history",
        "final_score",
        "time_to_k_proofs_seconds",
        "normalized_score_time_auc",
        "usage",
    ):
        if field not in metrics:
            raise ArtifactValidationError(f"metrics missing {field}")
    return dict(row)


def _read_object(path: Path, name: str) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError(f"cannot read {name}: {path}") from exc
    return _mapping(value, name)


def _runner_usage(run_dir: Path, final: Mapping[str, Any]) -> dict[str, Any]:
    """Aggregate bounded, candidate-independent usage from runner artifacts."""
    allocation = final.get("allocation")
    allocation = allocation if isinstance(allocation, Mapping) else {}
    usage: dict[str, Any] = {
        "solver_agent_count": len(final.get("agents") or []),
        "scheduler_agent_count": len(final.get("allocation_scheduler_agents") or []),
        "allocation_policy": allocation.get("policy"),
        "allocation_agent_calls": allocation.get("agent_calls"),
    }
    # pi_events records cumulative usage per session.  Take a max per session,
    # then sum sessions, matching runner's scheduler accounting and avoiding
    # double-counting repeated updates.
    sessions: dict[str, dict[str, int]] = {}
    try:
        lines = (run_dir / "pi_events.jsonl").read_text(encoding="utf-8").splitlines()
    except OSError:
        lines = []
    fields = {
        "input_tokens": "model_input_tokens", "output_tokens": "model_output_tokens",
        "cache_read_tokens": "model_cache_read_tokens", "cache_write_tokens": "model_cache_write_tokens",
        "total_tokens": "model_total_tokens",
    }
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        actor = str(row.get("actor_id") or "")
        session = str(row.get("session_id") or actor or "unknown")
        current = sessions.setdefault(session, {})
        for source, target in fields.items():
            value = row.get(source)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                current[source] = max(current.get(source, 0), value)
    usage["model_sessions"] = len(sessions)
    for source, target in fields.items():
        usage[target] = sum(item.get(source, 0) for item in sessions.values())
    return usage


def build_figure3_run_summary(
    run_dir: str | Path,
    *,
    comparison_contract: Any = None,
    task_order: Sequence[str] | None = None,
    paired_seed: int | None = None,
) -> dict[str, Any]:
    """Build a portable Figure 3 row from an existing runner artifact pair.

    New runs are bound to the runner-owned ``run_meta.figure3`` contract.  The
    optional arguments may assert (but never override) that identity.  Legacy
    selection-less artifacts may still pass the contract and task order
    explicitly so older offline fixtures remain readable.
    """
    root = Path(run_dir)
    final = _read_object(root / "final.json", "final.json")
    meta = _read_object(root / "run_meta.json", "run_meta.json")
    contract = _validated_figure3_contract(meta)
    if contract is not None:
        tasks = list(contract["task_order"])
        supplied_tasks = tasks if task_order is None else _validated_task_order(task_order)
        if supplied_tasks != tasks:
            raise ArtifactValidationError(
                "task_order disagrees with runner-owned run_meta.figure3 contract"
            )
        comparison_id = contract["comparison_contract_id"]
        supplied_id = _contract_id(comparison_contract)
        if supplied_id is not None and supplied_id != comparison_id:
            raise ArtifactValidationError(
                "comparison_contract disagrees with runner-owned run_meta.figure3 contract"
            )
        seed = contract["paired_seed"]
        if paired_seed is not None and paired_seed != seed:
            raise ArtifactValidationError(
                "paired_seed disagrees with runner-owned run_meta.figure3 contract"
            )
        selector_identity = {
            "selector_name": contract["selector_name"],
            "selector_version": contract["selector_version"],
            "selection_config_id": contract["selection_config_id"],
        }
    else:
        if task_order is None or comparison_contract is None:
            raise ArtifactValidationError(
                "legacy run has no Figure 3 contract; comparison_contract and task_order are required"
            )
        tasks = _validated_task_order(task_order)
        comparison_id = _contract_id(comparison_contract)
        if comparison_id is None:
            raise ArtifactValidationError("comparison_contract must be non-empty")
        seed = meta.get("seed") if paired_seed is None else paired_seed
        selector_identity = {
            "selector_name": None,
            "selector_version": None,
            "selection_config_id": None,
        }
    verdicts = _mapping(final.get("verdicts"), "final.verdicts")
    if set(verdicts) != set(tasks):
        raise ArtifactValidationError("task_order does not exactly match final verdict tasks")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ArtifactValidationError("paired_seed must be a non-negative integer")
    final_score = _number(final.get("score"), "final.score")
    verdict_score = sum(_number(_mapping(value, "verdict").get("score"), "verdict.score") for value in verdicts.values())
    if not math.isclose(final_score, verdict_score, rel_tol=0.0, abs_tol=1e-9):
        raise ArtifactValidationError("final.score does not match verdict scores")
    horizon = _number(final.get("horizon_seconds"), "final.horizon_seconds")
    if _number(meta.get("time_limit_seconds"), "run_meta.time_limit_seconds") != horizon:
        raise ArtifactValidationError("final and run_meta horizons differ")
    score_time = _mapping(final.get("score_time"), "final.score_time")
    normalized_auc = _number(score_time.get("normalized_score_time_auc"), "score_time.normalized_score_time_auc")
    time_to_k = _mapping(score_time.get("time_to_k_proofs_seconds"), "score_time.time_to_k_proofs_seconds")
    for key, value in time_to_k.items():
        if not isinstance(key, str) or (value is not None and _number(value, f"time_to_k[{key}]") > horizon):
            raise ArtifactValidationError("invalid score_time time_to_k_proofs_seconds")
    allocation = final.get("allocation")
    allocation_data = dict(allocation) if isinstance(allocation, Mapping) else {}
    runtime = {
        "effective_runtime_limits": meta.get("effective_runtime_limits"),
        "runtime_provenance": meta.get("runtime_provenance"),
        "pi_timeout_seconds": meta.get("pi_timeout_seconds"),
    }
    if not all(value is not None for value in runtime.values()):
        raise ArtifactValidationError("run_meta lacks runtime metadata needed for paired analysis")
    evaluator = {
        "judge_kind": meta.get("judge_kind"),
        "lean_env_id": meta.get("lean_env_id"),
        "lean_verification_profile": meta.get("lean_verification_profile"),
        "lean_max_concurrent_evaluations": meta.get("lean_max_concurrent_evaluations"),
        "judge_result_cache": final.get("judge_result_cache"),
    }
    if any(value is None for value in evaluator.values()):
        raise ArtifactValidationError("run artifacts lack evaluator metadata needed for paired analysis")
    selection_summary: Mapping[str, Any] | None = None
    if contract is not None:
        _check_identity_file(root, "selection_runtime.json", contract)
        selection_summary = _check_identity_file(root, "selection_summary.json", contract)
        final_selection = final.get("selection")
        if isinstance(final_selection, Mapping):
            for field in (
                "comparison_contract_id",
                "selector_name",
                "selector_version",
                "selection_config_id",
            ):
                expected = contract[field]
                actual = final_selection.get(field)
                if actual is not None and actual != expected:
                    raise ArtifactValidationError(
                        f"final.selection.{field} disagrees with run_meta.figure3"
                    )
    artifact_info, artifact_summary = _selection_artifact_evidence(root, selection_summary)
    if contract is not None and artifact_summary is not None:
        contract_ids = artifact_summary.get("comparison_contract_ids")
        if contract_ids and contract_ids != [contract["comparison_contract_id"]]:
            # SelectionStore canonicalizes opaque identities to a SHA-256.  A
            # runner contract that is already a digest must match exactly;
            # otherwise match its canonical hash.
            expected = contract["comparison_contract_id"]
            if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
                expected = hashlib.sha256(_canonical_json(expected).encode("utf-8")).hexdigest()
            if contract_ids != [expected]:
                raise ArtifactValidationError(
                    "selection artifact comparison contract disagrees with run_meta.figure3"
                )
        config_ids = artifact_summary.get("selector_config_ids") or []
        registered_id = None
        if selection_summary is not None:
            registered_id = selection_summary.get("registered_selector_config_id")
        if registered_id is not None and config_ids != [registered_id]:
            raise ArtifactValidationError(
                "selection artifact selector config disagrees with selection_summary"
            )
    selection_usage = dict(artifact_summary.get("usage") or {}) if artifact_summary else {}
    selection_counts = dict(artifact_summary.get("counts") or {}) if artifact_summary else {}
    usage = {
        **_runner_usage(root, final),
        "trace_selection": selection_usage,
        "feedback": {
            "feedback_event_count": selection_counts.get("feedback_events", 0),
            "effective_feedback_count": selection_counts.get("effective_feedback_events", 0),
            "terminal_feedback_count": selection_counts.get("terminal_feedback_events", 0),
            "nonterminal_feedback_count": selection_counts.get("nonterminal_feedback_events", 0),
            "conflicting_terminal_feedback_count": selection_counts.get(
                "conflicting_terminal_feedback_events", 0
            ),
        },
    }
    accepted_history = _read_accepted_score_history(root, tasks, horizon)
    return {
        "schema": SCHEMA_VERSION,
        "run_id": _text(meta, "run_id"),
        "metadata": {
            "comparison_contract": comparison_id,
            "comparison_contract_id": comparison_id,
            "task_order": tasks,
            "paired_seed": seed,
            "model": _text(meta, "model"),
            "horizon_seconds": horizon,
            "cps_capacity": _number(meta.get("max_parallel"), "run_meta.max_parallel"),
            "evaluator": evaluator,
            "runtime": runtime,
            **selector_identity,
        },
        "metrics": {
            "accepted_score_history": accepted_history,
            "final_score": final_score,
            "time_to_k_proofs_seconds": dict(time_to_k),
            "normalized_score_time_auc": normalized_auc,
            "usage": usage,
        },
        "artifacts": {
            "run_meta": "run_meta.json",
            "final": "final.json",
            "scoreboard_history": (
                "scoreboard_history.jsonl"
                if (root / "scoreboard_history.jsonl").exists()
                else None
            ),
            "selection_store": (
                "selection.sqlite3" if (root / "selection.sqlite3").exists() else None
            ),
            "selection_export": artifact_info,
        },
    }


def export_figure3_run_summary(
    run_dir: str | Path,
    *,
    comparison_contract: Any = None,
    task_order: Sequence[str] | None = None,
    paired_seed: int | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build and write ``figure3_summary.json`` (or ``output_path``)."""
    summary = build_figure3_run_summary(
        run_dir,
        comparison_contract=comparison_contract,
        task_order=task_order,
        paired_seed=paired_seed,
    )
    write_figure3_run_summary(output_path or Path(run_dir) / "figure3_summary.json", summary)
    return summary


def _store_int(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        qualifier = "positive" if minimum == 1 else "non-negative"
        raise ArtifactValidationError(f"{name} must be a {qualifier} integer")
    return value


def _store_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ArtifactValidationError(f"{name} must be a boolean")
    return value


def _store_sha256(row: Mapping[str, Any], name: str) -> str:
    value = _text(row, name)
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ArtifactValidationError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _store_object(row: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = _mapping(row.get(name), name)
    # ``json.loads`` accepts NaN/Infinity by default even though they are not
    # valid JSON and SelectionStore never emits them (its writer uses
    # ``allow_nan=False``).  Re-serializing every object here keeps the
    # portable validator fail-closed for hand-edited or otherwise malformed
    # exports instead of allowing non-replayable values through.
    try:
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ArtifactValidationError(f"{name} is not canonical JSON") from exc
    return value


def _store_json_value(value: Any, name: str) -> Any:
    """Validate a scalar-or-object field against SelectionStore JSON rules."""

    try:
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ArtifactValidationError(f"{name} is not canonical JSON") from exc
    return value


def _store_digest(value: Any, name: str) -> str:
    """Mirror SelectionStore's canonical JSON digest for exported payloads."""

    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ArtifactValidationError(f"{name} is not canonical JSON") from exc
    return hashlib.sha256(encoded).hexdigest()


def _validate_store_record(record_type: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one decoded SelectionStore row without importing its runtime."""

    row = dict(_mapping(raw, f"{record_type}.record"))
    _text(row, _STORE_PRIMARY_KEYS[record_type])
    if record_type == "search_candidate":
        for name in ("search_candidate_id", "search_event_id", "trace_id"):
            _text(row, name)
        _store_int(row.get("pool_order"), "search_candidate.pool_order", minimum=1)
        _store_sha256(row, "candidate_sha256")
        payload = _store_object(row, "candidate_payload")
        if row["candidate_sha256"] != _store_digest(payload, "candidate_payload"):
            raise ArtifactValidationError("search candidate hash does not match candidate payload")
        _store_object(row, "feedback_snapshot")
        # New exports derive the immutable selection watermark from their
        # parent search event and therefore omit this candidate field.  Keep
        # accepting the field for v1/legacy exports so old artifacts remain
        # replayable and can be compared without rewriting history.
        if "snapshot_watermarks" in row:
            _store_object(row, "snapshot_watermarks")
    elif record_type == "selector_config":
        _text(row, "selector_name")
        _store_sha256(row, "config_sha256")
        config = _store_object(row, "config")
        if row["config_sha256"] != _identity_sha256(config):
            raise ArtifactValidationError(
                "selector config hash does not match config payload"
            )
    elif record_type == "search_event":
        for name in (
            "request_key", "task_id", "actor_id", "selector_config_id",
            "search_event_id",
        ):
            _text(row, name)
        for name in (
            "search_sha256", "config_sha256", "comparison_sha256",
            "snapshot_sha256", "pool_sha256",
        ):
            _store_sha256(row, name)
        # A selector may accept a string or an object query.  What matters for
        # replay is that the decoded value is present and JSON-compatible.
        if "query" not in row:
            raise ArtifactValidationError("search_event.query is required")
        _store_json_value(row["query"], "search_event.query")
        # Pool snapshot columns are optional for legacy exports.  If one is
        # present, require the complete triplet so replay cannot silently
        # mix a candidate list with a different watermark.
        pool_fields = {
            "eligible_candidates_sha256",
            "snapshot_watermarks_sha256",
            "snapshot_watermarks",
        }
        present_pool_fields = pool_fields & set(row)
        def _has_pool_value(name: str) -> bool:
            value = row.get(name)
            return value not in (None, "", {})

        nonempty_pool_fields = {name for name in present_pool_fields if _has_pool_value(name)}
        # The store's migration defaults legacy rows to empty strings / an
        # empty object.  Treat an entirely absent triplet, or an explicitly
        # present all-empty triplet, as "not supplied".  Any partial triplet
        # is rejected even when its present values are empty: otherwise an
        # artifact can silently lose one side of the pool identity contract.
        if present_pool_fields and present_pool_fields != pool_fields:
            raise ArtifactValidationError("search event pool snapshot fields must be complete")
        if nonempty_pool_fields:
            _store_sha256(row, "eligible_candidates_sha256")
            _store_sha256(row, "snapshot_watermarks_sha256")
            _store_object(row, "snapshot_watermarks")
    elif record_type == "search_ranking":
        for name in ("search_ranking_id", "search_event_id", "trace_id"):
            _text(row, name)
        _store_int(row.get("rank"), "search_ranking.rank", minimum=1)
        _store_bool(row.get("selected"), "search_ranking.selected")
        scores = _store_object(row, "component_scores")
        for name, value in scores.items():
            if not isinstance(name, str) or not name:
                raise ArtifactValidationError("component score names must be non-empty strings")
            # Random's auditable component is a hash key rather than a
            # numeric score; all other selectors normally emit finite
            # numbers.  Preserve either representation without accepting
            # arbitrary nested values.
            if isinstance(value, str):
                if not value or len(value) > 512:
                    raise ArtifactValidationError(f"component_scores[{name!r}] string is invalid")
            else:
                _number(value, f"component_scores[{name!r}]", nonnegative=False)
        _store_object(row, "ranking_payload")
    elif record_type == "exposure":
        for name in ("exposure_id", "search_event_id", "actor_id"):
            _text(row, name)
    elif record_type == "exposure_item":
        for name in (
            "exposure_item_id", "exposure_id", "search_ranking_id", "trace_id",
        ):
            _text(row, name)
        _store_int(row.get("rank"), "exposure_item.rank", minimum=1)
    elif record_type == "feedback_event":
        for name in (
            "feedback_event_id", "request_key", "exposure_item_id", "trace_id",
            "actor_id", "event_class", "feedback_kind", "origin",
        ):
            _text(row, name)
        if row["event_class"] != "worker_interaction":
            raise ArtifactValidationError("feedback event is not worker interaction feedback")
        if row["feedback_kind"] not in _CANONICAL_FEEDBACK_KINDS:
            raise ArtifactValidationError("feedback event uses an unknown canonical kind")
        _store_bool(row.get("terminal"), "feedback_event.terminal")
        _store_bool(row.get("effective"), "feedback_event.effective")
        if row["effective"] and not row["terminal"]:
            raise ArtifactValidationError("effective feedback must be terminal")
        conflict = row.get("conflicts_with_feedback_event_id")
        if conflict is not None and (not isinstance(conflict, str) or not conflict):
            raise ArtifactValidationError("feedback conflict reference must be null or non-empty")
        if row["effective"] and conflict is not None:
            raise ArtifactValidationError("effective feedback cannot conflict with another event")
        _store_object(row, "payload")
    elif record_type == "verifier_evidence":
        for name in ("evidence_event_id", "request_key", "trace_id", "verifier_id", "status"):
            _text(row, name)
        _store_object(row, "evidence")
    elif record_type == "maintenance_event":
        for name in ("maintenance_event_id", "request_key", "trace_id", "actor_id", "maintenance_kind"):
            _text(row, name)
        _store_object(row, "payload")
    elif record_type == "trace_relation":
        for name in ("relation_id", "request_key", "source_trace_id", "target_trace_id", "relation_kind", "actor_id"):
            _text(row, name)
        if row["source_trace_id"] == row["target_trace_id"]:
            raise ArtifactValidationError("trace relation cannot target itself")
        if row["relation_kind"] not in _CANONICAL_RELATIONS:
            raise ArtifactValidationError("trace relation uses an unknown canonical kind")
        _store_object(row, "payload")
    return row


def _store_rows_by_type(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped = {record_type: [] for record_type in _STORE_EXPORT_RECORD_TYPES}
    seen: dict[str, set[str]] = {record_type: set() for record_type in _STORE_EXPORT_RECORD_TYPES}
    last_type_position = -1
    last_primary_key: dict[str, str] = {}
    for index, raw in enumerate(rows, 1):
        envelope = _mapping(raw, f"selection store row {index}")
        if set(envelope) != {"schema", "record_type", "record"}:
            raise ArtifactValidationError(
                f"selection store row {index} must contain only schema, record_type, and record"
            )
        if envelope.get("schema") != STORE_EXPORT_SCHEMA_VERSION:
            raise ArtifactValidationError(
                f"selection store row {index} has unsupported schema {envelope.get('schema')!r}"
            )
        record_type = envelope.get("record_type")
        if not isinstance(record_type, str) or record_type not in grouped:
            raise ArtifactValidationError(f"selection store row {index} has unknown record_type")
        type_position = _STORE_EXPORT_RECORD_TYPES.index(record_type)
        if type_position < last_type_position:
            raise ArtifactValidationError("selection store records are not in referential export order")
        last_type_position = type_position
        row = _validate_store_record(record_type, envelope.get("record"))
        primary_key = row[_STORE_PRIMARY_KEYS[record_type]]
        if primary_key in seen[record_type]:
            raise ArtifactValidationError(f"duplicate {record_type} {primary_key!r}")
        previous = last_primary_key.get(record_type)
        if previous is not None and primary_key < previous:
            raise ArtifactValidationError(f"{record_type} records are not sorted by primary key")
        seen[record_type].add(primary_key)
        last_primary_key[record_type] = primary_key
        grouped[record_type].append(row)
    return grouped


def _indexed_store_rows(
    grouped: Mapping[str, Sequence[Mapping[str, Any]]], record_type: str,
) -> dict[str, Mapping[str, Any]]:
    primary_key = _STORE_PRIMARY_KEYS[record_type]
    return {str(row[primary_key]): row for row in grouped[record_type]}


def _validate_store_joins(grouped: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    configs = _indexed_store_rows(grouped, "selector_config")
    searches = _indexed_store_rows(grouped, "search_event")
    rankings = _indexed_store_rows(grouped, "search_ranking")
    exposures = _indexed_store_rows(grouped, "exposure")
    items = _indexed_store_rows(grouped, "exposure_item")
    feedback = _indexed_store_rows(grouped, "feedback_event")
    candidates = grouped.get("search_candidate", ())

    request_keys: set[tuple[str, str]] = set()
    ranks: set[tuple[str, int]] = set()
    traces: set[tuple[str, str]] = set()
    exposure_searches: set[str] = set()
    exposure_ranks: set[tuple[str, int]] = set()
    exposure_traces: set[tuple[str, str]] = set()
    ranking_items: set[str] = set()
    effective_items: set[str] = set()

    for search in grouped["search_event"]:
        config = configs.get(search["selector_config_id"])
        if config is None:
            raise ArtifactValidationError(f"orphan search event {search['search_event_id']!r}")
        if search["config_sha256"] != config["config_sha256"]:
            raise ArtifactValidationError("search event config hash does not match selector config")
        request_key = ("search_event", search["request_key"])
        if request_key in request_keys:
            raise ArtifactValidationError("duplicate search request_key")
        request_keys.add(request_key)

    candidate_by_search: dict[str, list[Mapping[str, Any]]] = {}
    candidate_ids: set[str] = set()
    candidate_orders: set[tuple[str, int]] = set()
    candidate_traces: set[tuple[str, str]] = set()
    for candidate in candidates:
        search = searches.get(candidate["search_event_id"])
        if search is None:
            raise ArtifactValidationError(f"orphan search candidate {candidate['search_candidate_id']!r}")
        if candidate["search_candidate_id"] in candidate_ids:
            raise ArtifactValidationError("duplicate search candidate id")
        candidate_ids.add(candidate["search_candidate_id"])
        order_key = (candidate["search_event_id"], candidate["pool_order"])
        trace_key = (candidate["search_event_id"], candidate["trace_id"])
        if order_key in candidate_orders or trace_key in candidate_traces:
            raise ArtifactValidationError("duplicate candidate pool order or trace")
        candidate_orders.add(order_key)
        candidate_traces.add(trace_key)
        payload = candidate["candidate_payload"]
        if payload.get("trace_id") != candidate["trace_id"]:
            raise ArtifactValidationError("search candidate payload trace_id mismatch")
        payload_feedback = payload.get("feedback", {})
        if not isinstance(payload_feedback, Mapping) or dict(payload_feedback) != candidate["feedback_snapshot"]:
            raise ArtifactValidationError("search candidate feedback snapshot mismatch")
        if _identity_sha256(payload) != candidate["candidate_sha256"]:
            raise ArtifactValidationError("search candidate payload hash mismatch")
        if "snapshot_watermarks" in candidate:
            if (
                "snapshot_watermarks" not in search
                or candidate["snapshot_watermarks"] != search.get("snapshot_watermarks")
            ):
                raise ArtifactValidationError("search candidate watermark mismatch")
        candidate_by_search.setdefault(candidate["search_event_id"], []).append(candidate)
    for search_id, rows_for_search in candidate_by_search.items():
        orders = sorted(row["pool_order"] for row in rows_for_search)
        if orders != list(range(1, len(orders) + 1)):
            raise ArtifactValidationError(f"search event {search_id!r} has non-contiguous candidate pool order")
        watermark_presence = ["snapshot_watermarks" in row for row in rows_for_search]
        if any(watermark_presence) and not all(watermark_presence):
            # A partially rewritten export is ambiguous: consumers could not
            # tell whether the absent rows are new parent-derived records or
            # truncated legacy records.  Require one representation per
            # selection (all legacy child copies or all new parent-only rows).
            raise ArtifactValidationError(
                "search candidate watermark fields must be all present or all absent"
            )
        search = searches[search_id]
        ordered_rows = sorted(rows_for_search, key=lambda row: row["pool_order"])
        candidate_payloads = [row["candidate_payload"] for row in ordered_rows]
        if _identity_sha256(candidate_payloads) != search.get("eligible_candidates_sha256"):
            raise ArtifactValidationError("search event eligible candidate hash mismatch")
        watermarks = search.get("snapshot_watermarks")
        if _identity_sha256(watermarks) != search.get("snapshot_watermarks_sha256"):
            raise ArtifactValidationError("search event snapshot watermark hash mismatch")

    for search_id, search in searches.items():
        has_pool_identity = bool(search.get("eligible_candidates_sha256"))
        if has_pool_identity != (search_id in candidate_by_search):
            raise ArtifactValidationError(
                "search event candidate rows do not match its eligible-pool identity"
            )

    rankings_by_search: dict[str, list[Mapping[str, Any]]] = {}
    for ranking in grouped["search_ranking"]:
        if ranking["search_event_id"] not in searches:
            raise ArtifactValidationError(f"orphan search ranking {ranking['search_ranking_id']!r}")
        rank_key = (ranking["search_event_id"], ranking["rank"])
        trace_key = (ranking["search_event_id"], ranking["trace_id"])
        if rank_key in ranks or trace_key in traces:
            raise ArtifactValidationError("duplicate rank or trace within one search event")
        ranks.add(rank_key)
        traces.add(trace_key)
        rankings_by_search.setdefault(ranking["search_event_id"], []).append(ranking)
    for search_id in searches:
        if search_id not in rankings_by_search:
            raise ArtifactValidationError(f"search event {search_id!r} has no rankings")
        if search_id in candidate_by_search:
            candidate_traces_for_search = {row["trace_id"] for row in candidate_by_search[search_id]}
            ranking_traces_for_search = {row["trace_id"] for row in rankings_by_search[search_id]}
            if not ranking_traces_for_search.issubset(candidate_traces_for_search):
                raise ArtifactValidationError("search rankings contain traces absent from eligible candidate pool")

    exposure_by_search: dict[str, Mapping[str, Any]] = {}
    for exposure in grouped["exposure"]:
        search = searches.get(exposure["search_event_id"])
        if search is None:
            raise ArtifactValidationError(f"orphan exposure {exposure['exposure_id']!r}")
        if exposure["search_event_id"] in exposure_searches:
            raise ArtifactValidationError("multiple exposures reference one search event")
        if exposure["actor_id"] != search["actor_id"]:
            raise ArtifactValidationError("exposure actor does not match search actor")
        exposure_searches.add(exposure["search_event_id"])
        exposure_by_search[exposure["search_event_id"]] = exposure

    if set(exposure_by_search) != set(searches):
        missing = sorted(set(searches) - set(exposure_by_search))
        raise ArtifactValidationError(f"search events missing exposure records: {missing!r}")

    for item in grouped["exposure_item"]:
        exposure = exposures.get(item["exposure_id"])
        ranking = rankings.get(item["search_ranking_id"])
        if exposure is None or ranking is None:
            raise ArtifactValidationError(f"orphan exposure item {item['exposure_item_id']!r}")
        if ranking["search_event_id"] != exposure["search_event_id"]:
            raise ArtifactValidationError("exposure item joins different search events")
        if not ranking["selected"]:
            raise ArtifactValidationError("exposure item refers to an unselected ranking")
        if item["trace_id"] != ranking["trace_id"] or item["rank"] != ranking["rank"]:
            raise ArtifactValidationError("exposure item trace/rank does not match ranking")
        rank_key = (item["exposure_id"], item["rank"])
        trace_key = (item["exposure_id"], item["trace_id"])
        if rank_key in exposure_ranks or trace_key in exposure_traces:
            raise ArtifactValidationError("duplicate exposure item rank or trace")
        if item["search_ranking_id"] in ranking_items:
            raise ArtifactValidationError("one selected ranking has multiple exposure items")
        exposure_ranks.add(rank_key)
        exposure_traces.add(trace_key)
        ranking_items.add(item["search_ranking_id"])

    for search_id, exposure in exposure_by_search.items():
        selected = {
            ranking["search_ranking_id"]
            for ranking in rankings_by_search.get(search_id, ())
            if ranking["selected"]
        }
        exposed = {
            item["search_ranking_id"]
            for item in grouped["exposure_item"]
            if item["exposure_id"] == exposure["exposure_id"]
        }
        if selected != exposed:
            raise ArtifactValidationError("selected rankings and delivered exposure items differ")

    for event in grouped["feedback_event"]:
        item = items.get(event["exposure_item_id"])
        if item is None:
            raise ArtifactValidationError(f"orphan feedback event {event['feedback_event_id']!r}")
        exposure = exposures[item["exposure_id"]]
        if event["trace_id"] != item["trace_id"]:
            raise ArtifactValidationError("feedback trace does not match exposure item")
        if event["actor_id"] != exposure["actor_id"]:
            raise ArtifactValidationError("feedback actor does not match exposure actor")
        request_key = ("feedback_event", event["request_key"])
        if request_key in request_keys:
            raise ArtifactValidationError("duplicate feedback request_key")
        request_keys.add(request_key)
        if event["effective"]:
            if event["exposure_item_id"] in effective_items:
                raise ArtifactValidationError("one exposure has multiple effective terminal feedback events")
            effective_items.add(event["exposure_item_id"])
        if event["terminal"] and not event["effective"] and event.get("conflicts_with_feedback_event_id") is None:
            raise ArtifactValidationError("ineffective terminal feedback must reference its winning event")
        if not event["terminal"] and event.get("conflicts_with_feedback_event_id") is not None:
            raise ArtifactValidationError("nonterminal feedback cannot conflict with a terminal event")
        conflict_id = event.get("conflicts_with_feedback_event_id")
        if conflict_id is not None:
            winner = feedback.get(conflict_id)
            if winner is None:
                raise ArtifactValidationError("feedback conflict references a missing event")
            if winner["exposure_item_id"] != event["exposure_item_id"]:
                raise ArtifactValidationError("feedback conflict crosses exposure items")
            if not winner["terminal"] or not winner["effective"]:
                raise ArtifactValidationError("feedback conflict winner is not effective terminal feedback")

    for record_type in ("verifier_evidence", "maintenance_event", "trace_relation"):
        for event in grouped[record_type]:
            request_key = (record_type, event["request_key"])
            if request_key in request_keys:
                raise ArtifactValidationError(f"duplicate {record_type} request_key")
            request_keys.add(request_key)


def read_selection_store_export(path: str | Path) -> list[dict[str, Any]]:
    """Read a typed SelectionStore JSONL file without accepting partial JSON."""

    rows: list[dict[str, Any]] = []
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ArtifactValidationError(f"cannot read selection store export {path}: {exc}") from exc
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            raise ArtifactValidationError(f"blank JSONL row at {path}:{line_number}")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ArtifactValidationError(f"invalid JSON at {path}:{line_number}") from exc
        rows.append(dict(_mapping(value, f"selection store row {line_number}")))
    validate_selection_store_export(rows)
    return rows


def validate_selection_store_export(
    source: str | Path | Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Fail closed unless a SelectionStore export is complete and joinable.

    The validator deliberately does not trust SQLite foreign-key constraints:
    it operates solely on the portable JSONL records that paper analysis will
    consume.  A successful return therefore proves the exported artifact can
    reconstruct the complete selector/config/search/exposure/feedback chain.
    """

    if isinstance(source, (str, Path)):
        rows: list[dict[str, Any]] = []
        try:
            lines = Path(source).read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise ArtifactValidationError(f"cannot read selection store export {source}: {exc}") from exc
        for line_number, line in enumerate(lines, 1):
            if not line.strip():
                raise ArtifactValidationError(f"blank JSONL row at {source}:{line_number}")
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ArtifactValidationError(f"invalid JSON at {source}:{line_number}") from exc
            rows.append(dict(_mapping(value, f"selection store row {line_number}")))
    else:
        rows = [dict(_mapping(row, "selection store row")) for row in source]
    grouped = _store_rows_by_type(rows)
    _validate_store_joins(grouped)
    return summarize_selection_store_export(rows, _validated_grouped=grouped)


def summarize_selection_store_export(
    rows: Sequence[Mapping[str, Any]],
    *,
    _validated_grouped: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Return trace-oriented identities and counts from validated rows."""

    grouped = _validated_grouped
    if grouped is None:
        grouped = _store_rows_by_type(rows)
        _validate_store_joins(grouped)
    counts = {
        _STORE_TABLE_NAMES[record_type]: len(grouped[record_type])
        for record_type in _STORE_RECORD_TYPES
    }
    if grouped.get("search_candidate"):
        counts["search_candidates"] = len(grouped["search_candidate"])
    feedback = grouped["feedback_event"]
    counts.update(
        {
            "terminal_feedback_events": sum(row["terminal"] for row in feedback),
            "nonterminal_feedback_events": sum(not row["terminal"] for row in feedback),
            "effective_feedback_events": sum(row["effective"] for row in feedback),
            "conflicting_terminal_feedback_events": sum(
                row["terminal"] and not row["effective"] for row in feedback
            ),
        }
    )
    searches = grouped["search_event"]
    rankings = grouped["search_ranking"]
    selected = [row for row in rankings if row["selected"]]
    delivered_tokens = 0
    for row in selected:
        token_count = row["ranking_payload"].get("token_count")
        if isinstance(token_count, int) and not isinstance(token_count, bool) and token_count >= 0:
            delivered_tokens += token_count
    traces = sorted(
        {
            row["trace_id"] for row in rankings
        }
        | {row["trace_id"] for row in grouped["verifier_evidence"]}
        | {row["trace_id"] for row in grouped["maintenance_event"]}
        | {row["source_trace_id"] for row in grouped["trace_relation"]}
        | {row["target_trace_id"] for row in grouped["trace_relation"]}
    )
    return {
        "schema": STORE_EXPORT_SCHEMA_VERSION,
        "store_schema_version": STORE_SCHEMA_VERSION,
        "record_count": sum(len(grouped[record_type]) for record_type in _STORE_EXPORT_RECORD_TYPES),
        "record_type_counts": {
            record_type: len(grouped[record_type])
            for record_type in _STORE_EXPORT_RECORD_TYPES
            if record_type not in _OPTIONAL_STORE_RECORD_TYPES or grouped[record_type]
        },
        "counts": counts,
        "selector_config_ids": sorted(row["selector_config_id"] for row in grouped["selector_config"]),
        "selector_configs": [
            {
                "selector_config_id": row["selector_config_id"],
                "selector_name": row["selector_name"],
                "config_sha256": row["config_sha256"],
            }
            for row in grouped["selector_config"]
        ],
        "comparison_contract_ids": sorted({row["comparison_sha256"] for row in searches}),
        "search_event_ids": sorted(row["search_event_id"] for row in searches),
        "snapshot_sha256s": sorted({row["snapshot_sha256"] for row in searches}),
        "eligible_pool_sha256s": sorted({row["pool_sha256"] for row in searches}),
        "trace_ids": traces,
        "usage": {
            "search_count": len(searches),
            "eligible_ranking_count": len(rankings),
            "selected_trace_count": len(selected),
            "delivered_exposure_count": len(grouped["exposure_item"]),
            "delivered_trace_context_tokens": delivered_tokens,
            "eligible_candidate_count": len(grouped.get("search_candidate", ())),
            "feedback_event_count": len(feedback),
            "effective_feedback_count": sum(row["effective"] for row in feedback),
            "terminal_feedback_count": sum(row["terminal"] for row in feedback),
            "nonterminal_feedback_count": sum(not row["terminal"] for row in feedback),
            "conflicting_terminal_feedback_count": sum(
                row["terminal"] and not row["effective"] for row in feedback
            ),
        },
    }


def reconstruct_selection_chains(
    source: str | Path | Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Rebuild one auditable chain per delivered trace recommendation."""

    if isinstance(source, (str, Path)):
        rows = read_selection_store_export(source)
    else:
        rows = [dict(_mapping(row, "selection store row")) for row in source]
        validate_selection_store_export(rows)
    grouped = _store_rows_by_type(rows)
    configs = _indexed_store_rows(grouped, "selector_config")
    searches = _indexed_store_rows(grouped, "search_event")
    rankings = _indexed_store_rows(grouped, "search_ranking")
    exposures = _indexed_store_rows(grouped, "exposure")
    feedback_by_item: dict[str, list[Mapping[str, Any]]] = {}
    for event in grouped["feedback_event"]:
        feedback_by_item.setdefault(event["exposure_item_id"], []).append(event)
    evidence_by_trace: dict[str, list[Mapping[str, Any]]] = {}
    for event in grouped["verifier_evidence"]:
        evidence_by_trace.setdefault(event["trace_id"], []).append(event)
    maintenance_by_trace: dict[str, list[Mapping[str, Any]]] = {}
    for event in grouped["maintenance_event"]:
        maintenance_by_trace.setdefault(event["trace_id"], []).append(event)
    relations_by_trace: dict[str, list[Mapping[str, Any]]] = {}
    for event in grouped["trace_relation"]:
        relations_by_trace.setdefault(event["source_trace_id"], []).append(event)
        relations_by_trace.setdefault(event["target_trace_id"], []).append(event)
    chains: list[dict[str, Any]] = []
    for item in grouped["exposure_item"]:
        exposure = exposures[item["exposure_id"]]
        search = searches[exposure["search_event_id"]]
        ranking = rankings[item["search_ranking_id"]]
        config = configs[search["selector_config_id"]]
        events = sorted(
            feedback_by_item.get(item["exposure_item_id"], ()),
            key=lambda row: (str(row.get("created_at") or ""), row["feedback_event_id"]),
        )
        chains.append(
            {
                "selector_config": dict(config),
                "search_event": dict(search),
                "ranking": dict(ranking),
                "exposure": dict(exposure),
                "exposure_item": dict(item),
                "feedback_events": [dict(event) for event in events],
                "verifier_evidence": [
                    dict(event) for event in evidence_by_trace.get(item["trace_id"], ())
                ],
                "maintenance_events": [
                    dict(event) for event in maintenance_by_trace.get(item["trace_id"], ())
                ],
                "relations": [
                    dict(event) for event in relations_by_trace.get(item["trace_id"], ())
                ],
                "effective_terminal_feedback": next(
                    (dict(event) for event in events if event["terminal"] and event["effective"]),
                    None,
                ),
            }
        )
    return sorted(
        chains,
        key=lambda chain: (
            chain["search_event"]["search_event_id"],
            chain["exposure_item"]["rank"],
            chain["exposure_item"]["trace_id"],
        ),
    )


def collect_paired_trace_metrics(
    left: str | Path | Sequence[Mapping[str, Any]],
    right: str | Path | Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compare trace-delivery metrics after enforcing the paired contract.

    This complements :func:`collect_paired_metrics`: accepted-score/nAUC live
    in the run summary, while eligible/selected/exposure/feedback/token usage
    is reconstructed from the trace-selection JSONL artifact.
    """

    def load(source: str | Path | Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        if isinstance(source, (str, Path)):
            return validate_selection_store_export(source)
        return validate_selection_store_export(source)

    a, b = load(left), load(right)
    if len(a["comparison_contract_ids"]) != 1 or len(b["comparison_contract_ids"]) != 1:
        raise ArtifactValidationError(
            "each paired trace artifact must use exactly one comparison contract"
        )
    if a["comparison_contract_ids"] != b["comparison_contract_ids"]:
        raise ArtifactValidationError("paired trace artifacts mismatch comparison contract")
    if len(a["selector_config_ids"]) != 1 or len(b["selector_config_ids"]) != 1:
        raise ArtifactValidationError("each paired trace artifact must use exactly one selector config")
    numeric = (
        "search_count",
        "eligible_candidate_count",
        "eligible_ranking_count",
        "selected_trace_count",
        "delivered_exposure_count",
        "delivered_trace_context_tokens",
        "feedback_event_count",
        "effective_feedback_count",
        "terminal_feedback_count",
        "nonterminal_feedback_count",
        "conflicting_terminal_feedback_count",
    )
    return {
        "comparison_contract_ids": a["comparison_contract_ids"],
        "left_selector_config_id": a["selector_config_ids"][0],
        "right_selector_config_id": b["selector_config_ids"][0],
        "left": dict(a["usage"]),
        "right": dict(b["usage"]),
        "differences": {name: a["usage"][name] - b["usage"][name] for name in numeric},
    }


# Public aliases using the issue's shorter terminology.
read_trace_selection_export = read_selection_store_export
validate_trace_selection_export = validate_selection_store_export
summarize_trace_selection_export = summarize_selection_store_export
reconstruct_trace_selection_chains = reconstruct_selection_chains


def collect_paired_metrics(left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Join paired summaries and return left-minus-right metric differences.

    Any contract discrepancy raises instead of silently dropping the pair.
    """
    def indexed(rows: Sequence[Mapping[str, Any]]) -> dict[int, Mapping[str, Any]]:
        result: dict[int, Mapping[str, Any]] = {}
        for row in rows:
            metadata = _mapping(row.get("metadata"), "metadata")
            seed = metadata.get("paired_seed")
            if isinstance(seed, bool) or not isinstance(seed, int):
                raise ArtifactValidationError("paired_seed must be an integer")
            if seed in result:
                raise ArtifactValidationError(f"duplicate paired_seed {seed!r}")
            result[seed] = row
        return result
    left_by_id, right_by_id = indexed(left), indexed(right)
    if set(left_by_id) != set(right_by_id):
        raise ArtifactValidationError("paired runs have different paired_seed sets")
    pairs: list[dict[str, Any]] = []
    numeric_metrics = ("final_score", "normalized_score_time_auc")
    for paired_seed in sorted(left_by_id):
        a, b = left_by_id[paired_seed], right_by_id[paired_seed]
        run_id = f"{_text(a, 'run_id')}::{_text(b, 'run_id')}"
        ma, mb = _mapping(a.get("metadata"), "metadata"), _mapping(b.get("metadata"), "metadata")
        for field in _PAIR_FIELDS:
            if ma.get(field) != mb.get(field):
                raise ArtifactValidationError(f"pair {run_id!r} mismatches {field}")
        am, bm = _mapping(a.get("metrics"), "metrics"), _mapping(b.get("metrics"), "metrics")
        differences = {name: _number(am.get(name), name, nonnegative=False) - _number(bm.get(name), name, nonnegative=False) for name in numeric_metrics}
        pairs.append({"paired_seed": paired_seed, "run_id": run_id, "differences": differences})
    return {"pairs": pairs, "mean_differences": {name: (sum(pair["differences"][name] for pair in pairs) / len(pairs) if pairs else 0.0) for name in numeric_metrics}}


# A descriptive alias for callers that use the collector as a report builder.
collect_paired_selector_metrics = collect_paired_metrics

__all__ = [
    "SCHEMA_VERSION", "STORE_SCHEMA_VERSION", "STORE_EXPORT_SCHEMA_VERSION",
    "ArtifactValidationError", "write_jsonl", "read_jsonl",
    "write_selector_decisions", "read_selector_decisions", "write_exposures",
    "read_exposures", "write_feedback", "read_feedback", "write_relations",
    "read_relations", "validate_attribution_joins", "reconstruct_metrics",
    "read_selection_store_export", "validate_selection_store_export",
    "summarize_selection_store_export", "reconstruct_selection_chains",
    "collect_paired_trace_metrics", "read_trace_selection_export",
    "validate_trace_selection_export", "summarize_trace_selection_export",
    "reconstruct_trace_selection_chains",
    "write_figure3_run_summary", "read_figure3_run_summary", "collect_paired_metrics",
    "collect_paired_selector_metrics", "build_figure3_run_summary", "export_figure3_run_summary",
]
