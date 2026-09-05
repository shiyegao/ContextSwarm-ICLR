"""Typed, deliberately small experiment manifest loader."""

from __future__ import annotations

from dataclasses import dataclass, field
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tomllib
from typing import Any, Mapping


class ConfigError(ValueError):
    """Raised when a manifest is incomplete or internally inconsistent."""


# The staged ``evaluate.py`` helper is launched through Pi's Bash capability.
# Leave a bounded handoff margin after the Agent budget so that the shell
# wrapper cannot kill a legal helper request before the broker/evaluator does.
_AGENT_TIMEOUT_HELPER_GRACE_SECONDS = 120


def _deep_merge(base: dict[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in update.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _deep_merge(dict(result[key]), value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _load_mapping(path: Path, seen: set[Path] | None = None) -> dict[str, Any]:
    path = path.resolve()
    seen = set() if seen is None else seen
    if path in seen:
        raise ConfigError(f"manifest inheritance cycle at {path}")
    seen.add(path)
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"manifest not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc

    merged: dict[str, Any] = {}
    extends = payload.pop("extends", [])
    if isinstance(extends, str):
        extends = [extends]
    if not isinstance(extends, list) or not all(isinstance(item, str) for item in extends):
        raise ConfigError(f"extends must be a string or list of strings: {path}")
    for parent in extends:
        parent_path = (path.parent / parent).resolve()
        merged = _deep_merge(merged, _load_mapping(parent_path, seen.copy()))
    return _deep_merge(merged, payload)


def _as_dict(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"[{name}] must be a table")
    return dict(value)


def _text(value: Any, default: str = "") -> str:
    return str(default if value is None else value).strip()


def _positive_int(value: Any, name: str, default: int) -> int:
    try:
        result = int(default if value is None else value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if result <= 0:
        raise ConfigError(f"{name} must be positive")
    return result


def _nonnegative_int(value: Any, name: str, default: int) -> int:
    try:
        result = int(default if value is None else value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if result < 0:
        raise ConfigError(f"{name} must not be negative")
    return result


def _number(value: Any, name: str, default: float) -> float:
    try:
        result = float(default if value is None else value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a finite number") from exc
    if not math.isfinite(result):
        raise ConfigError(f"{name} must be a finite number")
    return result


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{name} must be a boolean")
    return value


def _required_nonnegative_int(
    table: Mapping[str, Any], key: str, table_name: str
) -> int:
    if key not in table:
        raise ConfigError(f"{table_name}.{key} is required when selection is enabled")
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{table_name}.{key} must be an integer")
    if value < 0:
        raise ConfigError(f"{table_name}.{key} must not be negative")
    return value


def _required_positive_int(
    table: Mapping[str, Any], key: str, table_name: str
) -> int:
    value = _required_nonnegative_int(table, key, table_name)
    if value == 0:
        raise ConfigError(f"{table_name}.{key} must be positive")
    return value


def _canonical_json_value(value: Any, name: str) -> Any:
    """Validate and normalize a TOML value for stable public hashing."""

    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ConfigError(f"{name} keys must be non-empty strings")
            normalized[key] = _canonical_json_value(item, f"{name}.{key}")
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, list):
        return [
            _canonical_json_value(item, f"{name}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, (str, bool, int)) or value is None:
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigError(f"{name} numbers must be finite")
        return value
    raise ConfigError(
        f"{name} must contain only strings, booleans, numbers, arrays, and tables"
    )


_FORMULA_DEFAULTS: dict[str, float] = {
    "active_balance_weight": 2.0,
    "candidate_quality_weight": 1.5,
    "recent_progress_weight": 1.25,
    "cps_evidence_weight": 0.75,
    "starvation_weight": 1.0,
    "failure_penalty": 0.75,
    "duplication_penalty": 0.5,
    "progress_window_seconds": 600.0,
    "starvation_window_seconds": 600.0,
    "evidence_saturation": 3.0,
    "failure_saturation": 3.0,
    "proved_quality": 1.0,
    "compiles_with_sorry_quality": 0.8,
    "verify_fail_quality": 0.35,
    "other_status_quality": 0.0,
}


_TASK_STATE_DEFAULTS: dict[str, float] = {
    "checker_quality": 1.0,
    "recent_progress": 1.0,
    "starvation": 1.0,
    "failure_no_progress": 1.0,
}

_TRACE_STATE_DEFAULTS: dict[str, float] = {
    "actionability": 1.0,
    "evidence_association": 1.0,
    "positive_feedback": 1.0,
    "negative_feedback": 1.0,
    # ``density_penalty_weight`` and the four component coefficients are the
    # registered D formula.  The historical ``drag`` key is accepted below
    # as a compatibility alias but is canonicalized to density_penalty_weight
    # before hashing or emitting artifacts.
    "density_penalty_weight": 1.0,
    "duplicate_component_weight": 1.0,
    "refutation_component_weight": 1.0,
    "staleness_component_weight": 1.0,
    "lineage_stagnation_component_weight": 1.0,
}

_TRACE_STATE_LEGACY_ALIASES: dict[str, str] = {
    "drag": "density_penalty_weight",
    "duplication_component_weight": "duplicate_component_weight",
}

_ALLOCATION_NORMALIZATION_DEFAULTS: dict[str, float] = {
    "progress_window_seconds": 600.0,
    "starvation_window_seconds": 600.0,
    "failure_saturation": 3.0,
    # Trace projection normalizers are part of the same manifest-owned
    # identity.  They are harmless for task-only arms but must remain fixed
    # across all Figure 4 policies.
    "frontier_saturation": 4.0,
    "association_saturation": 4.0,
    "feedback_exposure_floor": 1.0,
    "duplicate_saturation": 4.0,
    "refutation_saturation": 4.0,
    "staleness_saturation": 4.0,
    "lineage_stagnation_saturation": 4.0,
    "staleness_window_seconds": 600.0,
    "lineage_stagnation_window_seconds": 600.0,
}

_FIGURE4_ALLOCATION_POLICIES = frozenset(
    {"uniform_refill", "task_state", "trace_state", "llm_scheduler"}
)


_SELECTOR_NAMES = frozenset(
    {
        "random",
        "recency",
        "bm25_mmr",
        "smoothed_popularity",
        "feedback_diversity",
        "no_interaction_feedback",
        "unnormalized_feedback",
        "nustigmergy",
    }
)
_SELECTION_FIELDS = frozenset(
    {
        "enabled",
        "selector_name",
        "selector_version",
        "visibility",
        "trace_slot_limit",
        "context_token_budget",
        "tokenizer",
        "seed",
        "tie_break",
        "policy_params",
        "direct_messages",
        "candidate_transfer",
    }
)


@dataclass(frozen=True)
class SelectionConfig:
    """Manifest-owned trace-selection policy and comparison boundary.

    ``policy_params`` is deliberately opaque to the manifest loader: selector
    implementations own its schema.  Keeping it fully explicit and in the
    canonical identity prevents policy-specific numerical defaults from being
    hidden in this common configuration layer.
    """

    enabled: bool
    selector_name: str
    selector_version: str
    visibility: str
    trace_slot_limit: int
    context_token_budget: int
    tokenizer: str
    seed: int
    tie_break: str
    policy_params: dict[str, Any]
    direct_messages: bool
    candidate_transfer: bool

    def hash_inputs(self) -> dict[str, Any]:
        """Return the complete canonical selector-configuration identity."""

        return {
            "enabled": self.enabled,
            "selector_name": self.selector_name,
            "selector_version": self.selector_version,
            "visibility": self.visibility,
            "trace_slot_limit": self.trace_slot_limit,
            "context_token_budget": self.context_token_budget,
            "tokenizer": self.tokenizer,
            "seed": self.seed,
            "tie_break": self.tie_break,
            "policy_params": _canonical_json_value(
                self.policy_params, "selection.policy_params"
            ),
            "direct_messages": self.direct_messages,
            "candidate_transfer": self.candidate_transfer,
        }

    def comparison_hash_inputs(self) -> dict[str, Any]:
        """Return arm-invariant selection inputs for a comparison hash.

        The runner combines these inputs with the rest of the experiment
        contract.  Selector identity and policy parameters are intentionally
        absent because those are the sole registered differences among arms.
        """

        inputs = self.hash_inputs()
        for key in ("selector_name", "selector_version", "policy_params"):
            inputs.pop(key)
        return inputs

    @property
    def selection_config_id(self) -> str:
        canonical = json.dumps(
            self.hash_inputs(),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @property
    def identity_frozen(self) -> bool:
        """Whether this config carries a complete immutable selector identity."""

        return bool(
            self.enabled
            and self.selector_name
            and self.selector_version
            and self.visibility == "project_shared"
            and self.trace_slot_limit > 0
            and self.context_token_budget > 0
            and self.tokenizer
            and self.tie_break == "trace_id_asc"
            and not self.direct_messages
        )

    def public_dict(self) -> dict[str, Any]:
        result = self.hash_inputs()
        result["selection_config_id"] = self.selection_config_id
        return result


def _parse_selection(value: Any) -> SelectionConfig:
    selection = _as_dict(value, "selection")
    unknown = set(selection) - _SELECTION_FIELDS
    if unknown:
        raise ConfigError("unknown selection fields: " + ", ".join(sorted(unknown)))

    enabled = (
        _strict_bool(selection["enabled"], "selection.enabled")
        if "enabled" in selection
        else False
    )
    required = (
        "selector_name",
        "selector_version",
        "visibility",
        "trace_slot_limit",
        "context_token_budget",
        "tokenizer",
        "seed",
        "tie_break",
        "policy_params",
        "direct_messages",
        "candidate_transfer",
    )
    if enabled:
        missing = [key for key in required if key not in selection]
        if missing:
            raise ConfigError(
                "selection fields required when enabled: " + ", ".join(missing)
            )

    def optional_text(key: str, default: str = "") -> str:
        if key not in selection:
            return default
        raw = selection[key]
        if not isinstance(raw, str) or not raw.strip():
            raise ConfigError(f"selection.{key} must be a non-empty string")
        return raw.strip()

    selector_name = optional_text("selector_name")
    if selector_name and selector_name not in _SELECTOR_NAMES:
        raise ConfigError(
            "selection.selector_name must be one of " + ", ".join(sorted(_SELECTOR_NAMES))
        )
    selector_version = optional_text("selector_version")
    if selector_version and not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", selector_version):
        raise ConfigError("selection.selector_version has an invalid format")
    visibility = optional_text("visibility", "project_shared")
    if visibility != "project_shared":
        raise ConfigError("selection.visibility must be project_shared")
    tokenizer = optional_text("tokenizer")
    if tokenizer and not re.fullmatch(r"[A-Za-z0-9_.:/+-]{1,200}", tokenizer):
        raise ConfigError("selection.tokenizer has an invalid format")
    tie_break = optional_text("tie_break", "trace_id_asc")
    if tie_break != "trace_id_asc":
        raise ConfigError("selection.tie_break must be trace_id_asc")

    trace_slot_limit = (
        _required_positive_int(selection, "trace_slot_limit", "selection")
        if "trace_slot_limit" in selection
        else 0
    )
    context_token_budget = (
        _required_positive_int(selection, "context_token_budget", "selection")
        if "context_token_budget" in selection
        else 0
    )
    seed = (
        _required_nonnegative_int(selection, "seed", "selection")
        if "seed" in selection
        else 0
    )
    policy_params = _canonical_json_value(
        _as_dict(selection.get("policy_params"), "selection.policy_params"),
        "selection.policy_params",
    )
    direct_messages = (
        _strict_bool(selection["direct_messages"], "selection.direct_messages")
        if "direct_messages" in selection
        else False
    )
    candidate_transfer = (
        _strict_bool(selection["candidate_transfer"], "selection.candidate_transfer")
        if "candidate_transfer" in selection
        else False
    )
    return SelectionConfig(
        enabled=enabled,
        selector_name=selector_name,
        selector_version=selector_version,
        visibility=visibility,
        trace_slot_limit=trace_slot_limit,
        context_token_budget=context_token_budget,
        tokenizer=tokenizer,
        seed=seed,
        tie_break=tie_break,
        policy_params=policy_params,
        direct_messages=direct_messages,
        candidate_transfer=candidate_transfer,
    )


@dataclass(frozen=True)
class AllocationConfig:
    """Manifest-owned contract for post-initial CPS slot allocation."""

    policy: str
    piece_limit_per_task: int
    piece_body_chars: int
    agent_timeout_seconds: int
    formula: dict[str, float]
    task_state: dict[str, float]
    trace_state: dict[str, float]
    normalization: dict[str, float]
    # Hard, manifest-owned bounds for the read-only LLM scheduler wire prompt.
    # They remain arm-invariant even when another arm does not consume them.
    prompt_max_bytes: int
    prompt_max_tokens: int

    def public_dict(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "piece_limit_per_task": self.piece_limit_per_task,
            "piece_body_chars": self.piece_body_chars,
            "agent_timeout_seconds": self.agent_timeout_seconds,
            "formula": dict(sorted(self.formula.items())),
            "task_state": dict(sorted(self.task_state.items())),
            "trace_state": dict(sorted(self.trace_state.items())),
            "normalization": dict(sorted(self.normalization.items())),
            "prompt_max_bytes": self.prompt_max_bytes,
            "prompt_max_tokens": self.prompt_max_tokens,
        }


@dataclass(frozen=True)
class ExperimentConfig:
    manifest_path: Path
    repo_root: Path
    name: str
    mode: str
    communication: str
    dataset_root: Path
    problem_ids_path: Path
    output_root: Path
    max_parallel: int
    initial_agents_per_task: int
    max_attempts_per_task: int
    cancel_on_proved: bool
    assignment_policy: str
    allocation: AllocationConfig
    selection: SelectionConfig
    figure4_phase: str
    episodes_per_task: int
    max_tasks: int
    time_limit_seconds: int
    seed: int
    model: str
    thinking: str
    fast_mode: bool
    pi_binary: str
    pi_extension: str
    pi_timeout_seconds: int
    pi_http_idle_timeout_ms: int
    pi_retry_enabled: bool
    pi_retry_max_retries: int
    pi_retry_base_delay_ms: int
    pi_provider_max_retries: int
    pi_provider_max_retry_delay_ms: int
    pi_recovery_enabled: bool
    pi_recovery_max_restarts: int
    pi_recovery_base_delay_ms: int
    aisw_enabled: bool
    aisw_binary: str
    aisw_node_config: str
    aisw_coordinator_url: str
    aisw_account: str
    aisw_group: str
    aisw_max_in_flight: int
    aisw_lease_wait_seconds: int
    aisw_lease_retry_interval_seconds: int
    lean_server_url: str
    lean_env_id: str
    lean_timeout_seconds: int
    lean_max_lifecycle_seconds: int
    lean_max_concurrent_evaluations: int
    lean_verification_profile: str
    lean_judge_mode: str
    lean_require_result_cache_disabled: bool
    judge_kind: str
    formal_tools_enabled: bool
    formal_tools_version: str
    formal_tools_evaluate_calls_per_task: int
    formal_tools_evaluate_backend_jobs_per_task: int
    formal_tools_query_calls_per_task: int
    formal_tools_query_backend_probes_per_task: int
    formal_tools_max_candidate_bytes: int
    formal_tools_command_timeout_seconds: int
    formal_tools_decl_index: str
    formal_tools_decl_index_sha256: str
    formal_tools_mathlib_revision: str
    formal_tools_require_decl_index: bool
    docker_image: str
    docker_memory_mb: int
    docker_internet: str
    docker_network: str
    # Optional worker-proposed Judge/evaluate timeout capability.  It is
    # deliberately default-off so historical manifests keep the exact legacy
    # tool surface and retry behavior.
    judge_agent_timeout_enabled: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def uses_cps(self) -> bool:
        return self.mode == "cps" and self.communication != "none"

    @property
    def is_coding(self) -> bool:
        return self.judge_kind == "coding"

    @property
    def dataset_name(self) -> str:
        """Return the manifest-selected public dataset label.

        Dataset identity is part of the benchmark bundle, rather than a
        runner default.  Prefer the explicit ``[experiment].dataset`` value
        when present, then the bundle path name.  Keep the value bounded and
        filesystem-independent so it is safe to include in run metadata.
        """

        raw = self.extra.get("raw") if isinstance(self.extra, dict) else None
        if isinstance(raw, Mapping):
            experiment = raw.get("experiment")
            if isinstance(experiment, Mapping):
                explicit = _text(experiment.get("dataset"))
                if explicit:
                    return explicit
        name = self.dataset_root.name.strip()
        return name or "unknown"

    @property
    def resolved_output_root(self) -> Path:
        return self._resolve_path(self.output_root)

    def _resolve_path(self, value: Path | str) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path
        candidate = (self.repo_root / path).resolve()
        if candidate.exists() or not (self.manifest_path.parent / path).exists():
            return candidate
        return (self.manifest_path.parent / path).resolve()

    def resolve_runtime_path(self, value: str) -> Path:
        return self._resolve_path(value)

    def public_dict(self) -> dict[str, Any]:
        """Return a JSON-safe manifest snapshot without credentials."""
        return {
            "name": self.name,
            "mode": self.mode,
            "communication": self.communication,
            "dataset_root": str(self.dataset_root),
            "dataset": self.dataset_name,
            "problem_ids_path": str(self.problem_ids_path),
            "max_parallel": self.max_parallel,
            "initial_agents_per_task": self.initial_agents_per_task,
            "max_attempts_per_task": self.max_attempts_per_task,
            "cancel_on_proved": self.cancel_on_proved,
            "assignment_policy": self.assignment_policy,
            "allocation": self.allocation.public_dict(),
            "selection": self.selection.public_dict(),
            "figure4_phase": self.figure4_phase,
            "episodes_per_task": self.episodes_per_task,
            "max_tasks": self.max_tasks,
            "time_limit_seconds": self.time_limit_seconds,
            "seed": self.seed,
            "model": self.model,
            "thinking": self.thinking,
            "fast_mode": self.fast_mode,
            "pi_timeout_seconds": self.pi_timeout_seconds,
            "pi_http_idle_timeout_ms": self.pi_http_idle_timeout_ms,
            "pi_retry_enabled": self.pi_retry_enabled,
            "pi_retry_max_retries": self.pi_retry_max_retries,
            "pi_retry_base_delay_ms": self.pi_retry_base_delay_ms,
            "pi_provider_max_retries": self.pi_provider_max_retries,
            "pi_provider_max_retry_delay_ms": self.pi_provider_max_retry_delay_ms,
            "pi_recovery_enabled": self.pi_recovery_enabled,
            "pi_recovery_max_restarts": self.pi_recovery_max_restarts,
            "pi_recovery_base_delay_ms": self.pi_recovery_base_delay_ms,
            "pi_binary_configured": bool(self.pi_binary),
            # Public spelling follows the actual provider.  Keep the AISW
            # fields below for replaying older run metadata and fixtures.
            "provider_backend": "nurouter" if self.aisw_enabled else "pi",
            "nurouter_enabled": self.aisw_enabled,
            "nurouter_binary_configured": bool(self.aisw_binary),
            "nurouter_node_config_configured": bool(self.aisw_node_config),
            "nurouter_max_in_flight": self.aisw_max_in_flight,
            "aisw_enabled": self.aisw_enabled,
            "aisw_binary_configured": bool(self.aisw_binary),
            "aisw_node_config_configured": bool(self.aisw_node_config),
            "aisw_coordinator_configured": bool(self.aisw_coordinator_url),
            "aisw_account_configured": bool(self.aisw_account),
            "aisw_group_configured": bool(self.aisw_group),
            "aisw_max_in_flight": self.aisw_max_in_flight,
            "lean_server_configured": bool(self.lean_server_url),
            "lean_env_id": self.lean_env_id,
            "lean_timeout_seconds": self.lean_timeout_seconds,
            "lean_max_lifecycle_seconds": self.lean_max_lifecycle_seconds,
            "lean_max_concurrent_evaluations": self.lean_max_concurrent_evaluations,
            "lean_verification_profile": self.lean_verification_profile,
            "lean_judge_mode": self.lean_judge_mode,
            "lean_require_result_cache_disabled": self.lean_require_result_cache_disabled,
            "judge_kind": self.judge_kind,
            "judge_agent_timeout_enabled": self.judge_agent_timeout_enabled,
            "formal_tools_enabled": self.formal_tools_enabled,
            "formal_tools_version": self.formal_tools_version,
            "formal_tools_evaluate_calls_per_task": self.formal_tools_evaluate_calls_per_task,
            "formal_tools_evaluate_backend_jobs_per_task": self.formal_tools_evaluate_backend_jobs_per_task,
            "formal_tools_query_calls_per_task": self.formal_tools_query_calls_per_task,
            "formal_tools_query_backend_probes_per_task": self.formal_tools_query_backend_probes_per_task,
            "formal_tools_max_candidate_bytes": self.formal_tools_max_candidate_bytes,
            "formal_tools_command_timeout_seconds": self.formal_tools_command_timeout_seconds,
            "formal_tools_decl_index_configured": bool(self.formal_tools_decl_index),
            "formal_tools_decl_index_sha256": self.formal_tools_decl_index_sha256,
            "formal_tools_mathlib_revision": self.formal_tools_mathlib_revision,
            "formal_tools_require_decl_index": self.formal_tools_require_decl_index,
            "docker_image": self.docker_image,
            "docker_memory_mb": self.docker_memory_mb,
            "docker_internet": self.docker_internet,
            "docker_network": self.docker_network,
        }


def _resolve_manifest_arg(raw: str | Path, repo_root: Path) -> Path:
    candidate = Path(raw).expanduser()
    if candidate.is_file():
        return candidate.resolve()
    for option in (
        repo_root / candidate,
        repo_root / "configs" / candidate,
        repo_root / "configs" / f"{candidate}.toml",
    ):
        if option.is_file():
            return option.resolve()
    raise ConfigError(f"unable to resolve manifest: {raw}")


def load_config(raw: str | Path, repo_root: Path | None = None) -> ExperimentConfig:
    repo_root = (repo_root or Path(__file__).resolve().parents[1]).resolve()
    manifest_path = _resolve_manifest_arg(raw, repo_root)
    payload = _load_mapping(manifest_path)
    experiment = _as_dict(payload.get("experiment"), "experiment")
    pi = _as_dict(payload.get("pi"), "pi")
    # ``nurouter`` is the paper-facing spelling.  ``aisw`` is retained as a
    # compatibility alias for older manifests and for the on-wire environment
    # variables used by the released NuRouter launcher.  Prefer the explicit
    # NuRouter table when both are present (the inherited development bases
    # still carry the legacy table).
    legacy_aisw = _as_dict(payload.get("aisw"), "aisw")
    explicit_nurouter = payload.get("nurouter")
    if explicit_nurouter is None:
        aisw = legacy_aisw
    else:
        # Merge the alias first so a short [nurouter] override in a child
        # manifest does not discard private launcher defaults inherited from an
        # older base; explicit NuRouter keys always win.
        aisw = _deep_merge(legacy_aisw, _as_dict(explicit_nurouter, "nurouter"))
    lean = _as_dict(payload.get("lean"), "lean")
    judge = _as_dict(payload.get("judge"), "judge")
    formal_tools = _as_dict(payload.get("formal_tools"), "formal_tools")
    docker = _as_dict(payload.get("docker"), "docker")
    allocation = _as_dict(payload.get("allocation"), "allocation")
    allocation_formula = _as_dict(allocation.get("formula"), "allocation.formula")
    allocation_task_state = _as_dict(
        allocation.get("task_state"), "allocation.task_state"
    )
    allocation_trace_state = _as_dict(
        allocation.get("trace_state"), "allocation.trace_state"
    )
    allocation_normalization = _as_dict(
        allocation.get("normalization"), "allocation.normalization"
    )
    selection_config = _parse_selection(payload.get("selection"))

    mode = _text(experiment.get("mode"), "cps").lower()
    if mode not in {"mono", "parallel", "cps"}:
        raise ConfigError("experiment.mode must be mono, parallel, or cps")
    communication = _text(experiment.get("communication"), "none").lower()
    allowed_communication = {"none", "blackboard", "direct", "hybrid", "simple"}
    if communication not in allowed_communication:
        raise ConfigError(f"experiment.communication must be one of {sorted(allowed_communication)}")
    if mode in {"mono", "parallel"} and communication != "none":
        raise ConfigError("mono and parallel baselines must run with communication = none")
    if selection_config.enabled:
        if mode != "cps":
            raise ConfigError("enabled selection requires experiment.mode = cps")
        # Figure 3 isolates trace ranking.  ``direct`` would restore the
        # worker-to-worker surface and ``hybrid`` would additionally expose
        # the legacy global-scope channel; even with tool registration gates,
        # accepting either here would make the treatment boundary depend on a
        # second communication policy.  Require the one shared-trace surface
        # explicitly rather than treating every non-``none`` value as CPS.
        if communication != "blackboard":
            raise ConfigError(
                "enabled selection requires experiment.communication = blackboard"
            )

    figure4_phase = _text(experiment.get("figure4_phase")).lower()
    if figure4_phase not in {"", "development", "formal"}:
        raise ConfigError(
            "experiment.figure4_phase must be development or formal when set"
        )
    if figure4_phase and mode != "cps":
        raise ConfigError("Figure 4 phase requires experiment.mode = cps")
    if figure4_phase and communication != "blackboard":
        raise ConfigError(
            "Figure 4 phase requires experiment.communication = blackboard"
        )

    dataset_raw = _text(experiment.get("dataset_root"), "benchmarks/matholympiadbench")
    problem_raw = _text(experiment.get("problem_ids"), "benchmarks/matholympiadbench/problem_ids.json")
    dataset_root = Path(dataset_raw).expanduser()
    problem_ids_path = Path(problem_raw).expanduser()
    output_root = Path(_text(experiment.get("output_root"), "runs"))

    max_parallel = _positive_int(experiment.get("max_parallel"), "experiment.max_parallel", 12)
    initial_default = 2 if mode == "cps" else 1
    initial_agents = _positive_int(
        experiment.get("initial_agents_per_task"),
        "experiment.initial_agents_per_task",
        initial_default,
    )
    if mode != "cps":
        initial_agents = 1
    max_attempts = _nonnegative_int(
        experiment.get("max_attempts_per_task"),
        "experiment.max_attempts_per_task",
        0 if mode == "cps" else 1,
    )
    cancel_on_proved = bool(experiment.get("cancel_on_proved", True))
    assignment_policy = _text(experiment.get("assignment_policy"), "least_active")
    if assignment_policy not in {"least_active", "round_robin"}:
        raise ConfigError("experiment.assignment_policy must be least_active or round_robin")
    allocation_policy = _text(allocation.get("policy"), "uniform").lower()
    allowed_allocation_policies = {
        "uniform",
        "formula",
        "agent",
        "uniform_refill",
        "task_state",
        "trace_state",
        "llm_scheduler",
    }
    if allocation_policy not in allowed_allocation_policies:
        raise ConfigError(
            "allocation.policy must be one of "
            + ", ".join(sorted(allowed_allocation_policies))
        )
    if figure4_phase and allocation_policy not in _FIGURE4_ALLOCATION_POLICIES:
        raise ConfigError(
            "Figure 4 phase requires one of the four registered allocation policies: "
            + ", ".join(sorted(_FIGURE4_ALLOCATION_POLICIES))
        )
    if figure4_phase == "development" and selection_config.enabled:
        raise ConfigError(
            "Figure 4 development manifests require selection.enabled = false"
        )
    if selection_config.enabled and not figure4_phase:
        if selection_config.direct_messages or selection_config.candidate_transfer:
            raise ConfigError(
                "enabled selection requires direct_messages = false and "
                "candidate_transfer = false"
            )
    if figure4_phase == "development":
        if selection_config.direct_messages or not selection_config.candidate_transfer:
            raise ConfigError(
                "Figure 4 development sentinel requires direct_messages = false and "
                "candidate_transfer = true"
            )
    if figure4_phase == "formal":
        if not selection_config.identity_frozen:
            raise ConfigError(
                "formal Figure 4 requires a complete frozen enabled selector identity"
            )
        if not selection_config.candidate_transfer:
            raise ConfigError("formal Figure 4 requires candidate_transfer = true")
    unknown_formula = set(allocation_formula) - set(_FORMULA_DEFAULTS)
    if unknown_formula:
        raise ConfigError(
            "unknown allocation.formula fields: " + ", ".join(sorted(unknown_formula))
        )
    formula_parameters = {
        key: _number(allocation_formula.get(key), f"allocation.formula.{key}", default)
        for key, default in _FORMULA_DEFAULTS.items()
    }
    # Normalize the pre-component ``drag`` spelling before validating the
    # manifest table.  A manifest that supplies both spellings with different
    # values is contradictory and must fail closed rather than silently choose
    # one coefficient.
    trace_state_values = dict(allocation_trace_state)
    for alias, canonical in _TRACE_STATE_LEGACY_ALIASES.items():
        if alias not in trace_state_values:
            continue
        if canonical in trace_state_values:
            try:
                same = float(trace_state_values[alias]) == float(
                    trace_state_values[canonical]
                )
            except (TypeError, ValueError, OverflowError):
                same = False
            if not same:
                raise ConfigError(
                    f"allocation.trace_state.{alias} contradicts "
                    f"allocation.trace_state.{canonical}"
                )
        else:
            trace_state_values[canonical] = trace_state_values[alias]
        del trace_state_values[alias]

    parameter_tables = (
        ("allocation.task_state", allocation_task_state, _TASK_STATE_DEFAULTS),
        ("allocation.trace_state", trace_state_values, _TRACE_STATE_DEFAULTS),
        (
            "allocation.normalization",
            allocation_normalization,
            _ALLOCATION_NORMALIZATION_DEFAULTS,
        ),
    )
    parsed_parameter_tables: list[dict[str, float]] = []
    for table_name, values, defaults in parameter_tables:
        unknown = set(values) - set(defaults)
        if unknown:
            raise ConfigError(
                f"unknown {table_name} fields: " + ", ".join(sorted(unknown))
            )
        parsed = {
            key: _number(values.get(key), f"{table_name}.{key}", default)
            for key, default in defaults.items()
        }
        if any(value < 0 for value in parsed.values()):
            raise ConfigError(f"{table_name} fields must not be negative")
        parsed_parameter_tables.append(parsed)
    task_state_parameters, trace_state_parameters, normalization_parameters = (
        parsed_parameter_tables
    )
    if any(value <= 0 for value in normalization_parameters.values()):
        raise ConfigError("allocation.normalization fields must be positive")
    for key in (
        "frontier_saturation",
        "association_saturation",
        "duplicate_saturation",
        "refutation_saturation",
        "staleness_saturation",
        "lineage_stagnation_saturation",
    ):
        value = normalization_parameters[key]
        if not value.is_integer():
            raise ConfigError(f"allocation.normalization.{key} must be an integer")
    for key in (
        "failure_penalty",
        "duplication_penalty",
        "progress_window_seconds",
        "starvation_window_seconds",
        "evidence_saturation",
        "failure_saturation",
        "proved_quality",
        "compiles_with_sorry_quality",
        "verify_fail_quality",
        "other_status_quality",
    ):
        if formula_parameters[key] < 0:
            raise ConfigError(f"allocation.formula.{key} must not be negative")
    allocation_config = AllocationConfig(
        policy=allocation_policy,
        piece_limit_per_task=_positive_int(
            allocation.get("piece_limit_per_task"),
            "allocation.piece_limit_per_task",
            3,
        ),
        piece_body_chars=_positive_int(
            allocation.get("piece_body_chars"),
            "allocation.piece_body_chars",
            1_200,
        ),
        agent_timeout_seconds=_positive_int(
            allocation.get("agent_timeout_seconds"),
            "allocation.agent_timeout_seconds",
            120,
        ),
        formula=formula_parameters,
        task_state=task_state_parameters,
        trace_state=trace_state_parameters,
        normalization=normalization_parameters,
        prompt_max_bytes=_positive_int(
            allocation.get("prompt_max_bytes"),
            "allocation.prompt_max_bytes",
            64 * 1024,
        ),
        prompt_max_tokens=_positive_int(
            allocation.get("prompt_max_tokens"),
            "allocation.prompt_max_tokens",
            64 * 1024,
        ),
    )
    episodes = _positive_int(experiment.get("episodes_per_task"), "experiment.episodes_per_task", 1)
    max_tasks = _nonnegative_int(experiment.get("max_tasks"), "experiment.max_tasks", 0)
    horizon = _positive_int(experiment.get("time_limit_seconds"), "experiment.time_limit_seconds", 3600)
    seed = _nonnegative_int(experiment.get("seed"), "experiment.seed", 0)
    if selection_config.enabled and selection_config.seed != seed:
        # Ordinary Figure 3 arms use one seed for both the selector identity
        # and paired requests.  Formal Figure 4 is different: its selector
        # identity is frozen once, while experiment.seed is the independent
        # paired-repeat seed passed to SelectionRuntime.  Keeping these two
        # values separate prevents every allocator repeat from acquiring a
        # different selector configuration hash.
        if figure4_phase != "formal":
            raise ConfigError(
                "enabled selection requires selection.seed == experiment.seed"
            )
    if mode == "mono":
        max_parallel = 1
        episodes = 1
    elif mode == "parallel":
        episodes = 1

    model = _text(pi.get("model"), "openai-codex/gpt-5.6-sol")
    thinking = _text(pi.get("thinking"), "max")
    pi_timeout = _positive_int(pi.get("timeout_seconds"), "pi.timeout_seconds", horizon)
    pi_http_idle_timeout_ms = _nonnegative_int(
        pi.get("http_idle_timeout_ms"),
        "pi.http_idle_timeout_ms",
        600_000,
    )
    pi_retry = _as_dict(pi.get("retry"), "pi.retry")
    pi_retry_provider = _as_dict(pi_retry.get("provider"), "pi.retry.provider")
    pi_recovery = _as_dict(pi.get("recovery"), "pi.recovery")
    pi_retry_enabled = bool(pi_retry.get("enabled", True))
    pi_retry_max_retries = _nonnegative_int(
        pi_retry.get("max_retries"),
        "pi.retry.max_retries",
        10,
    )
    pi_retry_base_delay_ms = _positive_int(
        pi_retry.get("base_delay_ms"),
        "pi.retry.base_delay_ms",
        2_000,
    )
    pi_provider_max_retries = _nonnegative_int(
        pi_retry_provider.get("max_retries"),
        "pi.retry.provider.max_retries",
        0,
    )
    pi_provider_max_retry_delay_ms = _nonnegative_int(
        pi_retry_provider.get("max_retry_delay_ms"),
        "pi.retry.provider.max_retry_delay_ms",
        60_000,
    )
    pi_recovery_enabled = bool(pi_recovery.get("enabled", True))
    pi_recovery_max_restarts = _nonnegative_int(
        pi_recovery.get("max_restarts"),
        "pi.recovery.max_restarts",
        1,
    )
    pi_recovery_base_delay_ms = _nonnegative_int(
        pi_recovery.get("base_delay_ms"),
        "pi.recovery.base_delay_ms",
        1_000,
    )
    fast_mode = bool(pi.get("fast_mode", True))

    aisw_enabled = bool(aisw.get("enabled", True))
    aisw_binary = _text(aisw.get("binary"), "~/.local/share/contextswarm/aisw-linux-aarch64")
    aisw_node_config = _text(aisw.get("node_config"), "~/.aisw-codex/node.toml")
    coordinator = _text(aisw.get("coordinator_url"))
    account = _text(aisw.get("account"))
    group = _text(aisw.get("group"))
    aisw_max_in_flight = _positive_int(
        aisw.get("max_in_flight"),
        "aisw.max_in_flight",
        12,
    )
    lease_wait = _positive_int(aisw.get("lease_wait_seconds"), "aisw.lease_wait_seconds", 7200)
    lease_retry = _positive_int(
        aisw.get("lease_retry_interval_seconds"),
        "aisw.lease_retry_interval_seconds",
        2,
    )

    # The raw endpoint is an operator secret/capability.  It must enter only
    # through the supervisor environment, never through a tracked manifest.
    judge_kind = _text(judge.get("kind"), "formal").lower()
    if judge_kind not in {"formal", "coding"}:
        raise ConfigError("judge.kind must be formal or coding")
    if _text(lean.get("server_url")) or _text(judge.get("server_url")):
        raise ConfigError(
            "judge endpoint URLs are not allowed in manifests; set CONTEXTSWARM_JUDGE_URL at runtime"
        )
    lean_url = _text(os.environ.get("CONTEXTSWARM_JUDGE_URL"))
    # ``[judge]`` is the generic spelling used by coding manifests.  Keep the
    # historical ``[lean]`` fields as a fallback so all existing formal
    # manifests retain byte-for-byte behavior.
    def _judge_value(name: str, default: Any) -> Any:
        return judge[name] if name in judge else lean.get(name, default)

    lean_env = _text(_judge_value("env_id", "formal_matholympiadbench"))
    lean_timeout = _positive_int(
        _judge_value("timeout_seconds", 300), "judge.timeout_seconds", 300
    )
    lean_max_lifecycle = _positive_int(
        _judge_value("max_lifecycle_seconds", max(3_600, (8 * lean_timeout) + 120)),
        "judge.max_lifecycle_seconds",
        max(3_600, (8 * lean_timeout) + 120),
    )
    lean_max_evaluations = _positive_int(
        _judge_value("max_concurrent_evaluations", 1),
        "judge.max_concurrent_evaluations",
        1,
    )
    profile = _text(
        _judge_value("verification_profile", "formal_proof" if judge_kind == "formal" else "coding_contest")
    )
    judge_mode = _text(_judge_value("judge_mode", "fast" if judge_kind == "formal" else "coding"))
    require_result_cache_disabled = bool(
        _judge_value("require_result_cache_disabled", False)
    )
    if "agent_timeout_enabled" in judge:
        judge_agent_timeout_enabled = _strict_bool(
            judge["agent_timeout_enabled"], "judge.agent_timeout_enabled"
        )
    elif "agent_timeout_enabled" in lean:
        # Keep the historical [lean] table usable for local formal manifests,
        # while making the generic [judge] spelling canonical for new arms.
        judge_agent_timeout_enabled = _strict_bool(
            lean["agent_timeout_enabled"], "lean.agent_timeout_enabled"
        )
    else:
        judge_agent_timeout_enabled = False
    formal_tools_enabled = bool(
        formal_tools.get("enabled", judge_kind == "formal")
    )
    if judge_kind == "coding" and formal_tools_enabled:
        raise ConfigError("coding judge manifests cannot enable formal_tools")
    formal_tools_command_timeout_seconds = _positive_int(
        formal_tools.get("command_timeout_seconds"),
        "formal_tools.command_timeout_seconds",
        max(420, lean_timeout + _AGENT_TIMEOUT_HELPER_GRACE_SECONDS),
    )
    if judge_agent_timeout_enabled and formal_tools_enabled:
        # ``evaluate.py`` runs through the Pi Bash capability.  Keep that
        # outer process timeout at least one bounded handoff margin beyond the
        # Agent/Judge budget; otherwise a larger manifest cap would be
        # advertised but the shell could terminate the helper first.  The
        # default 300-second cap retains the historical 420-second value.
        formal_tools_command_timeout_seconds = max(
            formal_tools_command_timeout_seconds,
            lean_timeout + _AGENT_TIMEOUT_HELPER_GRACE_SECONDS,
        )
    formal_tools_version = _text(
        formal_tools.get("surface_version"),
        "contextswarm_mini_formal_tools_v1",
    )
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", formal_tools_version):
        raise ConfigError("formal_tools.surface_version has an invalid format")
    formal_tools_decl_index_sha256 = _text(
        formal_tools.get("decl_index_sha256")
    ).lower()
    if formal_tools_decl_index_sha256 and not re.fullmatch(
        r"[0-9a-f]{64}", formal_tools_decl_index_sha256
    ):
        raise ConfigError("formal_tools.decl_index_sha256 must be a SHA-256 digest")
    formal_tools_mathlib_revision = _text(
        formal_tools.get("mathlib_revision")
    )
    if len(formal_tools_mathlib_revision) > 256:
        raise ConfigError("formal_tools.mathlib_revision is too long")
    docker_network = _text(docker.get("network"), "host").lower()
    if docker_network not in {"host", "bridge"}:
        raise ConfigError("docker.network must be host or bridge")

    cfg = ExperimentConfig(
        manifest_path=manifest_path,
        repo_root=repo_root,
        name=_text(experiment.get("name"), manifest_path.stem),
        mode=mode,
        communication=communication,
        dataset_root=dataset_root,
        problem_ids_path=problem_ids_path,
        output_root=output_root,
        max_parallel=max_parallel,
        initial_agents_per_task=initial_agents,
        max_attempts_per_task=max_attempts,
        cancel_on_proved=cancel_on_proved,
        assignment_policy=assignment_policy,
        allocation=allocation_config,
        selection=selection_config,
        figure4_phase=figure4_phase,
        episodes_per_task=episodes,
        max_tasks=max_tasks,
        time_limit_seconds=horizon,
        seed=seed,
        model=model,
        thinking=thinking,
        fast_mode=fast_mode,
        pi_binary=_text(pi.get("binary")),
        pi_extension=_text(pi.get("extension")),
        pi_timeout_seconds=pi_timeout,
        pi_http_idle_timeout_ms=pi_http_idle_timeout_ms,
        pi_retry_enabled=pi_retry_enabled,
        pi_retry_max_retries=pi_retry_max_retries,
        pi_retry_base_delay_ms=pi_retry_base_delay_ms,
        pi_provider_max_retries=pi_provider_max_retries,
        pi_provider_max_retry_delay_ms=pi_provider_max_retry_delay_ms,
        pi_recovery_enabled=pi_recovery_enabled,
        pi_recovery_max_restarts=pi_recovery_max_restarts,
        pi_recovery_base_delay_ms=pi_recovery_base_delay_ms,
        aisw_enabled=aisw_enabled,
        aisw_binary=aisw_binary,
        aisw_node_config=aisw_node_config,
        aisw_coordinator_url=coordinator,
        aisw_account=account,
        aisw_group=group,
        aisw_max_in_flight=aisw_max_in_flight,
        aisw_lease_wait_seconds=lease_wait,
        aisw_lease_retry_interval_seconds=lease_retry,
        lean_server_url=lean_url,
        lean_env_id=lean_env,
        lean_timeout_seconds=lean_timeout,
        lean_max_lifecycle_seconds=lean_max_lifecycle,
        lean_max_concurrent_evaluations=lean_max_evaluations,
        lean_verification_profile=profile,
        lean_judge_mode=judge_mode,
        lean_require_result_cache_disabled=require_result_cache_disabled,
        judge_kind=judge_kind,
        formal_tools_enabled=formal_tools_enabled,
        formal_tools_version=formal_tools_version,
        formal_tools_evaluate_calls_per_task=_positive_int(
            formal_tools.get("evaluate_calls_per_task"),
            "formal_tools.evaluate_calls_per_task",
            120,
        ),
        formal_tools_evaluate_backend_jobs_per_task=_positive_int(
            formal_tools.get("evaluate_backend_jobs_per_task"),
            "formal_tools.evaluate_backend_jobs_per_task",
            120,
        ),
        formal_tools_query_calls_per_task=_positive_int(
            formal_tools.get("query_calls_per_task"),
            "formal_tools.query_calls_per_task",
            60,
        ),
        formal_tools_query_backend_probes_per_task=_positive_int(
            formal_tools.get("query_backend_probes_per_task"),
            "formal_tools.query_backend_probes_per_task",
            120,
        ),
        formal_tools_max_candidate_bytes=_positive_int(
            formal_tools.get("max_candidate_bytes"),
            "formal_tools.max_candidate_bytes",
            2 * 1024 * 1024,
        ),
        formal_tools_command_timeout_seconds=formal_tools_command_timeout_seconds,
        formal_tools_decl_index=_text(formal_tools.get("decl_index")),
        formal_tools_decl_index_sha256=formal_tools_decl_index_sha256,
        formal_tools_mathlib_revision=formal_tools_mathlib_revision,
        formal_tools_require_decl_index=bool(
            formal_tools.get("require_decl_index", True)
        ),
        docker_image=_text(docker.get("image"), "contextswarm-iclr-mini:latest"),
        docker_memory_mb=_positive_int(docker.get("memory_mb"), "docker.memory_mb", 16384),
        docker_internet=_text(docker.get("internet"), "online"),
        docker_network=docker_network,
        judge_agent_timeout_enabled=judge_agent_timeout_enabled,
        extra={"raw": payload},
    )
    if cfg.uses_cps and cfg.episodes_per_task < 1:
        raise ConfigError("CPS requires at least one episode per task")
    return cfg
