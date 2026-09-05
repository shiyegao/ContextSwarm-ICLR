"""Shared policy for optional Agent-proposed Judge validation budgets.

The timeout exposed to a worker is intentionally a small, integer-only
capability.  When supplied, it is the cumulative logical budget for one
validation call, including safe evaluator retries; it is not a transport
deadline or a run-horizon override.  The broker owns validation and the
evaluator applies the final defence-in-depth clamp before constructing Judge
jobs.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


# The default capability range is intentionally conservative.  The upper
# bound is a manifest default, rather than a process-wide constant: an
# experiment may choose a different evaluator timeout and the worker-facing
# prompt/tool schema must follow that choice.
AGENT_TIMEOUT_MIN_SECONDS = 5
AGENT_TIMEOUT_MAX_SECONDS = 300


@dataclass(frozen=True)
class AgentTimeoutBounds:
    """Effective worker-facing timeout range for one evaluator configuration."""

    min_seconds: int
    max_seconds: int

    def public_dict(self) -> dict[str, int]:
        return {
            "min": self.min_seconds,
            "max": self.max_seconds,
        }


def _configured_timeout_cap(value: Any) -> int:
    """Normalize a manifest/evaluator timeout into a positive integer cap."""

    if isinstance(value, bool):
        raise ValueError("configured evaluator timeout is invalid")
    try:
        configured = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("configured evaluator timeout is invalid") from exc
    if not math.isfinite(configured) or configured <= 0:
        raise ValueError("configured evaluator timeout is invalid")
    return max(1, int(configured))


def agent_timeout_bounds(
    configured_timeout_seconds: int | float | None = None,
) -> AgentTimeoutBounds:
    """Return the actual range advertised to an Agent.

    ``AGENT_TIMEOUT_MAX_SECONDS`` remains the backwards-compatible default
    when a caller does not provide an evaluator timeout.  Once a manifest
    supplies one, that value is the hard upper bound; it is deliberately not
    clipped back to the default 300 seconds.  This keeps the prompt, tool
    schema, broker and evaluator contracts aligned for both smaller and larger
    manifests.
    """

    cap = (
        AGENT_TIMEOUT_MAX_SECONDS
        if configured_timeout_seconds is None
        else _configured_timeout_cap(configured_timeout_seconds)
    )
    return AgentTimeoutBounds(
        min_seconds=min(AGENT_TIMEOUT_MIN_SECONDS, cap),
        max_seconds=cap,
    )


@dataclass(frozen=True)
class AgentTimeout:
    """The requested value and bounded total budget for one logical call."""

    requested_seconds: int
    effective_seconds: int
    clamped: bool

    def public_dict(self) -> dict[str, Any]:
        return {
            "requested_timeout_seconds": self.requested_seconds,
            "effective_timeout_seconds": self.effective_seconds,
            "timeout_clamped": self.clamped,
        }


def normalize_agent_timeout(
    value: Any,
    *,
    configured_timeout_seconds: int | float | None = None,
) -> AgentTimeout:
    """Validate and clamp one worker-proposed timeout.

    ``configured_timeout_seconds`` is the evaluator's own hard ceiling.  It is
    normally the manifest default of 300 seconds, but a deliberately smaller
    or larger manifest value becomes the actual Agent-facing cap.  A malformed
    value is rejected instead of being silently converted; numeric values
    outside the advertised range are clamped and recorded for audit.
    """

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("timeout_seconds must be an integer")

    bounds = agent_timeout_bounds(configured_timeout_seconds)
    cap = bounds.max_seconds
    floor = bounds.min_seconds
    effective = max(floor, min(cap, int(value)))
    return AgentTimeout(
        requested_seconds=int(value),
        effective_seconds=effective,
        clamped=effective != int(value),
    )


def timeout_fields(timeout: AgentTimeout | None) -> dict[str, Any]:
    """Return bounded, stable metadata suitable for worker/audit records."""

    if timeout is None:
        return {
            "requested_timeout_seconds": None,
            "effective_timeout_seconds": None,
            "timeout_clamped": False,
            "timeout_source": "configured_legacy",
        }
    return {
        **timeout.public_dict(),
        "timeout_source": "agent_requested",
    }


__all__ = [
    "AGENT_TIMEOUT_MAX_SECONDS",
    "AGENT_TIMEOUT_MIN_SECONDS",
    "AgentTimeout",
    "AgentTimeoutBounds",
    "agent_timeout_bounds",
    "normalize_agent_timeout",
    "timeout_fields",
]
