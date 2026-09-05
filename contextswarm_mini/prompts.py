"""Stable worker prompts for formal and coding experiment protocols."""

from __future__ import annotations

from typing import Iterable

from .models import Task
from .timeout_policy import agent_timeout_bounds


FORMAL_EXECUTION_CONTRACT = """Execution and verification contract (mandatory):
- Use only `judge_check`, the experiment-provided controlled Judge interface, for
  authoritative Lean checking. When the manifest exposes the bounded formal-helper
  surface, `python3 evaluate.py` and `./formal_query ...` are advisory diagnostics
  through the same controlled remote Judge capability; they never run Lean locally,
  select a candidate, or establish official success.
- Internet and web access are prohibited. Do not browse the web, use a browser or
  search engine, consult online Lean/Mathlib documentation, or use any
  internet-connected tool.
- Solve the task independently and answer carefully. Rely on your own mathematical
  reasoning, the statement, the neutral baseline, and permitted Judge/CPS feedback;
  do not copy or trust externally sourced proofs.
- The controlled Judge already owns the Lean/Mathlib toolchain, downloads,
  compilation, tests, and verification. Treat it as the only place where those
  operations may run; never reproduce any of them in the worker container.
  `CONTEXTSWARM_JUDGE_URL` is reserved for that tool and runner-owned helpers; it is
  a runner-injected, session-scoped capability, not a general endpoint or permission
  to construct another client. Do not read, print, modify, or contact it yourself.
- A mandatory early Judge checkpoint is required for every assigned task. After
  reading the theorem and current `result.lean`, immediately submit the current
  candidate through `judge_check` before any optional helper diagnostic,
  coordination, recipient discovery, or extended proof search.
  Do not wait for a polished proof: `COMPILES_WITH_SORRY`, `VERIFY_FAIL`, or another
  job-bound terminal candidate outcome is useful remote feedback. Resource and
  execution-limit outcomes are feedback rather than permission for a local fallback.
  Afterward, keep checks serial and submit again only after a material edit.
- Never invoke local `lean`, `lake`, `elan`, a local verifier, proof-search service,
  or any other local proof checker. Do not install or download Lean, Mathlib,
  toolchains, packages, caches, compilers, or solver infrastructure.
- Do not run resource-heavy local computation or start background, detached, or
  parallel processes. In particular, do not fan out candidate checks or use shell
  job control, `xargs -P`, GNU Parallel, subprocess pools, or similar mechanisms.
- Never call a raw Judge or evaluator HTTP endpoint with curl, wget, Python,
  JavaScript, or another network client. Do not probe ports, services, credentials,
  process sidecars, or evaluator implementation files.
- If `judge_check` is temporarily unavailable, overloaded, or returns a retryable
  result, wait/retry only through `judge_check` within the experiment budget, or
  leave the strongest candidate in `result.lean` for the runner. Never fall back to
  a local checker, raw HTTP, or a separately installed service.
- Your allowed proof context is this assigned workspace plus shared candidates,
  CPS state, and helper tools explicitly provided by the runner in this prompt.
  Do not browse unrelated workers, sessions, host paths, or runtime internals.
"""

# Historical callers import this name.  Keep it as an alias so existing formal
# prompt and problem-statement contracts remain byte-for-byte compatible.
SOLVER_EXECUTION_CONTRACT = FORMAL_EXECUTION_CONTRACT


CODING_EXECUTION_CONTRACT = """Execution and verification contract (mandatory):
- Use only `judge_check`, the experiment-provided controlled Judge interface, for
  authoritative C++ compilation, execution, and test verdicts. The Judge owns
  the compiler, runtime sandbox, test data, and resource limits; its terminal
  result is the only authority for success.
- Any public AC, provenance, repository, or other URL printed in `problem.md` is
  non-actionable metadata. Never open, follow, fetch, search, download, or copy
  a solution from such a URL; solve only from the statement, the neutral local
  skeleton, and feedback returned by `judge_check`.
- Internet and web access are prohibited. Do not browse the web, use a browser or
  search engine, DNS, an external API, or any other internet-connected tool.
- Solve the task independently and answer carefully. Rely on your own reasoning, the
  statement, the neutral baseline, and permitted Judge/CPS feedback; do not copy or
  trust externally sourced solutions.
- `CONTEXTSWARM_JUDGE_URL` is reserved for that runner-owned capability. It is
  session-scoped and is not permission to construct another client, contact an
  endpoint, or inspect Judge implementation details. Never read, print, modify,
  or contact it yourself.
- A mandatory early Judge checkpoint is required for every assigned task. After
  reading the statement and current `result.cpp`, immediately submit the current
  candidate through `judge_check` before optional diagnostics, coordination,
  recipient discovery, or extended solution search. Do not wait for a polished
  solution: compile errors, wrong answers, resource limits, and execution
  limits are useful job-bound feedback. Afterward, keep checks serial and submit
  again only after a material edit.
- Never invoke a local compiler, executable test runner, online judge, solver, or
  other verification service. Do not install or download compilers, libraries,
  packages, test data, caches, or solver infrastructure.
- Do not run resource-heavy local computation or start background, detached, or
  parallel processes. In particular, do not fan out candidate checks or use
  shell job control, `xargs -P`, GNU Parallel, subprocess pools, or similar
  mechanisms.
- Never call a raw Judge or evaluator HTTP endpoint with curl, wget, Python,
  JavaScript, or another network client. Use `judge_check` only.
- If `judge_check` is temporarily unavailable, overloaded, or returns a
  retryable result, wait/retry only through `judge_check` within the experiment
  budget, or leave the strongest candidate in `result.cpp` for the runner.
- Your allowed context is this assigned workspace plus shared CPS state and
  helper tools explicitly provided by the runner. Do not browse unrelated
  workers, sessions, host paths, or runtime internals.
"""


# This block is duplicated into the self-contained benchmark problem statements.
# Keep it centralized here, use scripts/sync_problem_work_mode.py to update all
# statements, and retain the test that rejects drift between those copies.
PROBLEM_WORK_MODE_CONTRACT = """- Follow the mandatory execution and verification contract in the worker prompt.
- Treat this as offline proof construction. All Lean execution belongs to the
  controlled external Judge; `judge_check` is the sole authoritative interface.
- If the manifest exposes `evaluate.py` or `formal_query`, use them only as bounded
  remote Judge diagnostics; they never run Lean locally, establish official success,
  or select a candidate. The runner injects and owns their Judge capability and URL.
- The Judge already provides Lean/Mathlib downloads, compilation, tests, and
  verification; submit those operations through the runner-controlled interfaces
  instead of doing them in the local worker environment.
- Never execute local Lean/lake/elan, install or download Lean/Mathlib/toolchains,
  run a local verifier or proof search, perform resource-heavy computation, or
  start background or parallel processes. Never call raw Judge HTTP endpoints.
- If `judge_check` is unavailable or overloaded, retry/wait only through that tool
  within budget, or leave the best `result.lean`; do not create a local fallback.
- The assigned scope includes this task directory and any shared CPS context,
  shared candidate, or helper tool explicitly named by the runner's worker prompt.
  Do not browse any other home, system, runtime, worker, or session artifacts.
- Edit `result.lean` only within the allowed proof surface described above."""


def render_problem_work_mode(*, indent: str = "        ") -> str:
    """Render the canonical block used by the self-contained task statements."""

    return "\n".join(
        f"{indent}{line}" if line else ""
        for line in PROBLEM_WORK_MODE_CONTRACT.splitlines()
    )


def _communication_instructions(
    enabled: bool,
    *,
    direct_messages: bool = True,
    selection_enabled: bool = False,
) -> str:
    if not enabled and not selection_enabled:
        return (
            "This is a no-communication baseline. Do not read or write any shared "
            "CPS/blackboard state; work only from the files in this workspace."
        )
    direct = """
Use `cps_inbox` to receive direct messages, `cps_send` to send one, `cps_ack` to
acknowledge one, and `cps_actors` only when recipient discovery is needed.""" if direct_messages else ""
    selection = """
Use `cps_feedback` only to record concise selection feedback for the runner-owned
allocation state; it is not a direct-message channel.""" if selection_enabled else ""
    return f"""This run exposes shared ContextSwarm state only through controlled CPS tools.
Before trying a route, use `cps_search` to find relevant shared evidence. After a
meaningful discovery, use `cps_publish` to leave a concise typed handoff. Use
{direct}{selection}
Do not access CPS through a local CLI, database, filesystem search, or custom
script. Never include credentials, absolute host paths, or full transcripts in a
piece or message."""


def _formal_tools_instructions(enabled: bool) -> str:
    if not enabled:
        return ""
    return """This manifest may expose a bounded formal-helper surface documented in
PUBLIC_FILES.md. Use only the exact staged helper commands named there; they are
advisory diagnostics and never official score or candidate-selection authority.
Do not inspect helper source, alter capability metadata, or use any other shell
command."""


def _scaled_timeout_value(cap: int, floor: int, ratio: float) -> int:
    """Scale a heuristic by the configured cap while preserving its bounds."""

    # Match JavaScript ``Math.round`` used by the Pi extension (positive
    # values only, so adding 0.5 gives the same half-up result).
    return max(floor, min(cap, int(cap * ratio + 0.5)))


def _timeout_tier(low: int, high: int, *, ratio_label: str) -> str:
    """Render a heuristic tier without claiming a percentage after rounding."""

    rendered = f"{low}–{high} seconds"
    if low == high:
        return f"{rendered} (rounded for this configured cap)"
    return f"{rendered} ({ratio_label} of the cap)"


def _agent_timeout_instructions(
    enabled: bool,
    *,
    formal: bool,
    formal_tools_enabled: bool = False,
    configured_timeout_seconds: int | float | None = None,
) -> str:
    """Render the opt-in worker guidance without changing baseline prompts."""

    if not enabled:
        return ""
    bounds = agent_timeout_bounds(configured_timeout_seconds)
    floor = bounds.min_seconds
    cap = bounds.max_seconds
    sanity_low = floor
    sanity_high = _scaled_timeout_value(cap, floor, 0.05)
    routine_low = _scaled_timeout_value(cap, floor, 0.10)
    routine_high = _scaled_timeout_value(cap, floor, 0.20)
    heavy_low = _scaled_timeout_value(cap, floor, 0.40)
    heavy_high = _scaled_timeout_value(cap, floor, 0.60)
    routine_example = routine_high
    routine_guidance = _timeout_tier(
        routine_low, routine_high, ratio_label="10–20%"
    )
    heavy_guidance = _timeout_tier(
        heavy_low, heavy_high, ratio_label="40–60%"
    )
    sanity_guidance = _timeout_tier(
        sanity_low, sanity_high, ratio_label="about 5% or less"
    )
    helper = (
        " For formal diagnostics, `python3 evaluate.py --timeout N` sends the same budget."
        if formal and formal_tools_enabled
        else ""
    )
    example = f'`{{"timeout_seconds": {routine_example}}}`'
    if formal and formal_tools_enabled:
        example += f" and the formal helper can use `python3 evaluate.py --timeout {routine_example}`"
    return f"""Agent-proposed validation budget (enabled for this run):
- `judge_check` accepts an optional integer `timeout_seconds` in the advertised
  range {floor}–{cap} seconds.{helper} The runner
  clamps values outside that range (and its hard evaluator ceiling) before
  submission; the effective value is reported in the bounded receipt.
- In this treatment, normally include a value on every validation call so the
  experiment can observe your estimate; omit it only when you deliberately
  choose the legacy timeout/retry behavior. For example, a routine check can use
  {example}.
- This is one cumulative logical validation budget across all evaluator retries.
  The runner owns one absolute deadline: a first attempt that fails abnormally
  after about one tenth of the selected budget leaves the remainder for a safe
  retry; an attempt that consumes the deadline leaves no retry budget.
  Retry count is therefore independent of the chosen seconds, and retries do
  not multiply the requested timeout. The budget includes broker admission and
  evaluator work, but it is not the outer experiment horizon or provider/Pi
  timeout. A short, bounded remote-settlement cleanup grace may make the final
  HTTP receipt arrive slightly after the deadline; it is recorded separately.
  Omitting the field keeps the configured legacy timeout/retry policy.
- As a starting heuristic relative to the configured cap ({cap} seconds), use
  about {routine_guidance} for routine incremental checks; {heavy_guidance} for
  a promising candidate with heavy imports, elaboration, or resource-sensitive
  code; and reserve the full {cap} seconds only for a genuinely likely
  near-complete but known-slow check. Use about {sanity_guidance} only for
  cheap sanity feedback after an obvious edit, not for the first checkpoint or
  immediately after changing imports/large definitions.
- `EXECUTION_TIMEOUT` is inconclusive candidate feedback, not `VERIFY_FAIL`,
  proof of correctness, or permission to run a local checker. After a timeout,
  inspect the feedback and make a material edit (or leave the best candidate)
  before trying again. Candidate-independent transport/runtime failures may be
  retried automatically while the same cumulative budget remains; deterministic
  verdicts and timeout/cancellation results are not automatically replayed.
  Keep calls serial and respect the session budget."""


def _is_coding_task(task: Task) -> bool:
    """Whether ``task`` is a C++ Judge task (formal remains the default)."""

    return task.candidate_filename == "result.cpp"


def _execution_contract(task: Task) -> str:
    return CODING_EXECUTION_CONTRACT if _is_coding_task(task) else FORMAL_EXECUTION_CONTRACT


def _candidate_context(task: Task) -> tuple[str, str, str]:
    """Return candidate filename, baseline glob, and candidate noun."""

    if _is_coding_task(task):
        return "result.cpp", "baseline/*.cpp", "solution"
    return "result.lean", "baseline/*.lean", "proof"


def build_task_prompt(
    task: Task,
    *,
    task_workspace: str,
    agent_id: str,
    episode: int,
    communication_enabled: bool,
    formal_tools_enabled: bool = False,
    agent_timeout_enabled: bool = False,
    agent_timeout_cap_seconds: int | float | None = None,
    direct_messages: bool = True,
    selection_enabled: bool = False,
    digest: str = "",
) -> str:
    context = digest.strip() or "(no prior shared context for this task)"
    candidate, baseline_glob, noun = _candidate_context(task)
    coding = _is_coding_task(task)
    kind = "coding" if coding else "formal-proof"
    statement = (
        f"The public statement is in problem.md. The immutable starting skeleton is in {baseline_glob}.\n"
        f"Write your candidate solution only to {candidate} and preserve the source contract."
        if coding
        else f"The public statement is in problem.md. The immutable starting skeleton is in baseline/.\n"
        f"Write your candidate proof only to {candidate} and preserve the theorem statement,\n"
        "imports, namespace, and source contract."
    )
    return f"""You are worker {agent_id}, episode {episode}, in a bounded {kind} experiment.

Task: {task.slug}
Workspace: {task_workspace}
{statement} The controlled external Judge, accessed only through `judge_check`, is
the only authority for success; do not claim success from intuition, a text scan,
or a local verification process.

{_execution_contract(task)}

{_communication_instructions(communication_enabled, direct_messages=direct_messages, selection_enabled=selection_enabled)}

{_formal_tools_instructions(formal_tools_enabled and not coding)}

{_agent_timeout_instructions(
    agent_timeout_enabled,
    formal=not coding,
    formal_tools_enabled=formal_tools_enabled and not coding,
    configured_timeout_seconds=agent_timeout_cap_seconds,
)}

Relevant shared context (possibly empty):
---
{context}
---

Work in small {noun}-construction increments. Inspect the existing {candidate} first,
make a concrete candidate, and leave the best candidate in {candidate} before ending.
When feedback is useful, check one candidate at a time with `judge_check`.
"""


def build_mono_prompt(
    tasks: Iterable[Task],
    *,
    workspace: str,
    communication_enabled: bool,
    formal_tools_enabled: bool = False,
    agent_timeout_enabled: bool = False,
    agent_timeout_cap_seconds: int | float | None = None,
    direct_messages: bool = True,
    selection_enabled: bool = False,
) -> str:
    task_list = list(tasks)
    if not task_list:
        raise ValueError("build_mono_prompt requires at least one task")
    coding_values = {_is_coding_task(task) for task in task_list}
    if len(coding_values) != 1:
        raise ValueError("Mono prompt cannot mix formal and coding tasks")
    coding = coding_values.pop()
    candidate = "result.cpp" if coding else "result.lean"
    baseline_glob = "baseline/*.cpp" if coding else "baseline/*.lean"
    contract = CODING_EXECUTION_CONTRACT if coding else FORMAL_EXECUTION_CONTRACT
    noun = "solutions" if coding else "proofs"
    bundle_kind = "coding" if coding else "formal"
    task_lines = "\n".join(f"- {task.slug}: tasks/{task.slug}/" for task in task_list)
    return f"""You are the Mono baseline worker for a fixed 12-task {bundle_kind} bundle.

One Pi session must work through the following task directories serially:
{task_lines}

For each task, read its problem.md and {baseline_glob}, then write the candidate to
tasks/<slug>/{candidate} (the runner also maintains the aggregate result bundle).
Do not modify the source statement or baseline. The runner
evaluates every candidate after this session and counts only canonical PROVED verdicts.

{contract}

Mono task-selection rule: this session owns multiple task directories. For every
`judge_check` call in Mono, pass the exact current task slug as
`{{"task_id": "<slug>"}}`; never make a no-argument call. A single-task
Parallel worker may omit `task_id`, but Mono may not.

{_communication_instructions(communication_enabled, direct_messages=direct_messages, selection_enabled=selection_enabled)}

{_formal_tools_instructions(formal_tools_enabled and not coding)}

{_agent_timeout_instructions(
    agent_timeout_enabled,
    formal=not coding,
    formal_tools_enabled=formal_tools_enabled and not coding,
    configured_timeout_seconds=agent_timeout_cap_seconds,
)}

Use the available wall-clock budget on concrete {noun} construction. Leave every
task directory with its best candidate, even if some targets remain incomplete.
"""


def build_finalization_prompt(task: Task, *, digest: str = "") -> str:
    candidate, _baseline_glob, noun = _candidate_context(task)
    guard = (
        "Do not change the problem contract or add non-solution code."
        if _is_coding_task(task)
        else "Do not change the theorem contract or add proof-bypass declarations."
    )
    return f"""Re-open {task.slug}/{candidate} and leave the strongest {noun} in place.
Review the latest evaluator feedback and any relevant shared handoff below. {guard}

{_execution_contract(task)}

{digest or '(no shared handoff)'}
"""
