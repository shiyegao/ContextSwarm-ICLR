# ContextSwarm ICLR working contract

- This worktree is an independent artifact. Do not edit or reset the sibling
  upstream `ContextSwarm` worktree.
- Keep credentials, `node.toml` contents, and private endpoints out of tracked
  files and run summaries.
- Preserve the registered comparison contract. Allocation arms may differ only
  in their manifest-selected policy; keep tasks, model, horizon, CPS capacity,
  evaluator/Judge contract, and runtime limits fixed across arms. Keep Mono and
  Parallel communication-free. Do not tune non-policy parameters between arms
  in response to observed outcomes.
- A formal arm ends at full score or at its configured horizon. There is no
  separate “stable convergence” requirement. A failed individual attempt must
  not terminate the arm while time and slots remain: retain the best candidate
  and CPS state, record the failure, and refill the released slot.
- Provider/coordinator/network instability is recoverable runtime noise when it
  is reported as an abnormal non-timeout result (including a transport/provider
  diagnostic that happens to contain “timeout” while ``AgentResult.timed_out``
  is false). Retry with backoff within the same horizon; when a session exhausts
  its retries, resume or relaunch only the affected agent/slot from persisted
  state. A task/Pi deadline timeout (``AgentResult.timed_out=True``) and a
  runner-owned intentional cancellation are terminal for that logical actor:
  do not same-session recover or same-actor refill them. CPS may release the
  slot and admit a fresh assignment under the fixed scheduler contract. Retry
  time still counts against the fixed horizon.
- A job-bound terminal Judge result about the submitted candidate—including
  compile/verification failure, `RESOURCE_LIMIT`, and `EXECUTION_TIMEOUT`—is a
  candidate-attempt outcome by default. Record it as feedback/zero progress and
  continue; do not call it an experiment infrastructure failure or retune
  resource limits merely because it occurred. A status label or a `retryable`
  flag alone is not grounds for discarding the arm.
- Classify a Judge problem as infrastructure only with candidate-independent
  evidence: for example a failed health/control check, a pre-admission
  transport outage, a malformed or contradictory receipt, or a job that cannot
  be reconciled to a terminal receipt. Retry/reconcile confirmed transient
  infrastructure failures within the remaining horizon. A delayed cancellation
  must not kill unrelated tasks; quarantine only the affected remote capacity
  until it settles. Abort an arm only when authoritative evaluation cannot be
  recovered, and report that as a runner/Judge protocol failure rather than a
  candidate failure.
- Before handing off a change, run `python3 -m compileall -q contextswarm_mini`,
  `python3 -m unittest discover -s tests`, and a `configs/smoke.toml` mock run.
