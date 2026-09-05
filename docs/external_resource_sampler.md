# External Judge/Lean host-side resource sampler

`scripts/external_resource_sampler.py` is a bounded, read-only observer for a
real run.  It is deliberately outside the runner and does not change the
ContextSwarm algorithm, start or stop a Judge/router, send an HTTP request, or
read credentials.  Run it from a host shell while the already-authorized
experiment is running.

The sampler is intended to close the attribution gap between the in-process
`profiling.jsonl` stream and the external formal stack.  In particular, it
keeps the following layers separate:

| layer | what it represents | what it must not absorb |
| --- | --- | --- |
| `backend` | external Judge/OJ/Lean backend process group and descendants | Pi/runner memory or router memory |
| `router` | external request router process group and descendants | Judge worker memory or Pi/runner memory |
| `worker` | external evaluator/Lean worker process group and descendants | the router and unrelated host services |
| `agent` / `wrapper` (optional) | an explicitly selected client-side process group | any service layer unless its root PID is intentionally selected |

The operator supplies root PIDs explicitly.  There is no command-line substring
search, no process environment read, and no endpoint discovery.  A root's
process group is included, as are descendants that are reachable by parent PID.
The root PID start time and process-group ID are pinned at the first sample;
PID reuse is reported and excluded from attribution.

## Safe invocation contract

Use a fresh task-local output directory on the workspace's disk-backed
filesystem.  Do not place the output or any build/cache data on `/tmp`,
`/dev/shm`, or another memory filesystem.  The directory is created as `0700`,
`samples.jsonl` and `summary.json` as `0600`.  An existing `samples.jsonl` or
`summary.json` is rejected; this prevents a second run from silently appending
to (or replacing) the first run's ledger.

The smallest useful invocation is:

```bash
python3 scripts/external_resource_sampler.py \
  --layer backend=<backend-root-pid> \
  --layer router=<router-root-pid> \
  --layer worker=<worker-root-pid> \
  --duration <run-window-seconds> \
  --interval 5 \
  --output-dir <task-output>/external-resource
```

`--layer` may be repeated and accepts a comma-separated list of roots.  The
named convenience flags (`--backend-pid`, `--router-pid`, `--worker-pid`,
`--agent-pid`, and `--wrapper-pid`) are equivalent.  Prefer the PID returned by
the service launcher or a task-owned pidfile.  Do not infer a service from a
port, and do not pass a shell, supervisor, or whole-host PID unless that is the
intended process boundary.  If two layers share a process or cgroup, the
sampler records the overlap instead of pretending the totals are disjoint.
The sampler process itself is excluded from memberships unless its PID is
explicitly selected, which avoids charging observer overhead to a service when
both are started from one shell.

The sampler is bounded by construction:

* default interval: 5 seconds (accepted range 0.1–60 seconds);
* default maximum samples: 720 (override with `--max-samples`, hard maximum
  100,000);
* default JSONL sample budget: 64 MiB (`--max-output-bytes`); and
* at most 64 PID values per layer are written (`--max-pid-sample`), while
  aggregate counters still include every discovered member.

Each sample also carries an `observer` object that accounts for the sampler's
own work without attributing it to any selected service layer. It includes the
wall and process-CPU time spent in `snapshot`, the number of `/proc` rows
enumerated, `stat`/`status`/`smaps_rollup` and cgroup file reads, the candidate
PID count, and the detailed-process count. `summary.json.observer.run_window`
contains fixed-width total, mean, and maximum values for these fields. This is
diagnostic overhead evidence: it does not change membership discovery or the
service CPU/RSS counters, and it must not be subtracted from service CPU
without a separately controlled comparison.

`--duration 0` records the warm baseline only.  A positive duration records a
baseline and then a run-window sample at each interval.  The sampler exits
with status 0 for complete collection *and* for collection with observability
warnings; inspect `summary.json.warnings` before treating a result as complete.
Invalid selectors, unsafe output paths, or an unreadable output directory exit
with status 2.

## Output contract

Every row carries `schema_version =
contextswarm_external_resource_profile_v1`.  The first JSONL row is:

```json
{"kind":"baseline","sample":{...}}
```

Subsequent rows have `kind = "sample"`.  A sample contains only bounded
scalars, short process names, PID samples, state counters, and one-way hashed
cgroup identifiers.  It never contains command-line arguments, environment
values, prompts, candidates, provider responses, credentials, endpoint URLs,
or host filesystem paths.

Each selected layer reports:

* process and root liveness: `pid_count`, `root_pids`,
  `live_root_pids`, `state_counts`, and a bounded `pid_sample`;
* memory: summed `rss_bytes`, summed `pss_bytes` when **all** selected
  processes expose `smaps_rollup`, and cgroup `memory.current`/`memory.peak`;
* CPU: cumulative process `cpu_user_seconds`, `cpu_system_seconds`,
  `cpu_time_seconds`, plus cgroup `usage_usec`, user/system usec, period and
  throttle counters;
* scheduler/process pressure: `thread_count`, `context_switches`, and
  cgroup `pids.current`/`pids.max`, plus `memory.events` and OOM-kill counters;
  and
* cgroup identity/quality: `cgroup_count`, hashed `cgroup_ids`, and warnings
  when PSS or cgroup counters are unavailable.

`summary.json` has three intentionally different views:

1. `baseline`: the warm service state before the run window;
2. `run_window`: first/last observed window sample, sample count, elapsed time,
   and absolute peaks; and
3. `delta_from_baseline`: final-minus-baseline counters and CPU rates.

This separation matters: a long-lived Judge's warm RSS must not be called
agent overhead, while a run-window peak is still important for capacity and
OOM analysis.

## CPU and memory interpretation

The process and cgroup CPU values are cumulative counters.  The sampler
differentiates counters between snapshots to report `cpu_percent_one_core` and
`cpu_percent_host_capacity` for a selected **process group**.  These values
cannot be divided by the number of model jobs to derive per-job CPU: process
sharing, idle time, retries, and cgroup accounting make that inference
invalid.  Use ContextSwarm's task/attempt events to correlate lifecycle, and
use this sampler only for host/process-group attribution.

RSS is a sum and can double-count shared pages.  PSS is preferable, but a
missing or partially readable `smaps_rollup` causes `pss_bytes = null` and a
warning rather than a fabricated value.  Cgroup counters are summed once per
unique cgroup *within a layer*; if backend/router/worker share a cgroup,
`layer_cgroup_overlap` and `layer_membership_overlap` make that fact explicit.
Do not add layer totals when either overlap map is non-empty.

## Mapping to the six profiling questions

The host sampler is complementary to the in-process profiler; it does not
replace event spans or SQLite/selection counters.

| question | sampler fields to compare | companion in-process evidence |
| --- | --- | --- |
| single-attempt agent vs wrapper | separate `agent` and `wrapper` roots; `rss/pss`, CPU deltas, PID/thread counts | `resource.process`, model/tool spans |
| selection full-chain cost | not measured by Judge processes; use layer `wrapper` only for host pressure | selection spans, candidate/token/JSON/hash/SQLite counters |
| `record_search` write-lock impact | router/backend CPU, throttling, PID liveness during contention | `lock_wait_seconds`, transaction and scheduler spans |
| Trace projection and repeat materialization | external counters only show resulting pressure, not cause | trace/snapshot read and projection spans |
| `max_parallel` process-tree amplification | per-layer absolute peaks and deltas, `pid_count`, `thread_count`, PSS/RSS, cgroup pids/memory | attempt registration and slot lifecycle events |
| CPS progress/SQLite overhead | wrapper/backend resource deltas around allocator windows | CPS scan, connection, WAL, lock and allocator spans |

For the first run, capture one baseline before admitting work and keep the
same layer roots and interval across comparison arms.  For a later 10/100/1000
scale sweep, preserve the selector contract and change only the declared scale
parameter; otherwise the resource deltas are not comparable.

## Failure and evidence boundaries

Warnings are evidence-quality annotations, not Judge verdicts.  Typical
warnings are `root_missing`, `root_pid_reused`, `pss_unavailable`,
`cgroup_unavailable`, `layer_membership_overlap`, and
`layer_cgroup_overlap`.  A process disappearing is a useful lifecycle signal;
it is not by itself proof of an OOM or a Judge failure.  Correlate with the
container/cgroup `memory.events` and the runner's terminal receipt.

The sampler has no per-request or per-job attribution.  It also cannot prove
that a process was making useful model progress; pair the samples with
ContextSwarm event timestamps and Judge receipts.  Never turn a missing PSS or
CPU counter into zero in analysis.

### Observer overhead calibration

Membership correctness requires a stat-only pass over the visible `/proc`
table: without seeing unrelated processes, the sampler cannot safely resolve
process-group membership, parent/descendant reachability, or PID reuse. The
pass is therefore intentionally not capped by an arbitrary PID count. Only
the subsequent detailed reads are restricted to selected members and the
configured member limit. The `observer` counters make the resulting cost
visible so a high-PID host does not hide observer CPU in a service bucket.

As a local calibration on `qiwen` (2026-08-29), `/proc` contained 240 numeric
PID directories. Twenty snapshots of one explicitly selected local process
group took a median of 24.1 ms wall time and a p95 of 31.0 ms; this is roughly
0.5% of one core at a 5-second interval for that process table. It is only a
machine-, kernel-, filesystem-, and selector-specific smoke measurement, not
a portable bound or a claim about the external Judge workload. Record the
`observer` totals in every real run and repeat this calibration if the host,
interval, or selected roots change.

## Offline validation

The implementation has no third-party dependencies.  Its parser and
aggregation tests use a synthetic `/proc` and cgroup tree under the repository
worktree and do not start a service or make a network request:

```bash
python3 -m unittest -v tests.test_external_resource_sampler
```

Before handoff, also run the repository's normal gates:

```bash
python3 -m compileall -q contextswarm_mini
python3 -m unittest discover -s tests
```
