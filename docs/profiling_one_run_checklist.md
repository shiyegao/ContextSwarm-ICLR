# Profiling-on 单次真实运行记录清单

本文档是一次 `CONTEXTSWARM_PROFILE=1` 真实运行的 operator 记录模板，配合
[`profiling_metric_contract.md`](profiling_metric_contract.md) 和
[`scripts/audit_profiling.py`](../scripts/audit_profiling.py) 使用。它记录运行边界和证据，
不把 mock/plumbing smoke 当成真实 baseline，也不要求把私有 endpoint、token、账号、prompt、
候选正文或主机绝对路径写进仓库。

## 目标命名：六个主目标，另有一个锁诊断 target

研究和性能比较仍使用六个主目标：

1. 单 attempt 的 Agent-vs-wrapper；
2. Selection 全链路；
3. `record_search` 的 SQLite write-lock 等待、持锁事务和本地 contender 计数；
4. Trace projection 与重复读取；
5. `max_parallel` 的进程树、CPU 和内存放大；
6. CPS progress/SQLite。

审计器另外输出一个独立的第七行 `record_search_lock`。它是 Selection persistence 的
`operation == record_search` 过滤诊断子目标，用来单独确认 writer 的
`persist.start → persist.lock → persist.end` 和等待/持锁字段；它不是第七个算法目标、
不是新的 allocation arm，也不能用一般 Selection summary 或普通 SQLite 事件替代。
Judge/evaluator/drain、scheduler 和外部 Judge/Lean sampler 是跨目标辅助证据，外部进程资源
仍须单独成桶。

因此报告中同时出现七个 coverage 状态是预期的：六个主目标加这个独立锁诊断；不要把
文档中的“六目标”理解为审计报告只能有六行。

## A. 运行前登记（必须）

在启动前把下表填入本次 run note（可放在 run 目录的 owner-only 证据中；不要把秘密复制到
tracked 文件）。字段名尽量沿用 `run_meta.json`、`run.configuration` 和 audit report，便于
自动对照。

| 类别 | 最小字段/证据 | 记录要求 |
| --- | --- | --- |
| 身份与时间 | `run_id`；UTC `started_at`、`horizon_started_at`、`finished_at`；profile 文件的 run-relative 路径（通常 `profiling.jsonl`） | `run_id` 把所有事件归到同一 run；不要只记录 shell 启动时间；真实 horizon 从 `horizon_started_at` 起算 |
| Schema 与审计 | profile schema（`contextswarm_profile_event_v1`）；audit schema；审计脚本版本/commit；审计命令；`audit_exit_code` | 保存机器可读 sanitized report；退出码原样记录，不要吞掉 shell 失败 |
| 不可变源码 | `source_commit`；实际 image ID/digest；相对 `manifest_path`；`manifest_sha256`；若有则记录 resolved config/selection contract hash | source、image、manifest 必须相互绑定；不能用当前工作树 HEAD 代替实际执行镜像 |
| 题目输入 | benchmark/dataset 名称与 revision；有序 task-set 的 bounded hash；任务数 | 不记录题面或答案正文；task 顺序固定，便于 off/on 和并发档位配对 |
| 运行时版本 | Pi/Codex/runner、NuRouter/provider、Judge/Lean capability 的版本或稳定标签；健康检查证据编号 | 只记录脱敏标签/版本；endpoint、凭据和私有 home 留在 owner-only 运维记录；审计器不验证远端内部真实性 |
| 资源采样 | 本地 cgroup 是否可见、CPU denominator 来源/affinity/quota；外部 sampler 证据编号及组件标签 | 外部 Judge/Lean/router 的资源不能并入本地 Agent/wrapper RSS；根 PID 映射只放 owner-only 记录 |

## B. 固定 workload 与分支条件（必须）

记录实际解析后的配置，而不是只记录 manifest 文件名：

- `mode`、`allocation.policy`、`selection_enabled`、`max_parallel`、`worker_count`；
- `task_count`、有序 task-set hash、`episodes_per_task`、`max_attempts_per_task`、
  `initial_agents_per_task`、planned/actual agent sessions 和 attempt 数；
- `horizon_seconds`、单 attempt/evaluation timeout、`lean_max_concurrent_evaluations` 或
  Judge 本地 gate、recovery 是否启用及 `max_restarts`/backoff；
- 模型/provider 的稳定版本标签和 thinking/transport 模式；不要记录 prompt、响应或请求体；
- Judge cache、result reuse、网络/认证 capability 的开关结论（只记状态，不记 token/URL）；
- profiling heartbeat/sample interval、artifact scan 上限、输出所在文件系统（必须是磁盘而非
  tmpfs）以及运行前可用空间。

off/on 或不同 `max_parallel` 的配对必须保持上述 task、seed、模型/Judge、horizon、timeout、
runtime limits 和非 policy 配置一致；唯一有意变化的 profiling 对照是 profiler 开关及其
输出路径，allocation policy 或并发档位变化则单独标为另一 arm。

## C. 运行中边界证据（按适用目标填写）

### 1. Agent-vs-wrapper 与并发资源

- attempt 的 planned/admitted/started/terminal 数，以及 `attempt.lifecycle` 和
  `agent.end`/`resource.process.unregister` 是否一一结束；
- `resource.process.self`（runner wrapper）、每个 `resource.process` Pi tree、
  `resource.sample` aggregate 的行数、峰值和 sample kind；三类集合有意重叠，不能相加；
- 每个短命 Pi 是否有 `sample_kind=terminal`，`process_alive`、`sample_count`、
  `process_tree_truncated`；PID 已消失或 cgroup 不可见时记 `unknown`，不能填 0；
- RSS/PSS、thread/fd、CPU delta/utilization、peak 值及 CPU denominator；记录 cgroup memory
  current/peak、OOM/throttle（若宿主支持）；
- 每个并发 run 只代表一个 `max_parallel` 点。不要从一次峰值推导完整放大曲线，后续用 matched
  1/24/48/96（或团队批准档位）重复。

### 2. Selection 与独立 `record_search_lock`

- Selection：eligible read/filter/query-terms/materialize、snapshot/rank/pack、payload
  prepare/serialization/hash、persist、readback、SQLite connect 的事件和 rows/bytes；
- 锁诊断必须单独筛选 `operation == record_search`，确认同一 correlation scope 内有
  `selection.persist.start`、`.lock`、`.end`（以及 queue/readback/connect 如适用）；
- 记录 `write_waiters/write_active/lock_queue_depth` 时注明它们只是 descriptive local
  in-flight/contender counters，不是 application queue；
- `queue_residence_seconds` 是本地 contender 登记到取得 SQLite lock 的停留；跨线程/进程
  SQLite busy wait 以 `lock_wait_seconds` 为准；`lock_wait=0` 只表示本次没有观测到等待；
- 记录 `lock_hold/transaction/body/commit_seconds`、rows written、WAL/DB before-after-delta、
  serializer-inside-lock；连接/BEGIN/body/commit 失败也要有终态及 `error_kind`；
- 没有实际 writer 时将 `record_search_lock` 记为 `conditional_missing`/`not_executed`，
  不要把“没有正等待”误报为 instrumentation 缺失或把 zero wait 当作锁不存在。

### 3. Trace 与 CPS 互斥分支

- Trace：每个 `projection_call_index`、schema/exposure/feedback query、page、adapter
  projection、audit materialize，以及 `trace_set_sha256`、`trace_watermark_sha256`、
  `source_snapshot_sha256`、`projection_snapshot_sha256`、`reuse_count/page_count`；
- CPS progress：只有实际走 legacy allocator 的 `cps.progress.query/materialize/summary` 才
  能填 full active-piece scan、logical rows/bytes、connect/read/lock/WAL；`rows_scanned` 是
  逻辑返回/取回行，不是 query planner 的物理页扫描；
- `trace_state`/`llm_scheduler` 正常路径跳过 `CPSStore.progress_snapshot`，故一次 trace run
  对 CPS target 记录 `conditional_missing` 是正确结果；不要在 trace arm 偷加 progress 查询；
- 若同一 run 同时出现 trace projection 和 ordinary progress chain，记录
  `trace_progress_exclusive_violation`，不能把两条路径相加声称“采齐”。

### 4. Judge、scheduler 与关闭阶段

- 记录 `judge.snapshot`、`judge.queued/running`、`judge.execute`、HTTP submit/poll/cancel/
  reconcile/settlement、`judge.audit`、`judge.receipt`、watcher 和 `drain` 的计数与终态；
- `attempt.wrapper.evaluate` 的 gate/RPC/post-processing 与 Pi agent CPU 分开；scheduler
  调用只在相应 policy 分支出现；
- 固定 horizon 中间没有 wrapper 行时，用 heartbeat 的 `process_alive/agent_state/idle_seconds`、
  Pi CPU delta、`agent.end`、Judge HTTP 和 drain 判断“活着/等待/已死”，不能只看日志空窗；
- remote watcher 的等待与外部 Judge 资源单独记录，不算本机 Agent CPU。

## D. 运行后审计与最小验收

只读审计实际 run directory 或 `profiling.jsonl`，并保存退出码和报告：

```bash
python3 scripts/audit_profiling.py <run-dir-or-profiling.jsonl> \
  --format json --output <owner-only-audit-report.json>
audit_exit_code=$?
```

至少回填以下结果：

- 七个 coverage 行：六个 primary target 加 `record_search_lock`；每行的 `state`、
  `goal_complete`、`required_families`、`missing_required_families`、`correlation`；
- `realness`（`real`、`non_real` 或 `realness_unknown`）及 provenance 是否完整；`real` 仍
  不是审计器对生产账号/endpoint 的在线证明；
- profile `rows`/bytes、sequence、span open/orphan、profile/run terminal、dropped/sensitive
  fields、resource sample/terminal cardinality、artifact scanned/truncated；
- `audit_exit_code=0/1/2` 原因：0 仅表示可解析、质量通过且每个适用 target 为 present；1 表示
  quality 或 partial/missing/conditional_missing/invalid；2 表示输入/schema 不可用；
- 对每个 `conditional_missing` 写明触发分支和补测 arm；对 `not_applicable` 写明可信配置
  证据；`realness_unknown`、跨 run/scope/attempt 或 terminal 缺失时，不得宣称 baseline 完整。

第一次真实 profiling-on run 的目的只是确认事件边界和资源桶能正常闭合。它可以同时覆盖
多个目标，但不能替代 trace arm 与 legacy CPS arm 的互斥补测，也不能替代后续并发矩阵或
统计重复。只有 run note、审计报告、source/image/manifest provenance 和外部资源证据都能
按本清单回读时，才把该 run 标记为可用于优化比较的 baseline candidate。
