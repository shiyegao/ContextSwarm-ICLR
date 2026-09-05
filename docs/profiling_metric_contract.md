# ContextSwarm profiling 指标合同（六主目标 + 独立 record_search_lock 诊断）

本文档是 `contextswarm_profile_event_v1` 的运行时观测合同。它回答的是：在不改
算法决策和任务合同的前提下，`ContextSwarm` 本地 runner、wrapper、Selection/CPS、
SQLite、Trace bridge、Judge broker 和 Pi 进程分别花了多少时间、CPU、内存和排队时间。
它不把一次实验结果当成算法结论，也不把远端模型、NuRouter 或 Judge 的机器资源冒充
为本机资源。

六个分析目标是：

1. 单 attempt 的 Agent-vs-wrapper 基线；
2. Selection 全链路（候选读取、分词、排序、JSON/hash、完整 SQLite 持久化）；
3. `record_search` 的 descriptive local in-flight/contender counters、SQLite write-lock 等待和持锁事务；
4. Trace projection 的重复读取、materialize 和 snapshot identity；
5. `max_parallel` 增长时的进程树、CPU 和内存放大；
6. CPS progress/SQLite 的全量扫描、连接、WAL 及读写锁开销。

审计器在这六个主目标之外，单独输出第七个 `record_search_lock` target。它只筛选
`operation == record_search` 的 writer transaction，用于确认 `persist.start → persist.lock
→ persist.end`、SQLite wait/hold 和终态；这是 Selection 的诊断子目标，不是新的算法目标或
allocation arm。因而文档继续使用“六目标”描述研究问题，而 audit report 出现七行 coverage
是有意设计，不能用普通 Selection summary/SQLite connect 事件代替锁诊断。

Judge 调用和关闭阶段 drain 是跨目标的必要辅助观测：没有它们，Agent 的“无输出”无法
与 evaluator 等待或远端 settlement 等待区分。

## 1. 启用、输出和隐私边界

profiling 默认关闭。一次受控运行显式设置：

```bash
CONTEXTSWARM_PROFILE=1 \
CONTEXTSWARM_PROFILE_HEARTBEAT_SECONDS=1 \
python3 -m contextswarm_mini.cli --config <manifest> run
```

`<manifest>` 由操作者在运行时选择；端点、token、账号和私有 home 不写进本文档或
仓库。输出目录中的 `profiling.jsonl` 由 runner 创建为 owner-only（目录 `0700`、文件
`0600`）。构建和长证据必须放在磁盘文件系统；不要把 profile、数据库或构建树写到
`/tmp`/`/dev/shm` 等 tmpfs。

profiling-on 真实 run 的最小 provenance、workload、分支、资源边界和审计回填字段见
[`docs/profiling_one_run_checklist.md`](profiling_one_run_checklist.md)；没有这些记录时，
profile 只能作为插桩 smoke，不能作为可比较的 baseline candidate。

关闭 profiling 时，不创建 profile 文件、不启动 sampler，也不执行 profiling 专用的
时钟、`/proc`/cgroup 读取或文件大小统计。所有观测调用均 fail-open：sink 出错不能
改变 AgentResult、Judge 结算、selection 返回值或 CPS 事务。

每行至少包含：

| 字段 | 语义 |
| --- | --- |
| `schema_version` | 固定为 `contextswarm_profile_event_v1` |
| `sequence` | 在一个 profile 文件内单调递增，从 1 开始；不是跨进程全局序号 |
| `at` | UTC 人类时间，仅供关联，不用于耗时计算 |
| `monotonic_ns` | 单调时钟采样；耗时以同一进程的 monotonic 差值为准 |
| `event` | 固定审阅过的低基数事件名 |
| `run_id/task_id/actor_id` | 受限关联标识；不能承载 prompt 或路径 |

文本字段、计数器和 hash 都是 allow-list。候选正文、prompt、模型响应、Judge response、
SQL、URL、主机路径、账号、凭据和 capability token 不进入 profile。`judge.session.*`
即使内部调用带有 claim，也只保留受限计数/状态；原始 claim、token 和请求体会被丢弃。
`*_sha256` 只用于关联，不可逆推出原文。未知字段会被丢弃并计入 `dropped_fields`，
不是向 JSONL 开放任意 payload 的后门。

## 2. 事件、span 和资源字段的共同规则

### 2.1 span 与通知

`*.start`/`*.end` 是成对 span；`agent.heartbeat`、`resource.sample`、
`judge.receipt`、`drain.sample` 等是通知或采样，不要人为寻找对应的 end。`end` 的
`wall_seconds` 包含被包住的等待；`cpu_thread_*` 是执行该 span 的 runner 线程 CPU，
`cpu_user/system_seconds` 是当前 runner 进程的进程级 CPU 差值。嵌套 span 有意重叠，
不能把父子 wall 或 CPU 直接相加。

### 2.2 三类进程集合不可相加

| 行/字段 | 包含什么 | 用途 |
| --- | --- | --- |
| `resource.sample`, `sample_kind=aggregate` | runner、可见的注册树和 cgroup 范围 | 整次 run 峰值、总进程数、OOM/throttle；artifact 统计按低频缓存 |
| `resource.process.self`, `role=runner` | 仅 runner PID | wrapper 自身 RSS/CPU/线程/fd |
| `resource.process`, `role=solver|scheduler` | 注册的 Pi 根 PID 及可见 descendants；`sample_kind=terminal` 为退出边界有界树 | 一个 Agent 或 scheduler 进程树归属；终态行不含 run-wide/cgroup/artifact 成本 |

注册/注销由 `resource.process.register/unregister` 标出；注销行带本次注册期间的
`sample_count` 与 `peak_*`。aggregate 与每个 Pi tree 有意重叠，不能求和；runner
self 也不能从 aggregate 中再减一次来当作精确排他成本。

`rss_bytes` 是 RSS，`pss_bytes` 是可用时的 proportional set size；cgroup v2 还可给出
`memory_current_bytes`、`memory_peak_bytes`、`oom_kill_count`、`cpu_throttled_seconds`。
`cpu_utilization` 按可见 CPU 数归一化，`cpu_utilization_cgroup` 按 cgroup quota 归一化；
`cpu_affinity_count`、`cpu_denominator_source` 或 cgroup 字段缺失表示宿主不支持该观测，
不是零占用。

Pi `agent.heartbeat` 包含 `process_alive`、`agent_state=active|quiet|dead`、
`idle_seconds`、事件计数和 stdout/stderr 缓冲字节数。`quiet` 只表示最近窗口没有 Pi
输出，不表示进程已死；须与 CPU delta、`resource.process` 和 `agent.end` 联读。短命
Pi 可能在周期 sampler 前退出，runner 会在退出边界发出一个
`resource.process`、`sample_kind=terminal` 的轻量单树采样，并在 unregister 写峰值；若
`sample_count=0`、终态行 `process_count=0`（PID 已在 `/proc` 消失）或 cgroup 不可见，
只能报告 unknown，不能把缺失改成 0。该终态采样只
读取当前注册 PID 的 descendants（最多 128 个进程），不会触发 run-wide aggregate、
cgroup 或 artifact 扫描；`process_tree_truncated=true` 表示树超过上限，计数是有界下限。
同一注册的 heartbeat 与 unregister 会去重，不重复读取这棵树。

`sample_now(force=true)` 只强制进程/CPU aggregate 的一次观察，不再默认强制 artifact
目录刷新；artifact 统计按较低频率复用缓存，只有 profiler closeout 才显式请求新快照。
每次 artifact 快照最多处理 4096 个文件和 1024 个目录，并报告
`artifact_files_scanned`、`artifact_directories_scanned`、`artifact_scan_truncated`。
截断时 `artifact_bytes/sqlite_bytes/wal_bytes` 只能当作已扫描部分，不能当作精确全目录
总量。profiling 关闭时上述所有路径仍是零开销 no-op。

## 3. 六目标能力矩阵

“required”是该目标要能下结论所需的事件族 conjunction；只有其中一个事件不算完整。
“conditional”表示代码路径是否执行取决于 manifest、任务数据或宿主能力。一个真实运行
可以把多个目标放在同一 profile，但不能把未执行的条件分支解释成“采集到了零”。

| 目标 | 要求/触发条件 | 事件族与关键字段 | 单次可否覆盖 | 缺失如何解释 |
| --- | --- | --- | --- | --- |
| Agent-vs-wrapper | 真实 Pi attempt；短进程也要有边界 sample | `attempt.lifecycle.*`（或专用 `attempt.wrapper.*`）、`attempt.agent.invoke.*`、`agent.*`、`resource.process.self/process/sample`；`wall_seconds`、`cpu_thread_*`、`rss/pss`、`peak_*` | 可以；一次至少得到一个 attempt 的分界 | `agent.*` 只在真实 Pi 启动时出现；mock 没有 Pi 树。资源不可见是 host-dependent，不能伪造 |
| Selection 全链路 | `[selection].enabled=true`，至少有一个 eligible candidate 和一个 selected row | `selection.eligible.read/filter/query_terms/materialize`、`trace.project.*`、`selection.snapshot/rank/pack`、`selection.payload.materialize`、`selection.persist.*`、`selection.sqlite.connect`；行/字节/token/serialization/hash/commit | 可以；选择器无结果、重试命中已有 request key 时持久化分支可能不执行 | 无候选/无 selected row 是业务条件；若本应有候选却缺事件，审计为 partial/missing |
| `record_search` write lock | 至少两个并发 writer 才可能出现正等待；单 writer 的 wait=0 正常 | `selection.persist.queue`、`.lock`、`.end`、`selection.persist.readback.query`；descriptive `write_waiters/active` contender counters（不是 application queue）、`queue_residence_seconds`、`lock_wait/hold_seconds`、`transaction/body/commit_seconds`、`rows_written`、WAL/DB before-after-delta | 可以观察一次 run 的实际竞争；不能保证每次都产生正 wait | `lock_wait=0` 是没有观察到等待，不是 instrumentation missing；缺 `.lock/.end` 才是异常 |
| Trace projection | `allocation.policy=trace_state|llm_scheduler` 且 selection/trace source 有记录 | `trace.bridge.sqlite.connect/query`、`trace.bridge.page`、`trace.bridge.project`、`trace.bridge.materialize`、`trace.bridge.summary`；`query_name/index`、`query_count`、`projection_seconds`、`materialize/hash`、三类 snapshot hash、`reuse_count/page_count` | 可以；推荐 trace-enabled C48 diagnostic run | 其他 policy 不调用 bridge；空/不可用 source 会产生 zero projection 与 class-only `fallback_reason` |
| `max_parallel` 放大 | `max_parallel>1`，有 admitted attempt 和可见 Pi process tree | `run.configuration`、`attempt.admitted/solver_slot_released`、`resource.sample/process`；`active_slots`、`process_tree_count`、`thread_count`、RSS/PSS、CPU delta/utilization、peak | 一次只能测一个并发点；放大系数必须用 1/24/48/96 等 matched sweep | `max_parallel<=1` 在 audit 中是 `not_applicable`；>1 却无 admission/process 是 partial/missing |
| CPS progress/SQLite | `mode=cps` 且走 legacy allocator snapshot；progress 是完整 active-piece scan | `cps.progress.query/materialize/summary`、`cps.sqlite.connect`、`cps.write.queue/lock/commit`、`cps.search/inbox/digest`；`scan_mode`、rows/bytes/query/fetch/read scope/transaction/materialize/connect/lock/WAL | 可以，但不能与正常 trace-state allocator snapshot 同时走到 | `trace_state|llm_scheduler` 正常路径跳过 `CPSStore.progress_snapshot`，因此是 `conditional_missing`，不是失败 |

Judge 辅助链：真实 evaluator 才有 `judge.http.start/end`（`submit/poll/cancel`,
`reconcile_poll`, `settlement_poll`）、`judge.execute.*`、`judge.audit.end`、`judge.receipt`。
`attempt.wrapper.evaluate` 还包含 runner gate wait 和结果后处理。远端取消尚未终态时，
`judge.settlement.watcher` 与 `drain.start/sample/end/timeout` 给出 watcher 数、最老年龄、
poll 次数和 `settlement_poll_seconds`；这些不是 Agent CPU。

高并发 evaluator backlog 还会把 runner 的 `evaluation_backpressure_wait`/
`evaluation_backpressure_expired` 映射为 `judge.queue.wait`/
`judge.queue.expired`，并保留受限标量 `backlog_limit`。它只表示本地 evaluator backlog
容量；当前事件本身不伪造 `wait_seconds`，应按同一 task/actor/episode 的
`monotonic_ns` 顺序与后续 settlement/receipt 事件关联，并把无法唯一归因的间隔单列。
不能把 `backlog_limit` 当作等待秒数。两个事件是通知而非 span，不要求 `.start`/`.end`
配对。

## 4. 一次运行能覆盖什么、不能覆盖什么

### 4.1 推荐的单次 trace/Selection coding 诊断

`configs/capacity_coding/cps48_selection_trace.toml` 是诊断 manifest，不是算法比较 arm。
它继承 coding C48、真实 Pi/NuRouter/Judge 合同，只打开 selection 并选择 `trace_state`。
在真实任务至少完成一次候选读取/选择/评估时，一次 profile 可以同时得到：

- runner wrapper 与一个或多个 Pi Agent 的边界、进程树和 heartbeat；
- Selection candidate/filter/token/rank/pack/payload/persist 全链路；
- SelectionStore 的 descriptive local in-flight/contender counters、`BEGIN IMMEDIATE` lock、事务 readback、commit 与 WAL/DB；
- Trace bridge 的 SQLite query、adapter projection、审计 materialize、snapshot identity；
- allocator admission/slot、真实 Judge execute/receipt 和 closeout drain。

该 manifest 的注释和代码都明确：Figure-4 trace-aware snapshot 不调用普通
`CPSStore.progress_snapshot()`。所以同一正常 scheduler 路径不能同时声称覆盖
`trace.bridge.*` 和 `cps.progress.*`。

### 4.2 CPS progress 的独立 arm

用保持任务、模型、Judge、horizon 和资源合同不变的 legacy `uniform`/`formula`/`agent`
allocator（例如 `cps48.toml`，或在其上打开 selection 的非 trace policy）观察
`cps.progress.*`。这条 arm 可以和 Selection 共存，但不会产生 trace bridge。不要为了
“一次出齐”在 trace policy 中偷偷增加 progress 查询；那会改变 allocator 的决策输入和
基线事务序列。

### 4.3 互斥与多次比较的硬边界

- `trace_state`/`llm_scheduler` 与 legacy `progress_snapshot` 是正常 allocator 路径的
  互斥分支；profile 审计若同时看到两者，当前合同把它视为
  `trace_progress_exclusive_violation`，除非未来显式登记一个不改变决策的 observational
  probe。即使有 probe，也必须在 manifest/报告中标注“非默认决策路径”。
- 一次 run 只能给一个 `max_parallel` 峰值，不能从一个峰值推出放大曲线。至少做同一
  contract 的 matched 1、24、48、96（或团队选定档位），再计算每 slot 的 RSS、CPU 和
  process-tree 增长。
- 单次高并发没有保证一定有 SQLite wait；若目标是验证 wait 事件本身，可用受控双 writer
  focused test 验证 instrumentation，再用真实 run 判断生产竞争量级。两者不能混称。
- 单次真实 run 不是统计学结论。先每条路径跑一次确认事件齐全，再只对真正需要 p95/长尾
  的 arm 重复；每次重复保持任务、模型、Judge、horizon 和非 policy 参数不变。

## 5. Selection、JSON/hash 和 SQLite 的精确定义

一次新的 `SelectionRuntime.search` 的观测顺序（事件可能因空分支而缺失）是：

1. `selection.eligible.read`：从 CPS 读取 active pieces；
2. `selection.eligible.filter`：去除控制/候选/验证类行；
3. `trace.project.query/read/materialize`：SelectionStore 的 exposure、feedback、
   verifier、relation、maintenance 投影；
4. `selection.eligible.query_terms` 与 `.materialize`：查询分词、每候选 token count、
   Python candidate 对象；
5. `selection.snapshot`、`selection.rank.*`、`selection.pack.*`：快照 identity/hash、
   排序和 token budget packing；
6. `selection.payload.materialize`：对完整 eligible pool 建立受控的 diagnostic payload，
   统计行数/字节/序列化时间；它不是数据库中每一列 JSON 的精确物理写入量；
7. `selection.persist.payload` `phase=pre_lock`：候选/ranking prepare、JSON 序列化和
   hash 在 `BEGIN IMMEDIATE` 前的成本；
8. `selection.persist.queue` → `.lock`：本进程 writer admission 后调用 SQLite
   `BEGIN IMMEDIATE`；
9. 持锁事务 body：INSERT 链、必要的查询/readback，以及仍在事务内执行的 serializer；
10. COMMIT/ROLLBACK 与唯一的 `selection.persist.end` 终态。

`selection.persist.payload` 的 `prepare_*` 与 `phase=pre_lock` 让报告可以把候选准备
和持锁成本分开。实际 transaction 仍可能序列化 query/watermark/ranking/candidate JSON，
这些调用累计到 `serialization_inside_lock_seconds/bytes/call_count`；不能根据“有
pre-lock 事件”就假定全部 JSON 都在锁外。`selection.persist.end` 会重复带上 payload、
prepare、hash 和 WAL/DB 摘要，便于按一次写事务 join。

`rows_written` 是已提交连接的 SQLite `total_changes` 差值，涵盖 search、candidate、
ranking、exposure、exposure_item 等完整链；失败/回滚事务按 0 计入吞吐分母。`payload_bytes`
是受控序列化字符串字节数，`wal_bytes_*`/`db_bytes_*` 是文件大小快照，不是 fsync 或
底层块写入字节。

### 5.1 queue、busy wait、持锁时间不要混为一谈

- `queue_residence_seconds`：从本实例登记一个 contender/writer waiter 到取得 SQLite
  lock 的总 admission 停留（即登记时刻到 `BEGIN IMMEDIATE` 成功取得锁）；包含该实例
  内的 in-flight/contender 停留和 SQLite busy wait，但不是一个独立 application queue
  的排队保证。
- `write_waiters/write_active/lock_queue_depth`：只描述当前 `SelectionStore` 实例的
  descriptive local in-flight/contender counters，不是 application queue，也不能代表
  跨进程等待。
- `lock_wait_seconds`：从调用 `BEGIN IMMEDIATE` 到 SQLite 成功取得 writer lock 的等待。
  跨线程/跨进程的实际 SQLite busy/锁等待应以这个字段为准；它是比本地 contender
  counters 更直接的 contention 证据。
- `lock_hold_seconds`/`transaction_seconds`：取得 lock 后到 COMMIT/ROLLBACK 的时间，包含
  SQL body、事务内 serializer、readback 和 commit；连接 close/profile 写入不应收费在内。
- `.lock` 和 `.end` 在连接失败、BEGIN 失败、body 失败和 commit 失败也应有终态；失败由
  `status=error,error_kind` 表示，而不是静默丢掉一条写。

### 5.2 读事务、WAL 和 checkpoint

`selection.read.end` 的 `read_mode=deferred_wal` 将：

- `begin_seconds`：执行 deferred `BEGIN` 的 setup 时间；
- `read_scope_seconds`：BEGIN 成功后、COMMIT 前的查询 scope；
- `read_transaction_seconds`：BEGIN 到 COMMIT/ROLLBACK 的完整 transaction；
- `read_lock_wait_seconds`：当前路径明确为 0，因为 deferred BEGIN 不取得 writer lock；

分开记录。兼容别名 `lock_wait_seconds=0` 也不能被解读为“系统绝无读竞争”。

CPS 的 `progress/search/inbox` 是保持基线不变的 autocommit `SELECT`。Python sqlite3
没有可移植的 busy-handler 分解，因此 `read_lock_wait_seconds` 通常为 0；隐式等待会
包含在 `query_seconds`/`fetch_seconds`/`read_scope_seconds`，不能凭空拆出一个等待值。

`cps.sqlite.connect`、`selection.sqlite.connect` 分别量每个短连接的连接/PRAGMA setup。
WAL 与 DB before/after/delta 只表示文件大小变化。checkpoint 只有显式调用内部 hook
才会产生 `*.sqlite.checkpoint`；checkpoint 会改变数据库状态，不能混入普通 baseline。

## 6. Trace projection、重复读取和 identity

SelectionRuntime 的 `trace.project.*` 是 selector 投影；Trace bridge 的 `trace.bridge.*`
是 allocator projection。两者都可能读 SelectionStore，但不是同一个阶段，也不读取
CPS progress snapshot。

SQLite v1 bridge 的一次完整读取目前分成 schema、exposure、feedback 三个逻辑 query，
通常 `trace.bridge.sqlite.query` 的 `query_index=1..3`、aggregate `query_count=3`；
分页 source 则可有多个 `trace.bridge.page`。每个 query 的 `query_name`、`query_seconds`、
`fetch_seconds` 和逻辑行数都独立记录。

`trace.bridge.project` span 是实际 `TraceAllocationProjectionAdapter` 业务 projection；
`trace.bridge.materialize` 的 `phase=audit_serialization` 只是把受限 public projection
转成诊断 JSON、计算 digest 和字节数，不能把后者算进 allocator projection CPU。`projection_seconds`
和 `materialize/hash/serialization_seconds` 应分别报表。

同一份状态有四种不同 identity，不能互换：

| hash | 代表什么 |
| --- | --- |
| `trace_set_sha256` | 本次请求/引用的去重、排序 trace ID 集合 |
| `trace_watermark_sha256` | source 提供的因果 trace watermark；SQLite v1 没有真正 causal watermark 时可缺失 |
| `source_snapshot_sha256` | source-owned snapshot identity/watermark 的 hash，不等于 projection 内容 |
| `projection_snapshot_sha256` | normalized projection 输出的 hash；用于发现同一结果是否被再次 materialize |

`projection_call_index/calls` 是本 runtime 的调用序号；`reuse_count` 是同一 normalized
identity 在本次 run 之前被观察的次数，顺序不同但集合相同也算同一集合。当前没有业务
projection cache，故 `snapshot_hit=false` 表示“没有缓存命中”，不是读取失败。reuse history
有界（最多 4096 个 identity）；发生淘汰后计数可能从 0 开始，只能说“在保留窗口内没有
观察到重复”。

source 不可用、分页不完整、watermark 矛盾或超过上限时，bridge fail-closed 产生受限
zero projection，并在 summary 写 `source=zero,complete=true,fallback_reason=<class>`。
这和“合法 source 返回了空 projection”不同；必须同时看 `source`、`complete`、records
和 fallback reason，不能把 zero fallback 当成没有 trace 数据。

## 7. CPS progress 的精确定义

`cps.progress.query` 的 `scan_mode=full_active_piece_scan` 表示业务 SQL 请求所有匹配的
active piece，再在 Python 中聚合每个 task 的计数和 recent rows。`rows_scanned`、
`input_rows`、`output_rows` 在本合同中都是**逻辑结果行/取回行**的计数，不是 SQLite
query planner 的物理 page、B-tree 或全表扫描行数；不能用它声称已经测到物理 I/O。
需要物理计划成本时，另做 `EXPLAIN/SQLite trace` 实验并标记为非 baseline。

`cps.progress.materialize` 的 `materialized_rows/bytes/materialize_seconds` 只统计输出
recent-piece projection 的 Python 物化；它不等于查询返回的全部行字节。`cps.search`、
`cps.inbox`、`cps.digest` 也按 query/fetch/materialize 分阶段；`cps.write.lock/commit`
分别给出写锁、body/transaction、commit 和 WAL/DB 变化。

## 8. Judge、scheduler 和长时间“没动静”的判读

Judge 阶段按以下边界读：

| 阶段 | 事件 | 是否算 Agent CPU |
| --- | --- | --- |
| runner candidate freeze | `judge.snapshot.start/end` | 否，wrapper |
| evaluator HTTP | `judge.http.start/end`，operation 是 submit/poll/cancel/reconcile/settlement_poll | 否，网络/客户端 |
| evaluator 本地调用 | `judge.execute.start/end` | 否，Judge/evaluator |
| receipt 审计 | `judge.audit.end`、`judge.receipt` | 否，broker/审计 |
| broker 关闭 | `drain.start/sample/end/timeout`、`judge.settlement.watcher` | 否，等待远端终态 |
| Pi 模型/工具 | `agent.*`、`model.request.*`、`tool.*`、Pi tree | 是 Agent 侧（仅本机可见部分） |

`attempt.wrapper.evaluate` 包含 evaluator gate wait 和 verdict post-processing；它不能被
直接拿来当 Agent 执行时间。`scheduler.invoke.*`/`scheduler.agent.invoke` 是 allocator
scheduler（通常只在自适应/LLM policy 出现），要和 solver attempt 分开。确定性 policy
没有 scheduler 调用是正常条件分支。

因此，固定 horizon 中间没有顶层 `events.jsonl` 新行时，先看 `agent.heartbeat` 的
`process_alive/agent_state`、Pi process CPU delta、`agent.end`，再看 `judge.http`、
`cps.*` 和 drain；不能把“无 wrapper 日志”当作 Agent 已死。

## 9. Profile audit 合同

每次 profile 结束后先做只读审计，不连接数据库、不访问 Judge：

```bash
python3 scripts/audit_profiling.py <profiling.jsonl-or-run-dir> --format text
# 需要机器可读结果时：--format json
```

审计报告包含 `coverage`（六个 primary goal 加独立 `record_search_lock` 诊断 target）、`coverage_detail`（required/observed/missing
族）、`realness`、`issues`、序列/span/终止状态和有限 numeric percentiles。覆盖状态的
精确定义如下：

| 状态 | 含义 | 是否 clean pass |
| --- | --- | --- |
| `present` | 该目标的 required conjunction 全部出现 | 是（其他质量检查也通过时） |
| `partial` | 至少一个 required 族出现，但 conjunction 不完整 | 否 |
| `missing` | 应适用且没有观察到 required 族 | 否 |
| `conditional_missing` | 代码明确知道该目标/子族只在条件分支执行，本次分支未执行；例如 trace policy 跳过 CPS progress | 否；它说明本 profile 不是“六个主目标（及适用诊断）全覆盖” |
| `not_applicable` | 由可信配置证明分支被关闭，例如 selection disabled、dry-run Judge 或 `max_parallel<=1` | 不因该目标失败 |
| `invalid` | 配置未知/冲突、schema/序列/span/互斥 invariant 违反或无法安全判断 | 否 |

`conditional_missing` 不是“把缺失当 0”，也不是采集器错误；它是单次运行审计对互斥
路径的明确说明。当前 trace policy 同时出现 `cps.progress.*` 会报告
`trace_progress_exclusive_violation` 并使 CPS target invalid；不要用它绕过分支合同。
其他业务条件（例如没有 selected row、没有取消 job）不会自动产生 watcher/持久化事件，
应在 run note 中记为 `not_executed`，不能把事件缺失静默改为 0。

`realness` 是真实性**证据等级**，不是远端探针：

- `non_real`：metadata 明确 `test_only`/mock；
- `real`：有 runtime provenance metadata，且没有 test/mock 标记；这仍需操作者确认真实
  模型、NuRouter、Judge，审计器不读取凭据也不验证远端内部；
- `realness_unknown`：缺少或无法解析 provenance；可以用于 plumbing 检查，但不能宣称
  生产真实性。

CLI 退出码固定为：

- `0`：profile 可解析、无质量/invariant 问题，且每个适用 target 都是 `present` 或
  `not_applicable`；
- `1`：JSONL 可解析但存在 sensitive/dropped/nested 字段、序列/span/终止问题，或
  `partial`/`missing`/`conditional_missing`/`invalid` coverage；
- `2`：输入不可读、profile 文件不存在或 schema/JSONL 无法解析。

所以推荐 manifest 的 trace diagnostic 首次运行即使得到 exit 1（CPS
`conditional_missing`）也不代表代码坏了；报告必须明确列出该条件缺失，随后用 legacy
CPS arm 把它补齐。反过来，若 expected `trace.bridge` 或 selection persist 缺失，不能
因为同样是 exit 1 就归咎于互斥分支。

## 10. 比较与归一化规则

报告阶段成本时使用同一 manifest、任务集、模型/Judge、horizon 和非 policy 参数。建议
同时输出 count、sum、p50/p95（审计器只保留有界样本），并按以下分母归一化：

- Agent：每 logical attempt、每 occupied slot-second；Pi tree 与 runner wrapper 分开；
- wrapper：每 attempt 的 thread CPU/墙钟，另报 gate/RPC wait；
- Selection：每 eligible candidate、每 token、每 selected row、每 persistence transaction；
- lock：每 write transaction、每 committed row；queue wait 和 SQLite wait 分列；
- Trace：每 projection call、每 source row、每 referenced trace/task；
- CPS：每 progress decision、每 logical returned row、每 input/output byte；
- Judge：每 submit/job/receipt，远端 watcher poll 单列；
- 并发：run aggregate 峰值、Pi tree 峰值、每 active slot 峰值分别报告。

不要把嵌套 span、aggregate/tree、query/fetch 与外层 wall 重复相加。CPU 可以在明确进程
集合后求 total compute，但并发 wall 不是可加的；远端 endpoint 内部资源永远不属于本机
profile。

## 11. 已知边界与未覆盖项

本合同刻意不声称：

- 远端 NuRouter、模型供应商或 Judge backend 的 CPU/内存/网络服务时间；
- SQLite query planner 的物理 page 扫描、fsync 或磁盘设备写放大；
- Python allocator/thread 内部逐对象内存分配（只能用进程/cgroup 间接估计）；
- 没有启动的 Pi、没有取消的 remote job、没有执行的 selection 持久化分支；
- 一次 run 的并发峰值可以代表整个 max_parallel 曲线；
- `read_lock_wait_seconds=0` 等于系统没有任何读等待；
- `realness=real` 等于审计器已经验证了生产账号或 endpoint。

只要报告保留这些边界，profile 才能作为后续优化的可比 baseline：先确认 plumbing 和
条件覆盖，再按并发档位与 workload 规模分层跑，不把算法分数、远端服务能力和本地
wrapper 优化混在同一个数字里。
