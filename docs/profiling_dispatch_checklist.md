# ContextSwarm profiling 派发清单

这是一份持续维护（living）的调度清单，不是一次性计划，也不代表已经完成真实
生产运行。每次新增任务、重新派发、agent 状态变化、测试结果或阻断变化，都先在
这里登记，再继续工作。状态以可回读的 Herd/命令证据为准；“已启动”不能写成
`done`。

## 当前上下文（2026-08-29，Asia/Shanghai）

| 项目 | 值 |
| --- | --- |
| 仓库 | `ContextSwarm-ICLR` |
| 隔离 worktree | `/home/ubuntu/workspace/.workspace/worktrees/ContextSwarm-ICLR/profiling-capacity-20260828` |
| 分支 | `profiling-capacity-20260828` |
| 本轮起点 | `6f49b4e` |
| Herd workspace | `w8`（`ContextSwarm`） |
| Profiling 实现阶段边界 | 只做源码/文档、静态检查、离线 fixture 和 `--mock-agent` plumbing；真实 baseline 在独立、固定 provenance 的 operator build 中执行，不把该 run 的结果混入本 worktree 的实现验收 |
| 产物边界 | 大型构建/测试/profile 输出放在 worktree 的磁盘目录；`/tmp` 仅允许短小临时控制文件 |
| Git 边界 | agent 不提交、不 push、不 merge；中央完成验证后再决定是否本地提交 |

## 当前验收口径

本轮验收必须把“代码能力”和“真实性能数据”分开：profiling 埋点、字段合同、
审计器、manifest 解析和离线/mock smoke 通过，只能证明代码具备采集和自检能力，
不能证明真实模型、NuRouter、Judge 或生产并发下的性能已经改善，也不能替代真实
运行的 baseline。真实性能结论必须来自同一任务/模型/Judge 合同下、由 operator
明确授权执行的真实 run，并保留 run 的配置、资源档位、horizon 和完整 profile
证据。

当前算法侧复现包的三项证据链（下载、内容阅读/完整性核对、离线尝试）均已完成。
此外，已在独立、固定 source/image provenance 的 build 中完成一次真实 formal 1×1
生命周期验证；该 run 不是 profiling-enabled，也不是“题目解出/性能改善”的证据，
详见下方交接快照。本 worktree 的 instrumentation 验收仍不发起真实请求。

coding canary 的本地前置材料已经具备：Judge bundle 来源 commit
`6e7291ba51fa3403daba49cf07674322b367882e`，并包含 12 个 ICPC coding package。
它们可作为后续 coding canary 的输入/前置核对材料；bundle commit、package 数量
或离线完整性通过，均不能单独证明 Judge 服务已启动、路由可用或 canary 已成功。

## 当前交接快照（2026-08-29 15:15 Asia/Shanghai）

下面这张表是给中央调度和后续 Herd Tab 交接用的“现在到底到哪一步”快照。`done`
只表示该项的证据已经收齐；`pending`/`blocked` 不得在没有新证据时改成完成。

| 工作流 | 当前状态 | 已有证据 | 下一道门 |
| --- | --- | --- | --- |
| 六个 profiling 目标（插桩能力） | `done`（含 PF-021~023 合同收紧）；尚无 profiling-enabled 真实 run | PF-001~023 focused/off-on mock/audit 证据；六项目标的 conditional 边界见下表；PF-021~023 专项见对应任务行 | 下一道门是在同一任务/模型/Judge 合同下跑一次 profiling-enabled 真实 1×1 配对；不能用 mock 结果代替 |
| 原版 faithful formal 1×1 | `done`（运行闭环；solver 未解出） | 修正版 source/image `e999929…` run `20260829T070759Z-35782fe7` 已完成：300s horizon、container rc=0、runner `COMPLETED`、`health.ok=true`，Pi events=2,404、formal calls=19、Judge checks=6；closeout `COMPILES_WITH_SORRY`，score=0 | 将该 run 作为可运行性/资源参考；不要把 score 0 当作 profiling 或算法质量结论；下一档前保留同一 route/manifest 合同 |
| Formal/Lean 资产 | `done`（runtime 能力；当前 `imo2024_p1` 无新增本地缺件） | Lean 4.9、Mathlib、REPL、SafeVerify、declaration index 153,467 rows/quick-check；direct/group smoke `PROVED`；formal 题目/Lean 文件已在算法复现包 | 不把资产就绪误写成 runner E2E；若以后要求官方 hidden/package 评分，再由 operator 私下确认 package root 与 endpoint mapping |
| Judge route/lifecycle | `ready/conditional`（compat route 已验证；strict Group route 仍不兼容 legacy evaluator） | observe-only compat route `28201` health `ok=true`, one ready worker；修正版 closeout 已正常返回（`1460s` receipt 与 client cap `3600s` 对齐）；legacy evaluator → required Group route `28100` 仍曾 403 | 可继续使用 28201 做 faithful baseline；Group-required client integration 另立任务，不把两种协议混为一条基线 |
| 10 → 100 规模矩阵 | `pending` | 参数顺序已登记为 `1 → 10 → 100`；尚未启动中规模真实负载 | 先通过 corrected 1×1 与 route/lifecycle gate；每档先原版、再 profiling-enabled 配对 |

## 当前真实运行与资产清单（2026-08-29）

本节是当前 operator-local 事实快照，专门区分“已经存在且跑过”的真实资产、
“仍需在官方严格协议下另行确认”的条件项，以及 profiling 插桩本身的验收边界。
旧版 intake 文档中的“Pi 暂停/没有真实模型或 Judge”是 intake 时间点的历史描述，
不能作为当前缺件；后续 corrected formal 1×1 已覆盖这些运行事实。

### 已确认的真实运行与资产

- **客户端与运行时版本**：本机直接核对的真实 Pi 为 `0.84.3`、Codex 为
  `0.150.1`；corrected operator run metadata 将 NuRouter 固定为 `0.2.2`，并且
  run `20260829T070759Z-35782fe7` 实际使用了真实 Pi、NuRouter、模型和 formal
  Judge。这里把“本机 client 版本”和“run provenance 版本”分开记录，不从历史
  `aisw` shim 是否存在推断 canonical launcher 已安装。
- **non-root 边界**：该 run 的 Docker launch 使用 `--user 1000:1000`；容器及其
  Pi/runner 进程树的采样 UID:GID 为 `1000:1000`，没有以 root 身份运行。这里的
  Codex 版本是本机 client readiness 证据，不把它误写成该 Pi 容器中的额外子进程。
- **formal 题目与工具链**：题目 `imo2024_p1`（MathOlympiadBench，Lean 声明
  `Imo2024P1`）已经绑定；Lean `4.9.0`、Mathlib `v4.9.0`、REPL、SafeVerify
  workspace 均已就绪。declaration index 为 `decl_index_v1`，`153,467` 行，
  `PRAGMA quick_check=ok`，operator-local 产物在
  `/home/ubuntu/workspace/.workspace/builds/formal-runtime-1x1-20260829/`。
- **Judge runtime**：direct/group kernel smoke 均返回 `PROVED`；corrected 1×1
  通过 task-local observe-only compatibility route `28201 → 28149` 完成真实
  evaluator closeout（health `ok=true`）。Judge/Lean 常驻 worker 属于外部服务
  资源桶，不能并入 ContextSwarm agent/wrapper 进程树或重复计入其 RSS。
- **profiling 边界**：上述真实 1×1 使用原版未插桩 source，因此没有
  `profiling.jsonl`；插桩能力由独立 off/on mock、focused tests 和 audit 验收，
  不能把两类证据混称为一次 profiled baseline。

### 仍需按官方严格协议另行确认的条件项

这些是 allocation/协议分支的条件门，不应倒推为当前 formal 1×1 缺少资产：

- **official strict route**：Group-required router `28100` 的 HMAC admission/
  permit/receipt 协议，以及 legacy evaluator 的兼容性，需要单独完成 client
  integration；当前 28201 是为 faithful baseline 保留的 observe-only 兼容路由。
- **official package mapping**：复现包有意不携带 Judge `packages/**`、hidden
  tests 或 oracle。若执行官方严格评分，operator 必须私下绑定与 benchmark
  revision 匹配的 package root 和 endpoint mapping；这不否定当前 `imo2024_p1`
  1×1 的本地运行闭环。
- **strict cache/revision contract**：要求 result-cache disabled、endpoint
  宣布匹配的 Mathlib revision 等 strict preflight 条件，需在选定的官方 route
  上重新核对；未完成该条件时只标记 `conditional_missing`，不能把普通 health 或
  compat smoke 当作 strict 通过。

## 状态和派发规则

- `pending`：已登记但未开始，或没有可回读证据。
- `working`：可见 Herd agent 正在执行；不等于实现可用。
- `done`：局部文件已落地且有对应 focused 验收证据；仍要过整合门。
- `blocked`：有可复现阻断，清单必须同时写复现命令、影响和下一步。
- `conditional`：只在某条 allocation/运行分支产生；最终报告必须写
  `conditional_missing`，不能声称单次运行覆盖全部目标。

每一项固定记录：目标、优先级、负责人角色、可见 Tab/pane/agent、文件边界、依赖、
验收命令、当前状态、阻断和下一步。实现 agent 只改自己的边界；review agent 默认
只读。任务被中断时保留已有改动，并将状态退回 `pending`/`blocked`，不能静默丢弃。

## 任务登记

| ID | 优先级与目标 | 可见 Herd owner | 文件边界 | 验收/依赖 | 当前状态 | 阻断与下一步 |
| --- | --- | --- | --- | --- | --- | --- |
| PF-001 | P0：统一 profile schema、sequence、start/end、字段白名单、敏感字段丢弃和 profiling-off 快路径 | `profiling-visible` `w8:t2/p3` `profile_schema_review` | `contextswarm_mini/profiling.py`、schema tests | `py_compile`、`tests/test_profiling.py`；无依赖 | done | 重新导入 allowlist；确认 recovery 字段可观测、`claim` 原始对象仍丢弃；中央 focused/full 分类已完成 |
| PF-002 | P0：单 attempt agent-vs-wrapper：生命周期、线程/进程 CPU、RSS/PSS、cgroup、进程树、峰值和短命 agent 终态 | `profiling-runner` `w8:tB/pF` `runner_profile_impl` | `runner.py`、`evaluator.py`、`judge_broker.py`、`pi_agent.py` 及对应 tests | focused runner/evaluator/Judge tests；依赖 PF-001 | done | off/on admission 单测、Judge 45-test 模块回归、异常/timeout/cancel 终态矩阵与 aggregate-root 回归均已通过 |
| PF-003 | P0：CPS connect/query/fetch/materialize、WAL/DB、进程内 queue 与 SQLite write-lock wait/hold/commit | `profiling-cps` `w8:t7/pB` `cps_profile_impl` | `contextswarm_mini/cps.py`、CPS profiling tests | CPS focused tests；依赖 PF-001 | done | begin 前后锁持有边界、失败出口与无 CPS 路径已核验 |
| PF-004 | P0：Selection 全链路 eligible/filter/trace/query-terms/rank/pack/payload/persist/readback/WAL | `profiling-selection` `w8:t8/pC` `selection_profile_impl` | `selection_runtime.py`、`selection_store.py`、selection tests | Selection focused tests；依赖 PF-001/PF-003 | done | 高层/事务 span、persist 终态、off-path SQL/clock 与 88 个 selection contract tests 已核验 |
| PF-005 | P0：Trace bridge 分页、projection identity/hash、call index/reuse、watermark 和 trace_state 分支 | `profiling-trace` `w8:tE/pK` `trace_bridge_review` | `allocation_trace_bridge.py`、trace tests、runner trace 分支 | trace focused tests；依赖 PF-001/PF-004 | done | 28 个 Trace/bridge/profiling focused tests、normalized trace-set reuse/call index、分页 replay 与 off-path 探针均通过 |
| PF-006 | P0：Runner/Judge/Pi/settlement 生命周期：HTTP、admission、execute、audit、receipt、drain、heartbeat、scheduler wrapper | `profiling-runner` `w8:tB/pF` `runner_profile_impl` | `runner.py`、`evaluator.py`、`judge_broker.py`、`pi_agent.py` | 生命周期/终态矩阵；依赖 PF-001 | done/conditional | Judge lock 修复、receipt/terminal cardinality 与 mock off/on 已通过；真实 formal 1×1 的 Pi/Judge/settlement 证据另见 PF-018/019；profiling-enabled 真实 run 仍待后续配对 |
| PF-007 | P0：流式 `scripts/audit_profiling.py`，检查 JSONL、隐私、`dropped_fields`、span/terminal、六目标 coverage | `profiling-audit` `w8:t9/pD` `audit_profile_impl` | `scripts/audit_profiling.py`、`tests/test_audit_profiling.py` | 已通过 focused tests；依赖 PF-001~006 | done | 最终 111-row smoke audit exit 0；event-label allowlist 与未知标签不回显回归通过 |
| PF-008 | P0：metric contract 文档，解释 agent/wrapper、lock/WAL、trace reuse、CPS/Judge drain 和条件性覆盖 | `profiling-docs` `w8:tC/pG` `docs_profile_impl` | `docs/profiling_metric_contract.md`、README 链接 | 文档与 allowlist 反向核对；依赖 PF-001~007 | done | 文档已与当前 allowlist、aggregate root 语义及 conditional coverage 对齐 |
| PF-009 | P0：coding capacity manifest，保证一次诊断运行尽量采齐 selection/trace/Judge 事件，不宣称 formal/算法结果 | 中央调度；复核 `final_validation` | `configs/capacity_coding/cps48_selection_trace.toml` | `validate --json`、`plan --json`；依赖 PF-004/PF-005 | done（`validate --json`/`plan --json` 已通过） | `trace_state`/Figure4 分支会跳过 `CPSStore.progress_snapshot`，CPS progress 必须标 conditional；真实模型/Judge 运行仍需 operator 单独授权 |
| PF-010 | P0：Judge admission timeout 单测 off/on 复现，判断 profiling 回归还是并行测试干扰；必要时最小修复 | `profiling-runner` `w8:tB/pF` `runner_profile_impl` | 先只读；修复限 PF-002/PF-006 文件与测试 | 外层 `timeout 20s`、faulthandler、单测；依赖 PF-001/PF-006 | done | off/on 均复现原死锁；`clear_probe_active` ownership 修复后两次各约 0.12s、45-test Judge 模块 4.691s 通过；无残留进程 |
| PF-011 | P1：跨模块 integration review，核对字段、事件命名、fail-open、off-path、资源重叠和 terminal cardinality | `profiling-review` `w8:tA/pE` `integration_profile_review` | 默认只读；修改需另行派发 | 静态调用点扫描 + focused matrix；依赖 PF-001~010 | done | AST/clock/SQL sink、事件映射、原始 payload 隔离、off-path 与 111-row span/terminal cardinality 均核验；资源重叠不可直接相加 |
| PF-012 | P1：最终验证编排：compileall、focused suites、串行 full unittest、mock smoke、audit、diff/hygiene | `profiling-final-validation` `w8:tF/pM` `final_validation` | 只读验证和报告 | 依赖 PF-001~011；不得真实网络 | done（有 3 个预期 dirty-worktree 合同失败） | 最新 worktree 全套串行 `694 tests`：仅 3 个正式 launch/container contract 测试因当前 worktree 有意存在 tracked profiling 修改而拒绝启动；同三项在干净基线 worktree 单独通过。它们是 dirty-worktree 前置合同失败，不是 profiling 运行时失败，也不能计作 CI 通过。Figure4 极短 50ms 测试在整套中本次通过；独立基线曾同样出现调度抖动失败。未触发真实网络。专项最新结果：profiling/audit/trace 36+81、CPS/evaluator/Judge 147、runner/recovery 47、selection contracts 88；manifest 45 中 1 个环境抖动失败，其他通过。最新 off/on mock 与审计仍通过。 |
| PF-013 | P1：Pi 侧可选 hook 评估，确认现有 `pi_agent.py` heartbeat/usage/process 指标是否足够 | `profiling-pi` `w8:tD/pH` `pi_profile_agent` | 优先只读；不得改持久 home | 依赖 PF-002/PF-006 | done/conditional | 已完成 binary/launcher/extension/config 与启动前置 bounded 检查；独立 faithful formal 1×1 已证明真实 non-root Pi 可持续运行并产生 720 条 Pi event、token usage 和正式工具调用。当前原版 run 未生成 `profiling.jsonl`；不要把外部运行采样冒充 instrumentation 结果 |
| PF-014 | P1：维护本清单，记录每次派发、状态变化、测试证据、阻断和交接项 | 中央调度 agent（本线程） | `docs/profiling_dispatch_checklist.md` | 每个里程碑回写 | done（本轮收口） | 已回写所有 visible Herd Tab、依赖、验收证据、阻断、远端 artifact intake 和最终 off/on smoke；后续真实 run 仍需按本表逐项更新 |
| PF-015 | P0：算法侧 profiling 复现包 intake：精确下载、checksum、成员安全检查、公开契约/缺失项盘点和 ≤30s 离线前置验证 | `profiling-artifact-intake` `w8:tG/pN` `artifact_intake` | `/home/ubuntu/workspace/.workspace/artifacts/contextswarm-profiling-repro-20260829`（不改源码） | 精确 URL 下载；`sha256sum -c`；安全 `tar -tf`/解包；README/report/manifest 只读核对；不启动服务/完整实验 | done（下载、阅读/完整性核对、离线尝试均完成；真实 run 未执行） | 外层 SHA `d58cc11f…eba4ef`；外层 386 entries；包内 checksum 340/340；4 source archives 成员 10433/763/603/599；Judge `packages/**`=0；offline smoke 约 1.2s 返回 0。`configs/smoke.toml` 不在包内但有 `configs/iclr/figure3_3min_smoke.toml`；正式 run 仍需 operator 私下补 Judge packages、Lean/runtime/NuRouter/凭据。详见 `docs/profiling_artifact_intake_20260829.md` |
| PF-016 | P0：修复真实 mock smoke 审计暴露的正常事件字段丢弃和 closeout span orphan，保证一次采集可审计且不放宽敏感字段 | `profiling-event-sanitize` `w8:tH/pP` `event_sanitize` | `profiling.py`、必要时 `runner.py`、`tests/test_profiling.py` | 复现 profile 109 rows；当前 `dropped_fields=70`、`span_orphan_end=1`；focused tests + 新 mock smoke + audit | done（中央复核通过） | logger mapping 已过滤 command/error/output/response 等 raw payload，同时保留受控布尔/计数；`agent_id` 只作为 actor identity；closeout 使用 `closeout.evaluation_call.start/end`，结果用 receipt；最终 111 rows 的 dropped/sensitive/orphan 均为 0 |
| PF-017 | P1：aggregate roots 修复，统一 run/runner/cgroup 与 Pi process-tree 汇总根的边界和去重语义 | 中央调度；aggregate owner | `profiling.py` 及对应审计/测试 | 依赖 PF-001/PF-002/PF-011；需静态 root 映射、重叠计数和 focused/full 回归 | done（focused 22 tests、compile、diff-check 已通过） | 已完成 runner root 与 registered/Pi process-tree root 的去重边界修复，并保留不可相加的重叠语义；真实 baseline 证据单独登记在 PF-018，不与 instrumentation smoke 混淆 |
| PF-018 | P0：忠实原版 formal 1×1 baseline（只降低规模，真实 Pi/NuRouter/模型/Lean Judge） | `pi_judge_1x1_preflight`、`one_x_one_monitor` | 独立 operator build：`original-formal-1x1-20260829`（不改本 worktree 源码） | 固定 source/image/manifest provenance；核对 horizon、Pi/Judge/Lean 活性、最终 artifacts；依赖 PF-015 与 formal assets | done（运行闭环；solver 未解出） | 修正版 run `20260829T070759Z-35782fe7`（source/image `e999929…`）启动→结束 `07:07:59–07:13:02Z`，horizon 300s，container/launcher rc=0；Pi events=2,404、formal calls=19、Judge checks=6，资源采样峰值约 269.4 MiB、CPU 6.47%、17 PIDs；closeout 约 1.02s、`COMPILES_WITH_SORRY`/`correct=false`、run `COMPLETED`/score 0。旧 f2eda9 run 的 lifecycle 错误已由 cap 对齐修正；原版 source 仍无 `profiling.jsonl` |
| PF-019 | P0：formal 资产完整性与 Judge route/lifecycle 合同 | `formal_runtime_provision`、`judge_compat_route` | task-local formal runtime 与 route 记录；不读写持久 home/凭据 | Lean 4.9/Mathlib/REPL/SafeVerify/declaration-index；direct/group kernel/evaluate smoke；legacy evaluator 与 Group-required route 分开核对 | done/conditional | 形式化 runtime、index（153,467 rows，quick_check=ok）及 direct/group `PROVED` smoke 已具备；observe-only compat route `28201` 已使 legacy evaluator 正常完成 corrected 1×1；旧 `1460s > 600s` closeout 问题已通过 faithful cap `3600s` 对齐。Group-required router `28100` 与 legacy evaluator 的 403 仍作为独立 integration 条件记录，不阻塞当前 faithful baseline |
| PF-020 | P1：10→100 规模矩阵（先原版、后 profiling-enabled 配对） | 中央调度；`test_matrix` | 新的 operator manifests/run dirs；不改算法选择策略或模型合同 | 每档只按用户批准的规模放大；保存 resolved config、source/image、资源峰值、阶段时序、terminal/closeout；依赖 PF-018/019，且先处理 PF-019 lifecycle gate | pending | 建议顺序 `1 → 10 → 100`；每档先完成原版可运行性，再以同一任务/模型/Judge 合同重跑 profiling-enabled。未获下一档明确窗口前不启动真实中规模负载 |
| PF-021 | P0 review：延迟 SQLite 写锁内的 profiling I/O，避免 JSON/序列化/文件写入拉长 `BEGIN IMMEDIATE` 持锁时间或改变锁顺序 | `profiling_instrumentation` | `contextswarm_mini/cps.py`、`selection_store.py`、`selection_runtime.py`、`profiling.py` 及 lock-order tests；只改 profiling 边界，不改算法策略 | 依赖 PF-003/PF-004/PF-011/PF-016；静态 sink/调用顺序审查 + focused lock-contention test + profiling off/on 回归；验收同时回读 `write_lock_hold_total_seconds`、`serialization_inside_lock_seconds`、`lock_wait_seconds` 与 terminal cardinality | done | `selection_store`/`cps` 将锁内事件放入有界 deferred buffer，在 COMMIT/ROLLBACK、连接关闭后再 flush；3 个专门 lock-I/O tests、相关 CPS/Selection/Profiling 回归通过。`write_waiters/write_active/lock_queue_depth` 已更名/解释为 descriptive local in-flight/contender counters，不是 application queue；跨进程 busy-handler 等待仍按 `lock_wait_seconds` 观察，无法拆分处保持 conditional。 |
| PF-022 | P0 review：收紧 Pi 退出边界，限制全局进程采样与递归 artifact 扫描的范围、次数和生命周期，避免 agent 退出后仍扫描整棵 run tree 或把非本次 run 纳入峰值 | `profiling_contract_docs` | `profiling.py` 的 `sample_now`/`close`/`unregister_process`/`_artifact_snapshot`、Pi recovery/runner 生命周期文档与 tests；不读取持久 Pi home | 依赖 PF-002/PF-006/PF-017；核对 sampler stop/join、Pi unregister/late sample 的边界；验收有 bounded process tree/artifact scan、退出后 no-late-sample/no-orphan 断言，并回读 `scan_scope`、`artifact_snapshot_seconds`、`process_alive` | done | `heartbeat(force=True)` 使用有界单 PID/descendant tree（上限 128），不做 cgroup/artifact/global scan；terminal sample 去重；artifact snapshot 上限 4096 files/1024 dirs 并记录 scanned/truncated，只有 closeout 显式强刷。P0 4/4、`tests.test_profiling` 17/17 通过；PID 消失记录为 unknown，不伪造零资源。 |
| PF-023 | P0 review：收紧六目标 coverage conjunction，禁止用跨 attempt/actor/episode 的零散事件拼成 `present`，并将条件分支/缺失明确保留 | `profile_audit_tool` | `scripts/audit_profiling.py`、`tests/test_audit_profiling.py`、`docs/profiling_metric_contract.md`；只改审计合同和 fixture，不改运行时算法 | 依赖 PF-001/PF-007/PF-008/PF-011；required-family conjunction 在同一 run/task/actor/episode scope 内计算，并检查 attempt terminal 配对；验收覆盖 split-scope、split-attempt、missing-terminal、duplicate/replay、cross-run、conditional branch，`coverage_detail` 与 exit code 一致 | done | `_coverage` 的六目标调用均传入 bounded correlation result；内部只保留 SHA-256 correlation handles，不回显原始身份。`tests.test_audit_profiling` 15/15 通过，含 5 个跨上下文/终态负例；零散跨 scope 事件不会再拼成 `present`，条件分支仍保留 `conditional_missing`。 |

## 六个分析目标与单次运行边界

| 分析目标 | 必须观察 | 单次运行限制 |
| --- | --- | --- |
| 单 attempt agent-vs-wrapper | attempt/agent span、runner self、solver tree、heartbeat、CPU/memory peaks | 至少一个 attempt 注册并完成/超时；aggregate 与 tree 有重叠，不能相加 |
| Selection 全链路 | eligible/filter、tokenize、rank、pack、JSON/hash、persist queue/lock/readback/end | 必须实际执行 search；空池/缓存路径标 conditional |
| `record_search` write lock | queue wait、SQLite `BEGIN IMMEDIATE` wait、lock hold、commit、rows/WAL | 必须发生写事务；跨进程等待只能由 SQLite wait 体现 |
| Trace projection/repeated read | query/page/materialize、identity hash、call index、reuse count | 至少两次同 identity 才能证明重复读取；一次调用不能证明“无重复” |
| `max_parallel` 放大 | admission/slot、resource sample、process tree、RSS/PSS、CPU delta/peak | 需按并发档位分别跑；profile overhead 单独估计 |
| CPS progress/SQLite | progress/search/inbox/digest query/fetch/materialize/connect/lock/WAL | `trace_state` 等分支不调用 progress snapshot，故 progress/read-lock 结论是 conditional；query/fetch 的 wall time 可以测量，但 Python `sqlite3` 没有可靠、独立的 busy-handler wait 指标，不能把 query wall time 拆成精确的 SQLite busy-handler 等待；需要另一个 CPS/non-Figure4 arm 或明确 `conditional_missing` |

## 统一验收队列

所有实现 agent 收尾后，中央按顺序执行并把结果回写本文件：

1. `findmnt -T <worktree> -no FSTYPE`、`umask 0022`，确认输出落磁盘。
2. `python3 -m compileall -q contextswarm_mini`。
3. 动态发现并串行运行 profiling/CPS/Selection/Trace/Evaluator/Judge/Runner/Audit focused suites。
4. `python3 -m unittest discover -s tests` 串行跑一次；并行 full-suite 仅作为诊断证据。
5. 分别跑未 profiling 和 `CONTEXTSWARM_PROFILE=1` 的 `configs/smoke.toml --mock-agent`，输出放 `runs/` 的唯一目录；mock 只验证 plumbing。
6. 用 `scripts/audit_profiling.py` 审计最终 `profiling.jsonl`，检查隐私、dropped fields、终态和 coverage。
7. `git diff --check`、最终 diff/status、精确清理本轮临时目录；不删除未知来源的 `runs/` 或共享 `/tmp` 文件。

## 更新记录

| 时间 | 变更 |
| --- | --- |
| 2026-08-29 | 建立本 living checklist；将 schema/CPS/Selection/Trace/Runner/Judge/Pi/audit/docs/manifest/integration/final-validation 全部绑定到可见 Herd Tab。 |
| 2026-08-29 | `scripts/audit_profiling.py` 与 7 个 focused tests 已落地并通过；新增 Judge admission timeout P0 复现项。 |
| 2026-08-29 | pF 完成 Judge admission P0：off/on 原死锁复现、最小锁 ownership 修复、45-test Judge 模块回归通过；Pi 按用户指令暂停。 |
| 2026-08-29 | 新增 PF-015：接收算法侧 `contextswarm-profiling-repro-20260829` 作为 profiling 样例；限定精确文件下载和离线完整性检查，不启动完整实验。 |
| 2026-08-29 | pK Trace bridge focused 复核完成：28 tests 通过；补齐 normalized trace-set reuse、分页 replay bound、off-path clock/SQL 探针；未启动完整实验。 |
| 2026-08-29 | PF-015 intake 完成：精确包下载与双层 checksum、归档成员/类型审计、公开契约盘点和离线 smoke 通过；确认 Judge `packages/**` 有意省略，正式评分条件性缺项已写入 intake 文档。 |
| 2026-08-29 | 中央首次 profiling-on mock smoke 返回 0，但 audit 在 109 rows 中发现 `dropped_fields=70` 与一个 `closeout.evaluation.end` orphan；新增 PF-016 最小修复，不把不干净 audit 当成完成。 |
| 2026-08-29 | PF-016 修复后中央 fresh off/on mock smoke：off 无 `profiling.jsonl`；on 111 rows、49,562 bytes；audit sequence/span/terminal/privacy/dropped 全通过（exit 0）。 |
| 2026-08-29 | focused profiling/audit/selection/trace 组合 36+81 tests、CPS/evaluator/Judge 147 tests、runner/recovery 47 tests、selection contracts 88 tests 均通过；manifest 组合 45 tests 有 1 个 50ms 调度抖动失败（同一测试在干净基线与重复单测中可复现为环境敏感，非插桩特有）。最新串行 full unittest 为 694 tests，仅 3 个 dirty-worktree launch/container contract 测试失败；三项在干净基线单独通过（此条记录时尚未启动真实 Judge/Pi；后续真实 run 见 PF-018）。 |
| 2026-08-29 | 中央重新执行 off/on mock smoke：off 运行正常且无 `profiling.jsonl`；on 运行生成 111 行、约 49.6 KB JSONL，`scripts/audit_profiling.py` exit 0，agent-wrapper/CPS/Judge=`present`，selection/trace/max_parallel 按 smoke 配置=`not_applicable`，sequence/span/privacy/dropped/terminal 均通过。证据位于本机磁盘构建目录，不进入提交。 |
| 2026-08-29 | 算法侧包最终复核：外层 SHA `d58cc11f…eba4ef`、包内 340/340 checksum、`verify_bundle.sh`、离线 smoke 全通过；四个 pinned source archive entries 为 10433/763/603/599，均无绝对/越界路径、链接或 `packages/**`。Judge `packages/**` 的缺失是脱敏边界，不是下载损坏；正式评分仍需 operator 私下补齐 package root、Lean/runtime、NuRouter/模型与凭据。 |
| 2026-08-29 | 算法侧复现包已落盘并完成外层/内层 checksum、386/340 成员安全检查和离线 smoke；远端临时文件服务不再是本轮依赖，可由操作者关闭。 |
| 2026-08-29 | 中央收口：PF-013 的 Pi readiness bounded 只读检查已完成；独立 operator build 后续已完成真实 non-root Pi/Judge formal 1×1（详见 PF-018），但本 worktree instrumentation 仍未发起真实请求。PF-015 artifact intake/offline smoke 与 PF-016 event sanitizer 保持 done；新增 PF-017 aggregate roots 修复为 P1 working。PF-012 明确串行 full `694 tests` 的 3 个 dirty-worktree launch/container contract failures 不是 profiling 运行时失败，也不能视为 CI success。 |
| 2026-08-29 | PF-017 收口：aggregate roots 修复完成，runner root 与 registered/Pi process-tree roots 的去重边界已固定；focused 22 tests、compile 与 diff-check 通过。本 worktree 未启动真实网络、模型或 Judge 请求；独立 baseline 的真实运行证据单独登记，避免与 instrumentation 数据混淆。 |
| 2026-08-29 | 新增 PF-018/019/020：记录 faithful formal 1×1 的旧版闭环与修正版 preflight、formal assets/Judge route 合同边界和后续 `1 → 10 → 100` 规模矩阵门禁；修正版 full run 已正常收口，Group-required legacy-client integration 与中规模真实负载仍分别保持 conditional/pending。 |
| 2026-08-29 | 修正版 faithful 1×1 已完成：run `20260829T070759Z-35782fe7`、source/image `e999929…`、observe-only compat route `28201`；horizon 300s 后正常 closeout、container rc=0，Pi=2,404/formal=19/Judge=6，run `COMPLETED`/score 0。该结果确认原版真实链路可运行，不代表 solver 成功，也不是 profiling-enabled 证据。 |
| 2026-08-29 | 新增 PF-021/022/023 三个 P0 review：分别审查 SQLite 锁内 profiling I/O、Pi 退出后的全局采样/递归 artifact 扫描边界、以及六目标 coverage conjunction；三项均标为 `working`，未启动真实负载，待各自 focused 证据和审计门通过后再收口。 |
| 2026-08-29 | 新增“当前真实运行与资产清单”：把 corrected formal 1×1 的真实 Pi/Codex/NuRouter、non-root、Lean/Mathlib/REPL/SafeVerify/index/Judge 事实与 official strict route/package/cache 条件分开；旧 intake 的时间点描述不再被当作当前缺件。 |
| 2026-08-29 | PF-021/022/023 冻结收口：锁内 profiling I/O 延迟 flush、Pi 终态有界采样/限额 artifact snapshot、以及同一 run/task/actor/episode 的 coverage conjunction 均已落地；sampler/audit/lock/CPS/Selection/Trace/Profiling focused 58/58 通过，compileall 与 diff-check 通过。三项只证明插桩/审计能力，尚未产生 profiling-enabled 真实 1×1 数据。 |
