# Agent 提议验证超时：MathOlympiadBench 详细实验记录（附录）

> 面向决策的结论版报告见 [`adaptive_timeout_experiment_20260903.md`](adaptive_timeout_experiment_20260903.md)。
> 本文件保留逐轮原始分母、运行审计、实现讨论和证据边界，供复核使用；不作为主要结论入口。

> **实验目的**：验证让 Agent 为 `judge_check` 和 `evaluate_local` 提议一次验证预算，
> 同时由 broker/evaluator 做硬上限裁剪，是否能缓解少量验证长尾占用大量总时长的问题。
>
> **记录状态**：实现与四轮 treatment run 已完成，并补做了一轮配置驱动提示修正后的
> confirmation run：`treatment-r1` 是旧的
> `max_retries=0` 语义，`treatment-r2/r3/r4` 使用本节所述的累计预算语义。r3/r4
> 按用户要求并发运行，并共享宿主机的外部模型/NuRouter 容量；它们的 worktree、Judge
> runtime、端口、容器和输出目录彼此隔离。本文的数值结论仅适用于列明的
> source/image/Judge 合同，不能替代同 source 的 matched control 或更大规模因果验证。

## 1. 问题与假设

此前的 MathOlympiadBench 12 题、一小时、CPS/blackboard、并发 32 实验显示，
`judge_check` 的 fresh accepted 请求大多数很快结束，但极少数请求会接近两个
300 秒 backend attempt 的总尾部。这个实验不把“较短等待”直接等同于“验证失败”，而是
测试以下四个可观测假设：

1. Agent 会在 treatment arm 中实际为大部分验证调用提供 `timeout_seconds`（或
   `evaluate.py --timeout N`），并能根据候选复杂度选择不同档位。
2. 在相同的一小时 horizon 下，`judge_check` 的 fresh elapsed 长尾（尤其是 >60 s、
   >120 s、>300 s）和累计等待时间下降。
3. `PROVED` 数量、最终 score 和候选反馈质量不会因为过早放弃而明显下降；
   `EXECUTION_TIMEOUT` 仍被视为不确定反馈，而不是 `VERIFY_FAIL`。
4. 取消、远端结算、gate release 和 closeout/drain 保持完整，不会以减少前台等待为代价
   留下 `remote_unsettled_jobs` 或隐藏的 backend 工作。

这是一轮启发式 Agent 行为实验，不是固定随机种子的算法因果证明。只有在 run 的 source、
image、manifest、Judge health、closeout 和 profiling 证据都齐全时，才把数值用于下一轮
设计。

## 2. 历史基线（只读）

### 2.1 主参考 run

主参考采用已有报告中的 `20260901T012227Z-8c90d3f0`（配置名
`mob-formal-1h-cps32-profiled`，源码 `33296b07634c708412326c2808d5782dab3f788e`）。
它与本 treatment 共享 12 题、CPS/blackboard、3600 s horizon、`max_parallel=32`、
Judge/evaluator gate 32 和 profiling 目标；差异是本次新增的 opt-in Agent timeout
能力。该历史 run 的最终健康标记为 `DEGRADED`，所以它适合作为 Judge 成本画像，不能单独
作为无故障算法 baseline。

`judge_checks.jsonl` 的主要口径如下（`fresh accepted` 排除 completed-cache reuse）：

| 指标 | 历史值 |
|---|---:|
| 全部 `judge_check` 记录 | 1,499 |
| accepted / rejected | 1,441 / 58 |
| fresh accepted | 1,252 |
| completed-cache reused | 189 |
| fresh elapsed 平均 / 中位数 | 9.891936 s / 1.274114 s |
| fresh elapsed P90 / P95 / P99 | 6.293134 s / 20.205775 s / 279.994298 s |
| fresh elapsed 最大 | 603.289839 s |
| fresh `EXECUTION_TIMEOUT` | 9 |

| fresh elapsed 阈值 | 请求数 | 累计耗时 | 占 fresh elapsed |
|---|---:|---:|---:|
| >60 s | 25 | 8,502.0 s | 68.650% |
| >120 s | 17 | 7,897.3 s | 63.767% |
| >300 s | 13 | 7,008.4 s | 56.589% |
| >600 s | 9 | 5,423.5 s | 43.792% |

历史长尾的直接机制是 backend 单次 hard timeout 300 s 且 `max_retries=1`，因此一次
超时后的第二次 attempt 会产生约 600 s 的端到端尾部。它不是单纯的全局排队问题：
backend active job 平均约 3.98、P95 9、峰值 19，queue depth 中位数 1、峰值 3。
题目异质性也很明显：例如 `imo2023_p3` 普通请求的中位数约 1.274 s 但有约 603 s
timeout；`imo2023_p2_v2` 的非 timeout P95 已约 46.491 s；`imo2023_p4` 整体中位数
约 21.413 s。因此不能把全局固定 60 s 当作所有题目的安全硬上限。

历史数据还给出两个保护性结论：四次首轮 `judge_check` 的 `PROVED` probe elapsed
约为 5.04、12.61、12.61、41.77 s，合法 proof 不能简单地被 60 s 硬截断；而 `EXECUTION_TIMEOUT`、
`RESOURCE_LIMIT`、取消和基础设施错误必须与候选 `VERIFY_FAIL` 分层统计。

### 2.2 可用的同类控制样本

工作区中还保留了若干同一 benchmark 家族的历史 run（例如 20260902 的 baseline 和
recovery/no-timeout arms）。它们的 Agent 轨迹、accepted 数量和健康标记不同，不能直接
平均成一个“基线均值”。本报告只把上面的 run 作为主参考，并在结果表中同时列出本次
treatment 的完整分母；若要做正式效应量，应再按同一 source/config/Judge 合同补跑
matched control。

## 3. Treatment 合同

### 3.1 能力开关与范围

- treatment manifest：`configs/formal_1h_cps32_profiled_adaptive_timeout.toml`；只在
  `[judge] agent_timeout_enabled = true` 打开能力，其他继承配置不变。
- baseline manifest 默认 `false`。关闭时工具 schema、prompt 和 broker response
  保持历史表面；旧 run 不会因为新增字段而改变分母。
- `judge_check` 请求字段：可选整数 `timeout_seconds`。
- `evaluate_local`：可选 `python3 evaluate.py --timeout N`，由同一 broker 字段承载。
- 默认广告范围为 **5–300 秒**；实际范围由 `[judge].timeout_seconds`（未填写时回退到
  `[lean].timeout_seconds`）驱动，始终显示为 `min(5, configured)`–`configured`。因此较小
  或较大的 manifest 会同步改变 prompt、工具说明、formal helper 文档和 broker receipt；
  300 秒只是默认值，不是写死的全局上限。broker 在 capability 边界校验类型并记录原值；
  超出实际配置范围进行裁剪，malformed 值拒绝。evaluator 再按同一配置做第二次防御性裁剪。
- 当 Agent timeout capability 和 formal helper 同时启用时，配置加载器会把
  `formal_tools.command_timeout_seconds` 至少提升到 `configured + 120` 秒；这是 Pi Bash
  外层的 handoff margin，不改变 Judge/evaluator cap。这样即使把 cap 配成 600 或更大，
  `evaluate.py --timeout N` 也不会先被 shell guard 截断；默认 cap=300 仍保持历史 420 秒。
- staged formal-tool client 的 HTTP transport ceiling 也从同一 Agent cap 推导：无 cap 环境时
  保留历史 480 秒默认值，启用 cap 后至少为 `max(480, configured + 120)`，再受 broker
  session deadline 约束。这样较大 cap 不会在 helper 客户端层被固定的 480 秒提前断开。
- broker 和 evaluator 的嵌套预算反馈也按同一 cap 做二次有界序列化，避免异常 evaluator
  返回超出硬边界的 `timeout_budget_*` 数值污染 Agent receipt。
- receipt、audit 和 profiling 记录 `requested_timeout_seconds`、
  `effective_timeout_seconds`、`timeout_clamped`、`timeout_source`。这四个字段只记录
  有界策略元数据，不记录 prompt、候选源码、token 或原始 Judge response。

### 3.2 时钟和 retry 语义（修订）

`timeout_seconds` 现在表示一次逻辑验证调用的**累计总预算**，而不是每个 backend
attempt 的独立预算。broker 在 capability 调用开始时建立一个绝对 deadline；它覆盖候选
快照、evaluator admission 和实际 Judge work（外层 run horizon 仍是更早的硬边界）。例如
Agent 选择 300 s，第一次因 candidate-independent runtime/transport 异常在 30 s 结束，
安全 retry 只能获得约 270 s；若第一次已经耗尽 300 s，则不再发起 retry。retry 次数由
既有 evaluator/overload policy 独立决定，不会因为选择较短 timeout 而被人为改成零，也不会
把每次 retry 的 timeout 相加成新的长尾。

Judge API 仍然只提供 per-job `timeout` 与 `max_retries` 字段，因此显式预算由 evaluator
外层逻辑循环拆成多个 fresh job，每个 job 使用 `max_retries=0`，并把剩余的绝对时间传给
下一次尝试。这个 retry 保持在同一个 broker handler、同一个 evaluator gate 和同一个
solver capability 调用中；不会退回 CPS allocator，也不会创建新的 Agent/Pi session。
已知的 remote cancellation/settlement 可能需要一个有界 cleanup grace，`elapsed_seconds`
会如实包含它，而 `timeout_budget_*` 字段只统计验证预算本身。

`evaluate_local` 还受 task-global `evaluate_backend_jobs_per_task` 约束。每次准备发起
fresh retry 前先占用一个 backend-job unit；配额不足不会偷偷绕过限制，而是保留前一
次反馈并返回 `formal_backend_budget_exhausted=true`、`retry_blocked_reason`。这项配额是
防止重试放大 formal helper 后端工作量的独立护栏，不改变 `judge_check` 的逻辑预算。

超时结果仍然是 `EXECUTION_TIMEOUT`/不确定反馈，不改写为 `VERIFY_FAIL`，也不开放本地
checker 或 raw Judge access。固定的 run horizon、candidate budget、session probe quota、
closeout evaluation 和 remote settlement 语义不变。每个 receipt/audit 额外记录
`timeout_budget_mode=cumulative_total`、预算消耗/剩余、attempt/retry 数和有界 attempt
明细，便于验证“总预算”而不是只看单次 payload。

### 3.3 Prompt 引导（实验 treatment）

启用能力的动态 prompt 和工具描述建议 Agent（下列比例按 manifest 的实际 cap 计算；默认
cap=300 时约等于括号中的历史档位）：

- routine incremental check 约为 cap 的 10–20%（默认 30–60 s）；
- 有重 import/elaboration/resource 风险、但候选很有希望时约为 cap 的 40–60%（默认
  120–180 s）；
- 只有已接近完整且已知偏慢时才用完整 cap（默认 300 s）；
- 约 cap 的 5% 或更低只适合明显小改动后的廉价 sanity check（默认 5–15 s），不适合
  首个 checkpoint 或刚改大定义。

Prompt 明确说明：值会被 runner clamp，生效值会在 receipt 返回；超时不是错误证明；超时
后应检查反馈并做实质修改或保留最佳候选，再决定是否重试。为了让 treatment 有可测的
adoption，prompt 默认要求正常验证调用显式给值；只有刻意测试 legacy fallback 时才省略。

静态 benchmark `problem.md` 不被写入这段实验提示，避免同步脚本和历史题面发生漂移；
提示只在 run 生成的动态 task/mono prompt 中出现。

## 4. 实验协议

1. 从本 worktree 的单一 commit 构建带 revision label 的镜像；正式 launcher 拒绝 dirty
   tracked tree，并在容器内再次校验 source/manifest/image 绑定。
2. 只使用 operator 注入的 Judge URL、cache-health capability、NuRouter binary/node
   config 和 revision-matched declaration index；这些值不写入 manifest、日志或本文。
3. treatment 使用自然 3600 s horizon、CPS/blackboard、并发 32 和 profiling；让 Agent
   正常结束，保留 closeout/drain，不人为提前杀 run。
4. 结果解析统一以 `judge_checks.jsonl` 的 `accepted`、`fresh`、status 和
   `elapsed_seconds` 为主；并独立读取 `profiling.jsonl` 的 evaluator/backend/queue
   层，避免把嵌套 span 重复相加。
5. 同时检查：timeout adoption、请求/生效值分布、clamp 数、>60/>120/>300/>600 尾部、
   fresh cumulative elapsed、backend job 数和 execution work-seconds、score/proof 数、
   `remote_unsettled_jobs`、closeout active handlers/FIFO/drain、profiling audit 结果。

### 指标定义

- **fresh**：`accepted == true` 且不是 `cache_reused`/`probe_cache_reused`；completed-cache
  命中单独报告。
- **tail share**：阈值以上 fresh `elapsed_seconds` 的累计值除以全部 fresh elapsed；这是
  Judge-facing wall/work 的描述，不等于 backend CPU 节省。
- **adoption**：treatment 中 fresh accepted 调用携带非空 Agent timeout 的比例；另报
  omitted legacy、clamped、按值档位和按题目分布。
- **安全结果**：任何 `remote_unsettled_jobs > 0`、未配对 lifecycle span、异常 closeout
  或 profiling audit error 都会把“只降低前台等待”的解释标为不成立。

## 5. 本轮结果（treatment-r1）

> **语义更正说明**：下表的 `treatment-r1` 运行发生在本次累计预算修订之前。它采用
> “显式 timeout 时 `max_retries=0`”的保守 treatment，因而可以说明旧方案确实压低了
> 前台长尾，但**不能**证明“异常失败后用剩余预算 retry”的新语义。后续 rerun 必须读取
> `timeout_budget_mode`、`judge_attempt_count` 和 `timeout_budget_remaining_seconds`，并
> 单独报告同一逻辑调用的 attempts；本文不把旧结果冒充新实现的验证。

本轮自然运行和 closeout 已完成。Judge elapsed 的分位数使用与历史表相同的线性插值；
`fresh` 仍定义为 accepted 且没有 completed/probe/remote cache reuse。运行身份为：

- run ID：`20260903T053854Z-c388b681`
- source commit：`3bac388895d7ae32267f8a308076fd9e67643fae`
- image ID：`sha256:c25872cdea49b237db614616626161647fe8f2f8a6710d583f098c4342be6240`
- manifest：`configs/formal_1h_cps32_profiled_adaptive_timeout.toml`，SHA-256
  `33f0506df80db26d946236e59e070b4b065431eea892957e469494e5f3a07289`

| 指标 | 主历史参考 | treatment-r1 | 变化/解释 |
|---|---:|---:|---|
| final status / score | `DEGRADED` / 5 | `DEGRADED` / 4 | 健康与算法分开；不能由这一轮归因 |
| all / accepted / rejected | 1,499 / 1,441 / 58 | 1,879 / 1,873 / 6 | treatment 调用更多，拒绝更少 |
| fresh accepted / cache reused | 1,252 / 189 | 1,636 / 237 | fresh 数量增加 30.67% |
| timeout adoption（fresh） | 不适用 | 1,636/1,636 = 100% | omitted legacy 0，clamp 0 |
| `judge_check` requested→effective（fresh） | 不适用 | 15:4，20:3，30:115，45:25，60:896，90:272，120:286，150:14，180:19，300:2 | 60 s 占 54.77%；所有生效值均在 5–300 s |
| fresh elapsed 平均 / 中位数 | 9.891936 / 1.274114 s | 4.614044 / 1.369249 s | 平均 -53.36%，中位数 +7.47% |
| fresh elapsed P90 / P95 / P99 | 6.293134 / 20.205775 / 279.994298 s | 10.085821 / 17.262217 / 62.371901 s | P95 -14.57%，P99 -77.72% |
| fresh elapsed 最大 | 603.289839 s | 122.214555 s | -79.74%；没有约 600 s 双 retry 尾部 |
| fresh elapsed 累计 | 12,384.704 s | 7,548.575 s | -39.05%，同时 fresh 请求更多 |
| fresh >60 s：n / 累计 / share | 25 / 8,502.041 s / 68.650% | 20 / 1,639.295 s / 21.717% | 累计 -80.72%，share -46.93 个百分点 |
| fresh >120 s：n / 累计 / share | 17 / 7,897.338 s / 63.767% | 4 / 487.037 s / 6.452% | 累计 -93.83%，share -57.32 个百分点 |
| fresh >300 s：n / 累计 / share | 13 / 7,008.378 s / 56.589% | 0 / 0 s / 0% | treatment 消除了该层 tail |
| fresh >600 s：n / 累计 / share | 9 / 5,423.513 s / 43.792% | 0 / 0 s / 0% | treatment 消除了该层 tail |
| `EXECUTION_TIMEOUT` / `RESOURCE_LIMIT`（fresh） | 9 / 3 | 10 / 2 | timeout 是不确定反馈，不计入 `VERIFY_FAIL` |
| proof 数 / final score | 5 / 5（历史 closeout） | 4 / 4 | treatment 少 1 个 proof；受 agent timeout/健康差异混杂 |
| normalized score-time AUC / first proof | 0.228833 / 93.256 s | 0.160912 / 162.375 s | 仅描述本轮轨迹，不作因果结论 |
| remote unsettled / closeout | 0 / 正常 drain | 0 / `drained=true` | active handlers=0，FIFO=0 |

### 5.1 `evaluate_local`、`formal_query` 与后端工作量

- treatment 有 117 次 `evaluate_local`，117/117 都携带 Agent timeout；请求值为 15 s（2）、
  30 s（46）、45 s（6）、60 s（53）、90 s（1）、120 s（9），没有 clamp。状态为
  `COMPILES_WITH_SORRY` 16、`VERIFY_FAIL` 95、`EXECUTION_TIMEOUT` 5、`CHEATING` 1；
  elapsed 累计 480.232302 s，最大 52.928130 s。
- treatment 有 894 次 `formal_query`，其工具合同仍是 legacy（`timeout_source=configured_legacy`，
  没有 Agent timeout 字段）。因此本实验只改变 `judge_check`/`evaluate_local`，不能声称所有
  formal-helper backend work 都受到新预算控制。
- 独立 Judge 后端共提交并完成 2,185 个 job，execution work 为 7,500.570 s。携带
  `max_retries=0` 的 custom-budget bucket 为 1,705 个 job、6,868.400 s、最大 121.062 s；
  `max_retries=1` 的 legacy bucket 为 480 个 job、632.170 s、最大 15.584 s（其中包含
  formal-query/closeout 等非 treatment 调用）。主历史参考后端为 2,055 个 job、
  9,747.248 s、最大 602.095 s；这是方向性证据，不是 matched causal estimate。
- 同一工作区其他 no-timeout/recovery 运行的 backend work 约为 6,036–9,663 s，说明
  Agent 轨迹和题目分配的随机波动很大；不能只拿 7,500.570 s 与某一个历史值比较就宣布
  固定收益。

### 5.2 运行健康与 profiling 质量

- `final.json`：`DEGRADED`，score 4/12；140 次 assignment，95 次 `AGENT_FAILURE`、
  127 次 solver timeout、13 次 solver cancellation，6 次 Judge probe infrastructure
  error；OOM/exit-137 和 unexpected process error 均为 0。
- broker closeout 是安全通过的：`active_handlers=0`、`fifo_depth=0`、
  `remote_unsettled_jobs=0`。后端 event log 中 2,185 个 submitted job 都有 terminal
  receipt；任务 supervisor 退出码为 0。服务进程在 SIGTERM 收尾时报告一次
  `shutdown exceeded its hard deadline`，但没有留下未结算 job；这仍是后端 teardown
  的残余风险，不应隐藏。
- `audit_profiling.py` 读取了 303,470 行、序列连续且无敏感字段命中；适用 coverage 均为
  `present`，`judge.execute` 的 start/end 为 1,873/1,873。审计退出码为 1，原因是既有
  profiler 质量问题：108 行共 324 个 dropped fields，以及 13 个未闭合 tool span；
  timeout metadata 本身未产生 dropped field。因而本轮 profiling 适合做受限性能分析，
  不应标成 clean audit。

### 5.3 修订后的累计预算 run（treatment-r2）

这一轮是在 `1de0079` 上重新构建镜像并完整跑满 3,600 s horizon 的匹配 treatment，目的是
验证“retry 次数独立于 Agent 选择的秒数，但所有 fresh attempt 共用一个绝对总预算”的实现，
而不是再次测量旧的 `max_retries=0` treatment。运行身份如下：

- run ID：`20260903T103509Z-251d9cbe`
- source commit：`1de0079c8e30c4a7f89899a28a1189e63f233d21`
- image ID：`sha256:c065a9ae8517616bb4b50328bea5ae441476f71522417a3a7eb6793d9c0ae054`
- manifest：`configs/formal_1h_cps32_profiled_adaptive_timeout.toml`，SHA-256
  `33f0506df80db26d946236e59e070b4b065431eea892957e469494e5f3a07289`

运行完成后，`b0b716b` 又加入了一个不改变 built-in evaluator 语义的 fail-closed 竞态保护：
若窄适配器在已获准 admission 后、真正调用前已经低于 5 秒预算，则释放 gate 并返回审计过的
`EVALUATOR_TIMEOUT`，不补发一轮完整 timeout；该边界由新增的确定性测试覆盖，r2 数值仍以
上面的 run-bound source commit 为准。

| 指标 | 主历史参考 | treatment-r1（旧语义） | treatment-r2（累计预算） |
|---|---:|---:|---:|
| final status / score | `DEGRADED` / 5 | `DEGRADED` / 4 | `DEGRADED` / 6 |
| 全部 / accepted / rejected | 1,499 / 1,441 / 58 | 1,879 / 1,873 / 6 | 1,506 / 1,505 / 1 |
| fresh accepted / cache reused | 1,252 / 189 | 1,636 / 237 | 1,505 / 0 |
| timeout adoption（fresh） | 不适用 | 1,636/1,636 = 100% | 1,505/1,505 = 100% |
| clamp / omitted（fresh） | 不适用 | 0 / 0 | 0 / 0 |
| fresh elapsed 平均 / 中位数 | 9.891936 / 1.274114 s | 4.614044 / 1.369249 s | 4.632415 / 2.522236 s |
| fresh elapsed P90 / P95 / P99 | 6.293134 / 20.205775 / 279.994298 s | 10.085821 / 17.262217 / 62.371901 s | 7.549156 / 12.607708 / 62.816472 s |
| fresh elapsed 最大 | 603.289839 s | 122.214555 s | 180.865324 s |
| fresh elapsed 累计 | 12,384.704 s | 7,548.575 s | 6,971.784 s |
| fresh >60 s：n / 累计 / share | 25 / 8,502.041 s / 68.650% | 20 / 1,639.295 s / 21.717% | 22 / 1,763.523 s / 25.295% |
| fresh >120 s：n / 累计 / share | 17 / 7,897.338 s / 63.767% | 4 / 487.037 s / 6.452% | 4 / 542.837 s / 7.786% |
| fresh >300 s：n / 累计 / share | 13 / 7,008.378 s / 56.589% | 0 / 0 s / 0% | 0 / 0 s / 0% |
| fresh >600 s：n / 累计 / share | 9 / 5,423.513 s / 43.792% | 0 / 0 s / 0% | 0 / 0 s / 0% |
| fresh `PROVED` / `EVALUATOR_TIMEOUT` | 5 / 9 | 4 / 10 | 6 / 24 |

`r2` 的 timeout 请求分布为 `15:4，20:9，25:3，30:287，35:4，40:2，45:72，60:928，
75:3，90:86，120:88，150:5，180:13，240:1`（请求值:次数）；1,505 个 fresh accepted
调用全部带有 `timeout_budget_mode=cumulative_total`，没有 clamp。1,502 个调用实际启动了
一个 backend attempt，另有 3 个 accepted 调用在 run horizon 收口前已无足够时间而零 attempt
结束；另有 1 个 rejected 的 `SESSION_PROBE_BUDGET_EXHAUSTED` 记录同样是零 attempt。需要
特别标注：这 3 个 accepted 零 attempt 行来自 `1de0079` 的旧 run-bound 镜像，仍记录了
`EVALUATOR_TIMEOUT`、`timeout_budget_exhausted=true` 和约 45/60 s 的剩余 Agent 预算；这是
“run horizon 先到”与“Agent 总预算耗尽”混淆的 metadata anomaly，不应当解释成真正消耗了
全部 Agent 预算。当前开发分支（包含 `b47afc6` 修复）已将该边界分类为
`OUT_OF_HORIZON`/`run_horizon`，并由
确定性测试覆盖；正式 r2/r3/r4 数值仍严格按冻结的 `1de0079` 镜像记录。所有记录的
`judge_retry_count` 都是 0。也就是说，这几轮 workload 没有自然触发可安全重试的
candidate-independent transient failure，不能把“没有 retry”误读成 retry 功能被关闭。

`evaluate_local` 共 91 次，全部携带 Agent timeout、没有 clamp，均为一个 backend-job unit
和零 retry；状态为 `VERIFY_FAIL` 53、`COMPILES_WITH_SORRY` 29、`EVALUATOR_TIMEOUT` 6、
`CHEATING` 2、`TASK_CANCELLED` 1。`formal_query` 仍有 886 次，继续使用 legacy timeout
合同，因此 r2 也没有覆盖所有 formal-helper backend work。

后端结构化日志显示 2,064 个 job submitted 且 2,064 个 finished：显式 custom bucket
（`max_retries=0`，由外层累计预算循环持有 retry 责任）1,580 个、execution work
6,186.572 s、最大 180.126 s；legacy bucket（`max_retries=1`，主要是 formal-query/closeout）
484 个、627.900 s、最大 65.433 s。这个 bucket 拆分证明单个 custom job 没有偷偷恢复
旧的 per-job retry；它不能单独作为总成本因果估计，因为 r2 的 Agent 轨迹和 fresh 请求数
与 r1 不相同。

运行健康和证据边界：`final.json` 为 `DEGRADED`、score 6/12，149 个 assignment，
127 个 solver timeout、22 个 solver cancellation、25 个 Judge probe infrastructure
error；OOM/exit-137 和 runner/worker unexpected error 为 0。broker closeout 安全通过：
`active_handlers=0`、`fifo_depth=0`、`remote_unsettled_jobs=0`，supervisor exit 0。
profiling 共 296,158 行，序列连续且无敏感字段；`judge.execute` start/end 为 1,505/1,505。
审计退出码为 1，具体是 351 个 dropped fields（117 行）和 1 个未闭合 span；这与 r1 的
profiler 质量问题同类，不能标成 clean audit。workload 日志中的少量 `BrokenPipe` 是客户端
在有界取消/超时收口后 broker 写回的既有 teardown 噪声；本次 closeout 仍确认没有未结算 job。

这轮对历史参考的方向性结果是：最大 Judge-facing elapsed 从约 603 s 降到 181 s，>300 s
和 >600 s tail 消失；但相对于旧 treatment-r1，>60 s tail 略高（25.295% 对 21.717%），
不能归因于累计 retry 语义，因为题目轨迹、健康状态和请求分母不同。更关键的是，r2 没有
自然瞬态 retry，所以“30 s 后剩 270 s”由确定性单元/集成测试和受控 fake-backend probe
验证，而不是由这轮自然 workload 直接观察到。要评估 retry 是否在不牺牲 proof 率的前提下
减少长尾，还需要 matched control 与故障注入的 transient-retry run。

### 5.4 第三、第四轮累计预算 treatment（treatment-r3/r4）

应用户要求，r3 和 r4 在同一时间窗口并发运行，以固定同一份累计预算实现和镜像。两轮
各自使用独立 detached worktree、build/evidence root、Judge runtime、backend/proxy 端口、
容器、HOME/NuRouter home、缓存和输出目录；它们仍共享宿主机 CPU/内存以及外部
NuRouter/model capacity，因此不是物理资源完全隔离的 paired run。运行期间未触碰宿主机上
另一套已有 formal workload。

共同身份：

- source commit：`1de0079c8e30c4a7f89899a28a1189e63f233d21`
- image：`sha256:c065a9ae8517616bb4b50328bea5ae441476f71522417a3a7eb6793d9c0ae054`
- manifest：`configs/formal_1h_cps32_profiled_adaptive_timeout.toml`，SHA-256
  `33f0506df80db26d946236e59e070b4b065431eea892957e469494e5f3a07289`
- horizon / parallelism：`3600 s / 32`
- model / thinking：`openai-codex/gpt-5.6-sol / max`，seed `0`

运行坐标和最终结果：

| run | run ID | worktree / build root | Judge ports | score / nAUC / first proof | all / accepted / fresh | fresh mean / P99 / max | >60 s share | >120 s share | >300 s | retry / budget exhausted | Judge infra errors |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| treatment-r2 | `20260903T103509Z-251d9cbe` | `adaptive-timeout-20260903` / `adaptive-timeout-20260903-r2` | 28659 / 28611 | 6 / 0.266490 / 149.457 s | 1,506 / 1,505 / 1,505 | 4.632 / 62.816 / 180.865 s | 25.295% | 7.786% | 0 | 0 / 24 | 25 |
| treatment-r3 | `20260903T124445Z-c244cd48` | `adaptive-timeout-r3-launch` / `adaptive-timeout-20260903-r3` | 28859 / 28811 | 6 / 0.262380 / 138.107 s | 1,722 / 1,717 / 1,717 | 3.884 / 46.521 / 153.372 s | 12.116% | 2.300% | 0 | 0 / 13 | 17 |
| treatment-r4 | `20260903T124445Z-bacd88ea` | `adaptive-timeout-r4-launch` / `adaptive-timeout-20260903-r4` | 28959 / 28911 | 5 / 0.275170 / 171.200 s | 1,770 / 1,763 / 1,763 | 4.176 / 45.751 / 120.580 s | 13.727% | 1.638% | 0 | 0 / 18 | 25 |

这里的 fresh 严格按 `accepted == true` 且没有 completed/probe/remote cache reuse 计算；表中
`all / accepted / fresh` 刻意保留 rejected 分母。三轮新增结果的共同点是：>300 s 和 >600 s
Judge tail 均为零，最大 fresh elapsed 只有 120.6–180.9 s；但三轮最终 health 都是
`DEGRADED`，不能把健康标记当成算法成功。r3/r4 的 degraded 原因主要是 Judge worker
recycle 后的 cleanup/floor deficit 和 17/25 次 probe infrastructure error；queue 始终为
0、toolchain/safeverify ready、OOM/runner/worker unexpected error 为 0。

三轮累计预算调用的 adoption 和 retry 证据：

- r2/r3/r4 的所有 fresh accepted `judge_check` 都显式携带 Agent timeout（分别
  1,505/1,505、1,717/1,717、1,763/1,763），clamp 均为 0，omitted 均为 0；有效值全部在
  5–300 s 范围内。
- Agent 选择的 timeout 均集中在 60 s：均值分别为 59.75、59.93、58.99 s；`>=90 s`
  占 12.82%、10.19%、9.53%，`<=45 s` 占 25.32%、18.29%、21.78%。这与 r1 的均值
  75.41 s、`>=90 s` 占 36.25% 有明显差异，说明 prompt 语义本身改变了 Agent 行为。
- 三轮 `judge_retry_count` 总和均为 0；r2/r3/r4 的 accepted fresh `attempt_count=0`
  各有 3 条，其余分别为 1,502/1,714/1,760 条 `attempt_count=1`。`budget_exhausted`
  分别为 24/13/18。因而正式 workload 没有自然触发 candidate-independent transient
  retry；不能把分数变化解释成“累计 retry 实际救回了候选”。
- r3/r4 各自有少量 accepted 零 attempt 的 near-horizon 行，和 r2 的三条相同，仍来自
  冻结的 `1de0079` 镜像旧分类；当前开发分支的修复不会回写这些历史 artifact。

Judge backend 的结构化日志均有完整 terminal receipt（submitted=finished）：

| run | jobs | custom `max_retries=0`（jobs / work s / max s） | legacy `max_retries=1`（jobs / work s / max s） | total work s |
|---|---:|---:|---:|---:|
| treatment-r2 | 2,064 | 1,580 / 6,186.572 / 180.126 | 484 / 627.900 / 65.433 | 6,814.472 |
| treatment-r3 | 2,248 | 1,807 / 5,673.858 / 152.898 | 441 / 730.138 / 59.385 | 6,403.996 |
| treatment-r4 | 2,289 | 1,842 / 6,448.541 / 120.061 | 447 / 553.539 / 65.771 | 7,002.080 |

custom bucket 的单 job 均没有偷偷恢复 backend per-job retry；legacy bucket 主要来自
`formal_query`/closeout，不能混入 custom timeout 的 tail 效应。三轮 profiling 都能读到完整
termination 和适用 coverage，且没有敏感字段，但 audit exit code 均为 1：r2/r3/r4 分别有
351/339/336 个 dropped fields（117/113/112 行）和 1/2/2 个未闭合 span。这是诊断证据
质量限制，不是 run 未收尾；三轮 broker closeout 都是 `active_handlers=0`、`drained=true`、
`fifo_depth=0`、`remote_unsettled_jobs=0`，supervisor exit 均为 0。

### 5.5 三轮 baseline 与三轮累计预算 treatment 的汇总

下面把原始同源 baseline 三轮（B0/B1/B2）与累计预算 treatment 三轮（r2/r3/r4）分别合并。
均值后的 `±` 是跨 run 样本标准差（n−1）；Judge 分布的 pooled 行先合并所有 fresh accepted
请求再计算分位数。两组不是同一 run 的随机配对，且 treatment 还改变了 cache/prompt/recovery
行为，所以这些是方向性描述，不是单因素显著性估计。

| 指标 | baseline n=3 | cumulative treatment n=3 | treatment − baseline / 解释 |
|---|---:|---:|---|
| score / 12 | 4.667 ± 0.577（4,5,5） | 5.667 ± 0.577（6,6,5） | +1.000；方向较好，但 n=3 且非 paired |
| nAUC | 0.231024 ± 0.039587 | 0.268013 ± 0.006530 | +0.036989；方向较好，仍不能归因 |
| first proof | 140.317 ± 40.911 s | 152.921 ± 16.817 s | +12.604 s；平均首证略慢 |
| pooled fresh n | 3,395 | 4,985 | +46.8%，请求分母不同 |
| pooled fresh elapsed | 24,145.146 s | 21,003.050 s | -13.0%（请求更多） |
| pooled fresh mean | 7.112 s | 4.213 s | -40.8% |
| pooled P50 / P90 / P95 / P99 | 1.277 / 6.309 / 13.938 / 69.694 s | 1.587 / 7.554 / 15.083 / 47.988 s | 极端 tail 改善；普通请求 P50/P90 不保证变快 |
| pooled max | 603.290 s | 180.865 s | -70.0%；600 s 双 attempt 尾部消失 |
| >60 s：n / 秒 / share | 49 / 13,808.790 / 57.191% | 46 / 3,582.172 / 17.055% | -40.136 个百分点 |
| >120 s：n / 秒 / share | 28 / 12,302.377 / 50.952% | 6 / 816.790 / 3.889% | -47.063 个百分点 |
| >300 s：n / 秒 / share | 19 / 10,322.027 / 42.750% | 0 / 0 / 0% | 目标 tail 消失 |
| >600 s：n / 秒 / share | 14 / 8,435.668 / 34.937% | 0 / 0 / 0% | 目标 tail 消失 |

backend 作为独立分母也呈同方向变化：baseline 三轮共 5,372 jobs、25,446.181 execution
work-seconds；treatment 三轮共 6,601 jobs、20,220.548 work-seconds。也就是说，treatment
在更多 backend job 下仍减少了约 20.5% 的 execution work，但这些 job 的候选轨迹、cache
路径和题目退休顺序不同，不能把这个差额全部标成 timeout 机制的纯因果节省。

逐题最终证明频次（baseline / treatment）为：`imo2023_p4=3/3`、`imo2024_p1=3/2`、
`imo2024_p2=1/3`、`imo2024_p6=1/3`、`uk2024_r1_p1=3/3`、`uk2024_r1_p2=3/3`；
其余六题两组均为 `0/0`。这说明 treatment 的 +1 平均 score 主要来自 `imo2024_p2` 和
`imo2024_p6` 的频次上升，同时 `imo2024_p1` 的一次缺失被抵消；它不是所有题目普遍变好。

### 5.6 为什么 treatment-r1 比原始差，而 r2/r3 又回到更好

当前证据支持“多因素共同导致轨迹分叉”，不支持把 r1→r2 的分数差写成单一 retry 因果：

1. **实现语义确实变了，但正式 workload 没有执行该分支。** r1 将 Agent timeout 直接
   映射为每个 backend attempt 的 timeout，并把 custom call 的 `max_retries` 设为 0；r2/r3/r4
   才建立一次逻辑调用的 absolute deadline，在同一 broker handler/evaluator gate 内按
   remaining seconds 组织安全 retry。可是 r2/r3/r4 的自然 `judge_retry_count` 都是 0，
   所以 4→6、6→6、6→5 不能说是“retry 救回了 proof”。30→270 s 的核心分支仍应由
   deterministic unit/integration test 或故障注入验证。
2. **r1/r2 共同带有另一个 recovery 混杂项。** 原始 baseline 来自 `33296b0`；r1/r2/r3/r4
   的祖先链先经过 `fefb764`，它把 task/Pi timeout 和 intentional cancellation 从同 actor
   recovery 中移除，只允许异常、非 timeout 的进程失败 recovery。工作区同一 benchmark 的
   三轮 matched no-timeout/recovery 对照显示：原策略 score `5,4,5`（均值 4.667），
   新 recovery score `4,2,4`（均值 3.333）；timeout→timeout 从 91 次降到 0，assignment
   却从约 80 增至约 138。这说明 r1 相对原始的 score=4 不能归因给 Agent timeout prompt，
   至少有 recovery/slot-turnover 的已知混杂。
3. **Agent 的实际 timeout 选择改变。** r1 fresh 请求的平均建议值约 75.41 s，`>=90 s`
   占 36.25%；r2/r3/r4 降至约 59–60 s，`>=90 s` 约 9.5–12.8%。prompt 从“单次 attempt
   timeout”改成“累计总预算”后，Agent 更保守、更频繁地拿到反馈；这可能改变候选质量和
   证明时机，但也可能在高复杂度题目上过早停止，不能只看 tail 数字判断方向。
4. **cache 与调度分母改变。** r1 accepted judge 中有 237 个 cache/probe reuse（fresh
   1,636），r2/r3/r4 的显式 timeout 路径绕过这些 cache（fresh 分别 1,505/1,717/1,763，
   cache reuse=0）。同时 assignment 为 140、149、145、144；证明任务会提前退休并把槽位
   给未解决任务，题目级搜索轨迹因此不同。
5. **题目级轨迹实际分叉。** 例如 `imo2023_p4` 在 r1 有 187 次 fresh check、0 proof，
   r2 有 78 次、1 proof；`uk2024_r1_p1` 在 r1 有 323 次、0 proof，r2 有 173 次、1 proof。
   这类 candidate/feedback 序列差异足以解释单轮 score 波动；r3/r4 的证明集合也显示
   `imo2024_p2`、`imo2024_p6` 的收益并非每轮都伴随所有题目改善。

因此，对“第一次改差、第二次改好”的最稳妥总结是：r1 的 4 分落在原始三轮的 4–5 分范围
内，本身不构成方法变差的证据；r2/r3 的 6 分高于原始三轮最高 5 分，且三轮 treatment
平均分/nAUC 方向更好，但这很可能是 timeout 上限、Agent 选择分布、cache/调度轨迹、
recovery 语义和随机候选共同作用。当前最可信的方案收益仍是“少量昂贵 Judge tail 被硬性
截断”，而不是已经证明数学解题质量有稳定提升。

### 5.7 配置驱动提示修正后的确认轮（primary-fix confirmation）

这轮用于确认用户指出的实现修正：prompt、Pi tool schema、formal helper 和 broker
均从 manifest 的实际 `timeout_seconds` 读取 Agent cap，而不是把 300 秒写死。正式
workload 在确认轮启动时固定于 primary fix 的 commit `57b115e`；运行期间 review
又发现并补上了两个不改变 cap=300 正常路径的边界硬化（helper transport ceiling 和
nested timeout metadata sanitizer），形成代码 hardening commit `5865956`；本节结果随后
由独立文档 commit 记录。因此以下一小时数值不能冒充 `5865956` 镜像的正式
workload 结果；后者另以完整测试和最终 mock smoke 确认。这个 source/image 边界保留在
报告中，避免把实验 artifact 与最终 PR head 混为一谈。

运行身份和合同：

- run ID：`20260903T205925Z-12d09a89`
- source commit：`57b115e1c1fdc6d65a940c7be43c9bb06bd5fbaf`
- image ID：`sha256:a02554a2d565f12d5cedf3ebb538fc5d59453c289f4771328e4b6dbccf26e69e`
- manifest：`configs/formal_1h_cps32_profiled_adaptive_timeout.toml`，SHA-256
  `33f0506df80db26d946236e59e070b4b065431eea892957e469494e5f3a07289`
- horizon / parallelism：`3600 s / 32`；model / thinking：`openai-codex/gpt-5.6-sol / max`；seed `0`
- configured Judge/Agent cap：`300 s`；formal helper command timeout：`420 s`

| 指标 | confirmation run |
|---|---:|
| final status / score / nAUC / first proof | `DEGRADED` / `5/12` / `0.302987` / `147.579 s` |
| all / accepted / fresh Judge rows | `2,122 / 2,116 / 2,116` |
| fresh elapsed mean / P50 / P90 / P95 / P99 / max | `4.879 / 1.320 / 10.070 / 20.316 / 60.386 / 180.391 s` |
| fresh elapsed total | `10,323.932 s` |
| >60 s：n / seconds / share | `25 / 2,119.638 s / 20.531%` |
| >120 s：n / seconds / share | `3 / 421.633 s / 4.084%` |
| >300 s / >600 s | `0 / 0` |
| timeout adoption / omitted / clamped | `2,116/2,116 = 100%` / `0` / `0` |
| `judge_retry_count` sum / nonzero requests | `0 / 0` |
| attempt count histogram | `1:2,114; 0:2` |

所有 fresh accepted `judge_check` 都带有显式 timeout；请求值主要集中在 60 s（1,311
次）、30 s（363 次）和 120 s（139 次），有效值全部落在配置的 5–300 s 范围。`evaluate_local`
共 93 次，93/93 显式携带 timeout、clamp=0，最大 elapsed 48.085 s；`formal_query` 的
931 次仍是独立的 legacy 查询契约。正式 workload 没有自然触发 transient retry，因而
confirmation 只能说明长尾仍被硬 cap 约束，不能把 score=5 解释成 retry 带来的质量变化。

Judge 后端共提交/完成 2,647/2,647 个 job：custom `max_retries=0` bucket 为 2,195 jobs、
9,262.045 execution work-seconds、最大单 job 180.083 s；legacy bucket 为 452 jobs、
722.145 work-seconds、最大 65.859 s。broker closeout 为 `active_handlers=0`、
`drained=true`、`fifo_depth=0`、`remote_unsettled_jobs=0`，supervisor exit=0，
transport preflight 为 `ok`，容器和本轮端口均已释放。最终 health 仍为 `DEGRADED`（127
solver timeout、19 cancellation、28 Judge probe infrastructure error；OOM/runner/worker
unexpected error 为 0）。profiling audit 没有敏感字段且 termination 有效，但 exit code=1：
114 行 dropped fields（342 个字段）和 1 个未闭合 span，故不能标为 clean audit。

## 6. 解释与决策门槛

### 已达到的门槛

- **adoption**：r2/r3/r4 的 fresh accepted `judge_check` 均为 100% 显式 timeout，clamp
  和 omitted 均为 0，说明 prompt/schema 能力确实被 Agent 使用。
- **长尾**：三轮累计 treatment 的 pooled >300 s、>600 s 均为 0；>120 s 耗时占比由
  baseline 的 50.952% 降至 3.889%，>60 s 由 57.191% 降至 17.055%；这也是目前最稳的
  方法收益证据。
- **后台工作与收尾**：treatment 在更多 backend jobs 下仍减少 execution work；每轮
  submitted=finished，broker 都 `drained=true` 且 `remote_unsettled_jobs=0`，没有用“前台
  返回更快”掩盖后台未结算任务。
- **反馈分层**：`EVALUATOR_TIMEOUT`/`RESOURCE_LIMIT` 仍作为不确定或资源反馈保留，没有
  被改写成候选 `VERIFY_FAIL`；closeout、端口和容器均正常收回。

### 尚未达到、需要保留的限制

- **质量因果**：三轮 treatment score 为 `6,6,5`，baseline 为 `5,4,5`，均值方向上高
  一题，但只有 n=3、非随机配对，而且 recovery/cache/prompt/任务轨迹都存在差异；这
  不能写成稳定的数学解题质量提升。r4 的 score=5 也说明 6 分不是每轮必现。
- **retry 分支**：四轮实际 `judge_retry_count` 都为 0，所以正式 workload 尚未观察到
  “30 s 异常后用剩余 270 s”这一分支；当前只能由单元/集成测试和后续故障注入证明。
- **健康/审计**：三轮 treatment 与 confirmation 均为 `DEGRADED`，worker recycle/floor
  deficit 和 probe infrastructure error 仍存在；profiling audit 均因 dropped fields/未闭合
  span 退出 1。因此性能方向可信，但不能声称是 clean、完全无噪声的生产健康实验。
- **普通请求**：P50/P90 并未单调下降，方案主要解决少量昂贵极端请求，不是把每一次
  Judge 都变快；`formal_query` 仍使用 legacy timeout 合同。

综合判断：**方案对“长尾验证耗时”有效且值得继续，但对最终解题质量的提升尚未构成因果
结论。** r1 的 4 分落在原始三轮的 4–5 分区间内，更像随机/混杂波动；r2/r3 的 6 分和
三轮 treatment 平均值的上升是积极信号，但不能归因给累计 retry，因为 retry 实际没有发生。

### 6.1 交付决策（2026-09-04）

针对最初的长尾问题，三轮 treatment 加上 confirmation 的证据已经足够支持提交实现 PR：
原始 Judge 工具的两次 300 秒 backend attempt 会把少数请求拖到约 600 秒；显式 Agent
budget 现在由 fresh backend
attempt、单次逻辑总预算和底层硬上限共同约束，timeout-specific retry 不会再复制一个
完整的长尾。
三轮累计 treatment 的 pooled 最大 fresh elapsed 为 180.865 秒，`>300 s`、`>600 s` 均为
零，`>120 s` 耗时占比由 50.952% 降至 3.889%；confirmation run 的最大 fresh elapsed
为 180.391 秒，`>300 s`、`>600 s` 同样为零。

这不是把所有异常 retry 都删除：对于有明确 candidate-independent transport/runtime 证据的
情况，当前 evaluator 仍可在同一个 broker handler/evaluator gate 内使用剩余总预算做安全
fresh retry；四轮正式 workload 的 `judge_retry_count` 恰好均为 0，因此这个分支没有被自然
流量触发。对本次目标而言，关键保证是超时本身不自动再开一轮完整 300 秒，而不是依赖降低
retry 次数来掩盖总预算问题。

实现上的配置修正也一并纳入 PR：`[judge].timeout_seconds`（或 `[lean]` fallback）现在是
Agent timeout cap 的单一来源，默认仍为 300；prompt、Pi tool description/schema 的说明、
formal helper `PUBLIC_FILES.md`、runner summary、broker policy 和审计字段都读取同一实际
cap。工具 schema 保留“无 maximum 字段”的软控制设计，让超 cap 请求可以到达 broker，记录
requested 值并由底层裁剪；因此既保留可观测性，也保留硬性兜底。

最终质量结论仍保持保守：baseline 三轮 score=`5,4,5`，treatment 三轮为 `6,6,5`，
confirmation run 为 `5`，这是积极但非因果、非稳定的方向性信号；本 PR 的明确结论是长尾
治理有效，不宣称数学解题质量已经被证明提升。

### 下一步决策

1. 先在同一 `1de0079` source、同一 model/Judge runtime、seed、horizon、并发和 recovery
   合同下补做 **matched flag-off control**；同时明确记录 cache policy 和 prompt 是否保持
   一致，否则仍只能得到方向性效果量。
2. 另做一个小型 **transient-fault injection**（例如第一次 backend job 在 30 s 以
   candidate-independent transport 异常结束，Agent 总预算 300 s），直接断言第二次
   payload 的剩余预算约 270 s、总 elapsed 不超过 300 s，并验证同一 handler/gate/session
   语义。这是验证 retry 设计的必要实验，不应等待自然 workload 偶然触发。
3. 在 matched control 与故障注入通过前，不把 60 s 固定成全局 hard cutoff，也不扩大到
   `formal_query` 或更激进的 task/history-aware 档位；若后续质量重复仍保持、且健康审计
   改善，再考虑更多重复轮次和按题目复杂度的提示策略。

## 7. 证据边界

- 本文的历史数值来自既有只读 run 报告；本轮数值必须从新的 run 目录和 commit-bound
  image readback 得到。
- profiler 的 nested/parallel spans 不相加；Judge-facing elapsed、backend execution、
  queue/admission 和 Agent/Pi wall 是不同分母。
- source/commit、image、run artifact、Judge health、installation/deployment 和 live
  runtime 是独立事实；本文不把源码或镜像构建当成部署成功。
- 不记录私有 endpoint、Admin token、邮箱密码、auth JSON、raw candidate、raw model
  response 或其他 owner-only 输入。

### 7.1 本轮可回读证据

以下路径均位于本机 workspace 的 ZFS 磁盘，保留原始 JSONL 与脱敏审计报告；路径中不包含
凭据或私有 Judge endpoint：

- r2 run：`/home/ubuntu/workspace/.workspace/worktrees/ContextSwarm-ICLR/adaptive-timeout-20260903/runs/adaptive-timeout-20260903/treatment-r2/20260903T103509Z-251d9cbe`
- r3 run：`/home/ubuntu/workspace/.workspace/worktrees/ContextSwarm-ICLR/adaptive-timeout-r3-launch/runs/adaptive-timeout-20260903/treatment-r3/20260903T124445Z-c244cd48`
- r4 run：`/home/ubuntu/workspace/.workspace/worktrees/ContextSwarm-ICLR/adaptive-timeout-r4-launch/runs/adaptive-timeout-20260903/treatment-r4/20260903T124445Z-bacd88ea`
- confirmation run（primary-fix image）：`/home/ubuntu/workspace/.workspace/worktrees/ContextSwarm-ICLR/adaptive-timeout-20260903/runs/adaptive-timeout-20260904-confirm/20260903T205925Z-12d09a89`
- pre-merge HEAD host mock smoke：`/home/ubuntu/workspace/.workspace/builds/adaptive-timeout-final-smoke/output/20260903T220703Z-ce533f47`（exit 0）
- canonical-main merge 后、文档收尾 commit 之前的 merged-code image mock smoke：
  `runs/adaptive-timeout-final-merged-image-smoke/20260903T222645Z-018be183`（相对路径相对于本
  worktree）。该 run 使用 `configs/3min_cps.toml`、`--mock-agent`，`status=COMPLETED`、
  `score=0/12`（12 个 `MOCK_SKIPPED`，不是正式 Judge 质量结果），
  `judge_broker_closeout={active_handlers:0, drained:true, remote_unsettled_jobs:0}`；镜像
  `sha256:cdaca50697e7903b92822af31fa07e55c3f5ed32b5268dc3210ab32815d6055d` 的 OCI
  revision label 与当时的 code merge head `40d1b71f19326c980ce5b0621b11e0b8727b4186` 一致；
  随后的提交只更新本报告，不改变 runtime code。
- profiling audit：`/home/ubuntu/workspace/.workspace/builds/adaptive-timeout-20260903-r2/profiling-audit.json`、
  `/home/ubuntu/workspace/.workspace/builds/adaptive-timeout-20260903-r3/profiling-audit.json`、
  `/home/ubuntu/workspace/.workspace/builds/adaptive-timeout-20260903-r4/profiling-audit.json`、
  `/home/ubuntu/workspace/.workspace/builds/adaptive-timeout-20260904-confirm/profiling-audit.json`；四份均
  `exit_code=1`，原因是上文列出的 dropped fields/未闭合 span，而不是敏感字段或 termination 缺失。
- Judge supervisor logs：`/home/ubuntu/workspace/.workspace/builds/adaptive-timeout-20260903-r3/judge/supervisor.log`
  和 `...-r4/judge/supervisor.log` 均记录 `supervisor_exit=0`；对应容器、backend/proxy
  进程和本轮端口在 closeout 后均已退出。
- 代码 hardening commit `5865956`（随后由独立文档 commit 记录）的 timeout/formal focused tests 为 `134 passed, 1 skipped`；
  merge 后新增/回归的 adaptive-timeout focused set 为 `27 passed`，3 个 launch-contract
  tests 也通过。`compileall`、`node --check` 和 `git diff --check` 均通过；在 clean
  worktree、`PYTHONWARNINGS=ignore::ResourceWarning`（只隐藏既有资源告警）下完整
  `python3 -m unittest discover -s tests -p 'test_*.py'` 为 `796 tests, OK (skipped=1)`。
  未抑制告警的完整 discovery 曾分别触发两个使用极短 horizon 的顺序/调度敏感断言，单测
  重复均通过；它们不是 timeout 合同断言，也没有在最终带告警抑制的完整复跑中重现，故不把
  这类环境性波动写成 timeout 实现回归。
