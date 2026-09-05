# MathOlympiadBench 中 Agent 超时恢复与连续运行预算的对照实验

数据范围为 2026-09-01 至 2026-09-03 的 9 次真实运行：三个 arm 各重复 3 次。分数为固定 12 题的最终得分；表中 `±` 为 3 次运行的样本标准差，不是置信区间。

## 背景和动机

旧流程给单次 Agent/Pi 调用 900 秒的运行上限。调用超时后，outer recovery 会对同一个 logical actor 再启动一次，最多一次，因此外部观察到的等待可能接近 1800 秒。历史记录中，很多 recovery attempt 的最终状态仍然是 timeout，于是产生了一个合理但尚未验证的假设：如果在第一次 900 秒超时时直接结束该 actor，就能把时间交给新的探索，结果至少不应变差。

这里的“超时不重试”只针对 Agent 的 outer recovery，含义是“不再对已经超时的同一个 actor/session 做第二次相同运行”。它不等于否定题目、路径或已有进展：已经结构化写入的 best candidate 和 CPS 状态仍保留，CPS 在释放 slot 后可以为该题接纳新的 assignment。它也不改变另外两层机制：

- Pi/provider session 内部的请求 retry；
- Judge/Lean 的 300 秒执行边界；Judge broker 的既有 retry/退避契约也未改变。

因此本实验要回答两个相互关联的问题：900 秒直接终止是否保持结果，以及如果不保持，差异是否来自 900 秒后同一连续 session 仍能产生有效 proof 的机会。为回答第二个问题，增加了“同一改动代码、单次连续运行 1800 秒”的对照 arm。报告只根据可观测的结构化事件和 authoritative final score 作判断；没有把未写入日志的内部推理解释成确定的永久数据损失。

## 具体改了什么

改动后的生命周期边界如下：

| 触发条件 | outer recovery 行为 | 对任务/调度的含义 |
| --- | --- | --- |
| Agent/Pi timeout（`timed_out=True`） | 当前 logical actor terminal，不再启动同 actor recovery | 保留已提交的 best candidate/CPS 状态，释放 slot；后续可由 CPS 接纳 fresh assignment |
| runner 主动取消（例如任务已解、operator stop 或正常 closeout） | terminal，不 recovery | 不把正常停止误报为进程故障 |
| 非 timeout、非主动取消的异常进程/调用失败 | 在 horizon 尚有预算时，对同一 actor/session/workspace 做有界 recovery，最多一次 | 只修复异常退出，不把 Judge verdict 当成进程故障 |
| Judge 的 PE/WA/verification 结果 | 不触发 Agent recovery | 这是 candidate-attempt 的结果，由任务状态和调度器处理 |
| 运行 horizon 到达 | terminal，不再启动新进程 | 结束本轮实验 |

`contextswarm_mini/agent_recovery.py` 负责上述分类和同 session/workspace 的异常 recovery；`contextswarm_mini/runner.py` 将“同 actor recovery”和“释放 slot 后的新 assignment”分开。因而改动没有把与超时 actor 相关的所有方向全盘否定，只取消了超时后的第二段相同 outer 运行。

以下边界保持不变：Judge/Lean 的 300 秒设置、Judge broker 的既有 retry/退避契约、allocator 的 900 秒 decision deadline、Pi/provider 内部 retry、CPS 的 task-local 状态、32 个并发 slot、1 小时 horizon 和最终评分规则。1800 arm 只把 `pi.timeout_seconds` 改为 1800；`allocation.agent_timeout_seconds=900` 仍是调度决策期限，不是 solver Pi 进程的 1800 秒上限。

## 具体的实验

| 项目 | 设置 |
| --- | --- |
| 实验问题 | 比较旧版 900 秒超时 recovery、改动版 900 秒不做 timeout recovery，以及改动版 1800 秒连续运行 |
| 任务范围 | MathOlympiadBench 固定 12 题 |
| 单轮 horizon | 3600 秒（1 小时） |
| 重复次数 | 每个 arm 3 次，共 9 次真实运行 |
| CPS 合同 | `max_parallel=32`，每题初始 2 个 Agent，`episodes_per_task=2`，`uniform` allocation |
| 模型 | `openai-codex/gpt-5.6-sol`，thinking `max`，seed `0` |
| Judge/Lean | Judge/Lean timeout 300 秒；本实验未改 Judge retry |
| 唯一处理变量 | baseline 的 timeout→outer recovery；或改动版的 Pi 连续 wall-time ceiling（900 对 1800） |
| 真实性 | real formal runs；mock 仅用于代码 smoke，不计入结果 |
| 隔离 | 每轮本地 Judge/backend、runtime、HOME、cache、TMPDIR、端口和输出目录隔离；provider capacity 按授权共享，运行按序执行 |

三个 arm 的来源和语义如下。`treatment_900` 与 `treatment_1800` 使用同一 recovery 改动；后者的提交只是增加 1800 秒 manifest，因此它们是解释连续预算的最直接对照。

| arm | source commit | Pi 单次上限 | timeout outer recovery | 异常 outer recovery |
| --- | --- | ---: | --- | --- |
| baseline-900-recovery | `33296b07634c708412326c2808d5782dab3f788e` | 900 s | 是（最多一次） | 是（最多一次） |
| treatment-900-no-timeout-recovery | `fefb7644ca10f27541d52434c5e0d1a20428de61` | 900 s | 否 | 是（最多一次） |
| treatment-1800-uninterrupted | `74998a53e43e62246ce0af2553b7193976e67b39` | 1800 s | 否 | 是（最多一次） |

1800 arm 的三次运行使用同一构建镜像摘要 `sha256:3225811d8ee880d8918547b2a9f08859039940ec45c1c730015596b63998af6d`；其余版本边界由上表的 source commit 标识。

## 结论

1. **机制层：** 改动实现了目标边界。900 秒 timeout 和主动取消不再对同一 actor 做 outer recovery；已提交的任务状态仍可被新的 CPS assignment 使用。因此“timeout 不重试”不是把整条路线判死，而是缩短一个 logical actor 的生命周期。

2. **结果层：** 900 秒直接终止在这 3 次重复中确实比旧版差；1800 秒连续运行则在同一改动代码下取得三组中最好的 final score 和 nAUC。这个结果支持“连续探索预算”是关键变量，但由于每个 arm 只有 3 次，仍是方向性证据，不是统计显著性声明。

3. **机制与成本层：** 旧版 `recovery_succeeded` 很少，不能据此判定 retry 没有正向作用；它只表示第二个进程最后正常返回，不统计该进程中途已经产生的 proof。事件归因显示，旧版有一部分 proof 出现在 recovery 已启动之后，而 1800 连续运行把对应的后段机会保留在同一个进程中。1800 的 assignment 数并没有增加，slot utilization 三组都接近 1，说明收益更像来自连续深度，而不是凭空增加空闲容量。

4. **决策：** 保留“timeout/cancel 不做同 actor outer recovery”的语义修复，但不要把“900 秒即终止”当作已验证的性能优化。1800 秒连续预算可作为候选默认值继续验证；在更多同合同重复、并改善 Judge/Coordinator 健康度之前，不应作更强的普遍性能或发布结论。

## 支撑结论的数据和分析

### 结果对比

下表是每个 arm 3 次运行的均值；`score` 和 `nAUC` 同时列出样本标准差。`score` 越高越好，`nAUC` 是按时间加权的归一化得分，越高越好；首次 proof 时间越低越早，但不能替代最终分数。

| 指标 | baseline 900 + recovery | treatment 900，无 timeout recovery | treatment 1800，连续运行 | 1800 − baseline | 1800 − treatment900 |
| --- | ---: | ---: | ---: | ---: | ---: |
| final score / 12 | 4.667 ± 0.577 | 3.333 ± 1.155 | **6.333 ± 1.155** | +1.667 | +3.000 |
| nAUC | 0.231024 ± 0.039587 | 0.164602 ± 0.035874 | **0.322684 ± 0.005440** | +0.091660 | +0.158082 |
| time to first proof (s) | 140.317 | 123.257 | 182.904 | +42.587 | +59.647 |
| scheduler assignment records | 80.000 | **137.667** | 86.667 | +6.667 | −51.000 |
| solver timeout events | 63.333 | **126.667** | 61.333 | −2.000 | −65.333 |
| solver cancellation events | 14.000 | 10.333 | 24.000 | +10.000 | +13.667 |
| outer recovery scheduled | **64.667** | 3.000 | 6.000 | −58.667 | +3.000 |
| outer recovery succeeded | 1.667 | 0.333 | 0.000 | −1.667 | −0.333 |
| solver slot utilization | 0.999976 | 0.999942 | 0.999983 | — | — |
| run wall time (s) | 3732.6 | 3752.2 | 3674.7 | −57.8 | −77.5 |
| Judge probe infrastructure errors | 20.000 | 8.333 | 14.000 | −6.000 | +5.667 |

900 treatment 的 assignment 记录比 baseline 多约 72%，但 solver timeout 也约翻倍，最终分数和 nAUC 反而更低。这说明“尝试次数”这一过程层计数不等于有效 proof 或最终采用；大量较快以 timeout 结束的 fresh assignment 没有补偿被提前截断的连续 session。1800 treatment 的 assignment 反而少于 900 treatment，却取得更高的时间加权得分。

### 每次运行的结果

下面列出全部 9 次 replicate，避免把均值掩盖的反向运行当成不存在。run ID 仅用于定位本地原始证据，原始 prompt/response/candidate/profiling 不在仓库中。

| arm | replicate | final score | nAUC | first proof (s) |
| --- | --- | ---: | ---: | ---: |
| baseline-900-recovery | `20260901T012227Z-8c90d3f0` | 5 | 0.228833 | 93.256 |
| baseline-900-recovery | `20260902T075657Z-eda06caf` | 4 | 0.192578 | 167.415 |
| baseline-900-recovery | `20260902T090313Z-ecee9c07` | 5 | 0.271661 | 160.280 |
| treatment-900-no-timeout-recovery | `20260902T051107Z-3d88881e` | 4 | 0.179770 | 125.138 |
| treatment-900-no-timeout-recovery | `20260902T100459Z-deb0d842` | 2 | 0.123635 | 118.360 |
| treatment-900-no-timeout-recovery | `20260902T110746Z-d7f1cc93` | 4 | 0.190401 | 126.273 |
| treatment-1800-uninterrupted | `20260903T104820Z-d67daadf` | 7 | 0.328913 | 161.250 |
| treatment-1800-uninterrupted | `20260903T120650Z-f6ef8a8a` | 7 | 0.318865 | 228.697 |
| treatment-1800-uninterrupted | `20260903T131006Z-7c6668fd` | 5 | 0.320274 | 158.767 |

1800 的首次 proof 平均较慢，但 nAUC 最高且三个 replicate 都不低于 0.318865；这正是“早期首个成功”和“整段时间的有效进展”可能方向不同的例子。最终分数的 5/7/7 也表明收益不是由单个异常高分 replicate 独占。

### 机制与成本证据

事件身份与分母必须分开：`judge_proof_credited` 是运行期间 evaluator/Judge 对 candidate 的实时 proof credit；`recovery_succeeded` 是 outer recovery 的第二个进程最后以正常 return code 结束。两者不是一一对应，也不能把后者当作 proof success rate。

| 事件投影（3 次运行合计） | baseline 900 + recovery | treatment 1800 连续 | 分母/解释 |
| --- | ---: | ---: | --- |
| live `judge_proof_credited` | 9 | 17 | 运行期间的实时 proof credit；不含 closeout-only 重复确认 |
| 其中发生在后段 | 6 | 11 | baseline：attempt 0 timeout 后 attempt 1 已启动；1800：同一 Pi 进程启动后超过 900 秒 |
| `recovery_scheduled` | 194 | 18 | 三轮合计的 outer recovery 调度事件；不是 proof 数 |
| `recovery_succeeded` | 5 | 0 | 第二进程终态正常返回；不能推断中途没有贡献 |

这组事件解释了此前看似矛盾的现象：旧版三轮共调度 194 次 recovery，但只有 5 次以“正常返回”结束；然而其中至少 6 个实时 proof credit 已发生在 recovery 启动之后。换句话说，第二段进程即使最终再次 timeout，也可能在终态前提交并被 Judge 记账。900 treatment 在第一次 timeout 就关闭 actor，因而不会保留这类同 session 的后段机会；1800 treatment 则把它们变成同一进程内的连续探索。这里的结论只覆盖已观测、已结构化的 proof credit，不声称所有未落盘的内部状态都被永久丢失。

过程计数还显示：三组 solver slot utilization 都在 0.99994–0.99998 之间，没有证据表明 900 treatment 因大量空闲 slot 而落后。1800 的平均 wall time 约 3675 秒，略低于另外两组；这只是本地运行窗口和 closeout 的观测，不能单独解释为 token 或 CPU 成本下降。900 treatment 的 Judge probe error 均值最低，但它的数学结果最低，进一步说明基础设施健康和数学质量应分开报告。

### 稳定性、限制与下一步

- 三次 1800 run 都写出唯一的 `final.json` 和 `run_meta.json`，外层返回码为 0；没有观察到 OOM、runner/worker 崩溃或未结算 Judge job。第二、第三轮各记录一次 `Coordinator response failed` 的 solver process error，按异常停止处理，不与 timeout 混淆。
- 1800 每轮仍有 13–15 次 Judge probe infrastructure error，因此健康标签为 degraded。该标签是运行可靠性限制，不直接改写 candidate score。
- profiling audit 仍有 dropped-field/open-span 的已知诊断限制；本报告不把 profiling 文件当作生产负载、资源独占或隐藏状态的证明。
- 每个 arm 只有 3 次重复，且 provider capacity 共享、运行非 paired；不能据此给出显著性、token 节省或跨模型泛化结论。

下一步最小实验是：在同一改动源码、同一健康的 Judge/Coordinator 条件下，预先固定合同并增加 paired 的 `pi.timeout_seconds=900`（无 timeout recovery）与 `1800` 连续运行重复；同时记录每个 live proof 的 actor/session/process 起点和最终 score。只有在更多重复和健康度改善后仍保持 1800 的优势，才适合把 1800 秒写入默认配置或作性能发布声明。

### 证据、代码与验证

脱敏的聚合数据见 [`agent_timeout_recovery_comparison_20260903.json`](./agent_timeout_recovery_comparison_20260903.json)。可复现的 1800 manifest 是 [`formal_1h_cps32_profiled_agent1800.toml`](../configs/formal_1h_cps32_profiled_agent1800.toml)；生命周期分类和 runner 接线见 [`agent_recovery.py`](../contextswarm_mini/agent_recovery.py) 与 [`runner.py`](../contextswarm_mini/runner.py)。原始运行和 profiling 证据保留在操作员本地目录，不包含在仓库。

代码交付前的验证（不计入上述 9 次真实实验）已通过：`compileall`；完整 unittest `Ran 700 tests in 79.572s, OK (skipped=1)`；recovery/CPS focused tests `31/31`；1800 manifest validation `ok: true` 且识别 12 题；`configs/smoke.toml --mock-agent` 返回码 0。此次报告修订未改变实验代码，也没有部署、修改远端 Coordinator/Judge、导入账号、合并 PR 或改变运行数据。
