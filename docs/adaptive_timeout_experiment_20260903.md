# MathOlympiadBench 验证预算与 Judge 长尾治理实验报告

日期：2026-09-05

实验对象：12 道数学奥林匹克题的 `judge_check` / `evaluate_local` 验证调用

## 1. 背景和动机

原版一小时实验显示，验证请求的耗时分布存在明显长尾：大多数请求很快完成，但少数请求会接近两个约 300 秒 backend attempt 的总时长。原始三轮 baseline 的 3,395 个 fresh accepted Judge 请求中，只有 19 个（约 0.56%）超过 300 秒，却消耗了 42.750% 的 fresh Judge 时间；超过 600 秒的请求只有 14 个（约 0.41%），却消耗了 34.937% 的时间。这个问题主要浪费总实验时长和并发槽位，并不等同于验证结果本身错误。

本实验要回答的问题是：能否让 Agent 根据候选复杂度提出一次验证预算，同时由底层设置不可突破的硬上限，并在异常重试时保证所有尝试共用同一个总预算，从而压缩长尾而不破坏正式验证、反馈分层和收尾语义。

这里的 `evaluate_local` 仍然是受控的远端 evaluator/helper 调用，不是绕过 Judge 的本地权威评分；最终分数仍只来自正式的、带 provenance 的外层评估。

## 2. 具体改了什么

### 2.1 Agent 可提议、底层可裁剪

- `judge_check` 接受可选整数 `timeout_seconds`；`evaluate_local` 通过 `python3 evaluate.py --timeout N` 使用同一预算字段。
- `[judge].timeout_seconds` 是 Agent timeout cap 的单一配置来源；没有该字段时回退到历史 `[lean].timeout_seconds`，默认值为 300 秒。
- Agent 看到的范围由实际 cap 计算为 `min(5, cap)–cap`，不再固定写成 5–300 秒。超出范围的值先由 broker 记录 requested 值，再由 broker/evaluator 裁剪为 effective 值并写入审计。
- prompt 建议使用相对于 cap 的比例：普通增量检查约 10–20%，复杂但有希望的候选约 40–60%，廉价 sanity check 约 5% 或更低；只有已知很慢且接近完成时才使用完整 cap。

例如 cap 为 600 秒时，routine 建议约 60–120 秒、heavy 建议约 240–360 秒；cap 为 60 秒时，建议相应缩放为约 6–12 秒和 24–36 秒。

### 2.2 累计总预算和 fresh retry

显式 `timeout_seconds` 表示一次逻辑验证调用的累计总预算，而不是每个 backend job 各自拥有一份预算：

- 第一次 fresh job 在约 30 秒因 candidate-independent transport/runtime 异常结束时，后续 fresh job 最多只能使用剩余约 270 秒；
- 第一次已经耗尽预算，或返回确定性 verdict、timeout、cancellation 时，不自动重放完整 timeout；
- 每个 backend job 都使用 `max_retries=0`，由 evaluator 外层按同一个绝对 deadline 管理安全 retry；
- retry 保持在同一个 broker handler、evaluator gate 和 Agent/Pi session 中，不回到 CPS allocator，也不创建新的 Agent/Pi session。

未改变的边界包括：实验 horizon、Agent/Pi 生命周期、task-global backend quota、正式评分路径和 remote settlement；未提供显式 timeout 时仍保留 legacy timeout/retry 合同，`formal_query` 也仍是 legacy 查询能力。

## 3. 具体的实验

| 项目 | 设置 |
|---|---|
| 实验问题 | Agent 提议预算 + 底层硬上限，是否能减少验证长尾并保持可接受的质量与收尾 |
| 任务范围 | MathOlympiadBench 固定 12 题，CPS/blackboard |
| 运行时长 | 每轮 3,600 秒（1 小时） |
| 重复次数 | baseline 3 轮（B0/B1/B2）；累计预算 treatment 3 轮（r2/r3/r4）；配置修正后另有 1 轮 confirmation |
| baseline | Agent timeout capability 关闭，使用历史 timeout/retry 合同 |
| treatment | capability 开启，cap=300 秒，显式调用使用累计总预算；r2/r3/r4 使用同一冻结实现和 manifest |
| confirmation | 验证 prompt、Pi schema、helper 和 broker 都从 manifest cap 读取，而不是写死 300 秒 |
| 模型与运行配置 | `openai-codex/gpt-5.6-sol`、thinking=`max`、seed=0、`max_parallel=32` |
| 真实性 | baseline、treatment、confirmation 都是真实一小时 formal workload；最终 PR head 的 mock smoke 仅用于启动/生命周期验证，不计入质量结果 |
| 版本边界 | baseline 主要参考 source `33296b0`；r2/r3/r4 冻结在 `1de0079`；confirmation 启动于 `57b115e`；后续 hardening 和报告提交由 PR 另行验证 |
| 可比性边界 | 任务、horizon、模型和 Judge 合同保持一致，但 treatment 还伴随 recovery/cache 路径和 Agent 轨迹变化；r3/r4 并发时共享宿主机及外部 NuRouter/model capacity，因此这是方向性对照，不是严格单因素因果实验 |

统计口径：`fresh` 指 accepted 且没有 completed/probe/remote cache reuse；tail share 指超过阈值的 fresh `elapsed_seconds` 总和除以全部 fresh elapsed。详细逐轮原始分母和运行身份见[详细实验记录附录](adaptive_timeout_experiment_20260903_details.md)。

## 4. 结论

1. **机制层：长尾治理有效。** 三轮累计预算 treatment 的 pooled 最大 fresh elapsed 从 baseline 的 603.290 秒降到 180.865 秒；超过 300 秒和 600 秒的耗时占比均降为 0。配置修正后的 confirmation 也得到 100% 的显式 timeout adoption，最大 fresh elapsed 为 180.391 秒。

2. **结果层：没有证据证明数学解题质量稳定提升。** treatment 的 score 均值为 `5.667/12`，高于 baseline 的 `4.667/12`，nAUC 方向也较好；但只有三轮、不是随机配对，且 recovery、cache、调度和候选轨迹不完全相同。confirmation score 为 5，且正式 workload 没有自然触发 retry，因此不能把质量差异归因于累计 retry。

3. **成本与可靠性：验证等待和 execution work 下降，但运行并非完全健康。** treatment 在更多 backend jobs 下仍减少了 execution work；所有正式 run 都完成 broker drain 且没有 remote unsettled jobs。另一方面，所有 run 最终状态都是 `DEGRADED`，profiling audit 仍有 dropped fields/未闭合 span，不能把结果称为 clean production benchmark。

4. **决策：建议合并实现，保持能力 opt-in。** 当前证据足以支持 PR 进入 review，并把“减少验证长尾”作为明确收益；暂不把质量提升写成已证实结论，也不扩大到 `formal_query` 或默认开启全部更激进的 timeout 档位。默认开启或质量性能声明前，应先完成 matched flag-off control 和一次受控 transient-fault injection。

## 5. 支撑结论的数据和分析

### 5.1 各轮结果

下表列出全部主对照 replicate，以及配置修正后的 confirmation。`>300 / >600` 是 fresh 请求数量，不是耗时占比。

| 组别 | replicate | score / 12 | nAUC | first proof | fresh n | `>300 / >600` 请求 |
|---|---|---:|---:|---:|---:|---:|
| baseline | B0 | 5 | 0.228833 | 93.256 s | 1,252 | 13 / 9 |
| baseline | B1 | 4 | 0.192578 | 167.415 s | 1,063 | 5 / 4 |
| baseline | B2 | 5 | 0.271661 | 160.280 s | 1,080 | 1 / 1 |
| treatment | r2 | 6 | 0.266490 | 149.457 s | 1,505 | 0 / 0 |
| treatment | r3 | 6 | 0.262380 | 138.107 s | 1,717 | 0 / 0 |
| treatment | r4 | 5 | 0.275170 | 171.200 s | 1,763 | 0 / 0 |
| confirmation | primary-fix | 5 | 0.302987 | 147.579 s | 2,116 | 0 / 0 |

baseline 三轮的最大 fresh elapsed 分别为 603.290、602.894、602.280 秒；treatment 三轮分别为 180.865、153.372、120.580 秒。这个变化在每一轮都出现，且不是由单个 pooled outlier 独立造成的。另一方面，score 在 treatment 内仍为 6、6、5，confirmation 为 5，说明长尾收益和质量波动应分开解释。

### 5.2 长尾、工作量与机制证据

#### Pooled fresh Judge 分布

| 指标 | baseline（3 轮） | treatment（3 轮） | treatment − baseline |
|---|---:|---:|---:|
| fresh 请求数 | 3,395 | 4,985 | +46.8%，分母不同 |
| fresh 总耗时 | 24,145.146 s | 21,003.050 s | -13.0% |
| fresh 平均耗时 | 7.112 s | 4.213 s | -40.8% |
| P50 / P90 / P95 / P99 | 1.277 / 6.309 / 13.938 / 69.694 s | 1.587 / 7.554 / 15.083 / 47.988 s | 极端 tail 改善；普通请求不保证变快 |
| 最大 fresh elapsed | 603.290 s | 180.865 s | -70.0% |
| `>60 s` 耗时占比 | 57.191% | 17.055% | -40.136 个百分点 |
| `>120 s` 耗时占比 | 50.952% | 3.889% | -47.063 个百分点 |
| `>300 s` 耗时占比 | 42.750% | 0% | 目标长尾消失 |
| `>600 s` 耗时占比 | 34.937% | 0% | 双 attempt 尾部消失 |

这组数据支持的是“少量昂贵验证请求被硬性截断”，不是“所有验证都变快”：treatment 的 P50/P90 并未单调下降。

#### Agent 使用和后台成本

| 指标 | baseline | treatment / confirmation | 解释 |
|---|---:|---:|---|
| fresh `judge_check` 显式 timeout | 不适用 | treatment `4,985/4,985=100%`；confirmation `2,116/2,116=100%` | prompt/tool 能力实际被使用 |
| omitted / clamped | 不适用 | treatment `0 / 0`；confirmation `0 / 0` | 未出现 legacy omission 或越界生效 |
| backend jobs / execution work | `5,372 / 25,446.181 s` | `6,601 / 20,220.548 s` | job 数更多但 execution work 更少；不作纯因果归因 |
| 自然 `judge_retry_count` | legacy 无统一字段 | r2/r3/r4 均为 0；confirmation 为 0 | 正式 workload 未触发 transient retry |
| `evaluate_local` | legacy 不可直接比较 | confirmation `93/93` 显式 timeout，最大 elapsed `48.085 s` | helper 路径遵循同一 cap |

显式 timeout 的 backend job 自身没有偷偷恢复旧的 per-job retry；安全 retry 责任在 evaluator 外层。由于自然 workload 没有出现 candidate-independent transient failure，`30 秒后剩余 270 秒` 由确定性测试覆盖，不能用正式 run 的 retry 计数冒充已观察到的故障注入结果。

#### 收尾与健康

三轮 treatment 和 confirmation 均满足 `submitted=finished`、`active_handlers=0`、`drained=true`、`fifo_depth=0`、`remote_unsettled_jobs=0`，supervisor 和本轮容器也正常退出。因此没有证据表明长尾下降是把后台未结算工作隐藏起来换来的。

但是，所有正式 run 最终标记都是 `DEGRADED`；profiling audit 因 dropped fields 和未闭合 span 返回 exit 1。该限制影响诊断精度，不否定已观测到的 tail 方向，但足以阻止“clean、无噪声、生产级”表述。

### 5.3 反向结果、限制与下一步

第一次 timeout 修改的 `treatment-r1` 得分为 4，确实低于后续 r2/r3 的 6；但它使用的是旧的“显式 timeout 直接对应每个 attempt，并把 backend retry 设为 0”语义，不是本报告主结论所依据的累计预算实现。r1 到 r2 之间还同时发生了 prompt 选择分布、cache 路径、recovery 语义和题目级候选轨迹变化；而且 r2/r3/r4 的自然 retry 均为 0。因此不能把 `4 → 6` 写成“retry 修复带来了质量提升”。

当前结论的边界是：

- 样本只有三轮，且 treatment 与 baseline 不是严格随机配对；
- r3/r4 并发共享外部模型/NuRouter capacity，物理资源并未完全隔离；
- `formal_query` 未纳入 Agent timeout treatment；
- profiling audit 不完整，正式 run 也未自然触发 transient retry；
- 质量指标只能作方向性观察，不能作稳定提升或统计显著性声明。

下一步只需要两个小而明确的验证：

1. 在相同 source、cache、recovery、model、Judge runtime 和调度合同下补一组 matched flag-off control；
2. 注入一次可控的 candidate-independent 故障，断言第一次消耗约 30 秒后，第二次只收到剩余约 270 秒，且逻辑调用总预算不超过 300 秒。

在这两项证据完成前，建议合并代码但保持 Agent timeout capability opt-in，不把本实验升级为质量或生产性能的普遍性结论。

详细逐轮数据、运行身份和原始证据索引：[详细实验记录附录](adaptive_timeout_experiment_20260903_details.md)。实现 PR：[Make Agent validation budgets config-driven and cumulative](https://github.com/nustarai/ContextSwarm-ICLR/pull/50)。
