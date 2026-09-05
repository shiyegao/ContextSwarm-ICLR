# ContextSwarm SelectionStore 同一 selection 的 snapshot watermark 去重：决策报告

日期：2026-09-05

实现基线：`8432d83b9b3f14f0a7367c47557826a3941269bc`（`Deduplicate selection snapshot watermarks`）

范围：只处理一个 `search_event`/selection 内的重复存储；跨 selection 或全局内容寻址去重不在本次任务内。

## 背景和动机

`SelectionStore` 原来在 `search_events` 父记录保存一次 immutable snapshot watermark，又在同一 selection 的每个 `search_candidates` 子记录重复保存同一份 JSON。候选 payload、hash 和 attribution 关系并不依赖这份子记录副本，因此这是确定性的存储冗余，而不是多份独立事实。

历史只读 profiling 盘点（895 个 `search_events`、128,205 个 candidate rows）记录了 834,667,185 B 的 child watermark 文本，而 parent watermark 文本为 4,537,892 B，未发现 parent/child mismatch。这说明问题的潜在存储影响很大；它不等于已经证明了同等幅度的 latency、WAL、lock-wait 或 throughput 收益。

本次要回答的决策问题是：能否只保留 parent 的一份物理副本，同时让新写入、旧库迁移、回放、重试、JSONL 导出和校验继续保持可验证的一致性，并用一个小而可复现的对比量化实际节省？

## 具体改了什么

| 边界 | 新行为 | 保持不变的语义 |
| --- | --- | --- |
| 新 schema / 新写入 | `search_events.snapshot_watermarks_json` 是 selection 的唯一持久化 watermark；`search_candidates` 不再创建或写入该列 | parent、candidate、ranking、exposure、feedback 仍在同一个 `BEGIN IMMEDIATE` 链中写入 |
| 旧 SQLite 重开 | 在锁内重新检查 schema，按 selection 流式校验 child 值、parent digest、pool/payload/hash/trace/order/feedback；通过后才删除 child 列 | 畸形、冲突或无法安全判断的数据 fail closed 并回滚，不静默丢记录 |
| API / replay | `_search_chain` 和 replay 仍可返回 candidate-level `snapshot_watermarks`，但这是从 parent 生成的独立 deep copy | 调用方看到的逻辑对象形状和 selection attribution 不变；没有第二个持久化来源 |
| export / validator | 新 JSONL 只在 parent 输出 watermark；旧 export 的 child 字段仍可读，但同一 selection 必须全有且与 parent canonical/equal；混合或矛盾数据拒绝 | record ordering、payload/hash、FK cascade、trace/order unique 约束和 trace index 保留 |
| 并发与兼容边界 | parent 加列和 child 删列共用带 post-lock recheck 的事务；SQLite `<3.35` 在 destructive step 前拒绝 | 迁移是一次性的；旧 writer 不得与新布局混跑，也没有直接 downgrade 路径 |

这项改动只改变 watermark 的物理归属，不改变 selector、ranking、checkpoint、retry 或 recovery 策略。实现和测试仍在实验分支，尚未迁移任何生产或共享测试数据库，也未部署到持久 runtime。

## 具体的实验

| 项目 | 设置 |
| --- | --- |
| 主问题 | 同一 selection 内删掉每个 candidate 的重复 watermark 后，存储大小和逻辑结果是否保持等价 |
| 对比 | 匹配的 legacy child-copy layout vs parent-only layout；另对 legacy 文件执行一次真实迁移 |
| 规模 | 1 个 selection，`N=1000` candidate rows，单份 watermark 为 12,043 B UTF-8 JSON |
| 固定条件 | SQLite page size 4,096；相同 candidate payload、feedback、ranking 和 watermark；WAL checkpoint 后再 `VACUUM` 测量 compact file |
| 运行次数 | 1 次匹配的 N=1000 A/B；1 次 16-opener 并发迁移 probe；另有 focused/full regression 与 mock smoke |
| 真实性 | storage/migration A/B 为离线 synthetic；并发 probe 为本地 SQLite synthetic；没有 Pi、模型、Judge、NuRouter、网络或生产数据库 |
| 主要保持项 | candidate row/payload、replay、export record/bytes/SHA、validator summary、FK/index/unique 约束和失败回滚行为 |

这不是性能实验：没有在本次 A/B 中测量真实并发吞吐、端到端延迟、WAL 增长或 lock-wait 改善。`N=1000` 也不是生产规模的统计抽样，而是用于隔离字段影响的明确分母。

## 结论

1. **存储机制层：已达到目标。** 在受控的 N=1000 对比中，child watermark payload 从 12,043,000 B 变为 0；checkpoint + `VACUUM` 后 compact SQLite 从 13,402,112 B 变为 1,286,144 B，回收 12,115,968 B（90.4034%）。
2. **语义与兼容层：没有观察到回归。** 1,000 个 candidate rows、payload/feedback 字节、parent/candidate replay chain、1,005 条 export 记录及其 bytes/SHA、validator summary 均保持等价；16 个并发 opener 全部成功，迁移后的 parent watermark 和 candidate rows 保持不变。
3. **运行性能层：本次没有证明收益。** 文件大小下降不能直接换算成 latency、WAL、lock-wait 或 throughput 下降；这些指标需要另一个匹配的并发实验，不能从本报告的 synthetic A/B 外推。
4. **决策：可以接受同一 selection 范围内的实现并继续 PR review/merge。** 先保留 parent-only schema、可重启且 fail-closed 的旧库迁移和 legacy export 兼容；生产迁移前必须冻结 SQLite 能力、旧 writer 停止窗口、备份/回滚和 `VACUUM` 维护流程。跨 selection 去重不应借此变更扩大。

## 支撑结论的数据和分析

### 1. 匹配 A/B 的存储结果

| 指标（分母明确为 1 selection / 1,000 candidates） | legacy child-copy | migrated / parent-only | 解释 |
| --- | ---: | ---: | --- |
| 单份 watermark JSON | 12,043 B | 12,043 B | 同一输入对象 |
| child watermark bytes | 12,043,000 B | 0 B | 由 `N` 份降为无 child 副本 |
| parent watermark bytes | 12,043 B | 12,043 B | canonical source 保留一份 |
| candidate rows | 1,000 | 1,000 | 逻辑行数未变 |
| candidate payload bytes | 345,890 B | 345,890 B | 非目标字段未变 |
| feedback snapshot bytes | 49,000 B | 49,000 B | 非目标字段未变 |
| compact DB file | 13,402,112 B | 1,286,144 B | 两侧均先 checkpoint，再 `VACUUM` |
| SQLite pages | 3,272 | 314 | page size 均为 4,096 B |
| file bytes reclaimed | — | 12,115,968 B（90.4034%） | 物理文件差值，不是 wall-time 差值 |

child 文本的理论重复量是 12,043,000 B；compact file 的回收量略有不同，因为 SQLite 页、索引和表布局也会改变。因此“字段 payload 节省”和“文件大小节省”是两条独立的测量，不能混为一个性能数字。

独立的 scaling sanity check（不同 watermark 形状，仅用于检查趋势）在 `N=1/10/100/500` 时观察到 compact-file reduction 分别为 `1.85%/18.46%/60.78%/87.25%`。这些点不是额外的真实数据库样本，也不与 N=1000 结果合并估计总体收益。

### 2. 逻辑等价、迁移和并发证据

| 检查 | 结果 | 身份层 / 含义 |
| --- | --- | --- |
| child watermark column | legacy 存在；迁移后不存在 | 物理 schema 结果 |
| candidate rows preserved | `true`（1,000/1,000） | logical data，不是 process-attempt 数 |
| parent 与 candidate payload chain | `true` | 通过 `search_event_id` 关联；watermark 不参与 candidate payload hash |
| replay idempotency | `true` | 同一 selection 的回放仍得到同一 parent watermark 和 candidate pool |
| export | 两侧各 1,005 records、878,454 B，bytes/SHA 相等 | 新 export 为 parent-only；legacy export 只在字段完整且相等时接受 |
| validator summary | `equal` | 结构/identity 校验结果相同，不等于业务结果被重新计算 |
| FK/index/unique | focused tests 通过 | `ON DELETE CASCADE`、trace index、trace/order uniqueness 保留 |
| concurrent legacy openers | `16/16` 成功，`errors=[]` | 并发进程数；验证 schema lock/recheck，不代表生产并发吞吐 |

迁移的关键安全边界是“先验证、后删列”：parent 为默认空值时，只在所有 child 值一致的情况下提升一个 canonical value；parent/child 不一致、JSON/digest/payload/hash/pool order 不合法、或存在无法重放的 dangling pool digest 时，事务回滚并保留旧数据，等待人工修复或重试。

### 3. 回归验证的范围与状态

| 验证层 | 结果 | 解释 |
| --- | --- | --- |
| focused selection/artifact suite | 最终 PR 分支记录 `114/114` 通过 | 覆盖 fresh schema、legacy reopen、retry/conflict、export/validator、rollback、FK/index 和并发迁移路径 |
| static checks | `py_compile`、`compileall`、`git diff --check` 通过 | 仅是静态/格式门禁 |
| mock smoke | exit `0` | 只验证编排 plumbing，不是 real workload |
| authoritative full discovery（实现 exact head） | `672` tests，`1` skipped，`OK` | 记录于独立、串行、唯一输出目录 |
| rebase 后 target-main discovery | `708` tests，`1` skipped，`2` 个 timing-sensitive failures | 一个在目标 main 基线可复现，另一个隔离重跑通过；不能表述为 CI 全绿，也不归因于本次字段去重 |

这些测试的“通过”只说明代码、迁移和离线行为满足检查；它不表示任何生产数据库已经执行迁移，也不表示 PR 已 merge/release/deploy。

### 4. 历史证据、限制与下一步

历史 profiling 的 895 个 selection / 128,205 个 candidate rows 为问题规模的只读背景；它与本次 N=1000 synthetic layout 的 page 分布、并发条件和运行时不同，不能拼成一个平均节省率。当前仍缺少：

- 代表性并发下的 WAL、lock-hold/lock-wait、写入吞吐和端到端 latency A/B；
- 真实部署数据库的备份、迁移窗口、checkpoint/`VACUUM` 维护验证；
- SQLite `<3.35` 的支持策略（当前实现保守拒绝，不提供 table-rebuild fallback）；
- 新旧 writer 不重叠以及 one-way migration 后的 downgrade/恢复演练。

下一步建议限定为两件事：

1. 在项目运行合同/CI 中明确 SQLite `>=3.35` capability gate，并为旧 writer 停止、备份、迁移、校验和 `VACUUM` 写出可回滚的 operator runbook；
2. 若要声称“运行更快”，另开一个固定 candidate-size、WAL/checkpoint、并发和硬件条件的 matched A/B，单独报告 lock/latency/throughput，不回写本次 storage 结果。

### 5. 可复核证据

代码与测试（仓库内）：

- [`selection_store.py`](../contextswarm_mini/selection_store.py)
- [`selection_artifacts.py`](../contextswarm_mini/selection_artifacts.py)
- [`test_selection_store_candidates.py`](../tests/test_selection_store_candidates.py)
- [`test_selection_artifacts_trace.py`](../tests/test_selection_artifacts_trace.py)

脱敏 owner-only 原始证据（仅在实验工作站可访问，不复制到仓库）：

- `CS-20260901-06/evidence/size_experiment.py`、`size_experiment_run/result.json`
- `CS-20260901-06/evidence/parent-race-final2.txt`
- `CS-20260901-06/evidence/review/ab.md`、`review/findings.md`
- `CS-20260901-06/evidence/full-unittest-authoritative-root.txt`、`impl-handoff.md`

这些是任务证据目录中的稳定文件名，不是仓库内的相对链接；这样不会把机器本地路径写入 PR。需要逐项复核时，按任务号在 owner-only evidence 根目录定位即可。实现 PR：[Deduplicate per-selection snapshot watermarks](https://github.com/nustarai/ContextSwarm-ICLR/pull/48)。

上述原始文件不包含模型凭据或生产数据库内容；它们的数字只用于支撑本报告列出的 bounded offline 结论。
