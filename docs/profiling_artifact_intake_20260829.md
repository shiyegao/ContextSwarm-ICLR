# 算法侧 profiling 复现包 intake（2026-08-29）

这份记录用于保存本轮从算法实验侧收到的 profiling/reproducibility bundle 核对结果。
以下内容是 **artifact intake 时间点（2026-08-29）** 的状态快照：它是 profiling 的
输入样例和 workload 参考，不是当前 `ContextSwarm-ICLR` worktree 的替代 source；文中
“尚未启动/没有访问真实模型或 Judge”均只描述当时的 intake 阶段，不代表后续运行状态。

## 来源和本地保存

- 来源：用户临时提供的局域网 HTTP 文件服务；本记录不持久化其主机、端口、凭据或 URL。
- 本地 artifact 目录：
  `.workspace/artifacts/contextswarm-profiling-repro-20260829/`
- 外层包：`contextswarm-profiling-repro-20260829.tar.zst`
- SHA-256（外层包）：
  `d58cc11f98069550f131af0ccc6c621c9eaf66abff93c0546e099dfc75eba4ef`
- 下载和解包目录位于 ZFS；artifact 根目录和 `extract/` 为 owner-only（0700）。
- 当前 profiling 实现仍在隔离 worktree 的 `profiling-capacity-20260828` 分支；两者
  没有混用或覆盖。

## 已完成的只读核对

| 检查 | 结果 |
| --- | --- |
| 精确外层 URL 响应 | HTTP 200；只请求同名 `.tar.zst` 和 `.sha256` 两个文件 |
| 外层 checksum | `sha256sum -c` 通过 |
| 外层成员 | 386 entries；无绝对路径、`..` 路径、symlink、hardlink 或特殊文件 |
| 包内 `checksums.sha256` | 340/340 条通过（`sha256sum -c` 返回 0） |
| `tools/verify_bundle.sh` | 返回 0；只做校验，不启动服务 |
| 内层 source archive | 4 个 archive checksum 均通过；成员数 10433、763、603、599 |
| Judge archive 私有包 | 两个 Judge archive 的 `packages/**` 成员数均为 0；hidden tests、validation/oracle、statements、evidence 未随包交付 |
| 内层归档类型 | 未发现 link 或非普通文件 |
| 离线 smoke | `reproduction/offline_smoke.sh`，隔离 HOME/TMPDIR、mock-agent、约 1.2s，返回 0；无残留 ContextSwarm 进程 |
| 公开证据索引 | `report.md`、`README.zh-CN.md`、`profiling_plan.md`、`experiment_catalog.csv`、`evidence.json` 均存在且可读 |

## 与当前仓库的关系

包内 pinned source 不是当前 worktree HEAD：

- `ContextSwarm`：`60874604d18b68d59ae0b88056e5f1b850446479`
- `ContextSwarm-ICLR`：`500a0be800167b86bcff996d7ef163d78fa3b665`
- `ContextSwarmJudge`：`6e7291ba51fa3403daba49cf07674322b367882e`
- `ContextSwarmJudge-ICLR`：`753cb12dd9c9cc4bf7003517e16cedbed0124f39`

因此，不能把包内配置直接复制到当前分支来宣称“同一实验”。它适合提供：

1. workload/cap/supply/horizon 的分组和历史异常样例；
2. agent-local、CPS、evaluator/Judge、closeout、资源采样的指标词汇；
3. 真实运行中“缺失 artifact 不等于 0”活动的审计边界；
4. profiling 单次运行应覆盖的阶段和 join key 设计。

## 已确认的缺项和影响

### 不影响离线完整性检查的项

- 包根本身没有当前仓库约定的 `configs/smoke.toml`；它提供的是
  `configs/iclr/figure3_3min_smoke.toml`，以及独立的
  `reproduction/offline_smoke.sh`。后者已经通过，因此这是两个仓库 smoke 入口不同，
  不是 checksum 或归档损坏。
- 包中的 `README` 明确说明 `127.0.0.1` 只是目标机 operator-local 服务占位，不能
  直接当作原实验机器地址。

### 不能仅凭本包启动官方真实评分的项

正式 coding/formal run 还需要由操作者在目标机私下提供并匹配 pinned revision 的：

- Judge `packages/**`（题面、hidden tests、validation/oracle、statements 和 evidence）；
- 与 manifest 对应的 Lean declaration-index/workspace（formal arm）；
- 运行时 data、result-cache/health contract，以及 Docker/Rust/Python 依赖；
- NuRouter/AISW ELF、node 配置、可访问 evaluator/Judge endpoint 和相应凭据；
- 若走真实 agent，则还需要约定版本的 Codex/Pi client。**在本 intake 时间点**，Pi
  按当时的用户指令暂停，因此当时不会启动或配置它；随后 Pi 已恢复使用，并在独立
  operator build 中完成了真实 formal 1×1（见文末“后续状态”）。

这些缺项不妨碍我们在 intake 阶段验证 profiling plumbing、off/on 开关、离线 mock
smoke 和审计脚本；但在当时缺项未补齐前，不能把该包当成可执行的官方 baseline。

## 对下一轮 profiling 的用法

- 先用当前 worktree 的 `configs/capacity_coding/cps48_selection_trace.toml` 和
  `configs/smoke.toml` 做 plumbing/指标完整性验证。
- 将本包 `profiling_plan.md` 的阶段词汇映射到当前事件 schema，尤其保留
  `agent-vs-wrapper`、Selection/SQLite lock、Trace projection reuse、CPS progress、
  Judge admission/execute/drain 和资源峰值的分界。
- 对后续 profiling-enabled 或官方评分运行，仍应先由用户/远端操作者补齐 Judge
  package root 与 runtime contract；只做一次受控的低档位 canary，再按并发档位扩展，
  不在本地复制账号、token 或持久 Agent home。
- 所有 profile 结果都要经过 `scripts/audit_profiling.py`；缺失事件标为
  `missing`/`conditional_missing`，不静默改成零。

## 证据边界

截至本 intake 时间点，本轮只完成精确下载、checksum、成员/类型安全检查、公开文档
读取和离线 smoke；当时没有启动完整实验、访问真实模型/Judge、读取或复制凭据、hidden
data、raw prompt/provider/session log，也没有修改远端服务。远端临时文件服务是否关闭由
用户决定；关闭前若要补交 Judge packages 或 runtime data，应另行提供与上述 pinned
revision 的对应关系。

## 后续状态（2026-08-29，独立于本 intake 快照）

在上述 intake 完成后，已使用修正版 source/image provenance 和 task-owned
observe-only Judge compatibility route 完成一次真实 formal 1×1 生命周期运行：

- Run：`20260829T070759Z-35782fe7`
- source/image revision：`e999929…`（exact match）
- 真实 non-root Pi、NuRouter、模型和 Lean Judge 均实际参与；300 秒 horizon 后正常
  closeout，container/launcher `rc=0`，最终 `COMPLETED`，`health.ok=true`。
- 运行证据：Pi events `2,404`、formal tool calls `19`、Judge checks `6`；最终
  solver 未解出（score `0`，closeout `COMPILES_WITH_SORRY`）。
- 这次运行使用原版未插桩 source，因此没有生成 `profiling.jsonl`；它证明真实运行链路
  可持续并正常收口，不替代后续 profiling-enabled 配对，也不代表算法质量结论。

因此，本文前面的“没有访问真实模型/Judge”和“Pi 暂停”是 intake 阶段的历史事实；不能
解读为当前项目状态。
