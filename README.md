# ContextSwarm ICLR Mini

这是一个与上游 `ContextSwarm` 隔离的研究运行时，并冻结了论文使用的六套公开题目包：USACO、ICPC、PutnamBench、MathOlympiadBench（imobench）、Clever 和 Verina。当前默认运行入口仍绑定 MathOlympiadBench latest12；其余五套用于题目定义、来源与完整性审计，不会暗中改变 Mono/Parallel/CPS 的 task/model/time/evaluator contract。当前目录的代码不会修改 sibling 上游仓库。

运行形态固定为同一套 Docker + NuRouter/AISW + Pi backend：

- `mono`：一个 Pi session 顺序处理 12 个任务，作为单体 baseline；
- `parallel`：每个任务一个独立 Pi session，不共享 CPS；
- `cps`：弹性 agent pool；默认每道题先分配 2 个 agent，总并行槽位为 24。agent
  完成后，空闲槽位会继续分配给未完成题目；同题 agent 通过 SQLite WAL/CPS
  context 和 task-local best candidate 合作；
- `cps_direct` / `cps_hybrid`：用于通信机制 ablation。

## 先做本地 smoke

不需要 Docker、NuRouter、Pi 或 Lean 服务即可检查数据和运行闭环：

```bash
python3 -m contextswarm_mini.cli --config configs/smoke.toml validate --json
python3 -m contextswarm_mini.cli --config configs/smoke.toml plan --json
python3 -m contextswarm_mini.cli --config configs/smoke.toml run --mock-agent
# 等价入口：python3 main.py --config configs/smoke.toml run --mock-agent
```

最后一条命令会在 `runs/`（或 `--output` 指定目录）生成：

```text
run_meta.json
transport_preflight.json   # real NuRouter/Lean run
events.jsonl
scoreboard_history.jsonl
final.json
cps.sqlite3              # CPS 模式
communication_trace.jsonl # CPS 事件投影
elastic_assignments.jsonl  # CPS 动态 agent 分配
elastic_scheduler_state.json # CPS 调度器收尾状态
allocation_decisions.jsonl   # 自适应分配决策及当时的因果快照
allocation_summary.json      # policy 延迟、fallback、token 与 slot 利用率
allocation_audit.jsonl       # Trace-State 同状态 Task-State 反事实与容量守恒
figure4_run_summary.json     # Figure 4 单次运行的 score-time、成本与合同摘要
closeout_candidates.json   # 三种模式统一的冻结候选索引与 SHA-256
closeout_candidates/<task>/result.lean # feedback-free 最终评分快照
formal_tool_calls.jsonl  # formal helper 调用审计
formal_tools_contract.json # helper/index 的公开版本合同
formal_tools_summary.json # 每题、跨 session 的 quota 计数
workers/<task>/result.lean # parallel
workers/<task>/agents/<actor>/result.lean # elastic CPS attempts
workers/<task>/best/result.lean # elastic CPS best candidate
workers/mono/tasks/<task>/result.lean # mono
```

`--mock-agent` 只验证编排和产物，不代表论文分数。

## Docker + NuRouter/AISW Pi

先构建镜像：

```bash
CONTEXTSWARM_MINI_PI_VERSION=0.84.2 scripts/build_image.sh
```

镜像同时固定 Codex compatibility binary（当前默认 `0.148.0`，可用
`CONTEXTSWARM_MINI_CODEX_VERSION` 覆盖）。
正式构建只接受 clean worktree，并使用 `git archive HEAD` 作为 Docker context；
镜像 label 绑定完整 source commit。启动器会校验实际 image ID 与 revision label，
并把两者写入 run provenance，因此三个 allocation arm 可以做精确版本联结。

默认 manifest 使用标准 provider routing（`fast_mode = false`），这样可以
兼容没有 runtime-policy endpoint 的 NuRouter release；确认 coordinator 的
`/core/v1/runtime-policy` 返回 `allowCodexFastMode=true` 后，再在 operator-local
manifest 中打开 fast mode。

真实运行前需要在宿主机准备：

1. 可在 Linux 容器执行的 NuRouter/AISW release ELF（优先使用 `nurouter`）；
2. NuRouter node/coordinator 配置（默认读取 `~/.nurouter/node.toml`）；
3. 可访问的 MathOlympiadBench Lean Judge，并在宿主环境中设置
   `CONTEXTSWARM_JUDGE_URL`。Judge 地址不会写入 tracked manifest。
4. 与 Judge 的 Mathlib revision 完全一致的 declaration-index SQLite；通过
   `CONTEXTSWARM_MINI_DECL_INDEX` 指向宿主文件。启动器会计算并绑定 SHA-256、
   只读挂载到容器，并要求 index、health、kernel probe 三方 revision 一致。

正式 allocation 和 canary manifest 还要求 supervisor 注入
`CONTEXTSWARM_JUDGE_CACHE_HEALTH_URL`。它必须指向同一 Judge backend 的
`/healthz`（也可以给 backend base URL），用于 fail-closed 地确认本次 Lean 环境
就绪且 result cache 已禁用。

如果使用同机的 `ContextSwarmJudge`，需要启动完整 formal stack（不要只启动
Lean-Eval slice），例如：

```bash
cd /path/to/ContextSwarmJudge
./scripts/start_formal_lean_stack.sh up
```

然后通过 Judge 的 health endpoint 确认 `accepted_lean_env_ids` 包含
`formal_matholympiadbench`。真实 run 和 preflight 在该变量未设置时会直接拒绝
启动；离线 `--mock-agent` smoke 不需要它。

默认运行 CPS：

```bash
# 先在本地 shell 或 secret manager 中设置并 export CONTEXTSWARM_JUDGE_URL。
scripts/run_docker.sh --config configs/cps.toml
```

启动脚本只把环境变量名（`-e CONTEXTSWARM_JUDGE_URL`）交给 Docker，避免私有值
出现在进程参数、命令日志或 run summary。runner 会把它转换成每个 solver session
独立的 loopback capability；Solver 看不到 raw Judge endpoint。
cache-health capability 也只按变量名注入，只供 supervisor preflight 使用；其私有值
不会交给 Solver，也不会写入 tracked manifest、preflight 证据或运行摘要。

`run_docker.sh` 会从完整解析 `extends` 后的 `[docker]` 读取 `image`、
`memory_mb` 和 `network`（`host` 或 `bridge`）。`bridge` 会配置
`host.docker.internal`，用于让 runner 访问显式开放给 Docker bridge 的独立
Judge，同时隔离容器内硬编码的宿主 `127.0.0.1` 端口。运维侧仍可用 `CONTEXTSWARM_MINI_IMAGE`、
`CONTEXTSWARM_MINI_MEMORY` 覆盖，两者优先级高于 manifest。
正式 run/preflight 的启动器拒绝有 tracked 修改的 worktree，并要求整条 manifest
继承链都来自当前 commit 的 tracked 文件；`runs/` 下的本地 manifest 只允许用于
mock/plan/validate。完整继承链的 SHA-256 会在容器 entrypoint 再验一次。正式链
来自只读、commit-bound 镜像，因此校验后也不能被宿主并发修改；开发态的 smoke
仍可在 dirty worktree 运行。

正式启动前可以只做 transport 检查（不会启动 Pi session）：

```bash
scripts/run_docker.sh --config configs/cps.toml preflight
```

运行三种 paper-facing cells：

```bash
scripts/run_docker.sh --config configs/mono.toml
scripts/run_docker.sh --config configs/parallel.toml
scripts/run_docker.sh --config configs/cps.toml
```

CPS 的弹性调度字段位于 `[experiment]`：

```toml
max_parallel = 24           # 全局 agent 槽位
initial_agents_per_task = 2 # 每题的初始 agent 数
max_attempts_per_task = 0   # 0 = 直到 horizon；可设有限重试上限
cancel_on_proved = true     # 题目证明后取消同题仍运行的 agent
assignment_policy = "least_active"
```

`assignment_policy` 仅保留为底层初始 lease 顺序；初始池之后的新 slot 由
manifest 的 `[allocation].policy` 选择：

```toml
[allocation]
policy = "uniform"          # uniform | formula | agent
piece_limit_per_task = 3    # 每题最多暴露给 policy 的近期 CPS pieces
piece_body_chars = 1200
agent_timeout_seconds = 120 # 只约束中央 Agent scheduler 的单次判断
```

- `uniform`：不读取 progress，按题目清单做确定性 round-robin；
- `formula`：读取统一 snapshot，使用 `[allocation.formula]` 中冻结的纯算术权重；
- `agent`：读取同一 snapshot 与有界 CPS 文本，不接收任何公式或权重，只输出
  严格的 `task_id/reason/evidence_piece_ids` JSON。它没有 CPS 写接口；非法、超时
  或在推理期间变为过期的选择会记录并回退 round-robin。

三者都不抢占正在运行的 solver，只有已释放的 slot 才会重新分配。Agent scheduler
的 wall-clock 和 token 会计入实验成本。`final.json.score_time` 给出固定 horizon 的
normalized score-time AUC。`solver_slot_utilization` 只统计实际解题时间；
`compute_slot_utilization` 还会把释放 slot 上的 scheduler 判断时间计入计算占用。
`scripts/compare_runs.py` 同时报告两者，避免把调度占用误读为解题吞吐。

Issue #39 的 Figure 4 路径使用四个独立的新 policy 名，旧三臂及其产物语义保持
兼容：

- `uniform_refill`：选择当前 active lease 最少的 eligible 题，按 task ID 破平局；
- `task_state`：只读取 candidate/Checker、进展、饥饿与失败状态；
- `trace_state`：在完全相同的 Task-State 分数上只增加有界 trace projection；
- `llm_scheduler`：读取与 Trace-State 相同的只读 snapshot，严格校验 JSON，并把
  调度调用的 token、延迟和同一固定容量中的 reservation 计入成本。

开发用的 `configs/figure4_dev_cps48_*.toml` 四臂固定 MathOlympiadBench、12×4
初始池、180 秒 horizon 和同一参数合同，仅 policy、运行名称和输出目录不同。
其中 selector 明确保持 disabled，且 `experiment.figure4_phase = "development"`；
该哨兵身份如实记录 legacy refill 会继承题内 best candidate，因此
`candidate_transfer = true`。这批运行只用于实现验证，不能当作正式 Figure 4
结果。正式 repeats 必须标记 `figure4_phase = "formal"`；配置加载器会拒绝未启用、
身份不完整、允许 direct messages 或禁用 task-local candidate transfer 的 selector，
确保只能使用 Figure 3 冻结的同一 identity/config，同时固定 RQ3 的题内解答传递。
Trace-State 额外产生 `allocation_audit.jsonl`，从每个实际 dispatch 的同一个
immutable snapshot 计算不 dispatch 的 Task-State counterfactual。

正式六数据集矩阵位于 `configs/figure4_formal_6datasets/`：六个数据集、四个
allocator、三个 paired repeats（共 72 个 commit-bound leaves）。每个 repeat wave
同时运行 6×4=24 个 CPS arm，每个 arm 的 `max_parallel` 和
`aisw.max_in_flight` 都是 24；wave 结束后才进入下一 repeat。selector 固定为
远端 Figure 3 结果中的 `recency/icpc_formal_v1` identity。矩阵由
`scripts/run_figure4_formal_matrix.py` 调度，完成后用
`scripts/collect_figure4_formal_matrix.py` 逐数据集生成 paired artifacts 和
allocator selection。三重复规则是小样本工程验证，不能冒充八重复的论文统计结论。

矩阵 supervisor 的 state 文件是原子写入的；默认重启会先复用已通过严格 closeout
检查的 artifact，再对仍在固定 horizon 内且 PID、工作目录、manifest 和 output root
均匹配的 child 做安全 adoption；若 child 已写入 horizon 但 state heartbeat 尚未
落盘，重启还会用同样的身份约束从 procfs 发现并接管它。没有 horizon 的可信
pre-admission child 会先被定向 quarantine，避免恢复时产生重复 arm。provider 的
candidate-independent burst 只会停止
尚未进入 horizon 的 slot；重启后只补这些 slot 或已经死亡/无法 reconcile 的单个
slot，不会重启其余已进入 horizon 的 arm。若确实要忽略旧 supervisor state，可显式
传入 `--no-resume`；这不会把已有合格 artifact 当成新的结果。

每次尝试使用独立 workspace，完成后把较强 candidate 合并到
`workers/<task>/best/result.lean`；后续 agent 会先读取该文件和该题的 CPS
pieces/messages。Mono 和 Parallel 仍保持通信关闭、固定 baseline 语义。

Pi transport 设置由共享 `[pi]` / `[pi.retry]` manifest 明确控制，三种模式
不会隐式继承宿主机 `~/.pi`。默认 provider idle timeout 为 600 秒；一次
`agent_end` 只代表底层调用结束，runner 会继续等待 Pi 自动 retry/compaction，
直到收到 `agent_settled` 才结束该 agent。外层 experiment horizon 仍是硬截止。
`agent_finished` 同时记录 `settled`、最终 assistant outcome、
`transport_diagnostic` 和 `transport_recovered`；因此 WebSocket/SSE 等中间
诊断会保留供审计，但只有未恢复的终态错误才会触发 experiment-level
provider circuit breaker 或使 formal artifact 失格。

如果整个 Pi RPC/session 进程在收到 `agent_settled` 前异常退出，`[pi.recovery]`
提供 runner 级的有界恢复：默认在原 horizon 内重启一次，沿用同一个逻辑
`actor/task/episode`、workspace、candidate 和确定性的 Pi session，因此已有进度
可以继续使用。只有非超时、非主动取消的异常进程/调用失败才会进入这层恢复；Pi
任务超时（包括 inner Pi timeout）、runner 主动取消和正常 horizon closeout 都是该
逻辑 actor 的终态，不会重启或同 actor refill。CPS 仍可在释放 slot 后由调度器接纳
新的 assignment；这不是对已停止 actor 的 recovery。退避时间计入 horizon；Judge
返回的候选 verdict 也不会触发这层恢复。每次失败、安排重启、恢复成功或耗尽都会写入
`events.jsonl`，其中耗尽原因区分 `task_timeout`、`intentional_cancel`、`horizon`
、`runner_failure`、`remote_settlement_unconfirmed` 和异常/重试预算路径，便于区分
agent 进程故障与候选本身的 PE/WA/超时。

每个 worker 的实际 Pi settings 写入其私有 `.pi/settings.json`；每次调用的原始
session 进一步隔离在该 worker 的 `.pi/sessions/<session-id>/`，避免 CPS 高并发
反复扫描其他 session，也不为 Mono/Parallel 增加共享通信面。它们位于 run 目录
内，因此容器使用 `--rm` 后仍会保留；`pi_session_index.jsonl` 记录对应相对路径。
session 可能包含完整 prompt、工具输出和 provider 错误，仅用于本地诊断，公开
artifact 或运行摘要前必须审查，不能提交到 Git。

Scaling sweep manifests：

```text
configs/scale_1h_mono.toml
configs/scale_1h_parallel.toml
configs/scale_1h_cps24.toml
configs/scale_1h_cps48.toml
configs/scale_1h_cps96.toml
```

其中 Parallel 保持每题一个 baseline agent；CPS24/48/96 分别从每题 2/4/8
个 agent 起步，总槽位分别为 24/48/96。

隔离的新 Judge 上运行完整七组正式 sweep 时，使用 commit-bound 的 bridge
manifests：

```text
configs/formal_1h_mono.toml
configs/formal_1h_parallel.toml
configs/formal_1h_cps12.toml
configs/formal_1h_cps24.toml
configs/formal_1h_cps48.toml
configs/formal_1h_cps96.toml
configs/formal_1h_cps192.toml
```

它们统一固定 12 道题、同一模型/时限/四路 evaluator，并要求 Judge result cache
关闭。CPS12/24/48/96/192 分别从每题 1/2/4/8/16 个 solver 起步，完成后的空闲
slot 由 `allocation.policy = "uniform"` 继续分配给尚未证明的已有题目。

一小时 CPS48 allocation 对照使用下面三个 manifest；它们都是 12 题、每题初始
4 个 solver、总 48 slots，除 `allocation.policy` 与输出目录/名称外合同相同：

```text
configs/allocation_1h_cps48_uniform.toml
configs/allocation_1h_cps48_formula.toml
configs/allocation_1h_cps48_agent.toml
```

完成后可直接比较：

```bash
python3 scripts/compare_runs.py \
  runs/1h_allocation/uniform/<run-id> \
  runs/1h_allocation/formula/<run-id> \
  runs/1h_allocation/agent/<run-id>
```

正式实验前先运行单题、单 agent、单次尝试的 180 秒 controlled-Judge canary，
并让 fail-closed 审计确认至少发生一次真实 `judge_check`：

```bash
scripts/run_docker.sh --config configs/canary.toml
python3 scripts/audit_canary_closeout.py runs/canary/<run-id>
```

审计同时检查真实镜像 provenance、受控 Solver command/tool allowlist、Judge probe
provenance、broker drain、health、429/OOM/worker error，并报告是否观察到远端 job
DELETE；自然完成且没有取消 job 的 canary 不强制伪造 DELETE。完整的 3 分钟
Mono/Parallel/CPS 调试 manifests 仍保留在 `configs/3min_*.toml`。

`judge_broker_closeout.json` 是 solver phase 与 feedback-free closeout 全部结束后的
最终 lifecycle 证据；只有 `active_handlers = 0`、`fifo_depth = 0` 且
`remote_unsettled_jobs = 0` 才算 drain 成功。任何无法绑定到 job-id terminal receipt
的提交、取消或对账结果都会锁存为 `REMOTE_SETTLEMENT_UNCONFIRMED`，永久停止本 arm
的后续 Judge admission，并让正式 run fail closed。

3 分钟 horizon 只限制 solver 与 CPS 通信：到点后 runner 停止 Pi session、拒绝
新的 CPS 写入，并按各模式定义冻结每题一个候选。随后 Mono、Parallel、CPS
统一进入 feedback-free closeout；此阶段不再改变候选，也不把 Judge 结果反馈给
agent。这样，各模式在 horizon 收口时选中并冻结的候选，不会因为最终 Judge
排队或执行跨过截止点而漏分。

paper-facing manifest 统一将 `[lean].max_concurrent_evaluations` 设为 4；建议同时给
独立 Goedel-Prover Judge 配置至少 4 个 worker。该值应当始终和 Judge worker/
内存容量一起调整。`[lean].timeout_seconds`（默认 300 秒）是 Judge 单个后端
命令的执行预算，不是提交到终态的总 wall time：合法 job lifecycle 还可能包含
queue、冷 REPL header/body 以及 formal finalization。它也不是 solver horizon 或
整个 closeout 的总预算。`[lean].max_lifecycle_seconds`（paper manifest 为 3600）
是客户端防御畸形 receipt 的显式安全上界，不会缩短 Judge 正常公布的预算。

如果 AISW binary 或 node config 不在默认路径：

```bash
CONTEXTSWARM_NUROUTER_BINARY=/path/to/aisw-linux-x86_64 \
CONTEXTSWARM_AISW_NODE_CONFIG=/path/to/node.toml \
CONTEXTSWARM_CODEX_HOME=$HOME/.codex \
scripts/run_docker.sh --config configs/cps.toml
```

脚本按 manifest 选择 Docker 网络：默认 `host`；需要隔离宿主回环端口时使用
`bridge`，并把 runner 的服务地址设为 `host.docker.internal` 上显式开放的独立
端口。AISW binary、node config 和可选 Codex home 都只读挂载，随后所需私有
metadata 会复制到容器的临时 `/run`。容器使用宿主 UID/GID、只读根文件系统、
受限 PID、`no-new-privileges` 和独立 tmpfs，因此新 run artifacts 不再由 root
创建。若服务不在宿主网络上，应把 `CONTEXTSWARM_JUDGE_URL` 设置为容器实际可达的
地址，而不是修改 tracked manifest。实验代码、Prompt、manifest 与 benchmark 均使用
构建时写入镜像的冻结副本；运行时 PID 上限默认为 2048（可用
`CONTEXTSWARM_MINI_PIDS_LIMIT` 覆盖），这是为 CPS48 每个 Pi/NuRouter session 的
线程余量设置的有界上限，不改变 48 个 in-flight agent 合同。运行时只挂载宿主 `runs/`
输出目录，避免一小时实验中途受到 worktree 修改影响。

`run_docker.sh` 会同时发现相邻的 `.nurouter-pi-launcher.json`（或旧
`.aisw-pi-launcher.json`），并在容器内重写 `real_pi`/`real_codex` 到镜像内
二进制；不要手工只挂载 ELF，否则 NuRouter 的 owner-only launcher 校验会失败。

Fast-mode 使用单独 manifest，并且必须先通过 transport preflight：

```bash
scripts/run_docker.sh --config configs/cps_fast.toml preflight
```

## CPS 接口

CPS worker 没有通用 shell 或本地 `context_piece` CLI。它只能通过 runner 注入的受控
tools 使用 CPS：`cps_search`、`cps_publish`、`cps_inbox`、`cps_send`、`cps_ack`
和 `cps_actors`。这些调用都经过 session-bound loopback broker；agent 不能直接读取
SQLite、跨 workspace 浏览，或自行发起网络请求。

`communication = none` 的 Mono/Parallel workspace 不会创建共享数据库或 CPS
helper，避免 baseline 意外获得通信能力。下面的 formal helpers 则由同一 manifest
为 Mono、Parallel、CPS 等同启用，不构成 agent 间通信。CPS 的实现集中在
`contextswarm_mini/cps.py`，后续可以只替换 policy、ranking 或 digest，而不改
NuRouter/Pi transport。

## Formal helper surface

`[formal_tools].enabled = true` 时，每个 task workspace 都会得到完全相同的两个
有界 helper；Mono 使用 `tasks/<slug>/...` 下对应版本：

```text
python3 evaluate.py
./formal_query search <terms...>
./formal_query decl <terms...>
./formal_query check <name...>
./formal_query type <expression...>
./formal_query check --snippet '<small Lean snippet>'
./formal_query check --tactics '<declaration header>' [--tactic '<tactic>']
./formal_query axioms <helper-name>
./formal_query deps <terms...>
```

`evaluate.py` 把当前 `result.lean` 的不可变字节快照送到真实 Lean/Mathlib 环境，
只返回有界 diagnostics；即使返回 `PROVED`，它仍固定 `score = 0`、
`official_score_eligible = false`，不会选择候选或写入正式分数。`formal_query` 是
受限的 Lean API/LSP scout：`search/decl/deps` 只查公开 task 文件和 revision-bound
声明索引；`check/type/axioms` 及 snippet/tactic portfolio 通过同一受控 kernel
capability elaboration。`deps` 表示相关候选 premises，不声称提供完整依赖图。

四类 quota（evaluate calls/backend jobs、query calls/backend probes）都由
run-global JudgeBroker 以 task 为键统计，所有 CPS agent/episode 共享同一公平预算；
精确 cache hit 不消耗新的 backend quota。helper 不能读取 raw endpoint/token，Bash
guard 只允许上述精确命令，并拒绝 pipe、redirect、substitution、glob、后台任务和
任意 Python。任何无法确认远端终态的调用都会保留 semaphore permit、熔断该 run
的后续 admission，并返回 `REMOTE_SETTLEMENT_UNCONFIRMED`。

正式 preflight 先把 operator index 流式复制成 run-private、content-addressed、0400
snapshot，再由 preflight 和 Broker 共同使用这一份 snapshot。SHA/schema 或
configured/index/endpoint revision 任一缺失或不一致都会 fail closed。最终候选仍
按 Mono/Parallel/CPS 原有策略在 horizon 后冻结；helper 反馈不会改变 freeze source，
正式分数只来自 broker revoke+drain 之后、对冻结字节发起的独立 fresh closeout。
当 cache-health 使用独立 endpoint 时，preflight 还要求它和实际执行 Judge
公布同一个稳定 deployment identity；没有 identity 或 identity 不匹配会拒绝运行，
避免把旁路 backend 的 cache 状态当成执行 backend 的证据。

## Lean evaluator contract

`contextswarm_mini/evaluator.py` 使用 ContextSwarmJudge 的公开 Lean router：

```text
GET  /healthz
POST /api/lean/jobs
GET  /api/lean/jobs/<job_id>?wait_ms=1000
DELETE /api/lean/jobs/<job_id>  # 客户端放弃未收口 job 时取消并对账
```

提交字段包括 candidate `code`、baseline `target_code`、`problem_id`、`lean_env_id`
和 `verification_profile`。客户端优先采用 Judge receipt 公布的 whole-job
`lifecycle_deadline_ms`；兼容旧 Judge 时，会根据 receipt 的 queue deadline 和
formal pipeline 上界保守推导。只有整个合法 lifecycle 加 terminal settlement
窗口都结束后，才会请求取消并再次有界对账；畸形或超过客户端安全上界的
lifecycle receipt 会 fail closed，不会造成无限轮询。客户端主动取消不会被记成普通
`CANCELLED` 零分，而会标记为 degraded evaluator timeout。`final.json` 不会把
`queued` / `running` 当作最终 verdict；无法确认终态会明确记录为
`EVALUATOR_TIMEOUT`；若客户端无法证明远端 job 已终止，则使用更强的
`REMOTE_SETTLEMENT_UNCONFIRMED` 并停止后续 admission。Judge 明确返回的
pre-admission overload 会在 30 秒
admission budget 内有界重试；已经排队后才返回的 terminal、retryable
`rejected_overloaded` 至多重交一次 whole job。结果不明的 socket/proxy 失败不会
盲目重交，以免复制仍在运行的 job。Judge 的 `error_kind`、
`terminal_reason`、queue/execution timing 会保留在安全摘要中，以区分证明错误、
执行超时、资源限制、过载和基础设施故障。只有 canonical `PROVED` / `AC`
verdict 计入分数。

## 数据来源

`benchmarks/catalog.json` 将六套题目统一固定到上游 `ContextSwarm` 的同一 revision；每套目录都包含 `manifest.json`、`problem_ids.json` 和上游 `benchmark_integrity.json`。Formal 题库迁移公开 `problem.md`、`metadata.json` 与 `baseline/*.lean`；ICPC worker bundle 包含完整公开题面与统一的中性 C++ skeleton，绝不迁移公开 AC reference；USACO 只迁移公开 metadata projection 和 resident test contract，不包含隐藏测试。

这次完整性修订使用新 task id 隔离语义变化：MathOlympiadBench 的 `imo2023_p2_v2` 取代 `imo2023_p2`，Clever 的四个修订题也使用 `_v2` id；旧结果不得与新 contract 合并。各题库的详细来源、修复分类和 hash 见对应 `PROVENANCE.md`（若有）与 `benchmark_integrity.json`。

原仓库的生产 evaluator、隐藏测试、oracle、解答和 Judge-side package 均未复制。默认 Math 运行时继续使用本仓库的精简 HTTP adapter，不会绕过 Judge 或改变 theorem contract。题库布局与维护同步命令见 `benchmarks/README.md`。
