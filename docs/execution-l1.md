# #10–#11：隔离 L1 异步执行与运行监督

本阶段使用真实 PostgreSQL Outbox、Redis transport 和 Linux Celery prefork worker；Engine 是独立 HTTP + SQLite 故障夹具。只验证异步控制，不连接 SeaTunnel，不执行 Doris 写入、数据搬运、正式发布或清理；不能记为 L2。普通 Compose 不启动 publisher/worker，默认执行关闭。即使手动调用异步入口，未显式配置 `EXECUTION_ADAPTER=l1` 时也会拒绝启动并保留 pending Outbox。

## 运行边界

`backend/app/outbox_publisher.py` 独立轮询 pending Outbox；每次尝试先在 PostgreSQL 记录投递证据，再向 Redis 发布 JSON `command_id`，最后记录 sent。发布前故障保留 pending；Redis 成功、sent 提交前故障会重复发布。没有 Beat，没有结果后端；Redis task ID 不承担去重。

`backend/app/celery_app.py` 使用晚 ACK、worker loss 拒绝、prefetch=1；子进程初始化时 `engine.dispose(close=False)`，不继承父进程连接池。worker 仅挂载部署资源策略并持有可信公钥与托管凭据解密密钥，不持有 Ed25519 私钥。签名核验使用 #9 既有 domain-separated ManagedCommand 格式，不安装 Celery auth extra。任务显式对已失效数据库连接、无服务端 SQLSTATE 的 psycopg OperationalError 连接/传输失败、SQLSTATE 08 类连接异常，以及 40001/40P01/55P03/57P01/57P02/57P03 的事务竞争与服务短暂不可用错误每 10 秒重试同一 command；其他数据库约束、数据及编程错误继续失败，不使用宽泛异常重试。晚 ACK 本身不保证普通失败重新入队，未更改 Celery 的 ACK-on-failure 配置。

`backend/app/execution_runtime.py` 的 `drive(command_id)` 是执行入口。每个 command 的 PostgreSQL advisory lock 跨越事务与外部 I/O，竞争者稍后重投；业务与证据变更沿用 tenant-first 锁顺序。命令的签名、范围、主体、冻结 manifest、预算、操作、工具与环境必须匹配已接受事实。首次提交核验当前操作员、审批人与职责、部署资源指纹、真实只读 schema/grants；已接受命令不重新消费 Capability，也不套用五分钟 Capability 窗口或 Preparation 到期时间。`metadata.read` 仅记录实时只读检查完成，永不创建 EngineAttempt 或调用提交。

物理目标身份由部署策略提供：`physical_cluster_id + database + table`。L1 的 Doris 策略必须显式配置稳定 `physical_cluster_id`；同一物理集群的资源别名必须使用同一值。身份不包含租户、项目、连接 ID、资源 ID 或策略指纹，也不从不可靠的 DNS/主机字符串推断。

首次提交在独立已提交事务中保存 TargetOwnership、唯一 EngineAttempt、correlation_id、原始请求、请求摘要、原始 Engine URL 与 submitting 阶段。随后在 tenant lock 下重新读取资源策略、核验当前权利和真实只读 schema/grants，并保持该锁至有界外部调用与结果写入完成。完成的撤权不能插入核验与提交之间。两次当前资格→本地冻结校验→受管资源检查共用私有 helper，仍分别位于原先的两个事务内，未合并或移出 tenant lock。

一旦 submitting 已提交，后续投递只能按原 correlation 在原 Engine 发现。没有 job ID、进程死亡、超时、找不到唯一原作业、请求摘要不符、Engine 配置变化都会保留 SubmissionUnknown 与目标占有，不得重新 POST、释放租约、发布或清理。EngineAttempt 的关联和配置不改变；成功发现只关联原 job ID。L1 尚无终止/质量发布契约，因此已提交与未知目标均持续占有，不提供自动释放接口。

## 独立启动

需要普通验收使用的只读合成 MySQL/Doris 夹具已经在 `13306/19030` 可用；这些外部 fixture 仅用于 #8 的真实只读 schema/grants，不会被 L1 启停或写入。此处不增加第二份重型 Doris。

```powershell
# 从本 worktree/repo 根目录执行。镜像 target 与普通部署相同，但标签独立。
docker build -f infra/aliyun/runtime.Dockerfile --target api -t etl-agent-l1-api:issue10 .
docker build -f infra/aliyun/runtime.Dockerfile --target web -t etl-agent-l1-web:issue10 .
python scripts/init_l1.py --connector-secrets <普通验收的私有secrets.json>
docker compose --env-file .runtime/l1/deployment.env -f compose.l1.yaml up -d postgres redis api engine
docker compose --env-file .runtime/l1/deployment.env -f compose.l1.yaml exec -T api python -m app.bootstrap --tenant demo --project demo
python scripts/seed_l1.py --connector-secrets <普通验收的私有secrets.json>
# 全新隔离 L1 上，先跑实际故障脚本；脚本最后启动常驻 worker/publisher/web。
python scripts/verify_execution_faults.py
python scripts/verify_execution_duplicates.py
python scripts/verify_execution_drift.py
python scripts/verify_execution_recheck.py
```

镜像构建遇到本机 DNS 超时，本次使用 `--add-host mirrors.aliyun.com:124.95.190.69` 的单次构建参数恢复解析；TLS 与固定依赖不变，未修改机器 DNS 配置。该 IP 不作为长期部署默认值。

L1 独立 Compose project `etl-agent-l1` 使用独立 PostgreSQL/Redis/SQLite volume、新生成的签名/加密密钥与独立身份。API `http://127.0.0.1:8010`、web `http://127.0.0.1:8090`、本机故障夹具只读账本 `http://127.0.0.1:8011/ledger`。私有 bootstrap 凭据在忽略的 `.runtime/l1/bootstrap-demo.json`，不输出到验收报告。

## 真实故障验收

`scripts/verify_execution_faults.py` 通过公开 Prepare → 双独立具名批准 → Capability → Commit 创建命令；私有脚本仅负责进程故障、坏签名/绑定夹具、读取独立 Engine 账本和观察 Redis unacked。生产路由没有故障控制接口。

- 停止真实 Redis，publisher 失败后 pending 保留；恢复 Redis 后再发布。
- 私有 publisher 包装在真实 Redis publish 后、PG sent 前退出，重复投递由 PostgreSQL command/Attempt 去重。
- 无 consumer 等待真实超过 300 秒，再执行原已接受 metadata.read；当前撤权或签名/env/scope/manifest 不匹配仍拒绝新提交。
- Engine SQLite 已创建 job、响应尚未返回时 KILL 整个 worker。恢复仅发现原关联；暂时隐藏发现结果时保留 unknown 和目标占有，跨项目/资源别名的同一物理目标被排除，其他物理表仍可前进。
- prefork task_postrun 私有屏障阻止 ACK，先观察 Redis unacked 中真实 task ID，再 KILL 整个 worker；等待 Redis visibility timeout 自行重投同 task ID，不以人工重发冒充 ACK 丢失。
- 多条并发重复消息不增加 Engine job；独立 Engine 重启后 SQLite 账本仍在，发现关联同一个原始 job。P2 请求使用 #8 冻结后的有效 10000 行预算。
- `scripts/verify_execution_drift.py` 在公开接受后仅改变 L1 策略副本，真实 worker 在 Attempt 产生前拒绝资源指纹漂移，并恢复策略。

`scripts/verify_execution_database_retry.py` 是追加的数据库恢复回归。必须从持有 `.runtime/l1` 的根 Compose 工作区运行；若代码位于 worktree，脚本自动以自身 `scripts` 目录挂载 `/tests`，无需把 helper 放进生产应用。它仅克隆一次性 PostgreSQL Schema、使用唯一 Redis 队列、public-key-only 子 worker 和独立临时 SQLite Engine；短暂停止/恢复真实 L1 PostgreSQL，再观察同一已接受且 Outbox=sent 的 metadata Run 自动完成。第二个场景通过克隆 Schema 中真实延迟触发器，在外部 POST 后的结果提交抛出 40001；持久 Attempt、原请求摘要/关联与占有仍在，重试只发现原单作业，Capability 消费仅一次。触发器、进程、队列及 Schema 随测试销毁；两个保留的 L1 原作业不增加。命令示例：

```powershell
python .worktrees/review-fixes/scripts/verify_execution_database_retry.py --output .worktrees/review-fixes/artifacts/issue-10/review-database-green.json
```

旧实现两场景分别停在 queued/submitting；修复后同 task ID 的 10 秒重试分别到达 metadata_checked/submitted。红绿报告为 `artifacts/issue-10/review-database-red.json` 和 `review-database-green.json`。最终镜像重建后的公开只读复核为 `review-restart-recheck.json`；可用 `scripts/verify_execution_recheck.py --runtime <根私有运行目录> --output <本分支报告路径>` 输出到 worktree，不改根证据。

本次真实验收中，已接受 metadata.read 的持久化接受时间至执行证据时间为 308.446 秒；真实未 ACK 的 task `9ae87326-1272-4460-a76b-1da44d1208b2` 在整个原 worker 被 KILL 后由 Redis 重投，原作业未增加。另八个并发重复 task 全部观察到消费完成后，仍只有一个原作业。整组场景最终仅有两条 Engine 作业，API/worker/Engine 重新创建后原关联、目标占有和被阻塞别名保持不变。

验收依赖 celery[redis]==5.6.2、kombu==5.6.2、redis==6.2.0；`artifacts/issue-10/installed-runtime.json` 记录实际镜像依赖与 pip check。`artifacts/issue-10/real-l1.json`、`consumed-duplicates.json`、`resource-drift.json`、`restart-recheck.json` 与 `lost-ack-worker-log.txt` 分别保留真实故障、已消费重复、策略漂移、重建后只读复核和丢 ACK 日志。依赖 dry-run 仅作解析证据，不替代真实执行。普通 GET/list 只查询持久化事实，不推进状态。

## #11：冻结监督契约与持久预算闭锁

`backend/app/runtime_supervision.py` 的 `RuntimeSupervisionContract` 冻结默认 100000 条源读取记录、268435456 byte（256 MiB）sink 实测累计接受写入字节、600 秒、2 倍输出放大和 0.2 拒绝率。版本 facts/review.conf、Preparation、Capability/ManagedCommand 和运行监督表均绑定可审查契约；P2 使用准备单收紧后的预算。只读 metadata.read 不创建数据面监督。新增契约会改变 execution facts，旧版本须重新评估冻结后再准备；不会替换旧制品。

计数边界为连接器发出的源记录、映射后按稳定键去重的质量判定（evaluated=valid+rejected，拒绝按记录一次）、质量和展开后的 sink 接收记录；放大比为 final_output/source_read，吞吐为 final_output/elapsed_seconds。字节只接受 `byte_kind=measured`，不接受估算、网络或扫描字节。零分母显示 `null`（中文页面“暂无数据”）。运行中只有拒绝率享受 evaluated<100 的观察期；最终小样本仍检查，其他预算没有豁免。空最终结果默认闭锁。计划显式批准 `allow_empty=true` 时，许可随版本、审批与监督合同冻结；只有终态完整观测、原始实测字节和全部精确零计数才允许空快照通过监督与发布门禁。该许可也允许满足原门禁的非空快照；缺计数、未知字节或许可不一致仍闭锁。

Worker 在 `submitted` 后每约 2 秒继续 `drive`；观测超过 6 秒、缺失、未经实测、计数阶段不一致、来源/epoch 变化或累计回退均闭锁并清空当前可信显示值，历史原始观测保留在 `runtime_observations`。除 Engine 累计时长外，还以 Attempt 持久开始时间核验平台墙钟预算。`runtime_supervisions` 在接受事务创建，保存冻结契约、当前指标、监控状态、来源、原因与发布 veto；`supervision.observed` 和 `supervision.stop_requested` 控制证据同事务落库。只有最终可靠指标全部通过才解除监督 veto；这不是正式发布授权或真实发布。

异常先提交 `stop_request`（稳定 id、检测/请求时间、原因、pending 状态和 `termination_confirmed=false`），再对原 Engine/原作业调用幂等 `POST /jobs/{id}/stop`。响应未知每 2 秒重送同一请求；acknowledged 仅表示 Engine 接收了请求。停止闭锁不可由后续观测解除；目标占有保留。物理资源限制和监督预算不同，轮询、网络及引擎停止延迟可能造成超调，本次未验证真实 SeaTunnel/L2 或零超调。

#12 接口：`backend/app/supervision_runtime.py` 的 `request_stop(db, run, row, reasons, now)` 可在现有 command advisory lock/tenant-first 事务内复用于撤权；`RuntimeSupervision.stop_request` 与 `RuntimeObservation` 保留检测时的指标与请求证据。`supervise_locked(command_id)` 完成请求投递后返回 `stop_requested`，后续由 #12 接终止轮询/确认、最终指标与超调核算、资源清理和 TargetOwnership 释放。本票不把 stop ACK 当终止，不清理资源。

`scripts/verify_supervision_l1.py` 复用 `isolated_api_fixture.py`：克隆真实 PG 至一次性 Schema、唯一 Redis 队列、公钥 Worker 和独立临时 HTTP+SQLite Engine。按同一公开探查/版本冻结 → Prepare → 双审批 → Capability → Commit → GET runs 链路验证 12 个场景：99/100、最终小样本、行/字节/时长/放大预算、缺失、陈旧、重置、估算字节、零分母与可靠最终完成。根容器及运行目录未改动。报告 `artifacts/issue-11/l1.json`；运行示例（在本工作树执行，把 `$baseline` 指向既有部署根目录）：

```powershell
$baseline = 'E:\课件\23-实战\生产级 ETL-Agent 数据集成平台'
$worktree = (Get-Location).Path
docker compose --project-directory $baseline --env-file "$baseline/.runtime/l1/deployment.env" -f "$baseline/compose.l1.yaml" run --rm --no-deps -T -e PYTHONPATH=/app:/tests -v "${worktree}/backend/app:/app/app:ro" -v "${worktree}/scripts:/tests:ro" -v "${baseline}/.runtime/l1:/run/identity:ro" -v "${worktree}/artifacts/issue-11:/results" api python /tests/verify_supervision_l1.py /results/l1.json
$env:PYTHONPATH = 'backend'
python scripts/test_runtime_supervision.py
npm ci --prefix frontend --ignore-scripts --no-audit --no-fund
npm run build --prefix frontend
```

`verify_supervision_browser.cjs` 用实际生产 Vue bundle 回放上述已持久化 L1 HTTP 事实，验证中文停止/闭锁/原因、缺失与零分母展示；这是独立 UI smoke，不替代前述真实 API/Worker 验证。可在本工作树安装忽略目录的浏览器驱动后运行：`npm install --prefix .runtime/browser --no-package-lock --no-save --ignore-scripts playwright@1.56.1`，`node scripts/verify_supervision_browser.cjs`（使用本机 Edge）。报告和截图位于 `artifacts/issue-11/browser.json`、`budget-stop.png` 与 `missing-metrics.png`。

## #12：受管停止、终止确认与未发布对象清理

`backend/app/run_control.py` 是单一深模块，入口只有 `request_control`、`advance_control(command_id)` 与 `control_view`。检测、停止请求、投递、ACK、终止确认、最终指标与清理结果各自独立落库，不使用单一宽泛状态推导：`run_controls` 保存租约、在途调用、终止依据、最终指标与超调，`managed_control_effects` 为每次受管停止/清理保存独立签名信封、幂等键、请求摘要与阶段。停止闭锁复用 #11 的 `request_stop`，ACK 只表示 Engine 收到请求，不表示终止；最终指标独立于终止确认计算，某指标缺失时该分项超调为 `null`，不用零代替。

公开受管控制路由只有 `POST /api/tenants/{tenant_id}/projects/{project_id}/runs/{run_id}/{stop|cleanup}`（仅 stop/cleanup，其他 kind 为 404）。Operator 具名请求携带 `idempotency_key` 与 `reason`；同键同摘要返回既有效果，同键不同摘要 409。租约用 CAS 而非外部 fencing：过期租约记录 `control.lease_reconciled`（保留目标占有）后再取新租约，在途调用永不重放，只按持久效果 ID 回读核对。清理要求当前 Operator 职责、原绑定凭据可解密、终止已由 Engine 确认且无在途调用、最终指标已知，并按签名信封中的固定对象范围核对 TargetOwnership 后才释放；任一不符即 `unknown` 并保留占有。

Worker 循环由 `backend/app/outbox_publisher.py` 的持久唤醒驱动：publish_once 分别查询执行 Outbox / `run_controls.wake_at`、`reconciliation_wake_at` 和发布意图 `recovery_at`，分别预约与派发，避免同一 command 的恢复任务被执行任务覆盖。各任务在 broker 调用前提交十五秒恢复期限，丢失 ACK 后仍可按原意图恢复；使用与 driver 不同的 advisory lock 域。撤销 Operator/Maker 职责或原绑定凭据不可用时，排队命令在提交前被拒且不产生 Engine 作业；运行中命令闭锁受管停止并禁止发布。

`scripts/verify_run_control_l1.py` 验证公开 Operator 停止、ACK 不等于终止、延迟最终指标、真实超调与独立受管清理；`scripts/verify_run_control_recovery_l1.py` 验证撤销职责/原绑定凭据不可用时排队命令被拒且无 Engine 作业、运行中撤销闭锁派生停止、丢失停止回执后保持 `unknown` 并保留占有、过期租约只核对不重放、丢失清理回执同样在核对后才释放。#11 的 `scripts/verify_supervision_l1.py`（12 个公开监督场景）在本代码上回归通过：预算闭锁与持久否决不变，且预算达标的运行只有在 Engine 确认终止后才进入 `supervision_passed`；为此隔离 Engine 增加“当前观测已报告 finished 的作业自行终止”的能力（`l1_engine_fixture.py` 的 `current_state`），不再要求先有停止请求才能终止。三者都只动一次性 Schema、唯一 Redis 队列与临时 SQLite Engine，根容器与运行目录未改动。运行示例（在本工作树执行，`$baseline` 指向既有部署根目录）：

```powershell
$baseline = 'E:\课件\23-实战\生产级 ETL-Agent 数据集成平台'
$worktree = (Get-Location).Path
$mounts = @(
  "--project-directory", $baseline, "--env-file", "$baseline/.runtime/l1/deployment.env",
  "-f", "$baseline/compose.l1.yaml", "run", "--rm", "--no-deps", "-T", "-e", "PYTHONPATH=/app:/tests",
  "-v", "${worktree}/backend/app:/app/app:ro", "-v", "${worktree}/scripts:/tests:ro",
  "-v", "${baseline}/.runtime/l1:/run/identity:ro", "-v", "${worktree}/artifacts/issue-12:/results")
docker compose @mounts api python /tests/verify_run_control_l1.py /results/l1.json
docker compose @mounts api python /tests/verify_run_control_recovery_l1.py /results/recovery-l1.json
```

报告为 `artifacts/issue-12/l1.json` 与 `artifacts/issue-12/recovery-l1.json`，均显式记录 `root_deployment_untouched`、`worker_private_signing_key` 与 `zero_overshoot_or_real_etl_or_rollback_verified`。本阶段未连接 SeaTunnel，不执行真实 Doris 写入、数据搬运、正式发布或回退；取消不等于回退，也没有可恢复暂停。真实 L2 的字节来源与作业关联由 #13 单独验收。

## #13–17：当前实现与证据边界（2026-11-15）

这些工单尚未完成。真实执行只有在冻结合同、原凭据、实测运行时和当前独占网络证明全部成立时才准入；缺少部署证明时仍在创建 EngineAttempt 和目标占有之前闭锁。隔离验收环境已通过公开审批、Capability/Commit 和 Celery 提交真实 SeaTunnel 作业并取得 Doris 原始回执，但完整受管 L2 尚未通过。父工单 #1 保持打开。

- 独立 writer 槽与 probe_readonly 分离。真实版本、Prepare、审批和 Commit 冻结 writer 的引用、版本及部署资源摘要；worker 使用原绑定解密。只读元数据探针仍拒绝 LOAD 权限。writer 默认仅接受原批准库内的 SELECT / LOAD / CREATE。清理或发布分别需要部署明确配置同库 Drop_priv / Alter_priv 并再次验证实际 grant；未配置的额外权限、全局权限和其他业务库权限仍拒绝，代码不自动授予权限。
- SeaTunnel 固定 2.3.12 / aa70e2d；Doris 实测版本为 3.0.8-rc01 / 09b0cc49a60ffdd444df3e40e5f3dc180299b561。预选 Long 作业 ID 在外发前持久，唯一一次 POST 后仅按原 ID/name/status GET 核对。未知 ID 回显 jobId 不构成关联证明，重复 ID 的协议返回也不构成服务端幂等保证。
- [受管写入 journal](../backend/app/managed_load.py) 在外发前提交批次意图，已发写入只有原始成功回执提供实测 LoadBytes。未结或未知写入不补零；观察到缺口、超过六秒未监督均永久关闭准入，迟到成功回执不能消除否决。两秒观察屏障与进行中写入相撞同样闭锁。
- [内部 ingress](../backend/app/real_ingress.py) 对稳定源主键去重，把质量判定和单行写入回执逐一关联；token、业务原文和任意异常文本不进入持久事实。隔离验收已部署 ingress 和唯一出口网络，当前容器身份、完整网络端点、内部网络拓扑、防火墙及 UTF-8 配置由受信主机见证持续核验，见证超过两秒失效。[监督组合器](../backend/app/real_observation.py) 只使用原作业 SourceReceivedCount 与完整 journal；HTTP sink 的 SinkWriteCount 不作为 Doris 交付成功计数。首次提交预约以 PG 时钟持久固定六秒启动截止；首次可信样本前，原 ID 暂不可见或缺少 SourceReceivedCount 仅安排两秒后只读核对，不开启 journal 准入窗口、不补零。截止、停止、撤权和预算超限仍闭锁；重复投递或重启不能延长截止。准入还要求原命令的可信组合样本已经提交，journal 完整观测本身不能开放写入；两者的原始采样时间均须满足 200ms 窗口。本轮真实质量场景已验证启动后完整写入。完整授权预检移到等待窗口前，聚合后以 PG 当前时间检查原窗口；发送前及 TCP 连接后的实时授权仍保留。早到请求的 2500ms 等待短于六秒启动截止，以及样本提交耗尽 200ms 的活性边界仍未消除；旧冻结合同不静默改变。
- Operator 主动核对具名签名意图、原作业只读 GET、真实 PG 故障恢复和中文页面已有实现；它不证明外部旧写静止、不释放占有。质量报告及中文事实区已有实现；本轮真实质量场景已核对 SeaTunnel → Doris 有效与拒绝两路，更多质量边界仍须按 #15 逐项验收。
- 发布已接通已有正式表的 P1 双审批合同和公开发布入口，冻结正式表物理身份，原 Run 通过独立质量、监督和静止门禁后才能持久预约一次交换。真实 PG 验证已覆盖完整授权正向发布、交换响应丢失和结果提交失败后只读核对，旧快照至少保留七天；该 PG fixture 的 Doris Adapter 是测试 seam；另有本轮真实交换正向证据，故障注入的真实 Doris 交换恢复仍待验收。首次正式表创建和回退不由该发布入口自动执行。
- 凭据轮转已通过公开 Operator P1 双审批、Capability/Commit、PG 原子回滚及真实 Celery L1 运行绑定验证：旧引用撤销触发原作业停止和发布否决，原凭据不可用时拒绝清理并保留目标占有。本轮真实轮转已确认旧引用撤销、新版本绑定和旧 preparation 失效。原作业在首次停止意图之前已自然 FINISHED；一个写入批次没有原回执，保持 unknown、发布否决、准入关闭和占有保留。该事实证明隔离保护，不能证明写入结算，也不能要求已经终止的原作业再变为 CANCELED。

真实协议证据分别为 [预选 ID](../artifacts/issue-13/preselected-job-id.json)、[对象与单次写入](../artifacts/issue-13/doris-object-transport-after-fix.json) 和 [HTTP sink 实测](../artifacts/issue-13/http-ingress-protocol-8243affe6ea5.json)。单次 Doris 回执证明两行 / 51 字节；它不证明持续完整计量或独占出口。HTTP 实测中接收端全回 503，任务仍 FINISHED，说明必须独立核对 Doris 回执。首次 [transport 未知结果](../artifacts/issue-13/doris-object-transport.json) 保持 unknown，原批次未重送，字节仍未知。

首次公开真实链路的 [失败报告](../artifacts/issue-13/real-managed-l2-0784dc239a1a.json) 记录了真实原始回执，但任务路由异常阻断监督，且 JVM 默认字符集损坏了非 ASCII 数据，因此明确不验收。任务路由已修复；后续引擎必须显式使用并实测 UTF-8，冻结传输合同和当前容器见证均校验该条件。旧运行保留原证据，不重送原批次，也不根据后续成功探针回填旧字节。

后续 [准入锁竞争报告](../artifacts/issue-13/real-managed-l2-507673fd7c62.json) 中，原引擎 FINISHED、源计数为三，但仅两条 Doris 原始成功回执。第三条 HTTP 请求因观察线程持有屏障锁返回 503；连接器吞掉该失败，平台因此保留 final_metrics_pending、发布否决和原目标占有。修复只允许尚未判定或发送的已认证 POST 在屏障外最多等待 2500ms，等待由真实完整观察开启的 200ms 准入窗口；不会自行刷新观察时间。Doris 单次传输仍限 1500ms，监督仍为每两秒观察、六秒陈旧闭锁；观察与在途写入相撞仍立即闭锁。超时持久关闭准入，未授权请求不能关闭他人运行。

实际数据面 CREATE、DROP 和单次 swap 均在 SQL 连接与权限检查结束后、紧邻变更语句前重新检查当前证明；每次 PUT 在连接建立后也执行原绑定及网络新鲜度检查，仍计入原传输时限。[验收库生命周期授权记录](../artifacts/issue-13/l2-lifecycle-grant.json) 仅增加现有合成库 writer 的 Alter_priv / Drop_priv，原只读探针授权未变；这份部署记录本身不代表运行或发布验收通过。

新 [启动修复后真实质量报告](../artifacts/issue-13/real-managed-l2-f38407ae3b15.json) 已恢复原作业并产生可信样本，但仍因 `ingress_admission_timeout` 关闭准入；三条源记录没有进入质量判定或 Doris 批次。原样本时钟为 17:44:25.318329 UTC，监督记录时钟为 25.379637，200ms 准入窗口剩余最多 138.692ms；日志没有分阶段计时，尚不能区分等待查询与授权复验各自耗时。本报告不计通过，目标占有和隔离 PG 证据保留；引擎 FINISHED 不能弥补零交付。

准入性能修复后的 [真实质量分流报告](../artifacts/issue-13/real-managed-l2-bf48f83b1f33.json) 完整核对三条源记录、两条有效记录、一条 FILTERED 拒绝记录，三份 Doris 原始成功回执合计 435 字节。清洗结果与错误归属均相等；最终拒绝率 1/3 超过冻结的 0.2，因此发布仍否决，场景验收通过不等于完整 L2 验收。[提交响应丢失报告](../artifacts/issue-13/real-managed-l2-7dcce81bbbf6.json) 实测上游仅一次提交、丢一次响应，后续 19 次原 ID GET 恢复原作业；三条中文记录与源相等，实测 116 字节，通过最终监督，未发布且占有保留。

[公开取消与清理报告](../artifacts/issue-13/real-managed-l2-39138bab1c00.json) 确认原作业 CANCELED，之后经公开清理入口删除原两张影子表、释放占有，并保留 schema 占位表。[真实发布报告](../artifacts/issue-13/real-managed-l2-a99ce2fd90b6.json) 核对三行 / 116 字节、质量及监督通过、公开发布 intent confirmed、正式表与影子表物理身份互换，正式数据与源相等，旧空快照保留 604800 秒。此正向场景不覆盖实际交换响应丢失或 PG 确认失败。[真实轮转报告](../artifacts/issue-13/real-managed-l2-c54dc8879e06.json) 记录原绑定撤销、新绑定生效和旧 preparation 失效，但原作业最终 FINISHED、写入回执 unknown，保持发布否决和 owner；原报告按旧 runner 的 CANCELED 判定未通过。只读时序复核确认：18:05:21.015959 UTC 已预留该批次，21.243248 撤销旧引用，21.574 原作业 FINISHED，23.043734 才首次持久停止意图；缺少传输阶段证据，不能断言该批次从未发出。不改写历史报告、不重送原批次。新 [轮转安全隔离报告](../artifacts/issue-13/real-managed-l2-8b969a6e52d6.json) 通过公开 P1 双审批轮转，在原 run submitted 时撤销旧引用；旧 preparation 失效，新 preparation 绑定新引用，原签名 writer 未改变。原作业 FINISHED，两路 LoadRun 与 IngressBinding 均持久关闭，发布否决、owner 保留且旧绑定 cleanup 被拒；没有产生批次，报告范围仅为 safety_isolation_only，未证明运行中 STOP 导致 CANCELED。已提交 run 在撤权后重复 drive 不再倒退为 submission_unknown；仅观察到终态而 confirmed 的 effect 不再虚构 STOP 发送账本，真实发送预留仍禁止重发。所有单场景报告仍显式声明完整 managed_l2_accepted=false。

历史 [real-engine-l2.json](../artifacts/issue-13/real-engine-l2.json) 的 level=L2 命名过宽，仅保留其协议探针历史，不计入当前受管 L2 验收。当前探针脚本与新报告显式声明 managed_l2_accepted=false 或 real_l2_qualified=false。

本轮通过的控制面检查包括独立 writer 与迁移、PG journal、PG ingress、质量分流与原回执关联、主动核对、发布恢复、轮转原子性、固定六秒启动与两秒持久唤醒、可信样本提交前禁止入场、显式空发布许可、基线冻结摘要兼容和整数过滤精度，以及 [公开停止回归](../artifacts/issue-13/regression-control-l1.json) 和 [轮转原引用 L1](../artifacts/issue-13/rotation-bound-l1.json)。PG fixture 的 Doris/SeaTunnel seam 是注入适配器，不能替代真实引擎验收；L1 使用真实 API/PG/Redis/Celery，但数据引擎是隔离 HTTP fixture。
