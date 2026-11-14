# #10：隔离 L1 异步执行控制

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
