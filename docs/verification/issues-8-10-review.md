# #8–#10 集成审查与验收

本批仅实现 #8 / V07、#9 / V08、#10 / V09；不把隔离 L1 故障 Engine 的验证算作真实 ETL 或 L2 验收。遵循维护者要求：不增加防御性分支，只保留证明功能和回归缺陷所需的验证。

审查基线：`c4f123b1de671a7aab42162acac181d7d3392db0`。审查快照：`a64be0e3854070e4065ebdae3ab85aaa3c94a2c2`，使用两者的三点 diff，58 个文件，2919 行增加、7 行删除。Standards 与 Spec 由两个独立只读代理并行审阅；修复统一交给一个实现代理。

## Standards

**硬性违规：0 项。** 未发现能依据指定仓库文档明确认定的违规，或与 ADR 0001–0003 的直接冲突。

**启发式判断：3 项，均非硬规则。**

1. **可能的 Duplicated Code**：[execution_runtime.py:121](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/backend/app/execution_runtime.py#L121) 与 [同文件:185](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/backend/app/execution_runtime.py#L185) 重复 `require_current_role(...)` → `validate_preparation(..., check_expiry=False)` → `check_execution_resources(...)` 的校验链。安全条件变更需要同步两处，容易遗漏。建议提取共用步骤，保留两次调用及各自事务、锁边界。
2. **可能的 Duplicated Code**：[verify_preparations_fixtures.py:32](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/scripts/verify_preparations_fixtures.py#L32) 与 [verify_commits_fixtures.py:31](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/scripts/verify_commits_fixtures.py#L31) 复制隔离 Schema、表克隆、依赖覆盖、Uvicorn 启停和清理流程，例如 `Base.metadata.create_all(...)`、`for table in Base.metadata.sorted_tables:`。隔离和清理修正需要改两处；可用小型共享上下文管理器集中维护，保留各自故障夹具。
3. **可能的 Repeated Switches**：[App.vue:216](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/frontend/src/App.vue#L216) 与 [同文件:217](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/frontend/src/App.vue#L217) 分别在 breadcrumb、h1 中扩展相同的 tab 分支链。新增页面需要重复登记，容易造成文案失配；可集中为一份 tab 到标题、面包屑文案的映射。

最值得优先整理的是 Worker 中重复的授权复核逻辑。

## Spec

**Confirmed issues: 2. Worst severity: P1.** The following is the independently reported Spec review at the pinned snapshot.

1. **[P1] #10 can permanently lose an accepted command on an ordinary transient PostgreSQL failure.** Spec #10: “Outbox 可靠转发受管命令”; ADR0002: “接受 Outbox/Celery 至少一次投递”. [celery_app.py:24](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/backend/app/celery_app.py#L24) retries only returned `submission_unknown`/`busy`; SQLAlchemy exceptions from the first query or final transaction escape. [outbox_publisher.py:13](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/backend/app/outbox_publisher.py#L13) only polls `pending`, while line 34 marks `sent`. Trigger: after publisher records `sent`, PostgreSQL is briefly unavailable when a healthy worker consumes the task, or the result transaction fails after POST. Celery ACKs the ordinary exception; Run stays queued/submitting, with no task/outbox recovery on restart. Independently inspected installed Celery 5.6.2 defaults + `Request.on_failure` read-only: `task_acks_on_failure_or_timeout=True`; ordinary exceptions acknowledge, worker-lost requeue is separate. Fix should explicitly retry transient transaction/connection failures while retaining durable Attempt/discovery behavior; do not merely set ACK-on-failure false (source rejects with `requeue=False`).
2. **[P2] #8 accepts an ApprovalDecision after the Preparation expires.** Spec #8: “24 小时有效，审批不续期；过期…拒绝推进”. [preparations.py:353](https://github.com/yb2915722928-cpu/-ETL-Agent-/blob/a64be0e3854070e4065ebdae3ab85aaa3c94a2c2/backend/app/preparations.py#L353) validates expiry before live resource I/O, then inserts the decision with fresh `decided_at` and emits `approval.decided` without another expiry check. Trigger: approve starts seconds before deadline and source/target probes finish after it. HTTP 200 and a durable late approval result, even though returned validation can already say expired. #9's completion-time checks correctly prevent subsequent expired Commit; this finding is the separate #8 approval contract.

No additional confirmed scope creep or demand for #11/#12 or L2.

## 修复与复查

统一修复提交：`f450ec238ba2b147403002644c7e8128503d95ac`；集成归并提交：`5b5f3736b237e1185cdad3efe82d905055441e80`。以下 5 项均已处理，没有新增依赖或生产故障注入入口。

| 原始发现 | 处理结果 | 验证 |
| --- | --- | --- |
| Spec P1：普通数据库故障导致任务被 ACK 且失去恢复 | 针对驱动连接故障及明确的临时 SQLSTATE 发起 Celery 重试；保留原 Attempt 和仅发现恢复路径 | [数据库红例](../../artifacts/issue-10/review-database-red.json) 与 [绿例](../../artifacts/issue-10/review-database-green.json)：真实停库恢复、POST 后 deferred 40001 均由相同 task_id 重试成功；隔离引擎只有一个原任务，无第二次 Capability 消费 |
| Spec P2：审批跨过截止时间 | 有界受管检查完成后重新取时钟，`>= expires_at` 即拒绝，在写入决定之前阻止推进 | [截止红例](../../artifacts/issue-8/review-expiry-red.json) 与 [绿例](../../artifacts/issue-8/review-expiry-green.json)：批准和拒绝都返回 409，无决定或 `approval.decided`，截止时间不变，拒绝证据持久化 |
| Standards：重复授权复核 | 共用 `_check_submission_authorization`，保留两次调用、两个事务及提交期间的租户锁 | 既有授权/故障验证通过；不减少提交前复核 |
| Standards：重复隔离夹具生命周期 | 新增仅供脚本使用的 [isolated_api_fixture.py](../../scripts/isolated_api_fixture.py)，共享 Schema/Uvicorn 创建和清理；各自保留时钟和故障控制 | #8、#9 隔离夹具通过；完整 scripts 挂载方式已在文档记录 |
| Standards：重复页面标题分支 | 单一 tab 元数据映射供标题和 breadcrumb 使用，保留原中文文案 | Vite 生产构建及两套环境实际 Chrome 导航通过 |

详细命令、镜像与源文件哈希、夹具清理和历史签名有效性处理见 [修复执行记录](../../artifacts/issue-10/review-fixes.md)。已验收的真实排队与 ACK 丢失实验没有重复执行；只复读持久化结果，并补充缺陷所必需的最小红/绿回归。

两个原审查代理已对修复 diff `a64be0e...f450ec2` 完成独立只读复查：Standards 的 3 个启发式项均已合理解决，未发现新增硬性违规或具体问题；Spec 的 P1 / P2 均已解决，未发现本次修复引入的具体问题。**本批已确认问题无未关闭项。**

## 必要真实验证

- #8：真实 PostgreSQL 公开 HTTP 并发、独立槽位和权限负例；24 小时、撤权、策略/凭据/Schema 漂移的隔离夹具；普通样本和统计变化不误判；真实 MySQL DDL 未重新探测即拒绝审批，恢复后受管检查通过；Chrome 双账号批准、拒绝、只读审计视图；服务重启后冻结事实和决定保留。
- #9：真实 PostgreSQL 原子接受、同键相同请求返回原 Run、不同请求冲突、同准备单并发唯一运行；真实 COMMIT 时故障回滚所有接受行和证据链；仅公钥验签；真实 Node JSON 往返后的签名兼容及历史签名不重写；Chrome 首次 Commit 已在服务器接受但响应丢失后，以原授权和原键重试返回原运行。
- #10：真实 Redis 8.0.4 / Celery 5.6.2 / Kombu 5.6.2，PostgreSQL 17.11；从持久化接受事件到消费事件实际间隔 **308.446 秒**，不是把整个测试耗时当排队时长；Redis 故障、发布成功但未标记 sent、提交后 Worker SIGKILL、原任务发现、跨项目物理别名隔离、8 条重复消息均确认被消费，以及真正的 ACK 丢失/相同 task_id 可见性重投。独立 SQLite Engine 每次 POST 都新建任务，不使用夹具去重掩盖应用重复提交；两个原始任务始终唯一。
- L1 私有配置已从实现工作树迁移至主工作区，沿用原密钥和 PostgreSQL / Redis / SQLite 卷。迁移后的公开 GET 复查保留原任务、关联、目标所有权和阻塞事实。

最终部署复用了同一次成功构建的镜像：API `sha256:a1a302d6fb1ad46d8618b8febd7b645038080893aed6d1bd3768a415ff0cadc3`，Web `sha256:62801deb93f0a2d2625fc355d290423823de7436f971091155f43fa3b0f98035`。普通部署只重建 API/Web 容器，没有重建其 PostgreSQL/Redis。

| 环境 | 实际入口 | 最终 Chrome 结果 |
| --- | --- | --- |
| 普通部署 | http://127.0.0.1:8080 | [验收 JSON](../../artifacts/issue-9/final-runtime/browser.json)：服务端已接受但响应中断后，以完全相同的请求重试得到原 Run；`queued`、`pending`、`execution.level=disabled`、无 Attempt；审计员无授权按钮；页面错误为 0 |
| 隔离 L1 | http://127.0.0.1:8090 | [验收 JSON](../../artifacts/issue-10/final-runtime/browser.json)：新 P3 受限只读操作经真实 Outbox/Celery 达到 `metadata_checked`，无 Engine job/Attempt；原任务、持续目标隔离和跨项目别名阻塞可见；页面错误为 0 |

最终画面：[普通部署接受事实](../../artifacts/issue-9/final-runtime/commit-accepted.png)、[L1 只读完成](../../artifacts/issue-10/final-runtime/metadata-completed.png)、[原任务恢复](../../artifacts/issue-10/final-runtime/reconciled-original-job.png)、[物理别名隔离](../../artifacts/issue-10/final-runtime/physical-alias-blocked.png)。

## 验收边界

普通部署的执行适配器保持关闭，Commit 仅接受唯一运行及待投递命令。专用隔离部署提供 L1 控制面和故障 Engine；未接入真实 SeaTunnel，不搬运数据，不发布或清理目标。物理身份使用部署方登记的 `physical_cluster_id + database + table`，这是 L1 环境的可信配置约束，不声称证明真实集群身份。未知结果持续保留目标隔离；本批不实现 #11 之后的运行监管、真实执行和发布能力。
