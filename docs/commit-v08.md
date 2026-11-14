# V08 单次 Commit 与原子授权接受（#9）

Operator 对有效 Preparation 签发五分钟 Ed25519 Capability，再显式提交 Commit。成功表示本地已接受唯一 ExecutionRun，状态为 `queued`；引擎任务 ID 为空。ManagedCommand 和待投递 Outbox 随运行持久查询，后续由 #10 Worker 消费。

## 公开 API

作用域为 `/api/tenants/{tenant_id}/projects/{project_id}`。

| 请求 | 结果 |
| --- | --- |
| `POST /preparations/{id}/capabilities` | 当前 Operator；201 返回 `{capability:{payload,signature},expires_at}` |
| `POST /preparations/{id}/commit` | `{capability:{payload,signature},idempotency_key}`；首次接受 201，相同请求重试 200 |
| `GET /runs` | 当前项目成员可读运行、命令、Outbox |
| `GET /runs/{id}` | 同作用域单个运行；不存在 404 |

Capability 签名绑定租户/项目、当前操作员、Preparation 及冻结 SHA、版本/执行/Bundle/各制品 SHA、工具/风险/环境、源目标资源及 Schema/凭据版本、有效预算、授权操作与策略/编译器版本。签发和 Commit 均重查 Maker/两名 Checker/Operator 当前启用账号和职责、24 小时准备单时效、审批与本地冻结事实，并通过受管只读出口实时核对外部 Schema/权限。P3 仅能得到 `metadata.read` 能力，不能授权 ETL 写入；P2 命令使用冻结后收紧的预算。

签名载荷采用排序、紧凑 UTF-8 JSON（`sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False`）。签发前递归将字典/列表中的整数值浮点数转换为整数（例如 `2.0→2,-0.0→0`），并将转换后的同一载荷存储、返回和签名，保证 JavaScript `JSON.parse`/`JSON.stringify` 往返不改变签名字节；非整数浮点值保留。此为当前受限载荷的规范，不声称 RFC8785。验签仍精确使用存储/收到的载荷，不转换旧载荷，已接受旧命令的签名保持有效。格式分别为 `etl-capability-v1` 和 `etl-managed-command-v1`。部署沿用现有 Ed25519 种子；格式和 key ID 一起被签名，拒绝两种载荷替代。Capability `issued_at`/`expires_at` 为 Unix 秒，固定相差 300 秒，截止时刻拒绝。签名令牌不写入证据，证据仅保存 jti、冻结指纹、命令 ID 和安全状态。受管检查结束后再次核验准备单及 Capability 截止时间，以完成时刻记录接受和消费，避免检查耗时穿过有效期。

## 原子接受及重试

所有写入按租户锁→当前权限→Preparation 行锁顺序。租户锁与账号/成员变更共用，撤权不能穿过仍使用旧资格的 Commit。

真实 PostgreSQL 同一事务包含单次 jti 消费、唯一 Preparation 的 ExecutionRun、签名 ManagedCommand、Outbox、作用域幂等键/请求摘要及成功证据链更新。任意数据库失败全部回滚。数据库唯一约束同时保护 jti、Preparation、Run 对应的命令/Outbox/消费与幂等记录。命令的原始 subject 不随重试改变。

同一项目内幂等键绑定规范请求摘要（含 Preparation、原始签名 Capability 和操作员）。每次重试首先核验当前账号、Operator 和项目范围；键相同且摘要相同返回原运行，不再消费 Capability，也不受后来五分钟到期影响。键相同摘要不同返回 409；不同键或操作员不能为已接受 Preparation 创建第二个 Run。原请求响应不明时，页面保留原 Capability 与幂等键，用相同请求重试；显式放弃才清除。

业务拒绝转为 JSONResponse，在事务中保存拒绝证据；数据库异常穿透事务由统一处理器返回错误，成功记录及证据一起回滚。

## #10 Worker 接口

`backend/app/execution.py` 导出 `ExecutionRun`、`ManagedCommand`、`Outbox`、`ReplayGuard`、`IdempotencyRecord`、`get_run`、`authorization_bindings`。表名依次为 `execution_runs`、`managed_commands`、`execution_outbox`、`capability_consumptions`、`commit_idempotency`。Outbox 当前字段为 `id,tenant_id,project_id,command_id,status,attempts,created_at`；初值 `pending,0`。每个 ManagedCommand 用 `run_id` 绑定唯一运行，`envelope` 保存已签名载荷，包含 `command_id,run_id,accepted_at` 和上述授权绑定。

```python
from app.command_signing import COMMAND_FORMAT, verify_envelope
payload = verify_envelope(command.envelope, configured_base64_public_key, COMMAND_FORMAT)
```

验签模块只导入标准库与 cryptography，不导入应用启动、数据库或 EvidenceSigner；Worker 无需私钥。可信公钥来自部署配置，不能使用令牌自带公钥。返回载荷只代表签名有效；Worker 仍须核对命令/运行 ID、作用域、原 subject、当前资格、Preparation/manifest 与实际工具/环境，并执行 #8 的显式受管检查。

已接受命令没有 Capability 的五分钟执行期限；Worker 不再消费 Capability，也不因排队超过五分钟撤销接受事实。Preparation 过期检查按 #8 接口使用 `check_expiry=False`，其他资格与漂移检查保留。有效预算取命令中签名的 `budget` 并核对 `row.frozen['budget']`，禁止回退到 `plan.budget`。P3 的 `authorized_operation.data_plane_mutation` 为 false，禁止提交 ETL 写入。

## 验证

`scripts/verify_commits_http.py` 通过公开 HTTP 和真实受管连接验证签发→Commit→持久查询、相同请求并发 201/200、独立有效令牌/键并发 201/409、幂等冲突/重放及租户/角色隔离；P3 命令保留只读授权。使用 `--runtime` 指向根私有运行目录，支持 `--recheck` 验证 API 重启后原事实不变。

`scripts/verify_commits_fixtures.py` 在 API 镜像内以 `PYTHONPATH=/app:/tests` 运行，将仓库 `scripts` 目录整体挂载到 `/tests`，以包含共用的 `scripts/isolated_api_fixture.py`；测试时钟与故障仍由本脚本独立设置。使用一次性真实 PostgreSQL Schema 与隔离 HTTP 应用验证时效、检查中途到期、撤权/禁用账号、策略/Schema/冻结指纹漂移、跨主体令牌与独立操作员、无私钥进程验签。使用 `DEFERRABLE INITIALLY DEFERRED` 约束触发器在数据库 COMMIT 阶段抛出真实异常，验证五张接受表、证据表、链头和租户序号一起回滚；移除故障后原 jti/键仍可成功。测试完成删除隔离 Schema。无生产故障端点或根数据库时间戳修改。最终报告记录于 `artifacts/issue-9/`。根编排器负责真实 Chrome 页面流程及最终审查，工单保持开放。
