# V07 冻结 Preparation 与独立审批（#8）

Preparation 绑定当前项目的已冻结 PipelineVersion。生成前使用现有受管只读出口核对资源、凭据可用性、只读权限和外部 Schema；记录受管检查证据，将 Profile 中的执行事实与本次检查回执一起固化。准备与审批不创建外部数据对象、不执行 HOCON、不提交引擎任务。

风险由工具登记确定：`pipeline.publish` 为 P1，`pipeline.trial` 为 P2，`metadata.read` 为 P3，未登记工具为 P0。P1/P2 必须独立数据与安全双审批；申请人不可自批，同一账号不能占据两个槽。P0 保留可审计的拒绝状态。P3 只授权元数据读取，不能作为 ETL 写入权限。试运行有效预算上限为 10,000 行、16 MiB、120 秒，且不会放宽版本原始预算。

冻结内容包括作用域、Maker、工具/风险/环境、版本执行及制品指纹、规范化计划、资源/Schema/凭据版本、HOCON、DAG、数据分类、质量口径、有效预算、影响范围、恢复说明、策略/编译器版本、创建及到期时间。固定创建后 24 小时到期，无更新、删除或续期接口；审批在受管检查完成后、写决定之前再次核对同一截止时间，批准和拒绝均不得在到期时刻或之后落单。超时返回 409，在同一事务保留 `approval.denied`，不写决定或 `approval.decided`；审批决定具名且不可修改。重新申请须创建新准备单并重新审批。

列表和详情仅做本地事实检查，不获取凭据、不触发外部读取。页面明确展示该检查边界。每次 Prepare、Approve，以及后续 Commit/Worker 提交推进前，调用独立受管资源检查。受管读取沿用现有资源白名单、授权检查、每资源最多 8 表/64 列、每表 5 样本、单 SQL 3 秒及总 20 秒限制。样本和统计不进入冻结回执或漂移判断。

## HTTP API

统一作用域为 `/api/tenants/{tenant_id}/projects/{project_id}`。

| 请求 | 作用 |
| --- | --- |
| `POST /preparations` | Maker 提交 `{version_id, tool, environment:"local"}`，成功 201 |
| `GET /preparations` | 项目成员查询准备单和本地当前状态 |
| `GET /preparations/{id}` | 查询冻结内容、两个具名决定与当前资格 |
| `POST /preparations/{id}/decisions` | 当前对应 Checker 提交 `{slot:"data"\|"security", decision:"approve"\|"reject"}` |
| `PUT/PATCH/DELETE /preparations/{id}` | 409，不可修改或续期 |

当前资格逐次读取启用账号和当前项目角色。失去 Maker 或任一已签 Checker 资格、24 小时到期、资源/凭据版本/Schema/策略/编译器/制品指纹变化均使推进失效。普通数据变化、重新探查的 Profile ID/时间、样本、统计不改变执行事实。拒绝决定终结当前准备单。账号/角色变更和 Prepare/Approve 统一先获取租户锁，重新读取当前资格；审批槽和账号还受数据库唯一约束保护。

## #9 / #10 复用接口

模块为 `backend/app/preparations.py`。调用者在同一个 PostgreSQL 事务中先完成作用域和当前操作人授权，按租户锁→准备单行锁顺序获取锁。不要先更改账号、角色或准备单再取租户锁。

```python
lock_tenant(db, tenant_id)
require_current_role(db, tenant_id, project_id, actor.id, 'operator')
row = get_preparation(db, tenant_id, project_id, preparation_id, lock=True)
version = get_version(db, tenant_id, project_id, row.version_id)
checked = check_execution_resources(
    db, policy, secret_provider, version, actor, 'operator', preparation_id=row.id)
validation = validate_preparation(db, policy, row)
```

- `get_preparation(db, tenant_id, project_id, preparation_id, lock=False)`：仅返回作用域内模型，缺失 404。
- `require_current_role(db, tenant_id, project_id, account_id, role)`：重新读取账号/成员资格，失败 403。
- `check_execution_resources(db, policy, secret_provider, version, actor, role, preparation_id=None)`：显式受管 P3 检查，返回 `{'facts': execution_facts, 'receipt': receipt}`；回执含检查时间、工具、风险、每资源预算、资源/Schema/凭据绑定。写入 `preparation.resources_checked` 或 `preparation.resources_check_denied`，不保存样本。失败抛 `HTTPException`，调用者须转为 `JSONResponse` 保留事务中的拒绝证据，不让异常穿透外层事务导致回滚。
- `validate_preparation(db, policy, row, require_approved=True, check_expiry=True, now=None)`：本地纯校验，返回 `{valid, reasons, state, current_eligibility}`。状态为 pending/approved/auto_authorized/rejected/expired/invalidated/denied。`now` 仅供测试时钟注入。Worker 在已接受 Commit 后使用 `check_expiry=False`，仍检查当前资格与所有执行事实；Commit 必须检查到期时间。

后续调用者仍须约束实际操作人、工具、风险、环境、版本和 manifest 的绑定。引擎写入只允许 P1/P2，禁止用 P3 的 `metadata.read` 提交 ETL。有效预算取 `row.frozen['budget']`，P2 不得回退使用未收紧的计划预算。受管检查回执仅供本次推进，不可复用为未来推进的凭证。

## 必要验证

`scripts/verify_preparations_http.py` 使用公开 HTTP、真实 PostgreSQL 与真实受管连接，验证并发双审批、越权/自批/职责隔离、撤权、拒绝终态、P0/P3、不可变与重启持久性。`--runtime` 指向现有私有运行目录，`--recheck` 在 API 重启后核对已存冻结事实及具名决定。

`scripts/verify_preparations_fixtures.py` 在 API 镜像内通过真实 HTTP 运行同一应用，克隆到一次性 PostgreSQL Schema；仅测试进程注入过期时钟与 Schema 回复，验证到期、Schema/资源/策略/凭据漂移、样本统计排除、GET/list 无外部调用和失败证据持久化。无生产故障端点，完成后删除测试 Schema。截止时间回归用一张准备单让真实受管检查跨过精确到期时刻，验证批准/拒绝均 409、截止时间不变、无晚决定且拒绝证据持久化，结果见 `artifacts/issue-8/review-expiry-green.json`（旧实现红灯见同目录 `review-expiry-red.json`）。准备单与 Commit 夹具共用 `scripts/isolated_api_fixture.py` 的 Schema 克隆、HTTP 生命周期及销毁；时钟与故障仍留在各自夹具。运行时挂载本仓库 `scripts` 到容器 `/tests`，设置 `PYTHONPATH=/app:/tests`。不需要额外测试依赖。

本工单不颁发 Capability、不提交运行；相关能力由 #9、#10 在集成后验证。根编排器收集浏览器截图与重启后证据。
