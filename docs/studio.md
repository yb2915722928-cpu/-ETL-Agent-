# V05 手工 Pipeline Studio

当前实现是 Maker 的 MySQL → Doris 单表 `bounded_full` 计划设计与静态审查。用户只编辑结构化 EtlPlan；HOCON 与 DAG 为同一份计划的只读派生制品。没有模型调用、外部数据库执行、SeaTunnel 提交、审批或正式发布。版本冻结与 Diff 由 #7 独立实现。

## 使用入口

控制台选择“手工集成 Studio”和所属项目。先在“连接与资产”登记受管连接并完成真实只读探查，再选择源/目标 MetadataProfile 与一张表。表单生成目标字段映射，可选择不同源字段实现直接投影/重命名，添加非空或数值范围规则，调整安全预算，校验后审查门禁、DAG 与 HOCON。此阶段不接受 SQL、表达式、多表、CDC 或其他连接器。

目标所有字段必须恰好映射一次，避免依赖档案中不存在的默认值或生成列事实。类型采用保守无损兼容：有符号整数只能保宽/扩宽、decimal 保留整数位和小数位容量、字符容量不缩窄、datetime 不丢精度；date、float、double、boolean、JSON 仅在支持的同类事实下对齐。未识别类型拒绝。源字段可空而目标非空时必须显式配置目标字段非空契约。质量门禁只验证规则结构和引用，不证明实际数据质量。数值范围边界为有限数值，不允许最小值大于最大值。

安全预算固定上限为 100000 行、268435456 字节、600 秒、2 倍放大、0.2 拒绝率；行/字节/时长/放大预算须大于零，拒绝率可为零。`mysql.read` / `doris.write` 是计划允许的工具意图，不是执行权限。未来执行阶段必须冻结实际指标口径并执行监督，本阶段不声称读取、写入、预算执行或质量分流已经发生。

## 候选与审查制品

`studio_candidates` 保存 Scope、owner、name、revision、EtlPlan 与当次 assessment；启动沿用现有增量建表入口，保留身份、连接和历史事实。当前项目成员可读取候选和下载制品；创建、校验与修改每次重新检查当前 Maker 角色，修改还检查原创建者与 `expected_revision`。修订冲突返回 409，可以另存为新候选。语法或确定性门禁失败的候选可保存与修复，但不生成 HOCON、DAG 或 execution_facts，不能作为冻结输入。

修改表单后隐藏旧审查制品；保存重新运行全部门禁。下载对应已保存的修订，附 `Content-Disposition` 和可重算的 `X-Content-SHA256`。读取旧候选展示保存时 assessment；重新校验才核对当前白名单、凭据版本和连接状态。HOCON 是显式标记 `review_only` 的 JSON 兼容 HOCON 审查格式，包含 `secret://<id>/versions/<version>` 安全引用，不能直接提交引擎。后续真正引擎编译器必须采用新版本并重新门禁/冻结。

## 公开 API

以下路径相对 `/api/tenants/{tenant_id}/projects/{project_id}/studio`；全部使用当前登录会话，Scope 由服务端核验。

| 方法与路径 | 请求与行为 |
|---|---|
| `POST /validate` | `{plan}`；返回逐项门禁、规范计划、派生制品与确定性事实，不创建候选 |
| `POST /candidates` | `{name, plan}`；201，保存成功或失败候选 |
| `GET /candidates` | 当前项目候选列表 |
| `GET /candidates/{id}` | 候选原文、修订与保存时 assessment |
| `PUT /candidates/{id}` | `{name, plan, expected_revision}`；仅原创建者 Maker，旧修订返回 409 |
| `GET /candidates/{id}/artifacts/{artifact}` | `plan.json`、`review.conf`、`dag.json`；失败候选的派生制品返回 409 |

assessment 包含 `passed, gates, normalized_plan, hocon, dag, execution_facts, profile_refs, versions, artifact_kind, limitations`。门禁名称为 `syntax, resources, tools, budget, mapping, schema, quality`；语法失败时只返回语法错误，字段定位与错误码不会回显被拒绝的输入值。跨 Scope 的连接、档案与候选 ID 返回 404；无项目或租户权限返回 403。创建、修订、校验和角色/Scope 拒绝写入同事务安全审计事实，不把计划原文或样本复制到审计。

## 给 #7 的消费契约

实现入口在 `backend/app/studio.py` 与 `backend/app/studio_plan.py`。

- `get_candidate(db, tenant_id, project_id, candidate_id, *, lock=False)`：只做 Scope 定位，404 表示当前项目不可见；冻结调用者先执行当前身份/Maker 授权，再以 `lock=True` 读取候选并核对预期 revision。
- `normalize_plan(raw) -> EtlPlan`：Pydantic 严格有限语法；额外键拒绝。映射按目标/源排序，质量规则按 canonical JSON 排序，工具排序。原列顺序仍保留于 Schema 数组。
- `assess_plan(db, policy, tenant_id, project_id, raw) -> dict`：调用者先授权；只读取控制面连接、SecretReference 与 Profile，并重新读取部署白名单，不连接源库或目标库。不信任客户端或保存时的 `passed`。检查连接类型/状态、白名单指纹、表范围、档案所属连接及资源/凭据绑定、档案 Schema hash、当前 SecretReference id/version。外部 Schema 是否再次漂移属于后续受管复核阶段。
- `freeze_facts(assessment) -> dict`：仅对本次服务器新鲜 assessment 使用；失败抛 409。返回 `execution_facts, profile_refs, normalized_plan, hocon, dag, versions`，不创建冻结版本、不授予执行权限。#7 必须对新鲜输出 canonical 序列化并冻结制品字节/摘要，不接受客户端 assessment 或旧 Candidate.assessment 代替复核。
- `canonical_json(value)`：UTF-8 语义、键排序、紧凑 JSON、禁止 NaN/Infinity，数组保留规范后的顺序；版本 `sorted-json-v1`。数字在 EtlPlan 模型中按 int/float 归一化。

`execution_facts.plan` 保留业务定义、工具、规则与预算，仅将源/目标引用简化为表名；各侧事实绑定 connection_id、resource_id、resource_fingerprint、所选 Schema 与档案 schema_hash、secret_reference_id/version。Profile ID 独立保存在 `profile_refs`；候选名称、owner、revision、Profile 采集时间、近似统计、样本及随机密文不进入执行事实。因此同一连接重新探查，只要 Schema 与绑定不变，执行事实不变。版本常量为 `compiler=studio-plan-v1`、`renderer=review-hocon-v1`、`capability=mysql-doris-projection-v1`、`policy=studio-policy-v1`，并包含当前连接器策略版本。变更编译、能力或策略应改变相应版本并重新冻结。

## 最小公开验收

[HTTP 验收脚本](../scripts/verify_studio_http.py)使用真实服务与公开接口，无内部函数测试或伪造 Profile。默认复用连接验收状态；`--probe` 可经受管公开接口创建真实 MySQL/Doris Profile。私有密码只在脚本进程读取，已知凭据扫描覆盖每个 HTTP 响应和下载制品，报告不包含密码或业务样本。

```powershell
python scripts/verify_studio_http.py --probe --output docs/verification/issue-6-studio.json
# 保留数据库与会话，重启 API 后验证同一候选
python scripts/verify_studio_http.py --recheck --output docs/verification/issue-6-persistence.json
npm --prefix frontend run build
git diff --check
```

可用 `--url, --credentials, --other-credentials, --secrets, --connector-state, --state` 指向现有环境。角色撤销验证保存原角色并在 `finally` 恢复；额外隔离项目使用随机标识。本次报告见 [Studio 验收](verification/issue-6-studio.json) 与 [重启持久化验收](verification/issue-6-persistence.json)。验收证明手工计划、权限、静态门禁与制品持久化，不计入模型准确率或真实引擎/发布验收。
