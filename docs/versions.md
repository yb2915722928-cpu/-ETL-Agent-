# V06 不可变 PipelineVersion 与 Diff

控制台“冻结版本与 Diff”选择所属项目和已保存候选，点击“冻结当前修订”。服务端先核验当前账号、项目成员与 Maker 权限，再锁定候选并核对 `expected_revision`，使用当前部署白名单、连接状态、凭据引用版本和 Profile 绑定重新运行 Studio 全部门禁。保存时的 `assessment.passed` 不作为冻结依据。冻结只访问控制面事实，不额外连接外部数据库；外部 Schema 是否漂移仍须通过新的受管探查确认。

通过后保存 `pipeline_versions` 的候选 ID/修订、名称、owner、Scope、创建时间、冻结事实与制品原始字节。所有写入及 `version.frozen` 证据在请求事务内提交，证据绑定版本与制品摘要。当前项目成员可读取历史版本；冻结权限不要求候选原创建者。对同一候选修订与同一执行摘要重复或并发冻结会返回同一版本，但每次仍重做当前授权和门禁。候选修订冲突或门禁失败返回 409；失败不会产生新版本。

版本 API 只有追加入口，`PUT/PATCH/DELETE` 均返回 409，并记录拒绝证据。修正时返回 Studio 保存候选新修订，再冻结新版本。候选后续修改、重新探查、部署白名单撤回不改变历史版本。

## 两种摘要

`execution_sha256 = SHA256(UTF8(facts.json))`。执行事实来自 Studio 的规范化输出，包含业务计划、映射、质量规则、预算、工具意图、所选 Schema 与 schema_hash、连接与资源指纹、凭据 id/version、规范化/编译器/渲染器/能力/策略版本。Profile 随机 ID 单独保存在 `frozen.profile_refs`；候选名称、owner、修订、采集时间、统计、样本和随机密文不进入执行事实。

因此同一连接重新探查，仅采集时间、Profile ID 或统计变化，不改变执行摘要及 HOCON/DAG。计划数组按 Studio 规则规范化；Schema 列顺序保留。Schema、业务计划、凭据引用/版本、资源或编译/策略版本变化会影响其绑定的执行事实摘要。当前 SecretReference 只有新增接口；切换凭据须登记新连接并重新探查及修订候选。

`bundle_sha256 = SHA256(UTF8(manifest.json))`。清单包含规范化版本、编译/渲染/策略版本、执行摘要以及其他四份制品的 SHA256。`plan.json` 保留原 Profile 引用，因此重新探查后的事实可以相同，而计划和制品清单摘要不同。清单不包含自身摘要，避免循环；详情 `artifact_hashes` 包含全部五份制品摘要。

规范 JSON 为 `sorted-json-v1`：UTF-8、键排序、紧凑分隔符、保留规范化后的数组顺序、禁止 NaN/Infinity。摘要覆盖下载原始字节，不重新渲染历史事实。

## 制品与 Diff

| 制品 | 内容 |
|---|---|
| `plan.json` | 规范结构化计划与冻结 Profile 引用 |
| `review.conf` | 原始 HOCON 审查字节，安全 `secret://<id>/versions/<version>` 引用 |
| `dag.json` | 规范 DAG |
| `facts.json` | 决定执行摘要的规范事实 |
| `manifest.json` | 编译/策略版本、执行摘要及制品 SHA256 清单 |

页面提供只读查看、复制、下载，每次复制或下载重新调用制品 API 检查当前会话与 Scope。响应附 `Content-Disposition`、`X-Content-SHA256`，下载按冻结字节返回。没有凭据明文或样本；HOCON 明确为 `review_only`，不能直接提交 SeaTunnel。

Diff 从当前版本比较至另一版本，分别返回字段映射、源/目标 Schema、质量规则的前后值，以及 HOCON unified diff；同时给出执行摘要是否变化。两端均在同一当前项目 Scope 内定位。资源、预算或策略变化也可使执行摘要变化；四类 Diff 展示其各自的实际内容变化。

## 公开 API

路径相对 `/api/tenants/{tenant_id}/projects/{project_id}/versions`，每次由服务端检查当前 Scope。

| 方法与路径 | 请求与行为 |
|---|---|
| `POST /` | `{candidate_id, expected_revision}`；201，重新门禁并追加或复用不可变版本 |
| `GET /` | 当前项目版本摘要列表，按创建时间倒序 |
| `GET /{id}` | 元信息、冻结事实、Profile 引用、制品原始文本及摘要 |
| `PUT/PATCH/DELETE /{id}` | 409，不能修改或删除历史版本 |
| `GET /{id}/artifacts/{artifact}` | 五份冻结制品之一，当前授权、原字节与摘要 |
| `GET /{id}/diff/{other_id}` | 两端当前 Scope 核验后返回四类 Diff |

无当前租户/项目权限为 403；已授权项目内看不到的版本或候选为 404。拒绝与读取证据只记录固定原因、ID、制品名称和摘要，不复制计划或输入原文。当前账号停用或成员/角色撤回立即作用于已有会话。

## 必要公开 HTTP 验收

先运行真实连接与 Studio 验收，或 `python scripts/verify_studio_http.py --probe` 取得真实 MySQL/Doris Profile 与候选。然后运行：

```powershell
python scripts/verify_versions_http.py --output docs/verification/issue-7-versions.json
# 保留 PostgreSQL/Redis，重启同一 API 后读取所有冻结版本及制品
python scripts/verify_versions_http.py --recheck --output docs/verification/issue-7-persistence.json
npm --prefix frontend run build
git diff --check
```

可指定 `--url, --credentials, --other-credentials, --secrets, --studio-state, --state`。验收通过公开 API 取得新 Profile、创建/修订候选、冻结、读取、Diff、下载，验证重复并发稳定、重探查稳定、映射/质量/预算变化、不可变、跨租户/跨项目与即时撤权，并验证证据中的版本/制品绑定。私有凭据和具体版本快照只存 `.runtime`，报告只含结果。

`--policy-file` 可指向临时 API 独用的白名单副本：临时移除源资源，确认保存时通过且已冻结的候选仍被当前门禁拒绝，然后在 `finally` 恢复。不要对共享正式 API 的白名单执行这一选项。此项不会改数据库或伪造 Profile。

本地验收报告：[真实 Studio 输入](verification/issue-7-studio-input.json)、[冻结与隔离验收](verification/issue-7-versions.json)、[重启持久化](verification/issue-7-persistence.json)。本阶段没有模型、引擎执行、质量规则执行、Preparation、审批或发布。
