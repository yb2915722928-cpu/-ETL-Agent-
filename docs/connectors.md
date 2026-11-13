# V03：受管连接、凭据引用与元数据档案

GitHub #4 实现 MySQL 源与 Doris 目标的登记、测试与只读元数据探查。中文控制台“连接与资产”按项目选择资源。当前项目成员可读；当前具有 `maker` 角色的成员才可登记和探查。租户管理员、项目管理员不会自动取得 Maker 权限。每次请求重新检查身份与成员角色，已有会话撤权立即生效。

## 部署与密钥

先运行 `python scripts/init_local.py`：仅在缺失时向本地 `.env` 追加 `SECRET_MASTER_KEY`，是 Base64 编码的随机 32 字节 AES-GCM 主密钥；保留既有密码、证据签名密钥、主密钥和资源文件。主密钥由 Compose 环境注入，不入 PostgreSQL。API lifespan 在迁移前检查主密钥与资源配置，缺失或格式错误直接启动失败，无临时密钥降级。升级已有环境时保留 PostgreSQL 卷并重建 API/Web；勿使用 `down -v`。本票不实现密钥轮换，丢失原主密钥后旧凭据无法恢复。

`CONNECTOR_RESOURCES_FILE` 指向仅部署者可修改的 JSON（Compose 默认 `/run/identity/connectors/resources.json`，宿主 `.runtime/connectors/resources.json`）。空对象允许启动且没有可登记资源。每项格式如下；其中不含密码：

```json
{
  "acceptance-mysql": {
    "kind": "mysql", "host": "connector-mysql", "port": 3306,
    "database": "connector_source", "username": "source_ro",
    "tables": ["customers", "orders"]
  },
  "acceptance-doris": {
    "kind": "doris", "host": "connector-doris", "port": 9030,
    "database": "connector_source", "username": "source_ro",
    "tables": ["customers", "orders"]
  }
}
```

`direction` 可省略：MySQL 为 `source`、Doris 为 `target`。主机、端口、库、账号和表均来自部署白名单；HTTP 只接收资源 ID、显示名称与密码，不能提交 SQL/URL/主机/表/预算。资源指纹覆盖完整规范化配置。每次登记/探查重新读取策略；既有连接对应策略消失或指纹改变返回 409，需登记新连接。该策略是当前单个部署的固定资源授权集合，所有项目共享候选集合，连接及其凭据和档案仍严格按租户/项目隔离。它不是网络防火墙；生产部署还需按对应受管资源限制 API 容器网络出口。

真实外部环境的最小初始化/固定依赖入口见 [infra/connectors](../infra/connectors/README.md)。真实环境私有配置 `.runtime/connectors/secrets.json` 已存在时，可运行 `python scripts/generate_connector_resources.py` 生成无密码白名单。该工具只支持已实现的 MySQL/Doris；后续 #5 扩展 PostgreSQL，不能提前列入不支持的类型。外部账号必须只有 SELECT（MySQL 允许 SHOW VIEW/USAGE）；Doris 允许内建 `normal: Usage_priv` 调度查询，其他写权限与继承角色均拒绝。真实只读权限检查先于业务表读取。

## 公共接口与持久化契约

公共前缀为 `/api/tenants/{tenant_id}/projects/{project_id}`。POST/GET 都执行当前项目范围授权；猜测其他项目的连接/档案 ID 不会跨范围读取。

| 接口 | 输入与结果 |
|---|---|
| `GET /connector-resources` | 可登记的安全资源摘要，含 `id/kind/direction/database/tables/fingerprint`，不公开网络地址或账号 |
| `POST /connections` | `{name,resource_id,password}`；新 Connection 和新 SecretReference 同事务创建，返回 201 |
| `GET /connections`、`GET /connections/{id}` | 返回连接引用、状态与版本，不含密码、nonce、ciphertext |
| `POST /connections/{id}/probe` | 请求体 `{}`，成功返回新 MetadataProfile（201）；重复探查追加档案 |
| `GET /connections/{id}/profiles`、`GET /profiles/{id}` | 按项目查询持久化档案，可作为 #6 的冻结输入 |

登记没有更新/删除入口：每次登记产生不同凭据引用，版本固定为 1，不能覆盖既有 SecretReference。`SecretReference` 持久化 `id,tenant_id,project_id,version,nonce,ciphertext,created_at`；加密使用 AES-GCM，关联数据绑定 `[tenant_id,project_id,id,version]`，密文不能移至另一引用或范围解密。只有应用内受管工具取得 SecretProvider，不提供解密 API。Connection 与 MetadataProfile 的复合外键绑定相同租户、项目、连接、凭据引用及版本。

Connection 持久化 `id,tenant_id,project_id,name,kind,direction,resource_id,resource_fingerprint,secret_reference_id,secret_version,status,created_at`。状态为 `registered/ready/failed`；失败不会伪造档案，也不删除旧档案。MetadataProfile 为追加记录，字段为 `id,tenant_id,project_id,connection_id,secret_reference_id,secret_version,resource_id,resource_fingerprint,schema_hash,content,created_at`。API 返回的 `content` 形状：

```json
{
  "server_version": "8.0.46",
  "version_kind": "server",
  "database": "connector_source",
  "tables": [{
    "name": "customers",
    "columns": [{"name": "balance", "type": "decimal(18,4)", "nullable": true, "key": ""}],
    "approx_rows": 3, "approx_bytes": 16384,
    "statistics_kind": "server_estimate",
    "samples": [{"balance": "[已脱敏]"}]
  }],
  "budget": {"sample_rows_per_table": 5, "max_tables": 8, "query_timeout_seconds": 3,
             "max_seconds": 20, "max_queries": 32, "queries_used": 12},
  "policy_version": "p3-connectors-v1"
}
```

以上数字为形状示例，实际来自真实数据库。Doris 的 `SELECT VERSION()` 返回 MySQL 协议兼容字符串 `5.7.99`，因此 `version_kind=mysql_protocol_compatibility`；真实部署 FE/BE 版本需健康记录的 Version 字段，当前验证为 `doris-3.0.8-rc01-09b0cc49a6`，不把兼容字符串当作 Doris 发布版本。

`schema_hash` 是 `[{name,columns}]` 的规范化 JSON SHA-256，不含样本、统计或时间。#6 应冻结档案 ID、schema_hash、源/目标连接 ID、secret_reference_id+secret_version、resource_fingerprint，重新授权后再使用 `get_connection(db, tenant_id, project_id, connection_id)` / `get_profile(db, tenant_id, project_id, profile_id)`；这两个范围查询函数本身不替调用方做角色授权。Profile 外层契约可被后续 PostgreSQL/CSV 复用，不要求未来源都有凭据；本票模型的凭据外键仅针对已实现的数据库连接，#5 可增量演进可空字段。

## P3 固定工具与证据

所有 SQL 在受管驱动出口固定生成。最多 8 表、每表 64 字段、每表 5 样本、32 查询，总时限 20 秒（查询前后检查）；服务端每查询限 3 秒，网络连接/读/写分别 3/4/3 秒。MySQL 设置只读事务；Doris 无 MySQL 式只读事务，先验证真实只读授权且只执行固定 SELECT 与会话限时命令。墙钟预算不能即时抢占驱动正在执行的调用；单条在途调用由驱动/服务端超时约束，超时不产生成功档案。

从 information_schema 获取字段实际类型与服务端近似行数/字节，统计可能为 0 或空值，不冒充精确计数。样本在数据库 SQL 内全部替换为 `[已脱敏]`（保留 NULL），原始业务值不穿过驱动出口；这是保守全列遮蔽，不做部分邮箱展示。密码、原始样本、URL 与驱动异常文字不进入 API、日志、证据或后续模型输入。错误只返回固定中文诊断及错误码，业务失败与拒绝写入安全证据；注册/成功档案/失败状态及证据在一个 PostgreSQL 事务内。

事件为 `connection.registered / connection.probed / connection.probe_failed / connection.probe_rejected / connection.rejected / connector.denied`，包含操作者、连接/资源/引用/档案 ID、风险级别 P3、策略版本、安全失败码。跨范围拒绝事件记录在操作者租户链，不向目标项目泄漏事实。证据不包含用户显示名称或样本值。

## 真实验证

依赖安装使用 `backend/requirements.txt`；先启动真实 PostgreSQL/Redis、MySQL 与 Doris，再初始化 demo/isolation 租户。全程不以模拟元数据或只回显驱动错误作为通过证据。

```powershell
python scripts/verify_connector_probe.py --secrets .runtime/connectors/secrets.json --kind mysql
python scripts/verify_connector_probe.py --secrets .runtime/connectors/secrets.json --kind doris
python scripts/verify_connectors_http.py --output artifacts/issue-4/http.json
# 保留数据卷与同一主密钥重启 API 后：
python scripts/verify_connectors_http.py --recheck --output artifacts/issue-4/persistence.json
```

驱动验收覆盖真实 schema/type/statistics/masked samples、错误密码安全失败、真实 root 可写账号被拒。HTTP 验收覆盖持久化引用、档案读取、重复版本不可覆盖、SQL/资源/样本预算注入、跨租户/项目/资源范围、撤权和签名证据哨兵扫描。`--kinds mysql` 只声明 MySQL 覆盖；默认 `mysql,doris`。结果只输出安全结论，私有状态保存在 `.runtime/connector-verification.json`。重启复查要求旧档案原样存在、引用版本不变并可用原加密凭据再次连真实源。

真实 API 缺密钥启动失败与 PostgreSQL 存储密文扫描、浏览器端表单操作/查看 schema/统计/脱敏样本属于整栈验收，应将实际运行结果单独附在 artifacts；本说明不替代验收记录。
