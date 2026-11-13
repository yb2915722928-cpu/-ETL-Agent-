# V03 / V04：受管连接、CSV 与元数据档案

GitHub #4 实现 MySQL 源与 Doris 目标，#5 扩展 PostgreSQL 源与项目受管 CSV 的登记、测试与只读元数据探查。中文控制台“连接与资产”按项目选择资源。当前项目成员可读；当前具有 `maker` 角色的成员才可登记和探查。租户管理员、项目管理员不会自动取得 Maker 权限。每次请求重新检查身份与成员角色，已有会话撤权立即生效。

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

`direction` 由类型确定：MySQL、PostgreSQL、CSV 为 `source`、Doris 为 `target`。PostgreSQL 资源额外必填 `schema`（例如 `public`），与 database 一起约束固定表读取。主机、端口、库、账号和表均来自部署白名单；HTTP 只接收资源 ID、显示名称与密码，不能提交 SQL/URL/主机/表/预算。资源指纹覆盖完整规范化配置。每次登记/探查重新读取策略；既有连接对应策略消失或指纹改变返回 409，需登记新连接。数据库策略是当前单个部署的固定资源授权集合，数据库候选集合共享，连接及其凭据和档案严格按租户/项目隔离。CSV 候选资源额外检查项目白名单。它不是网络防火墙；生产部署还需按对应受管资源限制 API 容器网络出口。

真实外部环境的最小初始化/固定依赖入口见 [infra/connectors](../infra/connectors/README.md)。真实环境私有配置 `.runtime/connectors/secrets.json` 已存在时，可运行 `python scripts/generate_connector_resources.py` 生成无密码白名单。默认生成已实现的 MySQL/Doris/PostgreSQL；`--kinds` 可限制类型，PostgreSQL 从私有配置读取 schema。未知类型会阻止启动，页面也不提供未知连接器的探查操作。外部账号必须只有 SELECT（MySQL 允许 SHOW VIEW/USAGE）；Doris 允许内建 `normal: Usage_priv` 调度查询，其他写权限与继承角色均拒绝。真实只读权限检查先于业务表读取。

## 公共接口与持久化契约

公共前缀为 `/api/tenants/{tenant_id}/projects/{project_id}`。POST/GET 都执行当前项目范围授权；猜测其他项目的连接/档案 ID 不会跨范围读取。

| 接口 | 输入与结果 |
|---|---|
| `GET /connector-resources` | 可登记的安全资源摘要，含 `id/kind/direction/database/tables/fingerprint`，不公开网络地址或账号 |
| `POST /connections` | 数据库输入 `{name,resource_id,password}`；新 Connection 和新 SecretReference 同事务创建，返回 201 |
| `POST /csv-uploads?resource_id=…&name=…` | Maker 上传原始字节，Content-Type 为 `text/csv` 或 `application/octet-stream`；返回持久化 CSV Connection（201），再显式探查 |
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

`schema_hash` 是 `[{name,columns}]` 的规范化 JSON SHA-256，不含样本、统计或时间。#6 应冻结档案 ID、schema_hash、源/目标连接 ID、secret_reference_id+secret_version、resource_fingerprint，重新授权后再使用 `get_connection(db, tenant_id, project_id, connection_id)` / `get_profile(db, tenant_id, project_id, profile_id)`；这两个范围查询函数本身不替调用方做角色授权。PostgreSQL/CSV 复用相同 Profile 外层契约。CSV 的凭据引用与版本为空；Connection 增加 `storage_id/source_sha256/source_bytes`，数据库约束保证 CSV 必须有存储事实且无凭据，数据库连接必须有凭据且无存储 ID。`v04-managed-csv` 增量迁移保留既有连接/凭据/档案，复合外键继续约束数据库凭据；CSV 原始文件保存在白名单根目录的 `tenant_id/project_id/storage_id.csv`，不存入证据或数据库。

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

## V04：PostgreSQL 与受管 CSV

PostgreSQL 使用 psycopg 固定只读事务与 3 秒 statement/lock timeout，读取 pg_catalog 的实际 `format_type`、主键/可空事实、`reltuples` 估计与 `pg_total_relation_size`。数值精度、时区时间、JSON 等保留服务端类型。源账号不能是控制库账号，也不能连接控制库 database；同时拒绝超级用户、建库/建角色/复制/绕过 RLS、任何继承角色、DB CREATE/TEMP、业务 schema CREATE、表/列写入/所有权、sequence 写权限和可执行业务函数。只读检查覆盖源实例当前数据库，业务读取仍只限 schema/table 白名单。生产源需为专用只读账号撤销 PUBLIC 的 TEMP/CREATE 等默认授权。连接串、密码、驱动错误不返回调用者。

CSV 白名单示例（`projects` 是实际项目 UUID 列表；`root` 必须绝对路径且由部署者管理）：

```json
{"acceptance-csv":{"kind":"csv","root":"/run/identity/managed-csv","projects":["<project-uuid>"]}}
```

上传时先检查 Maker、当前项目与资源白名单，再有限读取请求体，最多 2 MiB。服务端生成 storage_id 与 SHA-256，不接受文件路径/URL/外部 storage_id；不使用浏览器原文件名。路径必须留在白名单根目录的租户/项目子目录，API 没有原始文件读取/下载接口。Compose 现有 `.runtime:/run/identity` 挂载持久保存文件；升级保留此目录和控制库卷。原文保留在受限运行目录用于再次探查，API/模型/日志/证据只见脱敏内容和安全存储引用，备份与宿主权限由部署者管理。

格式契约：严格 UTF-8（允许 BOM）、逗号分隔、首行 1–64 个唯一英文字母/数字/下划线字段名（不能数字开头）；支持标准双引号转义和带引号的跨行字段。无外部 schema 的 CSV 字段类型为 `text`，空字段标记 nullable，不猜测日期/金额。探查先完整扫描整批再产出档案；`complete_file_scan` 的行数/字节是全文件事实，最多五行样本全列遮蔽。编码错误、CSV 语法错误、行宽不一致、文件过大或存储摘要改变都让整批以 `SourceParseFailure` 失败，不跳过坏行，不产生新档案。422 返回安全 `source_ref=csv:<storage_id>`、物理行号、字段序号/解码列号（解析器无法定位字段时为 1）；固定原因不含坏值。文件缺失/访问失败同样返回 422 `SourceParseFailure`，安全 source_ref 和初始位置（行 1、列 1），不暴露宿主路径；PG 停服返回 502 `external_unavailable`。中文页展示错误及位置，保留 failed 状态。

复现独立源（与控制 PostgreSQL/账号/数据卷分离）：

```powershell
python scripts/prepare_postgresql_csv_demo.py
# 使用现有已核验的本地 PG 17.11 镜像，不自动拉取；镜像不可用时应先按运行环境准备镜像。
docker compose -f infra/connectors/postgresql.compose.yaml up -d
python scripts/prepare_postgresql_csv_demo.py --initialize
# UUID 取私有 bootstrap 文件的 project_id；可重复 --csv-project 授权多个项目
python scripts/generate_connector_resources.py --csv-project <project-uuid>
# 重启 API 以应用白名单，并保持原主密钥与数据目录
python scripts/verify_connector_probe.py --kind postgresql --secrets .runtime/connectors/secrets.json --output artifacts/issue-5/postgresql-probe.json
python scripts/verify_connectors_http.py --kinds postgresql --state .runtime/pg-verification.json --output artifacts/issue-5/postgresql-http.json
python scripts/verify_csv_http.py --output artifacts/issue-5/csv-http.json
# 保留目录与数据卷重启 API 后：
python scripts/verify_connectors_http.py --kinds postgresql --state .runtime/pg-verification.json --recheck --output artifacts/issue-5/postgresql-persistence.json
python scripts/verify_csv_http.py --recheck --output artifacts/issue-5/csv-persistence.json
```

初始化脚本只在缺失时生成高熵私有配置与 PG env，支持已有多连接器 secrets；创建幂等合成 customers/orders/view、只读账号和五个 CSV（普通 UTF-8、跨行引号、未闭合引号、行宽错误、坏编码）。PG 镜像本地标签记录为 `etl-connectors-postgres:17.11-126f44c116f1`，源镜像 digest 为 `sha256:126f44c116f1541d3c274246bf6193b3247b2cb66d3549b7b11b890d132b4611`；现有独立源已运行时不要重复启动同名容器。脚本不打印秘密，私有文件不提交仓库。工具/HTTP 脚本接受 `--secrets/--credentials/--other-credentials/--fixtures/--state/--url` 中各自适用的参数，允许从独立 worktree 对同一真实服务验证。HTTP 验收断言坏文件不新增档案、跨项目目录白名单/跨租户/撤权拒绝、原始下载不可用和证据哨兵未泄漏。停服验收必须真实停止 PG 后通过公开 probe 核实 502/failed/旧档案仍在，并在 finally 中恢复服务。
