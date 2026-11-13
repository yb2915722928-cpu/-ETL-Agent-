# V02 审计证据与离线验签

审计页提供真实身份操作历史、完整证据导出、公钥与独立检查点下载。项目审计仅允许当前项目的 `auditor` 或 `project_admin`；租户账本仅允许启用的租户管理员。每次请求重新检查现有身份和项目成员关系，租户管理员不会因此自动获得其他项目的导出资格。

## HTTP 契约

所有接口使用既有登录 Cookie。`GET /api/evidence/public-key` 返回 `{algorithm: "Ed25519", key_id, public_key}`，其中公钥是 Base64 编码的原始 32 字节，`key_id` 是原始公钥的 SHA-256 十六进制指纹。

| 范围 | 路径 |
| --- | --- |
| 租户完整账本 | `/api/tenants/{tenant_id}/evidence` |
| 项目完整账本 | `/api/tenants/{tenant_id}/projects/{project_id}/evidence` |

在以上路径追加 `/export` 下载完整 JSON；追加 `/checkpoint` 只返回签名检查点。基础路径返回同样的完整 JSON 供页面读取。无分页、过滤或省略，首版面向有限规模账本。

导出格式为 `{format: "etl-evidence-v1", events: [...], checkpoint: {...}}`。事件包含 `id, tenant_id, project_id, sequence, occurred_at, actor_id, action, facts, version_id, preparation_id, run_id, artifact_hashes, previous_hash, event_hash`。序号从 1 连续递增，创世前序哈希为 64 个 `0`。租户链记录该租户全部事件；项目链为独立投影，只有该项目事件，因此不会通过过滤租户链泄露其他项目或制造序号缺口。两条链中的同一事实具有各自事件 ID、序号和哈希。

检查点包含 `format: "etl-evidence-checkpoint-v1", tenant_id, project_id, sequence, event_hash, issued_at, key_id, signature`。租户检查点的 `project_id` 为 `null`。签名为 Base64 编码的 Ed25519 签名。签名输入是去掉 `signature` 字段后的检查点 JSON；事件哈希是去掉 `event_hash` 字段后的事件 JSON 的 SHA-256。规范 JSON 使用 Python `json.dumps(ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)`，编码 UTF-8；时间统一为带 `+00:00` 的 UTC ISO 格式。跨语言验签实现需采用相同字段和值的数值序列化规则。

## 持久化与追加契约

`app.evidence.append_event(db, actor, action, facts, project_id=None, *, version_id=None, preparation_id=None, run_id=None, artifact_hashes=None)` 是后续业务事务的复用入口。既有 `app.security.append_event` 导入保持兼容。调用者先授权操作，传入已经持久化的真实主体、归属正确的项目和显式安全元数据；业务写入与调用处在同一个 PostgreSQL 事务中。该函数分配租户序号、保留身份事件兼容记录并追加租户/项目哈希链，执行 flush，但不 commit。事务失败时业务和证据同时回滚。事实中不得放入密码、凭据、Cookie、原始业务样本或完整请求体；制品只记录标识与哈希。

租户行锁串行化该租户的追加和导出，保证一次导出读取同一完整前缀与链头。导出不重新计算或修补数据库中已存的事件哈希。

API 和 bootstrap 均调用 `initialize_database()`。迁移在 PostgreSQL advisory lock 保护的事务内增建 `evidence_events`、`evidence_heads` 和 `schema_migrations`，按原序号回填所有 `identity_events`，成功后记录 `v02-evidence-chains`。不删除、替换或重新编号原有账号、项目和历史身份事件，不重建数据库。重复启动不会重新生成历史证据 ID 或哈希。升级应先停止旧 API 写入者，再启动新版本；混跑不会写证据投影的旧代码不受支持。

## 部署密钥

`EVIDENCE_SIGNING_KEY` 必须由部署环境注入，是 Base64 编码的 Ed25519 原始 32 字节私钥种子。缺失或无效时 API 启动失败，不生成临时密钥。`scripts/init_local.py` 在已有本地 `.env` 缺少该项时追加随机种子，保留原数据库密码和其他配置；重复运行保留签名密钥。Compose 将其注入 API，生产环境通过部署密钥管理系统提供同名环境变量。初始化脚本仅输出完成提示，不输出密钥内容。运行环境必须安装固定的 `cryptography==45.0.4`。

私钥不放入数据库、导出、响应、日志或验收材料。备份密钥时与数据库分开管理。首版不提供自动轮换、多密钥证明或第三方存证；更换私钥后需重新确认和分发公钥，保留旧公钥以核验旧检查点。

## 独立保存与验签

从经部署者核实的来源取得公钥并核对指纹，将公钥与检查点复制到不受平台写权限控制的位置。导出内不会提供供验签器自动信任的公钥。仅从同一可疑服务同时取得证据、公钥和检查点无法建立独立信任。

独立机器只需要 Python 与 `cryptography==45.0.4`，将 `scripts/verify_evidence.py` 单独复制过去即可，无后端代码、网络、数据库或 Redis 依赖：

```powershell
python scripts/verify_evidence.py evidence.json --public-key trusted-key.json
python scripts/verify_evidence.py evidence.json --public-key trusted-key.json --checkpoint externally-saved-checkpoint.json
```

成功退出码为 0，失败为 1，标准输出始终为 JSON。修改会定位事件哈希错误；删除、重排会定位序号或前序哈希错误；检查点签名和链头必须匹配。外部检查点需属于相同租户/项目、签名可信且其序号及哈希出现在当前完整前缀中。

识别尾截断依赖独立持有者事先保存的检查点。例如平台原本已到第 20 条，独立持有者保留了第 20 条检查点，后来提供到第 15 条的旧合法导出：单看旧签名可以通过，但与第 20 条检查点比较会报告 `tail truncation`。第 10 条检查点不能证明第 11–20 条从未存在；外部检查点之后的删除仍可能不可见。

安全边界是受信代码、数据库事务、可信公钥分发、私钥隔离和独立检查点持有者。只有数据库修改权限且无法获得签名私钥的攻击者，其修改可被验签发现；同时控制代码、私钥、公钥分发及外部检查点的特权管理员不在此保证范围内。签名证明受信签名者认可某个前缀，不证明操作本身合法、也不承诺绝对不可篡改。

## 验证入口

```powershell
python scripts/test_evidence_cli.py
python scripts/verify_evidence_http.py --url http://127.0.0.1:8000 --output artifacts/issue-3/http.json
# 保留数据库卷重启新版本服务后：
python scripts/verify_evidence_http.py --recheck --output artifacts/issue-3/persistence.json
```

HTTP 脚本需要与 V01 相同的 demo/isolation bootstrap 凭据，可用 `--credentials`、`--other-credentials` 指定私有路径；导出及独立检查点默认写入忽略提交的 `.runtime/evidence-verification`。它通过公开接口核对历史事实、项目/租户/角色隔离、既有会话撤权、失败写入、CLI 篡改定位，以及实际重放旧合法导出时的尾截断检测。脚本会在原演示项目追加审计角色事件，并创建一个独立验证项目后撤销自身成员关系，不修改原始演示账号密码。
