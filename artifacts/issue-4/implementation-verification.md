# V03 实现边界验证记录

日期：2026-11-13（Asia/Shanghai）。工作分支 `feat/issue-4-managed-connectors`，基于 #3 已实现的 PostgreSQL/Redis 身份和证据链。

已实际执行：

- `scripts/verify_connector_probe.py --kind mysql` 对独立真实 MySQL 容器验证通过：8.0.46；customers/orders 的真实 schema、decimal(18,4) 类型、服务端近似统计、数据库侧全列遮蔽样本；错误密码返回固定安全诊断；root 可写账号在读取业务数据前被只读策略拒绝。
- 同一脚本 `--kind doris` 对独立真实 Doris FE/BE 验证通过：customers/orders schema/type/statistics/masked samples、错误密码失败、root 可写账号拒绝。MySQL 协议 `SELECT VERSION()` 是 `5.7.99`，Profile/UI 已标识为兼容版本；环境健康记录的实际 FE/BE Version 为 `doris-3.0.8-rc01-09b0cc49a6`。
- 真实调用应用 `app.router.lifespan_context(app)`，未设置 `SECRET_MASTER_KEY` 时在数据库迁移前失败，固定 RuntimeError 指向缺失主密钥；未连接替代数据库、未修改根部署密钥。
- Python compileall、OpenAPI schema 生成通过；Vue `npm ci --no-audit --no-fund` + `npm run build` 通过；`git diff --check` 通过。

集成环境追加验收已通过：

- `scripts/verify_connectors_http.py`：真实 MySQL/Doris 全流程，角色撤销立即生效、跨租户/项目/档案隔离、禁止 SQL/表/预算覆盖、旧凭据不能覆盖、错密失败不生成伪档案，成功与失败均记录安全证据。结果见 `http.json`。
- 中文浏览器真实登记、探查、查看档案和脱敏样本；保存后密码框清空，页面无已知凭据和合成敏感原值，浏览器无运行错误。结果见 `browser.json`、`mysql.png`、`doris.png`。
- 控制 PostgreSQL 数据导出与 API/web 日志扫描 24 个已知敏感值，未发现连接/账号/部署密钥明文或合成敏感原值；独立 API 容器缺少主密钥时实际启动失败（退出码 3）。结果见 `security.json`。
- 保留数据卷与原主密钥重启 API 后，原档案逐项相同，原加密凭据可重新探查真实服务器并取得相同 schema_hash。结果见 `persistence.json`。

没有将私有凭据、请求 Cookie、原始业务样本或驱动错误写入此材料。
