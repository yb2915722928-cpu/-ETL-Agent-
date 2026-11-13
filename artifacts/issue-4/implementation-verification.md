# V03 实现边界验证记录

日期：2026-11-13（Asia/Shanghai）。工作分支 `feat/issue-4-managed-connectors`，基于 #3 已实现的 PostgreSQL/Redis 身份和证据链。

已实际执行：

- `scripts/verify_connector_probe.py --kind mysql` 对独立真实 MySQL 容器验证通过：8.0.46；customers/orders 的真实 schema、decimal(18,4) 类型、服务端近似统计、数据库侧全列遮蔽样本；错误密码返回固定安全诊断；root 可写账号在读取业务数据前被只读策略拒绝。
- 同一脚本 `--kind doris` 对独立真实 Doris FE/BE 验证通过：customers/orders schema/type/statistics/masked samples、错误密码失败、root 可写账号拒绝。MySQL 协议 `SELECT VERSION()` 是 `5.7.99`，Profile/UI 已标识为兼容版本；环境健康记录的实际 FE/BE Version 为 `doris-3.0.8-rc01-09b0cc49a6`。
- 真实调用应用 `app.router.lifespan_context(app)`，未设置 `SECRET_MASTER_KEY` 时在数据库迁移前失败，固定 RuntimeError 指向缺失主密钥；未连接替代数据库、未修改根部署密钥。
- Python compileall、OpenAPI schema 生成通过；Vue `npm ci --no-audit --no-fund` + `npm run build` 通过；`git diff --check` 通过。

这些是驱动/启动边界与构建结果。完整真实 HTTP、数据库密文扫描、保卷重启复查、中文浏览器交互由集成环境运行 `scripts/verify_connectors_http.py` 及 `--recheck` 并保存独立结果，本记录不提前宣称这些整栈验收通过。没有将私有凭据、请求 Cookie、原始业务样本或驱动错误写入此材料。
