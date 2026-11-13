# #5 PostgreSQL / CSV 实现验收记录

验收时间：2026-11-13。工作分支 `feat/issue-5-postgres-csv`，功能提交 `b509e6f`；随后以 `1168ff3` 合并当时最新 `integration/issues-3-7`（`d004ee0`，包含 #6）。验收只使用真实源与公共工具/HTTP 边界；没有内部函数测试、模拟源或成功档案回退。

## 已执行与证据

| 边界 | 实际执行与结论 | 记录 |
|---|---|---|
| PG 受管工具 | psycopg 连接真实独立 PG 17.11，customers/orders 的真实字段、numeric(18,4)、统计与全列脱敏；错误密码固定安全失败；真实可写 admin 被拒 | [PG工具](postgresql-probe.json) |
| CSV 受管工具 | 五个真实文件：普通 UTF-8 3 条、含跨行及转义引号 2 条；坏引号、行宽、编码整批 SourceParseFailure，安全位置，不输出原文 | [CSV工具](csv-probe.json) |
| PG 项目 HTTP | 登记与不可变 SecretReference、真实探查/档案、重复登记不同引用、注入拒绝、跨租户/项目与撤权、签名证据和已知秘密/敏感值扫描 | [PG HTTP](postgresql-http.json) |
| CSV 项目 HTTP | 真实字节上传/登记/探查与档案；坏文件422且无档案；2 MiB上限；Maker、租户/项目/目录白名单、猜测ID、撤权与安全证据；原文读取路径404 | [CSV HTTP](csv-http.json) |
| 真实 PG 停服 | 停止 etl-connector-postgres，公开 probe 返回502 external_unavailable，failed，旧档案保留且无新档案；finally 中启动恢复，随后复查成功 | [PG停服](postgresql-unavailable.json) |
| 真实 CSV 存储失败 | 暂移实际受管文件，probe返回502并有安全source_ref；恢复后实际改动字节，422 SourceParseFailure且不新增档案；finally恢复原字节，再探查201 | [CSV存储](csv-storage-failure.json) |
| 保持数据重启 | 重启独立API；原PG加密凭据再连真实源、原CSV文件再完整解析、旧不可变档案内容相同，重复迁移启动正常 | [PG持久化](postgresql-persistence.json)、[CSV持久化](csv-persistence.json) |

HTTP 服务为临时 `etl-issue5-review`，仅绑定127.0.0.1:8005，复用已有API镜像/控制PG/Redis/私有运行目录，worktree backend只读挂载。没有重建正式API/Web；临时服务在验收完成后停止。PG源为独立 `etl-connector-postgres`（17.11），账号 `source_ro`、database `connector_source`；控制面账号/database为另一实例的 `etl` / `etl_control`，没有授予源账号控制库权限。

## 构建与复现范围

- Python compileall：backend/app 与新增/修改公共验证、初始化、白名单脚本通过。
- `npm run build`：合入#6后通过，17模块，JS114.34kB/gzip41.59kB；没有新增前端依赖。
- `git diff --check`：通过；docs合并冲突保留#4初始化入口并更新为#5完整支持类型。
- `prepare_postgresql_csv_demo.py --initialize` 已在现有真实PG幂等执行；`docker compose ... config --no-env-resolution --quiet` 通过，卷名与现有真实源一致。没有为验证提炼入口重建源卷，不宣称冷机器完整安装。
- 父集成会话已在正式8000/8080部署并复验PG/CSV公开接口：[PG整栈](http-postgresql.json)、[CSV整栈](http-csv.json)。真实Chrome通过PG登记/探查、原生numeric类型、跨行CSV上传与脱敏、坏UTF-8安全源/行/列诊断及无成功档案；页面无已知密码或原始敏感值，无浏览器异常，见[浏览器结果](browser.json)、[正常CSV](csv-valid.png)、[失败CSV](csv-failure.png)。

私有bootstrap、源密码、cookie、完整HTTP连接/档案状态和原CSV均仅在忽略的`.runtime`，不在这些安全结论文件。CSV无schema时保留text类型而不猜测；最多2 MiB、64列、五条样本，UTF-8/英文唯一表头为明确支持范围。PG白名单/只读检查保守拒绝继承角色与可执行业务函数；它面向专用只读账号，未声明支持所有生产授权布局。源停服与文件异常已恢复，根整栈及其他worker服务保持运行。
