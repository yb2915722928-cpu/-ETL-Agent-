# #11 V10 验收记录

基线与范围：`impl/issue-11` 从 `integration/issues-11-17` 的 `427ad2d` 开始；收口前执行 `git merge integration/issues-11-17`，结果 `Already up to date.`。工单认领与写回由主代理统一处理，不关闭 #11。

实现：冻结 RuntimeSupervisionContract（默认 100000 行、268435456 byte、600 秒、2 倍、20%、约 2 秒观测；6 秒陈旧阈值）；阶段、去重、实测字节、分母与样本规则随版本/准备单/签名命令绑定。接受事务创建持久监督行，Worker 累积原始观测、指标、来源/状态、原因、发布 veto、停止 intent 和投递结果。先提交停止 intent 再发 HTTP；未知投递用同一请求 ID 重试。缺失/陈旧/重置/估算不显示为零，历史仍保留。可靠最终结果只解除监督 veto，不构成发布授权。

验证命令与结果：

- `PYTHONPATH=backend python scripts/test_runtime_supervision.py`：2 个必要公共 evaluator 测试通过；覆盖 99/100、最终小样本、其他预算无样本豁免、缺失/陈旧/重置/估算及零分母。首次 tracer 在实现前因 ModuleNotFoundError 变红。
- `python -m compileall -q backend/app scripts/verify_supervision_l1.py scripts/l1_engine_fixture.py`：通过。
- 使用既有 L1 API image、只读挂载本工作树源码与根身份目录，`docker compose ... run --rm --no-deps ... api python /tests/verify_supervision_l1.py /results/l1.json`：真实 PG 克隆 Schema、唯一 Redis 队列、公钥 Celery Worker 与独立 HTTP+SQLite Engine；公开探查/冻结→准备→双审批→Capability→Commit→GET runs 入口；12 场景全绿。完整复现命令见 `docs/execution-l1.md`。
- 12 场景：运行中 99/100、最终小样本、行数/字节/时长/放大预算、缺失/陈旧/计数重置/估算字节、零分母、可靠最终完成；持久停止/veto、目标占有保留、GET 无副作用、重复投递无新作业或新停止请求均通过。报告 `l1.json`，12 个 Engine job。
- `npm ci --ignore-scripts --no-audit --no-fund` 与 `npm run build`（frontend）：Vite 生产构建通过。
- `node scripts/verify_supervision_browser.cjs`：本机 Edge、实际生产 Vue bundle 回放上述已持久化 L1 HTTP 事实，中文停止/veto/原因、缺失与零分母展示通过，无 pageerror。报告 `browser.json`，截图 `budget-stop.png`、`missing-metrics.png`。该 smoke 与真实 API/Worker 验证分开，不将回放称为真实浏览器运行服务。
- `git diff --check`：通过。

隔离与限制：未修改、重建或停止根部署容器，未写根运行目录。未读取或输出秘密值；脚本通过既有密钥挂载与应用接口验证。L1 验证的是控制平面监督预算，不是真实 SeaTunnel 数据面资源限制或 L2；约 2 秒轮询和停止延迟可能超调，不声称零超调。

#12 接口：`backend/app/supervision_runtime.py::request_stop(db, run, row, reasons, now)` 在 command advisory lock + tenant-first 事务内复用；`RuntimeSupervision.stop_request` 含 id/detected_at/requested_at/reasons/delivery_status/termination_confirmed=false；`RuntimeObservation` 保留检测时原始样本及决策；`supervise_locked(command_id)` 在持久 intent 后投递原 Engine/原作业 stop。ACK 仅代表请求收到，不终止确认、不释放 TargetOwnership、不清理资源。#12 接续终止确认、最终指标/超调、撤权和清理。
