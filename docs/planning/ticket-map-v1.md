# ETL-Agent 首版实施票发布映射

用户于 2026-11-13 确认按 V01–V28 发布。父规格为 [#1](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/1)，完整切片与验收见[纵向实施票拆分](ticket-breakdown-v1.md)。本次只发布工单并核对关系，未开展业务实现，未修改或关闭父规格。

## 发布及核对结果

- 28 张实施子票已发布，均为 open、ready-for-agent、未认领；V 编号是方案标识，不能按数字推算 GitHub 票号。
- 28 条父票关联经写后读回及完整子票快照核对；工具仅证明 parentKey 为 #1，不能据此断言后端采用原生子层级还是契约链接。
- 35 条直接阻塞边经写后读回及完整快照核对，后端证据明确为原生依赖边；与确认方案一致，无循环，不重复登记传递依赖。
- 当前可接前沿只有 [V01 / #2](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/2)。ready-for-agent 表示规格可供接取，实际开工仍受阻塞边和运行条件限制。
- 两次中断仅造成现有 V23 / #22、V26 / #25 的关系未写完；核对现有正文后补齐缺边，未重复创建工单。
- 以下为发布时快照；后续状态以 GitHub 和 deck 实时查询为准，不以本表替代处理报告或验收证据。

## V 编号与真实票号

“被阻塞于”仅列直接依赖；所有行的父规格均为 #1。

| 方案标识 | GitHub 实施票 | 被阻塞于（GitHub 票号） |
|---|---|---|
| V01 | [#2 初始化、登录与隔离项目协作](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/2) | 无 |
| V02 | [#3 审计事件导出与独立验签](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/3) | #2 |
| V03 | [#4 受管凭据及 MySQL/Doris 连接探查](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/4) | #3 |
| V04 | [#5 PostgreSQL 与 CSV 资产探查](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/5) | #4 |
| V05 | [#6 手工 EtlPlan 与确定性派生审查](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/6) | #4 |
| V06 | [#7 冻结 PipelineVersion 与版本 Diff](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/7) | #6 |
| V07 | [#8 冻结 Preparation 与独立双人审批](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/8) | #7 |
| V08 | [#9 单次 Commit 与原子授权接受](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/9) | #8 |
| V09 | [#10 异步受管命令与 L1 幂等执行](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/10) | #9 |
| V10 | [#11 监督契约、指标与预算闭锁](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/11) | #10 |
| V11 | [#12 终止确认、撤权与发布前清理](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/12) | #11 |
| V12 | [#13 真实 Zeta 受管搬运至 Doris 影子表](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/13) | #12 |
| V13 | [#14 真实引擎待核对恢复与旧写入隔离](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/14) | #13 |
| V14 | [#15 质量双分支、错误记录与对账报告](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/15) | #13 |
| V15 | [#16 完整全量结果的质量受控原子发布](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/16) | #14、#15 |
| V16 | [#20 交换故障核对与控制状态恢复](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/20) | #16 |
| V17 | [#23 订单水位增量与完整历史保留](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/23) | #20 |
| V18 | [#26 当前维度变化刷新历史订单](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/26) | #23 |
| V19 | [#27 双审批回退上一快照与增量状态](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/27) | #26 |
| V20 | [#17 双审批凭据轮转与绑定失效](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/17) | #13 |
| V21 | [#21 可选 Vault Profile 的真实凭据闭环](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/21) | #17 |
| V22 | [#18 真实模型需求澄清与 Checkpoint 恢复](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/18) | #7 |
| V23 | [#22 有限自动修复与模型 Profile 切换](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/22) | #18 |
| V24 | [#19 Beat 每次触发独立待审批申请](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/19) | #10 |
| V25 | [#24 PostgreSQL/CSV 真实搬运与解析失败闭锁](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/24) | #5、#15 |
| V26 | [#25 真实模型生成 Benchmark 与逐例报告](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/25) | #5、#22 |
| V27 | [#28 安全 Benchmark 与受控策略激活](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/28) | #27、#17、#19 |
| V28 | [#29 六界面联合验收与可重复交付包](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/29) | #21、#24、#25、#28 |

## 接取规则

从当前未关闭、未认领且所有阻塞票已完成的前沿接取。V01 完成后才解锁 V02；V03 后可并行 V04/V05，V06 后模型分支与审批分支可并行。开始、切换或完成实际处理时按仓库工具规则上报，不以发布记录冒充实现进度。T00–T13 仅保留为[早期技术主题参考](implementation-plan.md)，不另行按该编号创建重复工单。
