---
status: accepted
---

# 完整快照单表 Swap 与可核对恢复

用户已确认先构建包含历史结果的完整 ShadowTable，通过质量门禁后使用 Doris 单条 `REPLACE WITH TABLE … swap=true` 发布；不能将本批增量直接替换完整正式表，也不承诺 PostgreSQL/Doris 跨库事务。相比原位写入或把多个重命名动作视为原子发布，此方案以额外完整结果存储和同一物理目标串行执行为代价，换取单表发布切换及最近一次旧快照恢复的清晰范围。

交换后的 shadow 承载原正式数据；最近一次旧表默认保留7天，发布后回退另行双审批，并恢复对应的规范化增量状态与水位。必须持久化发布意图、核对外部表身份后提交控制事实；结果不明时禁止盲目再交换，未确认引擎终止时禁止清理。接受的是这些发布与恢复策略，具体表身份核对方式、故障状态和 Interface 仍为拟议设计，所选版本兼容性未验证；详见[架构](../architecture.md)、[首版规格](../specs/etl-agent-v1.md)及[Doris官方语义](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/ALTER-TABLE-REPLACE/)。
