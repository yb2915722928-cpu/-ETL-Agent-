---
status: accepted
---

# 一次 PostgreSQL 授权事务与可重复投递的受管命令

用户已确认在一次 PostgreSQL 事务中消费短时单次 Capability、创建 ExecutionRun 并写入 Outbox；Worker 验证派生 ManagedCommand，不再次消费原令牌。相比将授权事实分别写入数据库和消息队列，此方案把本地授权完整性集中在一个事务，接受 Outbox/Celery 至少一次投递，并分别用授权入口防重放、命令幂等和外部作业核对处理重复；不宣称 PostgreSQL、Redis、SeaTunnel、Doris 的分布式 exactly-once。

代价是必须持久化未知状态和恢复证据：引擎提交超时不能因消息重发而盲目重提，不能把没有 jobId 当成没有作业；5分钟是 Capability 消费窗口，不是默认600秒运行时长预算或异步命令的寿命。最终版本的作业关联与取消能力仍需实测；具体命令封装、Interface和状态名是拟议设计，见[架构](../architecture.md)与[首版规格](../specs/etl-agent-v1.md)。
