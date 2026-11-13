---
status: accepted
---

# 以结构化 EtlPlan 作为唯一可编辑执行定义

用户已确认首版由 Maker 编辑 EtlPlan、字段映射和质量规则，以确定性渲染派生 HOCON 和 DAG，通过门禁后冻结不可变 PipelineVersion；不同时开放任意 HOCON 编辑和拖拽执行拓扑。相比两套可编辑定义，这牺牲配置自由度，换取审批所见计划、字段血缘和实际执行配置的一致性，避免手工文本绕过受管连接或 Secret 引用。修改冻结内容必须生成新版本并重新准备审批；自动修复最多两次，不能自动降低质量或预算要求。

本决策接受的是产品策略，不代表某个渲染器、Schema映射或模型输出已通过验证。具体 Interface、canonical摘要和版本实体仍为拟议设计，见[架构](../architecture.md)；验收范围见[首版规格](../specs/etl-agent-v1.md)。
