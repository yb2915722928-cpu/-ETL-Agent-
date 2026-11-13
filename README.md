# ETL-Agent 数据集成平台

面向异构数据集成的受管 ETL 控制面：自然语言需求经结构化计划、确定性门禁、独立审批和单次授权，交由 SeaTunnel 执行，再依据质量报告完成受管发布与恢复。

当前交付是**用户已确认的首版规格与实施前规划**。仓库尚未提供可运行业务代码、Compose 配置或测试成绩；文档中的运行环境与流程是后续交付要求，不代表已经运行通过。

## 阅读入口

| 文档 | 用途 |
|---|---|
| [正式规格发布稿](docs/specs/etl-agent-v1-issue.md) / [GitHub #1](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/1) | 已确认用户故事、实施决策、TS-01–TS-09 测试边界；spec / ready-for-agent |
| [首版规格与验收矩阵](docs/specs/etl-agent-v1.md) | 首版范围、用户行为、阈值、验收证据及失败条件 |
| [领域术语](GLOSSARY.md) | PipelineVersion、Preparation、ExecutionRun、质量与恢复的共同语言 |
| [架构设计](docs/architecture.md) | 模块 Interface、授权事务、执行状态、发布与故障核对 |
| [纵向实施票](docs/planning/ticket-breakdown-v1.md) / [真实票号映射](docs/planning/ticket-map-v1.md) | 已发布 V01–V28，28 张子票、35 条直接阻塞边及接取前沿 |
| [早期技术主题计划](docs/planning/implementation-plan.md) | T00–T13 技术主题与阶段门槛参考；实际接取按纵向实施票 |
| [架构决策](docs/adr/) | 影响后续实现的权衡及理由 |

## 首版基线

- Vue 控制台、FastAPI、LangGraph、Celery Worker/Beat、PostgreSQL、Redis、SeaTunnel Zeta 与 Docker Compose 初始化环境。
- MySQL、PostgreSQL、CSV 源到 Doris 目标；真实模型通过 OpenAI 兼容端点生成计划，演示 Provider 的成绩不计入模型评测。
- `prepare → approve → commit`，Maker 与独立审批职责分离，冻结版本与准备单，单次授权、事务 Outbox、可核验证据。
- 完整影子表质量通过后进行单表原子交换；超限则取消并阻止发布，支持发布前清理及受管恢复上一版本。
- 两条真实业务演示、L0/L1/L2 Benchmark；功能与安全先验收，性能如实报告。

## 首先验证什么

实施计划从环境及引擎事实开始：核实 Docker 服务、资源、模型调用条件和固定版本；实际验证提交超时核对、取消终止、质量对账、字节指标与 Doris 表交换。未知行为不能用模拟结果或文档推断代替 L2 验收。

CSV 等源输入无法可靠解析时整批失败，业务脏数据仅在可靠形成记录后分流。监督器检测阈值后取消，不能保证零超调；发布与控制面状态之间通过可恢复核对达成一致，不能宣称跨数据库原子事务。

## 协作

仓库采用单一领域语境，见 [领域文档约定](docs/agents/domain.md)。正式规格与工单后端为 [GitHub Issues](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues)，规则见 [工单约定](docs/agents/issue-tracker.md) 和 [分诊标签](docs/agents/triage-labels.md)。正式规格已发布为 [GitHub #1](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/1)，标记 ready-for-agent。经用户确认，28 张纵向实施子票已发布为 #2–#29，全部关联 #1 并标记 ready-for-agent，35 条直接阻塞边已核对；对应关系见[真实票号映射](docs/planning/ticket-map-v1.md)。当前仅 [V01 / #2](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/2) 无阻塞。T00–T13 保留为早期技术主题参考，不作为另一套待发布工单。工单就绪不表示功能已实现或真实链路已验收。
