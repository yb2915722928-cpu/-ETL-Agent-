# ETL-Agent 首版：受管设计、独立审批、真实执行与可核对发布

状态：Q1–Q33 产品共识及 TS-01–TS-09 测试边界均已确认，已发布为 [GitHub 规格 #1](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/1)。测试边界确认时间：2026-11-13 11:06（Asia/Shanghai）。类型：spec；标签：ready-for-agent；状态：open。本次仅发布一个规格，不创建实施子票、不开始业务实现。本文不表示功能已实现或验收已通过。

## Problem Statement

数据工程师需要把自然语言集成需求落实为可运行的数据管道，但异构连接、字段语义、敏感信息和质量要求容易在生成与执行之间失真。审批人难以确认实际执行是否就是自己审过的版本，操作员也难以在消息重投、引擎响应丢失或发布中断后判断真实结果。

项目需要一套可本地真实运行、可重复验收的 ETL-Agent 平台，使需求澄清、受管探查、结构化设计、冻结审批、授权执行、质量验证、发布与恢复形成可追溯闭环。首版使用本地真实数据库和合成业务数据验证生产治理机制，不以模拟执行或文档代替真实链路，也不把小规模演示等同企业吞吐或高可用保证。

## Solution

提供中文 Vue 控制台、FastAPI 控制面、可恢复的 LangGraph 设计流程、Celery Worker/Beat，以及 PostgreSQL、Redis、SeaTunnel Zeta、MySQL、Doris 的 Linux 容器 Compose 运行包。

Maker 登记受管连接并探查 MetadataProfile，通过自然语言与澄清生成可编辑 EtlPlan；确定性派生 HOCON 和 DAG，经门禁冻结 PipelineVersion。Preparation 冻结本次操作的资源、质量、预算及恢复事实，由独立数据与安全 Checker 审批；Operator Commit 后，单次 Capability 在一个本地事务内消费并建立运行及 Outbox 命令。SeaTunnel 搬运数据，平台监督预算与质量，达标后发布完整 ShadowTable；异常时阻断发布，核对实际状态后执行受管清理或独立审批的发布后回退。

首版实际覆盖 MySQL、PostgreSQL、CSV 源到 Doris，提供正常发布、脏数据超限中断及发布后回退的真实证据。其余连接器分期交付，界面不得把未实现能力显示为可用。

## User Stories

1. As a 项目管理员, I want 初始化 Tenant、Project 和独立成员账号, so that 每项设计与操作都有明确归属。
2. As a 项目管理员, I want 分配 Maker、数据 Checker、安全 Checker、Operator 和审计角色, so that 各职责由具名成员承担。
3. As a 项目管理员, I want 禁用账号或撤销项目角色后立即影响后续授权检查, so that 旧批准不能绕过当前资格。
4. As a 项目成员, I want 页面、API、下载及后台任务均遵守租户和项目隔离, so that 猜测对象标识不能访问其他范围的数据。
5. As a Maker, I want 登记 MySQL、PostgreSQL、CSV 源和 Doris 目标, so that 使用首版已验证的连接能力完成集成。
6. As a Maker, I want 凭据以受管 SecretReference 使用并支持版本轮转, so that 设计制品中不保存明文密钥。
7. As a Operator, I want 配置默认加密 SecretProvider 或可选 Vault KV v2, so that 凭据按部署条件受管存放。
8. As a Operator, I want 缺少主密钥时相关服务明确拒绝启动, so that 系统不以新密钥掩盖旧凭据不可恢复。
9. As a Maker, I want 执行受限连通测试和只读 MetadataProfile 探查, so that 获取 Schema、类型和近似统计而不越权扫描。
10. As a Maker, I want 查看有数量限制的脱敏样本, so that 判断字段含义而不暴露受保护原值。
11. As a Maker, I want 用自然语言描述源、目标、映射和质量要求, so that 减少手工配置成本。
12. As a Maker, I want 缺少业务参数时获得明确澄清问题, so that 系统不猜测关键业务语义。
13. As a Maker, I want 跨请求或服务重启后恢复原 WorkflowState 的 Checkpoint, so that 补充回答后继续原设计进度。
14. As a Maker, I want 对话、工作流进度和 ExecutionRun 分开保存, so that 生成成功不被误认为执行成功。
15. As a Maker, I want 使用可配置的真实模型 Profile 并主动切换演示 Provider, so that 我知道结果的实际来源。
16. As a Maker, I want 真实模型调用失败显式报错, so that 系统不会静默以模板结果冒充智能生成。
17. As a 安全 Checker, I want 外部模型只接收需求与筛选后的元数据, so that 凭据和原始行样本不被发送出去。
18. As a Maker, I want 编辑结构化 EtlPlan、字段映射与质量规则, so that 生成方案的业务含义可以审查和调整。
19. As a Maker, I want 查看同源派生的只读 HOCON、高亮、下载及 DAG, so that 配置展示与执行定义一致。
20. As a Maker, I want 查看版本间 Schema、映射、HOCON 和质量规则 Diff, so that 了解每次变更的影响。
21. As a Maker, I want 确定性门禁检查语法、Schema、资源、凭据引用、质量与预算, so that 不合法配置无法冻结执行。
22. As a Maker, I want 自动修复最多两次且每次重新校验, so that 修复过程有边界、结果可解释。
23. As a Maker, I want 扩大资源、提高预算或降低保护要求时必须澄清, so that 模型不能自行放宽约束。
24. As a Maker, I want 门禁通过后冻结不可原地修改的 PipelineVersion 和 SHA256, so that 审批与执行绑定同一制品。
25. As a Maker, I want Prepare 冻结本次输入、影响、数据分级、指标口径、预算和恢复计划, so that 审批基于确定事实。
26. As a 数据 Checker, I want 审查需求、字段映射、脱敏结构、DAG 和质量规则, so that 数据语义符合业务目标。
27. As a 安全 Checker, I want 审查资源、SecretReference、敏感分级、预算与恢复范围, so that 执行风险处于批准边界。
28. As a Checker, I want 申请人自批和同人占用两个职责槽被服务端拒绝, so that 职责分离不能通过界面绕过。
29. As a Checker, I want 批准或拒绝被记录为绑定冻结准备单的具名决定, so that 角色标签不能替代实际审批。
30. As a Operator, I want Commit 重查准备单期限、审批、当前权限、策略及指纹, so that 过期或已漂移的事实不能执行。
31. As a Operator, I want 重试相同 Commit 返回同一运行而相同幂等键的不同请求冲突, so that 网络重试不产生重复运行。
32. As a 安全 Checker, I want Ed25519 Capability 绑定主体、工具、环境、资源和制品且短时单次消费, so that 令牌不能被篡改、借用或重放。
33. As a Operator, I want Capability 消费、ExecutionRun 和 Outbox 原子落库, so that 不会出现授权与执行事实割裂。
34. As a Operator, I want 重复 Outbox/Celery 消息只驱动原 ManagedCommand, so that 消息重投不获得新授权。
35. As a Operator, I want 提交响应丢失时先核对原 SeaTunnel 作业, so that 系统不会盲目再提交。
36. As a Operator, I want Worker 只在已批准范围按需物化凭据并经 Tool Broker 调用, so that 任意消息或模型文本不能直接执行副作用。
37. As a Operator, I want 每次定时触发生成独立待审批申请, so that 一次批准不变成无限期周期执行权。
38. As a Maker, I want 清洗和质量校验分开表达且正常、错误分支互斥覆盖, so that 数据处理结果可以逐项解释。
39. As a 数据 Checker, I want 一条拒绝记录只计一次并可包含多个 ErrorCode, so that 错误规则数量不扭曲拒绝率。
40. As a Operator, I want 区分源读取、解析、关联去重、质量判定、有效、拒绝与最终写入计数, so that 报告不混用不同记录阶段。
41. As a Operator, I want 坏 CSV、解码或不完整读取使整批失败并保留脱敏诊断, so that 解析丢数不会被静默跳过。
42. As a 审计人员, I want 错误表敏感字段仅提供脱敏摘要和定位信息, so that 质量诊断不成为泄漏通道。
43. As a Operator, I want 查看实测行数、字节、时长、吞吐、放大比及拒绝率, so that 运行消耗与冻结预算可以比较。
44. As a Operator, I want 超限立即请求取消并禁止发布, so that 不合格结果不会覆盖正式表。
45. As a Operator, I want 分别看到检测、取消请求、终止确认和最终观测值, so that 理解监督延迟与在途超调。
46. As a Operator, I want 必要指标缺失时停止安全推进, so that 未知消耗不会被按零放行。
47. As a 数据 Checker, I want 即使引擎成功也需完整 QualityReport、Schema 和预算检查通过, so that 引擎状态不能替代质量验收。
48. As a Operator, I want 默认空输入不能替换正式表, so that 异常空批不清空有效结果。
49. As a Maker, I want 有界水位增量采用更新时间加主键、重叠回看与主键合并, so that 可验证边界内的重复运行得到一致结果。
50. As a 数据 Checker, I want 当前用户维度变化刷新受影响的历史订单, so that 宽表保持约定的当前维度语义。
51. As a Operator, I want ShadowTable 包含历史保留数据及本批变更, so that 发布增量不会丢失完整结果。
52. As a Operator, I want 同一物理目标跨 Pipeline 串行构建与发布, so that 两次运行不会相互覆盖。
53. As a Operator, I want 质量通过后用 Doris 单表 Swap 发布并保留旧表, so that 切换具有明确原子范围和回退来源。
54. As a Operator, I want Swap 与控制面落库之间故障时核对稳定对象身份, so that 恢复不会误交换或误删数据。
55. As a Operator, I want 仅在发布确认后推进 Watermark, so that 失败或待确认运行不跳过未发布数据。
56. As a Operator, I want 确认引擎终止后清理未发布结果, so that 清理不会与在途写入竞争。
57. As a Operator, I want 另行双审批后回退最近一次保留快照并恢复对应水位和合并状态, so that 后续重放仍有一致起点。
58. As a Operator, I want 旧快照缺失、修改、过期或身份不明时拒绝回退, so that 系统不伪装恢复成功。
59. As a Operator, I want 未知状态或旧 Worker 可能仍运行时保留目标隔离, so that 租约过期不触发危险接管。
60. As a 审计人员, I want 导出带序号、哈希链与签名检查点的关键证据, so that 能独立验证执行历史。
61. As a 审计人员, I want 修改、删除、重排及对照外部检查点的尾截断被检测, so that 账本完整性可验证。
62. As a 安全 Checker, I want 版本化 Benchmark 展示逐例结果、分母、漏拦截与误拦截, so that 安全指标不掩盖失败。
63. As a 安全 Checker, I want 策略建议经过受控验证和评测后才形成适用版本, so that 模型建议不直接改变安全边界。
64. As a Maker, I want 真实模型生成成绩与演示 Provider 分开报告, so that 至少二十例的正确率真实可复核。
65. As a 项目成员, I want 六类中文控制台界面连通设计、审批、运行和审计, so that 无需依赖内部数据库操作完成日常流程。
66. As a Operator, I want 可重复初始化、复位、健康检查和演示的固定版本 Compose 包, so that 没有企业业务库也能验收真实链路。
67. As a 审计人员, I want 正常发布、超限中断及发布后回退各有实际数据比较和证据, so that 不以截图或模拟状态代替业务结果。

## Implementation Decisions

1. **范围与交付**：首版仅 MySQL、PostgreSQL、CSV 源到 Doris；提供 Vue、FastAPI、LangGraph、Celery Worker/Beat、PostgreSQL、Redis、Zeta、MySQL、Doris 和合成数据的 Compose 包；可选 Vault KV v2。控制存储和源端 PostgreSQL 权限及命名隔离。固定版本、镜像与驱动通过真实验证后锁定，不预先承诺文档示例版本兼容。
2. **身份与隔离**：本地账号及密码哈希、项目成员角色；服务端从认证身份推导 Tenant/Project 授权范围，覆盖查询、下载、Checkpoint 与后台命令。验收使用独立 Maker、两位 Checker、Operator 和审计账号；禁止申请人自批、同账号双槽、越权与撤权后执行。同一人多个账号的真人核实由平台外管理承担。
3. **模块与接口职责**：设计模块对外推进与冻结候选；授权模块对外准备、具名审批与 Commit；执行模块驱动、停止及核对运行；发布恢复模块推进发布、申请回退及核对；证据模块追加、签名检查点及校验。HTTP、LangGraph 和 Celery 适配这些契约，不复制审批或发布规则。具体方法名、状态名和实体拆分仍是实施设计，不是已验证外部事实。
4. **三层状态**：ConversationState、WorkflowState 和 ExecutionRun 独立持久化并关联；设计回答绑定归属和修订，持久化 LangGraph Checkpoint 后跨请求、重启恢复原进度。不得以重建对话冒充恢复。
5. **结构化制品**：EtlPlan 是唯一可编辑执行定义；HOCON、DAG 和 Diff 从其派生。门禁包含语法、Schema/类型、资源、SecretReference、工具、质量、预算与恢复约束。最多两次自动修复，随后重跑相关门禁；业务不明或扩大权限/预算、降低保护要求必须澄清。冻结 PipelineVersion 和 SHA256，修改产生新版本。
6. **模型与适配器**：OpenAI 兼容真实模型和显式演示 Provider，共用结构校验；模型 Profile 支持端点、模型名、超时和有限重试。至少一个真实模型参与解析或生成，并验证另一个配置的切换能力、如实记录可调用条件。只发送需求及筛选元数据；禁止凭据和原始行样本；失败不静默降级。真实变化仅保留 LLM、SecretProvider 和 Engine 的适配契约，故障模拟不能代替真实集成。
7. **风险与工具出口**：确定性 PDP 决定 P0–P3；P0 最高且禁止，P1 正式发布/回退/轮转和 P2 受限写入/试运行均双审批，P3 只读操作在权限、资源白名单和预算下可自动授权。模型不能降级。探查、试运行、凭据物化、提交、取消、DDL、发布及清理均为受管动作，经 Tool Broker 执行。
8. **冻结审批**：Prepare 仅生成本地冻结事实，不搬运、不提前建影子表；必要外部事实来自独立受管检查。Preparation 绑定主体、计划/制品、规范资源、Schema、SecretReference 版本、策略版本、质量、指标口径、预算、影响与恢复方案；默认二十四小时有效，审批不续期。普通源业务值变化不自动失效，Schema、凭据版本或其他冻结事实漂移需重新准备和审批。
9. **Commit 与单次事务**：重新校验当前身份、审批独立性、期限、指纹与策略；相同幂等键相同请求返回原运行，不同请求冲突。Ed25519 Capability 绑定工具、主体、环境、资源和制品，五分钟单次消费。消费事实、ExecutionRun、ManagedCommand、Outbox 与关键证据在一次 PostgreSQL 事务提交或回滚；并发情况下不建立第二次运行机会。
10. **异步执行**：Outbox/Celery 至少一次投递；Worker 验证签名 ManagedCommand、运行与 manifest 范围，不再次消费原 Capability。五分钟是授权接受窗口，不是队列等待或六百秒运行预算的寿命。Commit 后排队或运行期间发生相关权限撤销、绑定密钥不可用时，未提交的命令不得继续提交，已运行的任务采取安全终止并禁止发布，记录原因；不得切换未批准凭据或扩权恢复。每次 Beat 调度仅产生独立待审批申请，重复 tick 去重，不复用历史批准自动启动。
11. **外部结果不明**：提交前持久化关联事实，提交超时或 Worker 中断后先核对原作业；无 jobId 不等于未提交。提交、停止或交换不明时阻断重提、发布及危险清理。目标所有权在租约到期时不自动释放；必须隔离旧调用路径、确认旧任务终止及在途结果后才能接管。
12. **数据与凭据保护**：加密 SecretProvider 的主密钥由部署注入并与数据库分离，缺失时依赖服务拒绝启动；可选 Vault KV v2 验证读写、拒绝和轮转。只存受管引用，授权后按需物化临时敏感配置并清理。API、模型、消息、日志、审计、制品和普通错误摘要不泄露凭据及受保护原值；敏感错误字段仅保留脱敏摘要，不提供解密查看。
13. **质量边界**：清洗与必需校验分开；已解析行通过全部适用规则才进入有效分支，否则生成一条可含多个 ErrorCode 的 ErrorRecord。分支互斥覆盖、NULL 明确；显式业务过滤单独计数。坏 CSV、解码或无法证明完整读取属于 SourceParseFailure，整批失败、禁止发布，保留受管源引用和脱敏诊断；禁止静默 SKIP，不承诺每条坏输入均能生成错误记录。
14. **计数与测量**：分别记录源读取、解析、去重/关联后质量判定、有效、拒绝、规则次数及最终写入。Preparation 冻结阶段、记录单元、重复重试办法、放大比分子分母和字节来源；只在同一判定阶段同一记录单元且去重后要求“已判定 = 有效 + 拒绝”。不能使用“源读取 = 完整影子表 + 错误表”。拒绝率为拒绝/已判定，零分母显示暂无数据；不把估算、网络字节或源扫描量冒充已约定的 sink 写入字节。
15. **默认预算**：最大读取 100,000 行、写入 256 MiB、时长 600 秒、输出放大比 2、拒绝率 20%，约两秒观察。运行中拒绝比例从累计判定一百条启用，最终检查不受此限制，其他预算不享受最小样本豁免。超限请求取消并阻止发布，记录检测/请求/确认时刻及超调；必要指标缺失、陈旧或无法解释时停止安全推进，不能填零放行。不承诺物理硬限额或零超调。
16. **独立发布门禁**：引擎成功只是必要条件；完整可对账 QualityReport、最终质量与预算、Schema、资源所有权、指纹及确认状态均须满足。默认空输入不可覆盖正式表，除非计划明确批准空表发布；双 sink 不具有默认跨表事务，任一分支失败禁止发布。
17. **增量与完整结果**：有界全量或更新时间加主键水位增量，重叠回看和主键合并；订单事实关联当前用户维度，维度变化刷新受影响历史订单。ShadowTable 必须包含历史保留结果和本批变更；规范化源/维度合并状态可恢复，控制面只存引用及摘要，大规模构建在数据面完成。不承诺 CDC、物理删除传播、跨源事务快照或无限迟到更新。
18. **目标与发布**：规范识别实际数据库/表，平台独占写入、DDL 与发布，外部应用只读；跨 Pipeline、项目别名同目标也须互斥。从完整影子构建至发布确认串行。平台先校验 Schema，再用 Doris 单条两 OLAP 表 Swap 保留原正式表；不依赖引擎自动检查结构。稳定对象身份及跨交换语义须实测，不凭名字、行数或相同数据判断结果。
19. **恢复与水位**：先记录发布意图，确认实际交换后在本地事务更新 PublishedSnapshot、合并状态与 Watermark；不声称 Doris/PostgreSQL 同事务。故障先 Reconciliation，再补交或安全继续。PrepublicationCleanup 要求确认运行已停止，仅清理未发布资源；PublicationRollback 是另行双审批的新操作，恢复最近一次发布前完整快照、对应合并状态及水位，默认保留七天；缺失、修改、过期或身份不明则拒绝。不能把交换后的旧正式表误删为临时影子表。
20. **证据与信任**：EvidenceEvent 记录主体、归属、事件、版本/准备单/运行、序号、哈希和前序哈希；Ed25519 签名检查点支持导出及独立离线校验。尾部截断检测依赖独立保存的可信旧检查点。管理员、执行环境及密钥托管受信，不宣称能抵御同时掌握数据库、密钥和验证器的攻击者或绝对不可篡改。
21. **用户界面**：交付总览、连接资产、Studio/版本 Diff、准备单审批、运行诊断、安全 Benchmark 六类中文界面；显示权限、到期、漂移、取消中及待核对等实际状态。HOCON 只读高亮/复制/下载、DAG 只读；前端按钮与服务端权限一致，不以隐藏按钮承担安全校验。
22. **可验收交付**：约一千订单和一百用户；约 5% 异常正常发布，约 30% 异常超过 20% 阈值后中断并清理，另外验证发布后回退。提供初始化、健康检查、数据复位、演示、评测与导出入口和中文说明；报告实际资源、固定版本、耗时及吞吐。模型凭据和 Docker 条件不足必须标记未执行，不以演示结果补齐真实验收。

## Testing Decisions

### 测试入口与边界（用户已确认）

主入口采用控制面公开 API 的完整业务行为：认证身份 → 登记与探查 → 设计澄清与冻结 → Prepare/Approve/Commit → 查询运行、监督、发布及恢复。观察返回结果、实际持久化业务事实、源/目标查询和导出证据，不断言内部私有函数、LangGraph 节点数量、ORM 调用顺序或前端组件内部状态。以下边界是测试组织方式，不新增业务模块或专门的测试业务 API。

五个内部模块共享主入口验收：设计、审批授权、执行监督、发布恢复通过同一业务流程调用；证据的独立离线校验作为产品本来需要的第二个外部入口。仅对无法由纯请求断言证明的事务和外部协议补充集成测试，不按每个模块建立重复的测试金字塔。

| 边界标识 | 入口及依赖 | 具体判定与失败注入 | 保留证据与验收关联 |
| --- | --- | --- | --- |
| TS-01 身份与受管资产 | 真实 API、真实 PostgreSQL；受管 MySQL/PostgreSQL/CSV、SecretProvider；选配 Vault 独立运行 | 合法/越权/撤权访问；跨项目 ID、下载及 Checkpoint；探查范围、脱敏、缺主密钥启动拒绝、轮转后绑定失效；用合成敏感标记检查泄漏 | 脱敏响应、拒绝事件、实际 Profile、受控凭据与泄漏扫描结果；AC-01、AC-02 |
| TS-02 设计与不可变制品 | 公开设计/恢复/冻结 API；真实 Checkpoint 存储；L0/L1 用可控 Provider，L2 用真实模型 | 缺参数 interrupt、重启恢复；确定性派生/摘要、两次修复上限、非法计划拒绝、漂移及冻结后修改拒绝；真实失败不静默转演示 | 原工作流关联、结构化输出、门禁及摘要重算、真实 Provider 证明；AC-03、AC-04 |
| TS-03 授权事务与并发 | API 的 Prepare/Approve/Commit 与真实 PostgreSQL 并发事务；可控时钟/事务故障在测试环境注入 | 自批、同人双槽、拒绝、24h 过期、撤权、指纹/策略/凭据版本漂移；5min 过期及重放、并发 Commit、同键异请求；事务各关键落库点失败 | 一次授权消费和一个 Run/Command/Outbox，或全部回滚；Prepare 外部数据对象无变更；AC-01、AC-05 |
| TS-04 消息与引擎边界 | API 发起后由真实 Outbox/Redis/Celery 驱动；L1 故障 Engine 适配器，L2 真实 Zeta | 重复消息、Worker 重启/确认丢失、提交成功响应丢失、命令篡改/作用域不符、队列超过五分钟；重复调度仅一个本次待审批申请 | 原 Run/command/job 关联、无盲重提、未知状态及范围拒绝；真实引擎不得用 fake 代替；AC-05、AC-06 |
| TS-05 数据质量与解析 | 同一受管运行入口；真实各源、SeaTunnel 和 Doris 查询 | NULL、多规则错误、显式过滤、双分支/重试；坏 CSV/JDBC 读取失败整批闭锁；分别使有效或错误 sink 失败 | 同阶段已判定=有效+拒绝，错误码不重数，最终表与阶段差异可解释；失败不发布；AC-02、AC-06 |
| TS-06 监督与发布否决 | API 接受冻结契约后异步执行；L1 用可控指标/时钟，L2 用可测真实指标 | 行数/字节/时长/放大比/拒绝率阈值两侧；判定99/100条启用边界、小样本最终判断；指标缺失/陈旧/重置、取消超时、引擎成功但报告缺失、空输入；Commit后排队/运行期间撤权或绑定密钥不可用 | 检测/停止请求/终止确认时间、最终观测与超调；无必要字节不算通过；未知/不合格不发布且不提前清理；撤权/密钥故障时不提交或安全终止、不发布、不扩权恢复；AC-07、AC-08 |
| TS-07 完整发布与恢复 | API 与真实 PostgreSQL/Doris；受控进程中断/网络故障；同物理目标并发请求 | 正常 Swap；同时间戳、回看重跑、维度更新和历史保留；交换前/后响应丢失及控制事务未完成；失联 Worker/过期租约；7天保留到期、旧表缺失/修改；清理与双批回退 | 实际对象身份、预期完整数据、唯一目标所有权、控制快照/合并状态/水位一致；回退后可重放；AC-07、AC-08 |
| TS-08 证据与 Benchmark | API 导出/评测入口及独立离线 verifier；固定版本用例；可信外部检查点 | 修改/删中间事件/重排/尾截断及正常对照；L0/L1 安全集合和至少20例真实模型集合；初次与最多2次修复后的结果分报 | 具体断点及签名校验；生成最终正确率≥90%，安全硬门槛100%，P0/P1分母与误漏报；AC-09 |
| TS-09 浏览器与运行包 | Vue 通过同一 API；真实 Compose；少量跨角色浏览器流程 | 六类界面主流程、脱敏展示、只读派生视图与 Diff、错误/到期/待核对/取消状态；初始化健康检查和复位后复跑 | 正常发布、超限中断及另行发布后回退的数据比较、截图和证据引用；AC-01–AC-09可追踪，截图不能替代业务断言 |

### 测试原则与已有基础

- 仓库目前只有规划文档，没有业务代码、测试框架或可复用的既有测试案例；不声称已有测试缝隙或用例通过。沿用已确认架构中的模块契约与 LLM、SecretProvider、Engine 适配器，具体测试工具在实施时选择。
- 好测试判断身份和输入作用后可观察的业务结果与不变量，包含合法对照及失败路径；注入故障的位置可在实现调整时变化，期望行为不随内部重构变化。真实事务的唯一性和回滚不能靠内存替身证明。
- L0 静态注入验证确定性门禁及安全拒绝；L1 模拟故障验证未知状态、去重、取消延迟及指标异常；L2 验证真实模型、源/目标、引擎、质量路由和交换恢复。三层报告显式区分，缺少条件写未执行，不冒充通过。
- 时间边界、并发及多种故障组合优先在受控环境确定性验证；真实 L2 再确认外部协议与实际终态。故障注入只用于测试环境，不提供可被项目用户任意调用的生产调试端点。
- 使用合成敏感标记扫描 API、模型请求、消息、日志、制品、错误摘要和导出；输出报告本身脱敏，不为测试真实泄漏企业凭据。正常业务 sink 数据不自动取得向模型或普通诊断公开的权限。
- 每例关联稳定 AC 标识、测试集/策略/制品/模型/依赖版本、输入、预期、实际、失败原因及证据。只比对可复核事实，不将模型文字解释或作业成功作为语义正确的唯一证据。

### 稳定验收要求

| ID | 完成条件 | 不能接受的替代 |
| --- | --- | --- |
| AC-01 | 独立角色完成合法业务，服务端拒绝自批、槽位复用、跨归属及撤权操作 | 只隐藏按钮、共用账号或角色标签代替具名批准 |
| AC-02 | MySQL、PostgreSQL、CSV 与 Doris 实际接入；受限探查与脱敏；凭据及敏感原值不进入禁止通道 | 模拟 Profile、遗漏首版源类型、任意资源探查或泄漏 |
| AC-03 | 真实模型解析/生成，缺参 interrupt，跨请求/重启恢复原 Checkpoint | 仅模板、重建工作流冒充恢复或静默降级 |
| AC-04 | 非法语法/Schema/资源/引用等被门禁拒绝，有限修复，冻结摘要可重算且不可改 | 模型放宽要求、无限修复、可改已冻结制品 |
| AC-05 | 三阶段、24h准备单、双批、5min单次能力、真实原子事务与幂等通过 | Prepare外部变更、过期仍执行、令牌重放、局部残留或多Run |
| AC-06 | Celery/SeaTunnel实际搬运三类源并正确分流和对账；解析失败整批拒绝 | 控制面代搬、伪jobId、静默漏读、忽略失败分支或重复提交 |
| AC-07 | 完整结果经质量否决检查后实际Swap，运行报告和水位正确，同目标串行 | 本批增量替换历史、缺指标填零、空批误覆盖或跨库事务承诺 |
| AC-08 | 超限中断确认、脱敏诊断、安全清理；另外双批回退上一快照及对应水位/状态 | 提前清理、未知盲重试、清理替代回退、旧表不明仍声称恢复 |
| AC-09 | L0/L1/L2版本化报告，≥20例真实模型最终正确率≥90%，硬安全用例100%，独立证据破坏校验 | 隐去失败、混入演示分数、L2未跑报通过、无可信检查点却声称检测尾截断 |

## Out of Scope

- Oracle、ClickHouse、Excel、JSON、Parquet、S3/MinIO、REST API 等后续连接器；未实现项不显示为可用。
- 持续 CDC、物理删除传播、跨源事务一致快照、无限迟到更新、历史时态维度还原及任意多目标原子发布。
- 任意 HOCON 编辑、任意 DAG 拖拽编程、自由 SQL/URL 工具、模型自行扩权或取消审批。
- 无人值守永久周期授权、任意历史时点回滚、旧表不可用时的伪恢复、敏感错误原值解密查看。
- PostgreSQL/Redis/SeaTunnel/Doris 分布式 exactly-once、跨 sink 事务、取消即回滚、立即零停写、绝对零预算超调。
- 企业 SSO、每租户独立基础设施、企业吞吐、高可用/灾备 SLO、抵御完全控制基础设施和密钥的管理员。
- 自动第三方审计存证；首版采用人工独立保存签名检查点与离线验证。
- 本次规格发布不创建实施票据，不安装依赖、启动服务、开发业务代码或执行部署。

## Further Notes

- 本稿复用已确认需求、统一领域术语、三项已接受 ADR 和实施规划；不改变原范围及默认阈值。架构方法签名和状态名称仍是拟议设计，测试以可观察业务契约为准。
- 核心技术门槛仍须实测：Docker服务与资源、固定引擎/驱动组合、真实模型条件；Zeta提交关联/响应丢失核对、取消终态；字节及重试计数；质量路由/双sink失败与坏CSV闭锁；Doris稳定对象身份、Swap后旧表落点及跨库恢复。失败须保留证据和影响，不能静默降低验收。
- SeaTunnel 3.0.0 与 Doris 4.x 仅为已有研究的官方文档基线，部署版本尚未锁定。参考：[Zeta生命周期](https://seatunnel.apache.org/docs/3.0.0/engines/zeta/rest-api-job-lifecycle/)、[DataValidator](https://seatunnel.apache.org/docs/3.0.0/transforms/data-validator/)、[Doris sink](https://seatunnel.apache.org/docs/3.0.0/connectors/sink/Doris/)、[LocalFile](https://seatunnel.apache.org/docs/3.0.0/connectors/source/LocalFile/)、[Doris表替换](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/ALTER-TABLE-REPLACE/)。文档能力不等同实际兼容与验收证据。
- 本地 T00–T13 是实施计划标识，不是远程票号；本次不创建这些子票。正式规格发布后记录唯一 GitHub 链接，后续拆票复用该规格，不另建重复规格。
- 用户已于 2026-11-13 11:06（Asia/Shanghai）确认 TS-01–TS-09 测试边界，并授权发布一个 spec、标记 ready-for-agent。该标签表示规格已充分明确，不表示环境门槛、实现或 L2 评测已经通过；所有技术门槛仍由后续实施验证。
