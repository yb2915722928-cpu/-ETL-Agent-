# 以持久原始回执和独占出口证明 Doris 写入计量

真实 Doris 3.0.8-rc01 的 LoadBytes 随完成回执返回；历史 SHOW STREAM LOAD 不能证明进行中请求的完整累计字节。受管真实执行采用 SeaTunnel JDBC → 内部 ingress → 单次 Doris PUT：引擎不持有 Doris 写入凭据，ingress 在外发前提交批次意图，以稳定源主键去重，并把质量判定、目标物理身份和原始回执逐项关联。该选择允许直接保留原始 LoadBytes，同时使原始回执丢失成为持久 unknown；尚未证明独占网络、原绑定授权、真实停止和对象清理之前，公开真实执行保持闭锁。

两秒观察屏障遇到未结写入，或六秒没有可信监督，永久关闭准入并否决发布，完整累计字节为未知。不能用请求体大小、引擎估算、完成回执的历史总和补出缺口，也不增加启动宽限。调度阶段只有关联到原作业的真实零值才可采用。尚未判定和发送的记录可以在屏障外有界排队，只有最近一次完整观察之后的短准入窗口才开始单次写入；初始绑定不算完整观察。等待期间不持有观察屏障、不创建外部写入意图、不把等待时间当作新观测。上游 HTTP 等待期限与 Doris 单次传输期限分别冻结，延长前者不延长后者，也不延长两秒观察或六秒失效规则。此设计主动接受慢请求被停止的代价；若以后需要在长请求期间持续可用的实测字节，应增加经过验证的 BE 进度协议。

SeaTunnel 2.3.12 的 Http sink 会在接收端全部返回 503 时仍报告 FINISHED 和正 SinkWriteCount。因此独立采用原作业 SourceReceivedCount 证明源发出量，以 journal 的原始成功回执证明目标接收量，以质量稳定键及回执正文摘要对账；引擎终态、关闭准入和外部写入静止分别记录。协议依据为 [HTTP 实测](../../artifacts/issue-13/http-ingress-protocol-8243affe6ea5.json)、[Doris 原始回执实测](../../artifacts/issue-13/doris-object-transport-after-fix.json) 和 [原批次结果未知](../../artifacts/issue-13/doris-object-transport.json)，三者均不构成完整受管 L2 验收。
