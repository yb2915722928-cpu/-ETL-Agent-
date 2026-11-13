# ETL-Agent 数据集成平台

面向异构数据集成的受管 ETL 控制面：自然语言需求经结构化计划、确定性门禁、独立审批和单次授权，交由 SeaTunnel 执行，再依据质量报告完成受管发布与恢复。

当前实现覆盖 **V01 / #2：身份、租户与项目协作**、**V02 / #3：审计导出与独立验签** 和 **V03 / #4：MySQL/Doris 受管登记与只读探查**：Vue 中文控制台、FastAPI、PostgreSQL 持久身份与事务证据链、Redis 会话、Ed25519 签名检查点与独立离线 CLI，以及可重复初始化和公开 HTTP 验证。另已实现 **V05 / #6：手工 Pipeline Studio**：单表 MySQL→Doris 结构化计划、持久候选、确定性门禁与同源只读 HOCON/DAG 审查，使用与 #7 消费契约见[Studio 说明](docs/studio.md)。模型规划、版本冻结、审批、执行和发布能力仍按后续工单实施。审计接口、迁移、可信边界和验证命令见[证据说明](docs/evidence.md)。连接的部署白名单、外部主密钥、加密引用、P3 预算、Profile 契约和真实验证命令见[受管连接说明](docs/connectors.md)。升级时重新运行初始化补充缺失主密钥，保留原密钥和数据卷。

## 启动 V01

需要 Docker Desktop（Linux 容器）和 Python 3。所有下载使用阿里云：基础 rootfs 与 Alpine 包来自 `mirrors.aliyun.com`，Python 包来自阿里云 PyPI，前端包来自 `registry.npmmirror.com`。[阿里云构建配置](infra/aliyun/runtime.Dockerfile)从本地构建基础镜像，不依赖 Docker Hub 镜像拉取。

在仓库根目录执行：

```powershell
python scripts/init_local.py
python infra/aliyun/build_base.py
python infra/aliyun/build_runtime.py
docker compose -f compose.yaml -f compose.aliyun.yaml up -d --no-build
docker compose -f compose.yaml -f compose.aliyun.yaml exec -T api python -m app.bootstrap
```

打开 http://127.0.0.1:8080 。租户标识为 `demo`，六个独立账号为 `admin`、`maker`、`data_checker`、`security_checker`、`operator`、`auditor`。每个账号的随机初始密码保存在本地私有初始化文件 `.runtime/bootstrap-demo.json`，请用本地编辑器读取；初始化命令只输出路径。数据库密码保存在忽略提交的 `.env`。不要将这些文件、密码或会话 Cookie 放进截图、日志或验收材料。

[初始化脚本](backend/app/bootstrap.py)重复运行保留既有密码、禁用状态、租户管理员标志及已有项目的成员角色。撤销成员后再次初始化不会恢复授权。`admin` 是租户管理员，也是初始项目的项目管理员；租户管理员管理账号和创建项目，管理某个项目的成员仍需当前项目的 `project_admin` 成员身份。五个业务角色属于独立账号，可以查看自己的项目与成员，不能管理账号或成员。角色用于后续业务工单授权，V01 不模拟尚未实现的审批或执行操作。

控制台提供登录、项目选择与创建、项目成员增删和角色编辑、账号创建/禁用/密码重置。点击“刷新权限”从服务端重新读取身份与项目；每个受保护 API 请求都会重新读取 PostgreSQL 中的启用状态和成员角色，Redis 仅保存会话到账号 ID 的映射。浏览器界面是否显示按钮不构成授权依据。

[健康接口](http://127.0.0.1:8000/api/health)执行 PostgreSQL `SELECT 1` 和 Redis `PING`，返回实际服务/运行时版本、数据库大小、Redis 内存以及容器可见 CPU、内存与磁盘资源；依赖不可用时返回 503。资源数值为当前容器可见事实，不是性能基准。API 与 Web 仅绑定本机 8000/8080，数据库和 Redis 不映射宿主端口；通过 HTTPS 反向代理部署时，在本地环境配置 `COOKIE_SECURE=true`。V02 使用增量迁移回填旧身份事件，保留既有数据库和身份；升级前重新运行初始化脚本补充签名密钥，并停止旧 API 写入者，详见[迁移与密钥说明](docs/evidence.md)。

## 公开 API 与持久化验证

[验证脚本](scripts/verify_identity.py)仅使用 Python 标准库和真实 HTTP 接口；不替换 PostgreSQL、Redis 或授权逻辑。先增加第二个租户以验证越权，再运行一次主流程：

```powershell
docker compose -f compose.yaml -f compose.aliyun.yaml exec -T api python -m app.bootstrap --tenant isolation --project private
python scripts/verify_identity.py
```

覆盖独立角色登录、租户/项目/账号 ID 越权、项目管理员独立授权、现有会话立即撤权与禁用、密码和校验错误脱敏、带 actor 的连续审计顺序，以及失败写入不产生成功审计事实。脚本创建带随机后缀的验证账号与私有项目，原六个演示账号保持可用；生成的验证密码只保存于忽略提交的 `.runtime/verification-state.json`。

保留命名卷重建服务后，重新运行初始化并复查原账号 ID、原密码、成员角色和历史审计事件：

```powershell
docker compose -f compose.yaml -f compose.aliyun.yaml down
docker compose -f compose.yaml -f compose.aliyun.yaml up -d
docker compose -f compose.yaml -f compose.aliyun.yaml exec -T api python -m app.bootstrap
python scripts/verify_identity.py --recheck
```

`down` 保留持久卷；不要加 `-v`，否则会删除数据库和会话数据。可选参数 `--url` 指定已启动服务，`--output` 将不含密码的结果 JSON 写入验收文件。身份变更与本地审计事实使用同一个 PostgreSQL 事务提交，事件包含租户内序号、操作者、项目与安全字段；V02 已将相同事务追加扩展为租户及项目独立哈希链、签名检查点与导出，见[证据验签与 HTTP 验证入口](docs/evidence.md)。

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

仓库采用单一领域语境，见 [领域文档约定](docs/agents/domain.md)。正式规格与工单后端为 [GitHub Issues](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues)，规则见 [工单约定](docs/agents/issue-tracker.md) 和 [分诊标签](docs/agents/triage-labels.md)。正式规格已发布为 [GitHub #1](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/1)，标记 ready-for-agent。经用户确认，28 张纵向实施子票已发布为 #2–#29，全部关联 #1 并标记 ready-for-agent，35 条直接阻塞边已核对；对应关系见[真实票号映射](docs/planning/ticket-map-v1.md)。[V01 / #2](https://github.com/yb2915722928-cpu/-ETL-Agent-/issues/2) 已完成本票功能及真实服务验收，见[验收记录](artifacts/issue-2/acceptance.md)。后续接取以 GitHub 工单的当前依赖为准。T00–T13 保留为早期技术主题参考，不作为另一套待发布工单；其他工单就绪不表示功能已实现或真实链路已验收。
